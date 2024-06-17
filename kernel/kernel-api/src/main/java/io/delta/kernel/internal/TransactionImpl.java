/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.internal;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.kernel.*;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.ConcurrentWriteException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;

import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.ConflictChecker;
import io.delta.kernel.internal.replay.ConflictChecker.TransactionRebaseState;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.VectorUtils;
import static io.delta.kernel.internal.TableConfig.CHECKPOINT_INTERVAL;
import static io.delta.kernel.internal.actions.SingleAction.*;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Preconditions.checkState;
import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

public class TransactionImpl
        implements Transaction {
    private static final Logger logger = LoggerFactory.getLogger(TransactionImpl.class);

    public static final int DEFAULT_READ_VERSION = 1;
    public static final int DEFAULT_WRITE_VERSION = 2;

    /**
     * Number of retries for concurrent write exceptions to resolve conflicts and retry commit. In
     * Delta-Spark, for historical reasons the number of retries is really high (10m). We are
     * starting with a lower number for now. If this is not sufficient we can update it.
     */
    private static final int NUM_TXN_RETRIES = 200;

    private final UUID txnId = UUID.randomUUID();

    private final boolean isNewTable; // the transaction is creating a new table
    private final String engineInfo;
    private final Operation operation;
    private final Path dataPath;
    private final Path logPath;
    private final Protocol protocol;
    private final Metadata metadata;
    private final boolean shouldUpdateMetadata;
    private final SnapshotImpl readSnapshot;
    private final Optional<SetTransaction> setTxnOpt;

    private boolean closed; // To avoid trying to commit the same transaction again.

    public TransactionImpl(
            boolean isNewTable,
            Path dataPath,
            Path logPath,
            SnapshotImpl readSnapshot,
            String engineInfo,
            Operation operation,
            Protocol protocol,
            Metadata metadata,
            Optional<SetTransaction> setTxnOpt,
            boolean shouldUpdateMetadata) {
        this.isNewTable = isNewTable;
        this.dataPath = dataPath;
        this.logPath = logPath;
        this.readSnapshot = readSnapshot;
        this.engineInfo = engineInfo;
        this.operation = operation;
        this.protocol = protocol;
        this.metadata = metadata;
        this.setTxnOpt = setTxnOpt;
        this.shouldUpdateMetadata = shouldUpdateMetadata;
    }

    @Override
    public Row getTransactionState(Engine engine) {
        return TransactionStateRow.of(metadata, dataPath.toString());
    }

    @Override
    public List<String> getPartitionColumns(Engine engine) {
        return VectorUtils.toJavaList(metadata.getPartitionColumns());
    }

    @Override
    public StructType getSchema(Engine engine) {
        return readSnapshot.getSchema(engine);
    }

    @Override
    public TransactionCommitResult commit(Engine engine, CloseableIterable<Row> dataActions)
            throws ConcurrentWriteException {
        try {
            checkState(!closed,
                    "Transaction is already attempted to commit. Create a new transaction.");

            long commitAsVersion = readSnapshot.getVersion(engine) + 1;
            int numRetries = 0;
            do {
                logger.info("Committing transaction as version = {}.", commitAsVersion);
                try {
                    return doCommit(engine, commitAsVersion, dataActions);
                } catch (FileAlreadyExistsException fnfe) {
                    logger.info("Concurrent write detected when committing as version = {}. " +
                            "Trying to resolve conflicts and retry commit.", commitAsVersion);
                    TransactionRebaseState rebaseState = ConflictChecker
                            .resolveConflicts(engine, readSnapshot, commitAsVersion, this);
                    long newCommitAsVersion = rebaseState.getLatestVersion() + 1;
                    checkArgument(commitAsVersion < newCommitAsVersion,
                            "New commit version %d should be greater than the previous commit " +
                                    "attempt version %d.", newCommitAsVersion, commitAsVersion);
                    commitAsVersion = newCommitAsVersion;
                }
                numRetries++;
            } while (numRetries < NUM_TXN_RETRIES);
        } finally {
            closed = true;
        }

        // we have exhausted the number of retries, give up.
        logger.info("Exhausted maximum retries ({}) for committing transaction.", NUM_TXN_RETRIES);
        throw new ConcurrentWriteException();
    }

    private TransactionCommitResult doCommit(
            Engine engine,
            long commitAsVersion,
            CloseableIterable<Row> dataActions)
            throws FileAlreadyExistsException {
        List<Row> metadataActions = new ArrayList<>();
        metadataActions.add(createCommitInfoSingleAction(generateCommitAction()));
        if (shouldUpdateMetadata || isNewTable) {
            metadataActions.add(createMetadataSingleAction(metadata.toRow()));
        }
        if (isNewTable) {
            // In the future, we need to add metadata and action when there are any changes to them.
            metadataActions.add(createProtocolSingleAction(protocol.toRow()));
        }
        setTxnOpt.ifPresent(setTxn -> metadataActions.add(createTxnSingleAction(setTxn.toRow())));

        try (CloseableIterator<Row> stageDataIter = dataActions.iterator()) {
            // Create a new CloseableIterator that will return the metadata actions followed by the
            // data actions.
            CloseableIterator<Row> dataAndMetadataActions =
                    toCloseableIterator(metadataActions.iterator()).combine(stageDataIter);

            if (commitAsVersion == 0) {
                // New table, create a delta log directory
                if (!engine.getFileSystemClient().mkdirs(logPath.toString())) {
                    throw new RuntimeException(
                            "Failed to create delta log directory: " + logPath);
                }
            }

            // Write the staged data to a delta file
            engine.getJsonHandler().writeJsonFileAtomically(
                    FileNames.deltaFile(logPath, commitAsVersion),
                    dataAndMetadataActions,
                    false /* overwrite */);

            return new TransactionCommitResult(
                    commitAsVersion,
                    isReadyForCheckpoint(commitAsVersion));
        } catch (FileAlreadyExistsException e) {
            throw e;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public boolean isBlindAppend() {
        // For now, Kernel just supports blind append.
        // Change this when read-after-write is supported.
        return true;
    }

    public Optional<SetTransaction> getSetTxnOpt() {
        return setTxnOpt;
    }

    private Row generateCommitAction() {
        return new CommitInfo(
                System.currentTimeMillis(), /* timestamp */
                "Kernel-" + Meta.KERNEL_VERSION + "/" + engineInfo, /* engineInfo */
                operation.getDescription(), /* description */
                getOperationParameters(), /* operationParameters */
                isBlindAppend(), /* isBlindAppend */
                txnId.toString() /* txnId */
        ).toRow();
    }

    private boolean isReadyForCheckpoint(long newVersion) {
        int checkpointInterval = CHECKPOINT_INTERVAL.fromMetadata(metadata);
        return newVersion > 0 && newVersion % checkpointInterval == 0;
    }

    private Map<String, String> getOperationParameters() {
        if (isNewTable) {
            List<String> partitionCols = VectorUtils.toJavaList(metadata.getPartitionColumns());
            String partitionBy = partitionCols.stream()
                    .map(col -> "\"" + col + "\"")
                    .collect(Collectors.joining(",", "[", "]"));
            return Collections.singletonMap("partitionBy", partitionBy);
        }
        return Collections.emptyMap();
    }

    /**
     * Get the part of the schema of the table that needs the statistics to be collected per file.
     *
     * @param engine           {@link Engine} instance to use.
     * @param transactionState State of the transaction
     * @return
     */
    public static List<Column> getStatisticsColumns(Engine engine, Row transactionState) {
        // TODO: implement this once we start supporting collecting stats
        return Collections.emptyList();
    }
}
