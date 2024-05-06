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
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.VectorUtils;
import static io.delta.kernel.internal.TableConfig.CHECKPOINT_INTERVAL;
import static io.delta.kernel.internal.actions.SingleAction.*;
import static io.delta.kernel.internal.util.Preconditions.checkState;
import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

public class TransactionImpl
        implements Transaction {
    public static final int DEFAULT_READ_VERSION = 1;
    public static final int DEFAULT_WRITE_VERSION = 2;

    private final UUID txnId = UUID.randomUUID();

    private final boolean isNewTable; // the transaction is creating a new table
    private final String engineInfo;
    private final Operation operation;
    private final Path dataPath;
    private final Path logPath;
    private final Protocol protocol;
    private final Metadata metadata;
    private final SnapshotImpl readSnapshot;
    private final Optional<SetTransaction> setTxn;

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
            Optional<SetTransaction> setTxn) {
        this.isNewTable = isNewTable;
        this.dataPath = dataPath;
        this.logPath = logPath;
        this.readSnapshot = readSnapshot;
        this.engineInfo = engineInfo;
        this.operation = operation;
        this.protocol = protocol;
        this.metadata = metadata;
        this.setTxn = setTxn;
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
    public TransactionCommitResult commit(
            Engine engine,
            CloseableIterable<Row> dataActions)
            throws ConcurrentWriteException {
        checkState(
                !closed,
                "Transaction is already attempted to commit. Create a new transaction.");
        List<Row> metadataActions = new ArrayList<>();
        metadataActions.add(createCommitInfoSingleAction(generateCommitAction()));
        if (isNewTable) {
            // In the future, we need to add metadata and action when there are any changes to them.
            metadataActions.add(createMetadataSingleAction(metadata.toRow()));
            metadataActions.add(createProtocolSingleAction(protocol.toRow()));
        }
        setTxn.ifPresent(setTxn -> metadataActions.add(createTxnSingleAction(setTxn.toRow())));

        try (CloseableIterator<Row> stageDataIter = dataActions.iterator()) {
            // Create a new CloseableIterator that will return the metadata actions followed by the
            // data actions.
            CloseableIterator<Row> dataAndMetadataActions =
                    toCloseableIterator(metadataActions.iterator()).combine(stageDataIter);

            try {
                long readVersion = readSnapshot.getVersion(engine);
                if (readVersion == -1) {
                    // New table, create a delta log directory
                    if (!engine.getFileSystemClient().mkdirs(logPath.toString())) {
                        throw new RuntimeException(
                                "Failed to create delta log directory: " + logPath);
                    }
                }

                long newVersion = readVersion + 1;
                // Write the staged data to a delta file
                engine.getJsonHandler().writeJsonFileAtomically(
                        FileNames.deltaFile(logPath, newVersion),
                        dataAndMetadataActions,
                        false /* overwrite */);

                return new TransactionCommitResult(newVersion, isReadyForCheckpoint(newVersion));
            } catch (FileAlreadyExistsException e) {
                // TODO: Resolve conflicts and retry commit
                throw new ConcurrentWriteException();
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } finally {
            closed = true;
        }
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

    private boolean isBlindAppend() {
        // For now, Kernel just supports blind append.
        // Change this when read-after-write is supported.
        return true;
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
     * @param engine      {@link Engine} instance to use.
     * @param transactionState State of the transaction
     * @return
     */
    public static List<Column> getStatisticsColumns(Engine engine, Row transactionState) {
        // TODO: implement this once we start supporting collecting stats
        return Collections.emptyList();
    }
}
