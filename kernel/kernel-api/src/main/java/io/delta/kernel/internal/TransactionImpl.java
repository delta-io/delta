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

import io.delta.kernel.*;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;

import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;
import static io.delta.kernel.internal.TableConfig.CHECKPOINT_INTERVAL;
import static io.delta.kernel.internal.actions.SingleAction.*;
import static io.delta.kernel.internal.util.Preconditions.checkState;

public class TransactionImpl implements Transaction {
    public static final int DEFAULT_READ_VERSION = 1;
    public static final int DEFAULT_WRITE_VERSION = 2;

    private final String engineInfo;
    private final String operation;
    private final Path dataPath;
    private final Path logPath;
    private final Optional<StructType> newSchema;
    private final Protocol protocol;
    private final Metadata metadata;
    private final Optional<Long> readVersion;
    private final Optional<Predicate> readPredicate;
    private final SnapshotImpl readSnapshot;
    private final Optional<SetTransaction> txnIdentifier;

    private boolean committed; // To avoid trying to commit the same transaction again.

    public TransactionImpl(
            Path dataPath,
            Path logPath,
            SnapshotImpl readSnapshot,
            String engineInfo,
            String operation,
            Optional<StructType> newSchema,
            Protocol protocol,
            Metadata metadata,
            Optional<Long> readVersion,
            Optional<Predicate> readPredicate,
            Optional<SetTransaction> txnIdentifier) {
        this.dataPath = dataPath;
        this.logPath = logPath;
        this.readSnapshot = readSnapshot;
        this.engineInfo = engineInfo;
        this.operation = operation;
        this.newSchema = newSchema;
        this.protocol = protocol;
        this.metadata = metadata;
        this.readVersion = readVersion;
        this.readPredicate = readPredicate;
        this.txnIdentifier = txnIdentifier;
    }

    @Override
    public Row getState(TableClient tableClient) {
        return TransactionStateRow.of(metadata, dataPath.toString());
    }

    @Override
    public List<String> getPartitionColumns(TableClient tableClient) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public StructType getSchema(TableClient tableClient) {
        return newSchema.orElseGet(() -> readSnapshot.getSchema(tableClient));
    }

    @Override
    public TransactionCommitResult commit(
            TableClient tableClient,
            CloseableIterable<Row> stagedData) throws ConcurrentWriteException {
        checkState(!committed, "Transaction is already committed. Create a new transaction.");
        List<Row> metadataActions = new ArrayList<>();
        metadataActions.add(createCommitInfoSingleAction(generateCommitAction()));
        metadataActions.add(createMetadataSingleAction(metadata.toRow()));
        metadataActions.add(createProtocolSingleAction(protocol.toRow()));
        addSetTransactionIfPresent(metadataActions);

        CloseableIterator<Row> stageDataIter = stagedData.iterator();

        // Create a new CloseableIterator that will return the commit action, metadata action, and
        // the stagedData
        CloseableIterator<Row> stagedDataWithCommitInfo = new CloseableIterator<Row>() {
            private int metadataActionReturned = -1;

            @Override
            public boolean hasNext() {
                return metadataActionReturned < metadataActions.size() - 1 ||
                        stageDataIter.hasNext();
            }

            @Override
            public Row next() {
                if (metadataActionReturned < metadataActions.size() - 1) {
                    metadataActionReturned += 1;
                    return metadataActions.get(metadataActionReturned);
                } else {
                    return stageDataIter.next();
                }
            }

            @Override
            public void close() throws IOException {
                stageDataIter.close();
            }
        };

        try {
            long readVersion = readSnapshot.getVersion(tableClient);
            if (readVersion == -1) {
                // New table, create a delta log directory
                tableClient.getFileSystemClient().mkdir(logPath.toString());
            }

            long newVersion = readVersion + 1;
            // Write the staged data to a delta file
            tableClient.getJsonHandler().writeJsonFileAtomically(
                    FileNames.deltaFile(logPath, newVersion),
                    stagedDataWithCommitInfo,
                    false /* overwrite */
            );

            committed = true;

            return new TransactionCommitResult(newVersion, readyForCheckpoint(newVersion));
        } catch (FileAlreadyExistsException e) {
            // TODO: Resolve conflicts and retry commit
            throw new ConcurrentWriteException();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public Optional<Long> getReadVersion() {
        return readVersion;
    }

    public Optional<Predicate> getReadPredicate() {
        return readPredicate;
    }

    /**
     * Get the part of the schema of the table that needs the statistics to be collected per file.
     *
     * @param tableClient      {@link TableClient} instance to use.
     * @param transactionState State of the transaction
     * @return
     */
    public static List<Column> getStatisticsColumns(TableClient tableClient, Row transactionState) {
        // TODO: implement this once we start supporting collecting stats
        return Collections.emptyList();
    }

    private Row generateCommitAction() {
        return new CommitInfo(System.currentTimeMillis(), engineInfo, operation).toRow();
    }

    private void addSetTransactionIfPresent(List<Row> actions) {
        txnIdentifier.ifPresent(txnId -> actions.add(createTxnSingleAction(txnId.toRow())));
    }

    private boolean readyForCheckpoint(long newVersion) {
        int checkpointInterval = CHECKPOINT_INTERVAL.fromMetadata(metadata);
        return newVersion > 0 && newVersion % checkpointInterval == 0;
    }
}
