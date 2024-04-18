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

import java.util.*;
import static java.util.Objects.requireNonNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.kernel.*;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.*;

import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.*;
import io.delta.kernel.internal.util.Tuple2;
import static io.delta.kernel.internal.TransactionImpl.DEFAULT_READ_VERSION;
import static io.delta.kernel.internal.TransactionImpl.DEFAULT_WRITE_VERSION;
import static io.delta.kernel.internal.util.VectorUtils.stringArrayValue;
import static io.delta.kernel.internal.util.VectorUtils.stringStringMapValue;

public class TransactionBuilderImpl implements TransactionBuilder {
    private static final Logger logger = LoggerFactory.getLogger(TransactionBuilderImpl.class);

    private final long currentTimeMillis = System.currentTimeMillis();
    private final TableImpl table;
    private final String engineInfo;
    private final String operation;
    private Optional<StructType> schema = Optional.empty();
    private Optional<Set<String>> partitionColumns = Optional.empty();
    private Optional<Predicate> readPredicate = Optional.empty();
    private Optional<Long> readVersion = Optional.empty();
    private Optional<SetTransaction> transactionId = Optional.empty();

    public TransactionBuilderImpl(TableImpl table, String engineInfo, String operation) {
        this.table = table;
        this.engineInfo = engineInfo;
        this.operation = operation;
    }

    @Override
    public TransactionBuilder withSchema(TableClient tableClient, StructType newSchema) {
        this.schema = Optional.of(newSchema);
        // TODO: this needs to verified
        return this;
    }

    @Override
    public TransactionBuilder withPartitionColumns(
            TableClient tableClient,
            Set<String> partitionColumns) {
        if (!partitionColumns.isEmpty()) {
            this.partitionColumns = Optional.of(partitionColumns);
        }
        return this;
    }

    @Override
    public TransactionBuilder withReadSet(
            TableClient tableClient,
            long readVersion,
            Predicate readPredicate) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public TransactionBuilder withTransactionId(
            TableClient tableClient,
            String applicationId,
            long transactionVersion) {
        SetTransaction txnId = new SetTransaction(
                requireNonNull(applicationId, "applicationId is null"),
                transactionVersion,
                Optional.of(currentTimeMillis));
        this.transactionId = Optional.of(txnId);
        return this;
    }

    @Override
    public Transaction build(TableClient tableClient) {
        SnapshotImpl snapshot;
        try {
            snapshot = (SnapshotImpl) table.getLatestSnapshot(tableClient);
        } catch (TableNotFoundException tblf) {
            logger.info(
                    "Table {} doesn't exist yet. Trying to create a new table.",
                    table.getPath(tableClient));
            schema.orElseThrow(() -> new IllegalArgumentException(
                    "Table doesn't exist yet. Must provide a new schema."));
            // Table doesn't exist yet. Create an initial snapshot with the new schema.
            Metadata metadata = getInitialMetadata();
            Protocol protocol = getInitialProtocol();
            LogReplay logReplay = getEmptyLogReplay(tableClient, metadata, protocol);
            snapshot = new InitialSnapshot(
                    table.getLogPath(),
                    table.getDataPath(),
                    logReplay,
                    metadata,
                    protocol);
        }

        validate(tableClient, snapshot);

        return new TransactionImpl(
                table.getDataPath(),
                table.getLogPath(),
                snapshot,
                engineInfo,
                operation,
                schema,
                snapshot.getProtocol(),
                snapshot.getMetadata(),
                readVersion,
                readPredicate,
                transactionId);
    }

    /**
     * Validate the given parameters for the transaction.
     */
    private void validate(TableClient tableClient, SnapshotImpl snapshot) {
        // Validate the table has no features that Kernel doesn't yet support writing into it.
        TableFeatures.validateWriteSupportedTable(
                snapshot.getProtocol(),
                snapshot.getMetadata(),
                snapshot.getMetadata().getSchema());
        // TODO: if a new schema is given make sure it is valid

        long currentSnapshotVersion = snapshot.getVersion(tableClient);
        if (readVersion.isPresent() && readVersion.get() != currentSnapshotVersion) {
            throw new IllegalArgumentException(
                    "Read version doesn't match the current snapshot version. " +
                            "Read version: " + readVersion.get() +
                            " Current snapshot version: " + currentSnapshotVersion);
        }
        if (currentSnapshotVersion >= 0 && partitionColumns.isPresent()) {
            throw new IllegalArgumentException(
                    "Partition columns can only be set on a table that doesn't exist yet.");
        }

        if (transactionId.isPresent()) {
            Optional<Long> lastTxnVersion =
                    snapshot.getLatestTransactionVersion(transactionId.get().getAppId());
            if (lastTxnVersion.isPresent() &&
                    lastTxnVersion.get() >= transactionId.get().getVersion()) {
                throw new ConcurrentTransactionException(
                        transactionId.get().getAppId(),
                        transactionId.get().getVersion(),
                        lastTxnVersion.get());
            }
        }
    }


    private class InitialSnapshot extends SnapshotImpl {
        InitialSnapshot(
                Path logPath,
                Path dataPath,
                LogReplay logReplay,
                Metadata metadata,
                Protocol protocol) {
            super(
                    logPath,
                    dataPath,
                    LogSegment.empty(table.getLogPath()),
                    logReplay,
                    protocol,
                    metadata);
        }
    }

    private LogReplay getEmptyLogReplay(
            TableClient tableClient,
            Metadata metadata,
            Protocol protocol) {
        return new LogReplay(
                table.getLogPath(),
                table.getDataPath(),
                -1,
                tableClient,
                LogSegment.empty(table.getLogPath()),
                Optional.empty()) {

            @Override
            protected Tuple2<Protocol, Metadata> loadTableProtocolAndMetadata(
                    Optional<SnapshotHint> snapshotHint, long snapshotVersion) {
                return new Tuple2<>(protocol, metadata);
            }

            @Override
            public Optional<Long> getLatestTransactionIdentifier(String applicationId) {
                return Optional.empty();
            }
        };
    }

    private Metadata getInitialMetadata() {
        return new Metadata(
                java.util.UUID.randomUUID().toString(), /* id */
                Optional.empty(), /* name */
                Optional.empty(), /* description */
                new Format(), /* format */
                schema.get().toJson(), /* schemaString */
                schema.get(), /* schema */
                stringArrayValue(partitionColumns.isPresent() ?
                        new ArrayList<>(partitionColumns.get()) :
                        Collections.emptyList()), /* partitionColumns */
                Optional.of(currentTimeMillis), /* createdTime */
                stringStringMapValue(Collections.emptyMap()) /* configuration */
        );
    }

    private Protocol getInitialProtocol() {
        return new Protocol(
                DEFAULT_READ_VERSION,
                DEFAULT_WRITE_VERSION,
                null /* readerFeatures */,
                null /* writerFeatures */);
    }
}
