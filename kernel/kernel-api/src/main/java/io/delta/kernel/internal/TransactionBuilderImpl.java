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
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.snapshot.SnapshotHint;
import io.delta.kernel.internal.util.SchemaUtils;
import io.delta.kernel.internal.util.Tuple2;
import static io.delta.kernel.internal.DeltaErrors.requiresSchemaForNewTable;
import static io.delta.kernel.internal.DeltaErrors.tableAlreadyExists;
import static io.delta.kernel.internal.TransactionImpl.DEFAULT_READ_VERSION;
import static io.delta.kernel.internal.TransactionImpl.DEFAULT_WRITE_VERSION;
import static io.delta.kernel.internal.util.SchemaUtils.casePreservingPartitionColNames;
import static io.delta.kernel.internal.util.VectorUtils.stringArrayValue;
import static io.delta.kernel.internal.util.VectorUtils.stringStringMapValue;

public class TransactionBuilderImpl implements TransactionBuilder {
    private static final Logger logger = LoggerFactory.getLogger(TransactionBuilderImpl.class);

    private final long currentTimeMillis = System.currentTimeMillis();
    private final TableImpl table;
    private final String engineInfo;
    private final Operation operation;
    private Optional<StructType> schema = Optional.empty();
    private Optional<List<String>> partitionColumns = Optional.empty();
    private Optional<SetTransaction> setTxnOpt = Optional.empty();
    private Optional<Map<String, String>> tableProperties = Optional.empty();

    public TransactionBuilderImpl(TableImpl table, String engineInfo, Operation operation) {
        this.table = table;
        this.engineInfo = engineInfo;
        this.operation = operation;
    }

    @Override
    public TransactionBuilder withSchema(Engine engine, StructType newSchema) {
        this.schema = Optional.of(newSchema); // will be verified as part of the build() call
        return this;
    }

    @Override
    public TransactionBuilder withPartitionColumns(Engine engine, List<String> partitionColumns) {
        if (!partitionColumns.isEmpty()) {
            this.partitionColumns = Optional.of(partitionColumns);
        }
        return this;
    }

    @Override
    public TransactionBuilder withTransactionId(
            Engine engine,
            String applicationId,
            long transactionVersion) {
        SetTransaction txnId = new SetTransaction(
                requireNonNull(applicationId, "applicationId is null"),
                transactionVersion,
                Optional.of(currentTimeMillis));
        this.setTxnOpt = Optional.of(txnId);
        return this;
    }

    @Override
    public TransactionBuilder withTableProperties(Engine engine, Map<String, String> properties) {
        this.tableProperties = Optional.of(new HashMap<>(properties));
        return this;
    }

    @Override
    public Transaction build(Engine engine) {
        SnapshotImpl snapshot;
        try {
            snapshot = (SnapshotImpl) table.getLatestSnapshot(engine);
        } catch (TableNotFoundException tblf) {
            String tablePath = table.getPath(engine);
            logger.info("Table {} doesn't exist yet. Trying to create a new table.", tablePath);
            schema.orElseThrow(() -> requiresSchemaForNewTable(tablePath));
            // Table doesn't exist yet. Create an initial snapshot with the new schema.
            Metadata metadata = getInitialMetadata();
            Protocol protocol = getInitialProtocol();
            LogReplay logReplay = getEmptyLogReplay(engine, metadata, protocol);
            snapshot = new InitialSnapshot(table.getDataPath(), logReplay, metadata, protocol);
        }

        boolean isNewTable = snapshot.getVersion(engine) < 0;
        validate(engine, snapshot, isNewTable);

        Metadata  metadata = snapshot.getMetadata();
        boolean shouldUpdateMetadata = false;
        if (tableProperties.isPresent()) {
            Map<String, String> validatedProperties =
                    TableConfig.validateProperties(tableProperties.get());
            Map<String, String> newProperties =
                    metadata.filterOutUnchangedProperties(validatedProperties);
            if (!newProperties.isEmpty()) {
                shouldUpdateMetadata = true;
                metadata = metadata.withNewConfiguration(newProperties);
            }
        }

        return new TransactionImpl(
                isNewTable,
                table.getDataPath(),
                table.getLogPath(),
                snapshot,
                engineInfo,
                operation,
                snapshot.getProtocol(),
                metadata,
                setTxnOpt,
                shouldUpdateMetadata);
    }

    /**
     * Validate the given parameters for the transaction.
     */
    private void validate(Engine engine, SnapshotImpl snapshot, boolean isNewTable) {
        String tablePath = table.getPath(engine);
        // Validate the table has no features that Kernel doesn't yet support writing into it.
        TableFeatures.validateWriteSupportedTable(
                snapshot.getProtocol(),
                snapshot.getMetadata(),
                snapshot.getMetadata().getSchema(),
                tablePath);

        if (!isNewTable) {
            if (schema.isPresent()) {
                throw tableAlreadyExists(
                        tablePath,
                        "Table already exists, but provided a new schema. " +
                                "Schema can only be set on a new table.");
            }
            if (partitionColumns.isPresent()) {
                throw tableAlreadyExists(
                        tablePath,
                        "Table already exists, but provided new partition columns. "
                                + "Partition columns can only be set on a new table.");
            }
        } else {
            // New table verify the given schema and partition columns
            SchemaUtils.validateSchema(schema.get(), false /* isColumnMappingEnabled */);
            SchemaUtils.validatePartitionColumns(
                    schema.get(), partitionColumns.orElse(Collections.emptyList()));
        }

        setTxnOpt.ifPresent(txnId -> {
            Optional<Long> lastTxnVersion =
                    snapshot.getLatestTransactionVersion(engine, txnId.getAppId());
            if (lastTxnVersion.isPresent() && lastTxnVersion.get() >= txnId.getVersion()) {
                throw DeltaErrors.concurrentTransaction(
                        txnId.getAppId(),
                        txnId.getVersion(),
                        lastTxnVersion.get());
            }
        });
    }

    private class InitialSnapshot extends SnapshotImpl {
        InitialSnapshot(Path dataPath, LogReplay logReplay, Metadata metadata, Protocol protocol) {
            super(dataPath, LogSegment.empty(table.getLogPath()), logReplay, protocol, metadata);
        }
    }

    private LogReplay getEmptyLogReplay(Engine engine, Metadata metadata, Protocol protocol) {
        return new LogReplay(
                table.getLogPath(),
                table.getDataPath(),
                -1,
                engine,
                LogSegment.empty(table.getLogPath()),
                Optional.empty()) {

            @Override
            protected Tuple2<Protocol, Metadata> loadTableProtocolAndMetadata(
                    Engine engine, Optional<SnapshotHint> snapshotHint, long snapshotVersion) {
                return new Tuple2<>(protocol, metadata);
            }

            @Override
            public Optional<Long> getLatestTransactionIdentifier(
                    Engine engine, String applicationId) {
                return Optional.empty();
            }
        };
    }

    private Metadata getInitialMetadata() {
        List<String> partitionColumnsCasePreserving = casePreservingPartitionColNames(
                schema.get(),
                partitionColumns.orElse(Collections.emptyList()));

        return new Metadata(
                java.util.UUID.randomUUID().toString(), /* id */
                Optional.empty(), /* name */
                Optional.empty(), /* description */
                new Format(), /* format */
                schema.get().toJson(), /* schemaString */
                schema.get(), /* schema */
                stringArrayValue(partitionColumnsCasePreserving), /* partitionColumns */
                Optional.of(currentTimeMillis), /* createdTime */
                stringStringMapValue(Collections.emptyMap())
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
