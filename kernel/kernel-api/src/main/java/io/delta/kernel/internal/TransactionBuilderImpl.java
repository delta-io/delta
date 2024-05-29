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
import io.delta.kernel.data.ArrayValue;
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
import static io.delta.kernel.internal.DeltaErrors.unsupportedPartitionColumnsChange;
import static io.delta.kernel.internal.TransactionImpl.DEFAULT_READ_VERSION;
import static io.delta.kernel.internal.TransactionImpl.DEFAULT_WRITE_VERSION;
import static io.delta.kernel.internal.util.SchemaUtils.casePreservingPartitionColNames;
import static io.delta.kernel.internal.util.VectorUtils.stringArrayValue;


public class TransactionBuilderImpl implements TransactionBuilder {
    private static final Logger logger = LoggerFactory.getLogger(TransactionBuilderImpl.class);

    private final long currentTimeMillis = System.currentTimeMillis();
    private final TableImpl table;
    private final String engineInfo;
    private final Operation operation;
    private Optional<StructType> schema = Optional.empty();
    private Optional<List<String>> partitionColumns = Optional.empty();
    private Optional<SetTransaction> setTxnOpt = Optional.empty();
    private Optional<Map<String, String>> configuration = Optional.empty();

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
    public TransactionBuilder withConfiguration(Engine engine, Map<String, String> configuration) {
        if (!configuration.isEmpty()) {
            this.configuration = Optional.of(configuration);
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

    private void validateTableEvolution(SnapshotImpl snapshot) {
        Metadata metadata = snapshot.getMetadata();
        ArrayValue partitionColumns = metadata.getPartitionColumns();
        this.partitionColumns.map(ele -> {
            if (partitionColumns.getSize() != ele.size()) {
                throw unsupportedPartitionColumnsChange();
            }
            int size = partitionColumns.getSize();
            for (int i = 0; i < size; i++) {
                String partitionColumn = partitionColumns.getElements().getString(i);
                if (!partitionColumn.equals(ele.get(i))) {
                    throw unsupportedPartitionColumnsChange();
                }
            }
            return null;
        });
        this.schema.map(ele -> {
            StructType schema = metadata.getSchema();
            SchemaUtils.validateSchemaEvolution(schema, ele);
            return null;
        });
    }

    @Override
    public Transaction build(Engine engine) {
        SnapshotImpl snapshot;
        try {
            snapshot = (SnapshotImpl) table.getLatestSnapshot(engine);
            validateTableEvolution(snapshot);
            Metadata metadata = snapshot.getMetadata();
            configuration.map(ele -> {
                metadata.updateConfiguration(ele);
                return null;
            });
            schema.map(ele -> {
                metadata.updateSchema(ele);
                metadata.updateSchemaString(ele.toJson());
                return null;
            });
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
        validate(engine, snapshot);

        return new TransactionImpl(
                isNewTable,
                table.getDataPath(),
                table.getLogPath(),
                snapshot,
                engineInfo,
                operation,
                snapshot.getProtocol(),
                snapshot.getMetadata(),
                setTxnOpt);
    }


    /**
     * Validate the given parameters for the transaction.
     */
    private void validate(Engine engine, SnapshotImpl snapshot) {
        String tablePath = table.getPath(engine);
        // Validate the table has no features that Kernel doesn't yet support writing into it.
        TableFeatures.validateWriteSupportedTable(
                snapshot.getProtocol(),
                snapshot.getMetadata(),
                snapshot.getMetadata().getSchema(),
                tablePath);
        // New table verify the given schema and partition columns
        schema.map(ele -> {
            SchemaUtils.validateSchema(ele, false /* isColumnMappingEnabled */);
            SchemaUtils.validatePartitionColumns(
                ele, partitionColumns.orElse(Collections.emptyList()));
            return null;
        });


        setTxnOpt.ifPresent(txnId -> {
            Optional<Long> lastTxnVersion = snapshot.getLatestTransactionVersion(txnId.getAppId());
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
                configuration.orElse(Collections.emptyMap())  /* configuration */
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
