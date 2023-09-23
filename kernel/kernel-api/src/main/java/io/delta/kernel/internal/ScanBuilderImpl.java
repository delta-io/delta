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

import java.util.List;
import java.util.Optional;

import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;

/**
 * Implementation of {@link ScanBuilder}.
 */
public class ScanBuilderImpl
    implements ScanBuilder {

    private final StructType snapshotSchema;
    private final CloseableIterator<FilteredColumnarBatch> filesIter;
    private final Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata;
    private final TableClient tableClient;
    private final Path dataPath;

    private StructType readSchema;
    private Optional<Predicate> predicate;

    public ScanBuilderImpl(
        Path dataPath,
        Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata,
        StructType snapshotSchema,
        CloseableIterator<FilteredColumnarBatch> filesIter,
        TableClient tableClient) {
        this.dataPath = dataPath;
        this.snapshotSchema = snapshotSchema;
        this.filesIter = filesIter;
        this.protocolAndMetadata = protocolAndMetadata;
        this.tableClient = tableClient;

        this.readSchema = snapshotSchema;
        this.predicate = Optional.empty();
    }

    @Override
    public ScanBuilder withFilter(TableClient tableClient, Predicate predicate) {
        if (this.predicate.isPresent()) {
            throw new IllegalArgumentException("There already exists a filter in current builder");
        }
        this.predicate = Optional.of(predicate);
        return this;
    }

    @Override
    public ScanBuilder withReadSchema(TableClient tableClient, StructType readSchema) {
        // TODO: validate the readSchema is a subset of the table schema
        this.readSchema = readSchema;
        return this;
    }

    @Override
    public Scan build() {
        // TODO: support timestamp type partition columns
        // Timestamp partition columns have complicated semantics related to timezones so block this
        // for now
        List<String> partitionCols = protocolAndMetadata.get()._2.getPartitionColumns();
        for (String colName : partitionCols) {
            if (readSchema.indexOf(colName) >= 0 &&
                readSchema.get(colName).getDataType() instanceof TimestampType) {
                throw new UnsupportedOperationException(String.format(
                    "Reading partition columns of TimestampType is unsupported.\n" +
                        "readSchema: %s\npartitionColumns: %s",
                    readSchema,
                    partitionCols
                ));
            }
        }

        return new ScanImpl(
            snapshotSchema,
            readSchema,
            protocolAndMetadata,
            filesIter,
            predicate,
            dataPath);
    }
}
