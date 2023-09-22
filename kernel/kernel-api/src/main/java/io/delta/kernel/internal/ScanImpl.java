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

import java.util.Optional;

import io.delta.kernel.Scan;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.types.TableSchemaSerDe;
import io.delta.kernel.internal.util.InternalSchemaUtils;

/**
 * Implementation of {@link Scan}
 */
public class ScanImpl
    implements Scan {
    /**
     * Schema of the snapshot from the Delta log being scanned in this scan. It is a logical schema
     * with metadata properties to derive the physical schema.
     */
    private final StructType snapshotSchema;

    private final Path dataPath;

    /**
     * Schema that we actually want to read.
     */
    private final StructType readSchema;
    private final CloseableIterator<FilteredColumnarBatch> filesIter;
    private final Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata;
    private final Optional<Predicate> filter;

    private boolean accessedScanFiles;

    public ScanImpl(
        StructType snapshotSchema,
        StructType readSchema,
        Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata,
        CloseableIterator<FilteredColumnarBatch> filesIter,
        Optional<Predicate> filter,
        Path dataPath) {
        this.snapshotSchema = snapshotSchema;
        this.readSchema = readSchema;
        this.protocolAndMetadata = protocolAndMetadata;
        this.filesIter = filesIter;
        this.dataPath = dataPath;

        this.filter = filter;
    }

    /**
     * Get an iterator of data files in this version of scan that survived the predicate pruning.
     *
     * @return data in {@link ColumnarBatch} batch format. Each row correspond to one survived file.
     */
    @Override
    public CloseableIterator<FilteredColumnarBatch> getScanFiles(TableClient tableClient) {
        if (accessedScanFiles) {
            throw new IllegalStateException("Scan files are already fetched from this instance");
        }
        accessedScanFiles = true;
        return filesIter;
    }

    @Override
    public Row getScanState(TableClient tableClient) {
        return ScanStateRow.of(
            protocolAndMetadata.get()._2,
            protocolAndMetadata.get()._1,
            TableSchemaSerDe.toJson(readSchema),
            TableSchemaSerDe.toJson(
                InternalSchemaUtils.convertToPhysicalSchema(
                    readSchema,
                    snapshotSchema,
                    protocolAndMetadata.get()._2.getConfiguration()
                        .getOrDefault("delta.columnMapping.mode", "none")
                )
            ),
            dataPath.toUri().toString());
    }

    @Override
    public Optional<Predicate> getRemainingFilter() {
        return filter;
    }
}
