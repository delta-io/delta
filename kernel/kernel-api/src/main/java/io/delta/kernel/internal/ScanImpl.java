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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import io.delta.kernel.Scan;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;

import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.data.AddFileColumnarBatch;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.types.TableSchemaSerDe;
import io.delta.kernel.internal.util.PartitionUtils;

/**
 * Implementation of {@link Scan}
 */
public class ScanImpl
    implements Scan
{
    /**
     * Complete schema of the snapshot to be scanned.
     */
    private final StructType snapshotSchema;

    private final Path dataPath;

    /**
     * Schema that we actually want to read.
     */
    private final StructType readSchema;

    /**
     * Partition schema.
     */
    private final StructType snapshotPartitionSchema;

    private final CloseableIterator<AddFile> filesIter;
    private final TableClient tableClient;
    private final Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata;

    private final Optional<Expression> filter;
    private final Optional<Expression> metadataFilterConjunction;
    private final Optional<Expression> dataFilterConjunction;

    public ScanImpl(
        StructType snapshotSchema,
        StructType readSchema,
        StructType snapshotPartitionSchema,
        Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata,
        CloseableIterator<AddFile> filesIter,
        Optional<Expression> filter,
        Path dataPath,
        TableClient tableClient)
    {
        this.snapshotSchema = snapshotSchema;
        this.readSchema = readSchema;
        this.snapshotPartitionSchema = snapshotPartitionSchema;
        this.protocolAndMetadata = protocolAndMetadata;
        this.filesIter = filesIter;
        this.dataPath = dataPath;
        this.tableClient = tableClient;

        this.filter = filter;
        if (filter.isPresent()) {
            final List<String> partitionColumns = snapshotPartitionSchema.fieldNames();
            final Tuple2<Optional<Expression>, Optional<Expression>> metadataAndDataConjunctions =
                PartitionUtils.splitMetadataAndDataPredicates(filter.get(), partitionColumns);

            this.metadataFilterConjunction = metadataAndDataConjunctions._1;
            this.dataFilterConjunction = metadataAndDataConjunctions._2;
        }
        else {
            this.metadataFilterConjunction = Optional.empty();
            this.dataFilterConjunction = Optional.empty();
        }
    }

    /**
     * Get an iterator of data files in this version of scan that survived the predicate pruning.
     *
     * @return data in {@link ColumnarBatch} batch format. Each row correspond to one survived file.
     */
    @Override
    public CloseableIterator<ColumnarBatch> getScanFiles(TableClient tableClient)
    {
        return new CloseableIterator<ColumnarBatch>()
        {
            private Optional<AddFile> nextValid = Optional.empty();
            private boolean closed;

            @Override
            public boolean hasNext()
            {
                if (closed) {
                    throw new IllegalStateException("Can't call `hasNext` on a closed iterator.");
                }
                if (!nextValid.isPresent()) {
                    nextValid = findNextValid();
                }
                return nextValid.isPresent();
            }

            @Override
            public ColumnarBatch next()
            {
                if (closed) {
                    throw new IllegalStateException("Can't call `next` on a closed iterator.");
                }
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                List<AddFile> batchAddFiles = new ArrayList<>();
                do {
                    batchAddFiles.add(nextValid.get());
                    nextValid = Optional.empty();
                }
                while (batchAddFiles.size() < 8 && hasNext());
                return new AddFileColumnarBatch(Collections.unmodifiableList(batchAddFiles));
            }

            @Override
            public void close()
                throws IOException
            {
                filesIter.close();
                this.closed = true;
            }

            private Optional<AddFile> findNextValid()
            {
                if (filesIter.hasNext()) {
                    return Optional.of(filesIter.next());
                }
                return Optional.empty();
            }
        };
    }

    @Override
    public Row getScanState(TableClient tableClient)
    {
        return new ScanStateRow(
            protocolAndMetadata.get()._2,
            protocolAndMetadata.get()._1,
            TableSchemaSerDe.toJson(readSchema),
            TableSchemaSerDe.toJson(convertToPhysicalSchema(readSchema, snapshotSchema)),
            dataPath.toUri().toString());
    }

    @Override
    public Expression getRemainingFilter()
    {
        return filter.orElse(Literal.TRUE);
    }

    /**
     * Helper method that converts the logical schema (requested by the connector) to physical
     * schema of the data stored in data files.
     */
    private StructType convertToPhysicalSchema(StructType logicalSchema, StructType snapshotSchema)
    {
        String columnMappingMode = protocolAndMetadata.get()._2.getConfiguration()
            .get("delta.columnMapping.mode");
        if ("name".equalsIgnoreCase(columnMappingMode)) {
            StructType newSchema = new StructType();
            for (StructField field : logicalSchema.fields()) {
                DataType oldType = field.getDataType();
                StructField fieldFromMetadata = snapshotSchema.get(field.getName());
                DataType newType;
                if (oldType instanceof StructType) {
                    newType = convertToPhysicalSchema(
                        (StructType) field.getDataType(),
                        (StructType) fieldFromMetadata.getDataType());
                }
                else if (oldType instanceof ArrayType) {
                    throw new UnsupportedOperationException("NYI");
                }
                else if (oldType instanceof MapType) {
                    throw new UnsupportedOperationException("NYI");
                }
                else {
                    newType = oldType;
                }
                String physicalName = fieldFromMetadata
                    .getMetadata()
                    .get("delta.columnMapping.physicalName");
                newSchema = newSchema.add(physicalName, newType);
            }
            return newSchema;
        }
        if ("id".equalsIgnoreCase(columnMappingMode)) {
            throw new UnsupportedOperationException(
                "Column mapping `id` mode is not yet supported");
        }

        return logicalSchema;
    }
}
