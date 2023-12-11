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
package io.delta.kernel.defaults.internal.parquet;

import java.util.*;
import static java.util.Objects.requireNonNull;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.defaults.internal.data.vector.DefaultStructVector;
import static io.delta.kernel.defaults.internal.DefaultKernelUtils.findSubFieldType;

class RowConverter
    extends GroupConverter
    implements ParquetConverters.BaseConverter {
    private final StructType readSchema;
    private final Converter[] converters;
    // The delta may request columns that don't exists in Parquet
    // This map is to track the ordinal known to Parquet reader to the converter array ordinal.
    // If a column is missing, a dummy converter added to the `converters` array and which
    // generates all null vector at the end.
    private final Map<Integer, Integer> parquetOrdinalToConverterOrdinal;

    // Working state
    private boolean isCurrentValueNull = true;
    private int currentRowIndex;
    private boolean[] nullability;

    /**
     * Create converter for {@link StructType} column.
     *
     * @param initialBatchSize Estimate of initial row batch size. Used in memory allocations.
     * @param readSchema       Schem of the columns to read from the file.
     * @param fileSchema       Schema of the pruned columns from the file schema We have some
     *                         necessary requirements here: (1) the fields in fileSchema are a
     *                         subset of readSchema (parquet schema has been pruned). (2) the
     *                         fields in fileSchema are in the same order as the corresponding
     *                         fields in readSchema.
     */
    RowConverter(int initialBatchSize, StructType readSchema, GroupType fileSchema) {
        checkArgument(initialBatchSize > 0, "invalid initialBatchSize: %s", initialBatchSize);
        this.readSchema = requireNonNull(readSchema, "readSchema is not null");
        List<StructField> fields = readSchema.fields();
        this.converters = new Converter[fields.size()];
        this.parquetOrdinalToConverterOrdinal = new HashMap<>();

        // Initialize the working state
        this.nullability = ParquetConverters.initNullabilityVector(initialBatchSize);

        int parquetOrdinal = 0;
        for (int i = 0; i < converters.length; i++) {
            final StructField field = fields.get(i);
            final DataType typeFromClient = field.getDataType();
            final Type typeFromFile = field.isDataColumn() ?
                findSubFieldType(fileSchema, field) : null;
            if (typeFromFile == null) {
                if (StructField.METADATA_ROW_INDEX_COLUMN_NAME.equalsIgnoreCase(field.getName()) &&
                    field.isMetadataColumn()) {
                    checkArgument(field.getDataType() instanceof LongType,
                        "row index metadata column must be type long");
                    converters[i] =
                        new ParquetConverters.FileRowIndexColumnConverter(initialBatchSize);
                } else {
                    converters[i] = new ParquetConverters.NonExistentColumnConverter(
                        typeFromClient);
                }
            } else {
                converters[i] = ParquetConverters.createConverter(
                    initialBatchSize, typeFromClient, typeFromFile);
                parquetOrdinalToConverterOrdinal.put(parquetOrdinal, i);
                parquetOrdinal++;
            }
        }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters[parquetOrdinalToConverterOrdinal.get(fieldIndex)];
    }

    @Override
    public void start() {
        isCurrentValueNull = false;
    }

    @Override
    public void end() {
    }

    public ColumnarBatch getDataAsColumnarBatch(int batchSize) {
        ColumnVector[] memberVectors = collectMemberVectors(batchSize);
        ColumnarBatch batch = new DefaultColumnarBatch(batchSize, readSchema, memberVectors);
        resetWorkingState();
        return batch;
    }

    @Override
    public void finalizeCurrentRow(long currentRowIndex) {
        resizeIfNeeded();
        finalizeLastRowInConverters(currentRowIndex);
        nullability[this.currentRowIndex] = isCurrentValueNull;
        isCurrentValueNull = true;

        this.currentRowIndex++;
    }

    public ColumnVector getDataColumnVector(int batchSize) {
        ColumnVector[] memberVectors = collectMemberVectors(batchSize);
        ColumnVector vector = new DefaultStructVector(
            batchSize,
            readSchema,
            Optional.of(nullability),
            memberVectors
        );
        resetWorkingState();
        return vector;
    }

    @Override
    public void resizeIfNeeded() {
        if (nullability.length == currentRowIndex) {
            int newSize = nullability.length * 2;
            this.nullability = Arrays.copyOf(this.nullability, newSize);
            ParquetConverters.setNullabilityToTrue(this.nullability, newSize / 2, newSize);
        }
    }

    @Override
    public void resetWorkingState() {
        this.currentRowIndex = 0;
        this.isCurrentValueNull = true;
        this.nullability = ParquetConverters.initNullabilityVector(this.nullability.length);
    }

    private void finalizeLastRowInConverters(long prevRowIndex) {
        for (int i = 0; i < converters.length; i++) {
            ((ParquetConverters.BaseConverter) converters[i]).finalizeCurrentRow(prevRowIndex);
        }
    }

    private ColumnVector[] collectMemberVectors(int batchSize) {
        final ColumnVector[] output = new ColumnVector[converters.length];

        for (int i = 0; i < converters.length; i++) {
            output[i] = ((ParquetConverters.BaseConverter) converters[i])
                .getDataColumnVector(batchSize);
        }

        return output;
    }
}
