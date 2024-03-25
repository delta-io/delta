/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.VariantType;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.defaults.internal.data.vector.DefaultVariantVector;
import io.delta.kernel.defaults.internal.parquet.ParquetColumnReaders.BaseColumnReader;
import io.delta.kernel.defaults.internal.parquet.ParquetColumnReaders.BinaryColumnReader;

class VariantColumnReader
    extends GroupConverter
    implements BaseColumnReader {
    private final BinaryColumnReader valueConverter;
    private final BinaryColumnReader metadataConverter;

    // working state
    private int currentRowIndex;
    private boolean[] nullability;
    // If the value is null, start/end never get called which is a signal for null
    // Set the initial state to true and when start() is called set it to false.
    private boolean isCurrentValueNull = true;

    /**
     * Create converter for {@link VariantType} column.
     *
     * @param initialBatchSize Estimate of initial row batch size. Used in memory allocations.
     */
    VariantColumnReader(int initialBatchSize) {
        checkArgument(initialBatchSize > 0, "invalid initialBatchSize: %s", initialBatchSize);
        this.nullability = ParquetColumnReaders.initNullabilityVector(initialBatchSize);

        this.valueConverter = new BinaryColumnReader(BinaryType.BINARY, initialBatchSize);
        this.metadataConverter = new BinaryColumnReader(BinaryType.BINARY, initialBatchSize);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        checkArgument(
            fieldIndex >= 0 && fieldIndex < 2,
            "variant type is represented by a struct with 2 fields");
        if (fieldIndex == 0) {
            return valueConverter;
        } else {
            return metadataConverter;
        }
    }

    @Override
    public void start() {
        isCurrentValueNull = false;
    }

    @Override
    public void end() {
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
        ColumnVector vector = new DefaultVariantVector(
            batchSize,
            VariantType.VARIANT,
            Optional.of(nullability),
            valueConverter.getDataColumnVector(batchSize),
            metadataConverter.getDataColumnVector(batchSize)
        );
        resetWorkingState();
        return vector;
    }

    @Override
    public void resizeIfNeeded() {
        if (nullability.length == currentRowIndex) {
            int newSize = nullability.length * 2;
            this.nullability = Arrays.copyOf(this.nullability, newSize);
            ParquetColumnReaders.setNullabilityToTrue(this.nullability, newSize / 2, newSize);
        }
    }

    @Override
    public void resetWorkingState() {
        this.currentRowIndex = 0;
        this.isCurrentValueNull = true;
        this.nullability = ParquetColumnReaders.initNullabilityVector(this.nullability.length);
    }

    private void finalizeLastRowInConverters(long prevRowIndex) {
        valueConverter.finalizeCurrentRow(prevRowIndex);
        metadataConverter.finalizeCurrentRow(prevRowIndex);
    }
}
