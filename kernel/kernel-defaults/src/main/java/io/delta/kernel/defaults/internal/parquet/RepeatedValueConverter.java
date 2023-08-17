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

import java.util.Arrays;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;

import io.delta.kernel.data.ColumnVector;

import io.delta.kernel.defaults.internal.parquet.ParquetConverters.BaseConverter;
import static io.delta.kernel.defaults.internal.parquet.ParquetConverters.initNullabilityVector;
import static io.delta.kernel.defaults.internal.parquet.ParquetConverters.setNullabilityToTrue;

/**
 * Abstract implementation of Parquet converters for capturing the repeated types such as
 * list or map.
 */
abstract class RepeatedValueConverter extends GroupConverter implements BaseConverter {
    private final Collector collector;

    // working state
    private int currentRowIndex;
    private boolean[] nullability;
    private int[] offsets;
    // If the repeated value is null, start/end never get called which is a signal for null
    // Set the initial state to true and when start() is called set it to false.
    private boolean isCurrentValueNull = true;

    /**
     * Create instance.
     *
     * @param initialBatchSize  Starting batch output size.
     * @param elementConverters List of converters that are part of the repeated type.
     */
    RepeatedValueConverter(int initialBatchSize, Converter... elementConverters) {
        this.collector = new Collector(elementConverters);
        // initialize working state
        this.nullability = initNullabilityVector(initialBatchSize);
        this.offsets = new int[initialBatchSize + 1];
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        if (fieldIndex != 0) {
            throw new IllegalArgumentException("Invalid field index: " + fieldIndex);
        }
        return collector;
    }

    @Override
    public void start() {
        this.isCurrentValueNull = false;
    }

    @Override
    public void end() {}

    @Override
    public void moveToNextRow(long prevRowIndex) {
        this.offsets[currentRowIndex + 1] = collector.currentEntryIndex;
        this.nullability[currentRowIndex] = isCurrentValueNull;
        this.isCurrentValueNull = true;

        currentRowIndex++;
        resizeIfNeeded();
    }

    @Override
    public void resizeIfNeeded() {
        if (nullability.length == currentRowIndex) {
            int newSize = nullability.length * 2;
            this.nullability = Arrays.copyOf(this.nullability, newSize);
            setNullabilityToTrue(this.nullability, newSize / 2, newSize);

            this.offsets = Arrays.copyOf(this.offsets, newSize + 1);
        }
    }

    @Override
    public void resetWorkingState() {
        this.currentRowIndex = 0;
        this.isCurrentValueNull = true;
        this.nullability = initNullabilityVector(nullability.length);
        this.offsets = new int[offsets.length];
    }

    protected boolean[] getNullability() {
        return nullability;
    }

    protected int[] getOffsets() {
        return offsets;
    }

    /**
     * @return the {@link ColumnVector}s from the underlying element vectors. Once retrieved
     * the converters are reset and requires {@link #resetWorkingState()} before using
     * this repeated converter again.
     */
    protected ColumnVector[] getElementDataVectors() {
        return collector.getDataVectors();
    }

    /**
     * GroupConverter to collect repeated elements. For each repeated element value set, the call
     * pattern from the Parquet reader: start(), followed by value read for each element converter
     * and end().
     */
    private static class Collector extends GroupConverter {
        private final Converter[] elementConverters;

        // working state
        private int currentEntryIndex;

        Collector(Converter... elementConverters) {
            this.elementConverters = elementConverters;
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            if (fieldIndex < 0 || fieldIndex >= elementConverters.length) {
                throw new IllegalArgumentException("Invalid field index: " + fieldIndex);
            }
            return elementConverters[fieldIndex];
        }

        @Override
        public void start() {}

        @Override
        public void end() {
            for (Converter converter : elementConverters) {
                // Row indexes are not needed for nested columns
                long prevRowIndex = -1;
                ((ParquetConverters.BaseConverter) converter).moveToNextRow(prevRowIndex);
            }
            currentEntryIndex++;
        }

        ColumnVector[] getDataVectors() {
            ColumnVector[] dataVectors = new ColumnVector[elementConverters.length];
            for (int i = 0; i < elementConverters.length; i++) {
                dataVectors[i] = ((ParquetConverters.BaseConverter) elementConverters[i])
                    .getDataColumnVector(currentEntryIndex);
            }
            currentEntryIndex = 0;
            return dataVectors;
        }
    }
}
