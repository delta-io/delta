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

package io.delta.kernel.parquet;

import static io.delta.kernel.parquet.ParquetConverters.initNullabilityVector;
import static io.delta.kernel.parquet.ParquetConverters.setNullabilityToTrue;
import java.util.Arrays;
import java.util.Optional;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.vector.DefaultArrayVector;
import io.delta.kernel.types.ArrayType;

class ArrayConverter
    extends GroupConverter
    implements ParquetConverters.BaseConverter
{
    private final ArrayType typeFromClient;
    private final ArrayCollector converter;

    // working state
    private int currentRowIndex;
    private boolean[] nullability;
    private int[] offsets;
    private int collectorIndexAtStart;

    public ArrayConverter(
        int initialBatchSize,
        ArrayType typeFromClient,
        GroupType typeFromFile)
    {
        this.typeFromClient = typeFromClient;
        final GroupType innerElementType = (GroupType) typeFromFile.getType("list");
        this.converter = new ArrayCollector(
            initialBatchSize,
            typeFromClient,
            innerElementType
        );

        // initialize working state
        this.nullability = initNullabilityVector(initialBatchSize);
        this.offsets = new int[initialBatchSize + 1];
    }

    @Override
    public Converter getConverter(int fieldIndex)
    {
        switch (fieldIndex) {
            case 0:
                return converter;
            default:
                throw new IllegalArgumentException(
                    "Invalid field index for a array column: " + fieldIndex);
        }
    }

    @Override
    public void start()
    {
        collectorIndexAtStart = converter.currentEntryIndex;
    }

    @Override
    public void end()
    {
        int collectorIndexAtEnd = converter.currentEntryIndex;
        this.nullability[currentRowIndex] = collectorIndexAtEnd == collectorIndexAtStart;
        this.offsets[currentRowIndex + 1] = collectorIndexAtEnd;
    }

    @Override
    public ColumnVector getDataColumnVector(int batchSize)
    {
        ColumnVector vector = new DefaultArrayVector(
            batchSize,
            typeFromClient,
            Optional.of(nullability),
            offsets,
            converter.getArrayVector()
        );
        resetWorkingState();
        return vector;
    }

    @Override
    public boolean moveToNextRow()
    {
        currentRowIndex++;
        resizeIfNeeded();

        return nullability[currentRowIndex - 1];
    }

    @Override
    public void resizeIfNeeded()
    {
        if (nullability.length == currentRowIndex) {
            int newSize = nullability.length * 2;
            this.nullability = Arrays.copyOf(this.nullability, newSize);
            setNullabilityToTrue(this.nullability, newSize / 2, newSize);

            this.offsets = Arrays.copyOf(this.offsets, newSize + 1);
        }
    }

    @Override
    public void resetWorkingState()
    {
        this.currentRowIndex = 0;
        this.nullability = initNullabilityVector(nullability.length);
        this.offsets = new int[offsets.length];
    }

    public static class ArrayCollector
        extends GroupConverter
    {
        private final Converter converter;

        // working state
        private int currentEntryIndex;

        public ArrayCollector(int maxBatchSize, ArrayType typeFromClient, GroupType innerArrayType)
        {
            this.converter = ParquetConverters.createConverter(
                maxBatchSize,
                typeFromClient.getElementType(),
                innerArrayType.getType("element"));
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            switch (fieldIndex) {
                case 0:
                    return converter;
                default:
                    throw new IllegalArgumentException(
                        "Invalid field index for a map column: " + fieldIndex);
            }
        }

        @Override
        public void start()
        {
            if (!converter.isPrimitive()) {
                converter.asGroupConverter().start();
            }
        }

        @Override
        public void end()
        {
            if (!converter.isPrimitive()) {
                converter.asGroupConverter().end();
            }
            ((ParquetConverters.BaseConverter) converter).moveToNextRow();
            currentEntryIndex++;
        }

        public ColumnVector getArrayVector()
        {
            ColumnVector vector = ((ParquetConverters.BaseConverter) converter)
                .getDataColumnVector(currentEntryIndex);

            currentEntryIndex = 0;
            return vector;
        }
    }
}
