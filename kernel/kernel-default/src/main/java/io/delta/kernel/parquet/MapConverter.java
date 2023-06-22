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
import io.delta.kernel.data.vector.DefaultMapVector;
import io.delta.kernel.types.MapType;

class MapConverter
    extends GroupConverter
    implements ParquetConverters.BaseConverter
{
    private final MapType typeFromClient;
    private final MapCollector converter;

    // working state
    private int currentRowIndex;
    private boolean[] nullability;
    private int[] offsets;
    private int collectorIndexAtStart;

    public MapConverter(
        int initialBatchSize,
        MapType typeFromClient,
        GroupType typeFromFile)
    {
        this.typeFromClient = typeFromClient;
        final GroupType innerMapType = (GroupType) typeFromFile.getType("key_value");
        this.converter = new MapCollector(
            initialBatchSize,
            typeFromClient,
            innerMapType
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
                    "Invalid field index for a map column: " + fieldIndex);
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
        ColumnVector vector = new DefaultMapVector(
            batchSize,
            typeFromClient,
            Optional.of(nullability),
            offsets,
            converter.getKeyVector(),
            converter.getValueVector()
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
        this.converter.currentEntryIndex = 0;
        this.nullability = initNullabilityVector(nullability.length);
        this.offsets = new int[offsets.length];
    }

    public static class MapCollector
        extends GroupConverter
    {
        private final Converter[] converters;

        // working state
        private int currentEntryIndex;

        public MapCollector(
            int maxBatchSize,
            MapType typeFromClient,
            GroupType innerMapType)
        {
            this.converters = new Converter[2];
            this.converters[0] = ParquetConverters.createConverter(
                maxBatchSize,
                typeFromClient.getKeyType(),
                innerMapType.getType("key"));
            this.converters[1] = ParquetConverters.createConverter(
                maxBatchSize,
                typeFromClient.getValueType(),
                innerMapType.getType("value"));
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            switch (fieldIndex) {
                case 0: // fall through
                case 1:
                    return converters[fieldIndex];
                default:
                    throw new IllegalArgumentException(
                        "Invalid field index for a map column: " + fieldIndex);
            }
        }

        @Override
        public void start()
        {
            Arrays.stream(converters)
                .filter(conv -> !conv.isPrimitive())
                .forEach(conv -> ((GroupConverter) conv).start());
        }

        @Override
        public void end()
        {
            Arrays.stream(converters)
                .filter(conv -> !conv.isPrimitive())
                .forEach(conv -> ((GroupConverter) conv).end());

            Arrays.stream(converters)
                .map(converter -> (ParquetConverters.BaseConverter) converter)
                .forEach(converter -> converter.moveToNextRow());

            currentEntryIndex++;
        }

        public ColumnVector getKeyVector()
        {
            return ((ParquetConverters.BaseConverter) converters[0])
                .getDataColumnVector(currentEntryIndex);
        }

        public ColumnVector getValueVector()
        {
            return ((ParquetConverters.BaseConverter) converters[1])
                .getDataColumnVector(currentEntryIndex);
        }
    }
}
