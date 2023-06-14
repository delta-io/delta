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

import java.util.Arrays;
import java.util.Optional;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.vector.DefaultMapVector;
import io.delta.kernel.types.MapType;

public class MapConverter
        extends GroupConverter
        implements ParquetConverters.BaseConverter
{
    private final MapType typeFromClient;
    private final MapCollector converter;

    // working state
    private int currentRowIndex;
    private final boolean[] nullability;
    private final int[] offsets;

    public MapConverter(
            int maxBatchSize,
            MapType typeFromClient,
            GroupType typeFromFile) {
        this.typeFromClient = typeFromClient;
        final GroupType innerMapType = (GroupType) typeFromFile.getType("key_value");
        this.converter = new MapCollector(
                maxBatchSize,
                typeFromClient,
                innerMapType
        );

        // initialize working state
        this.nullability = new boolean[maxBatchSize];
        // Initialize all values as null. As Parquet calls this converter only for non-null
        // values, make the corresponding value to false.
        Arrays.fill(this.nullability, true);

        this.offsets = new int[maxBatchSize + 1];
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
        converter.asGroupConverter().start();
    }

    @Override
    public void end()
    {
        converter.asGroupConverter().end();
    }

    @Override
    public ColumnVector getDataColumnVector(int batchSize)
    {
        return new DefaultMapVector(
                batchSize,
                typeFromClient,
                Optional.of(nullability),
                offsets,
                converter.getKeyVector(),
                converter.getValueVector()
        );
    }

    @Override
    public boolean moveToNextRow()
    {
        boolean isNull = converter.isLastValueNull;
        nullability[currentRowIndex] = isNull;
        offsets[currentRowIndex + 1] = converter.currentEntryIndex;
        currentRowIndex++;

        return isNull;
    }

    public static class MapCollector
            extends GroupConverter
    {
        private final Converter[] converters;

        // working state
        private int currentEntryIndex;
        private boolean isLastValueNull;

        public MapCollector(
                int maxBatchSize,
                MapType typeFromClient,
                GroupType innerMapType) {
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

            long memberNullCount = Arrays.stream(converters)
                    .map(converter -> (ParquetConverters.BaseConverter) converter)
                    .map(converters -> converters.moveToNextRow())
                    .filter(result -> result)
                    .count();

            isLastValueNull = memberNullCount == converters.length;

            if (!isLastValueNull) {
                currentEntryIndex++;
            }
        }

        public ColumnVector getKeyVector() {
            return ((ParquetConverters.BaseConverter) converters[0])
                    .getDataColumnVector(currentEntryIndex);
        }

        public ColumnVector getValueVector() {
            return ((ParquetConverters.BaseConverter) converters[1])
                    .getDataColumnVector(currentEntryIndex);
        }
    }
}
