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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.vector.DefaultArrayVector;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.MapType;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;

import java.util.Arrays;
import java.util.Optional;

public class ArrayConverter
        extends GroupConverter
        implements ParquetConverters.BaseConverter
{
    private final ArrayType typeFromClient;
    private final ArrayCollector converter;

    // working state
    private int currentRowIndex;
    private final boolean[] nullability;
    private final int[] offsets;

    public ArrayConverter(
            int maxBatchSize,
            ArrayType typeFromClient,
            GroupType typeFromFile)
    {
        this.typeFromClient = typeFromClient;
        final GroupType innerElementType = (GroupType) typeFromFile.getType("list");
        this.converter = new ArrayCollector(
                maxBatchSize,
                typeFromClient,
                innerElementType
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
        converter.start();
    }

    @Override
    public void end()
    {
        converter.end();
    }

    @Override
    public ColumnVector getDataColumnVector(int batchSize)
    {
        return new DefaultArrayVector(
                batchSize,
                typeFromClient,
                Optional.of(nullability),
                offsets,
                converter.getArrayVector()
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

    public static class ArrayCollector
            extends GroupConverter
    {
        private final Converter converter;

        // working state
        private int currentEntryIndex;
        private boolean isLastValueNull;

        public ArrayCollector(
                int maxBatchSize,
                ArrayType typeFromClient,
                GroupType innerArrayType) {
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
            isLastValueNull = ((ParquetConverters.BaseConverter) converter).moveToNextRow();

            if (!isLastValueNull) {
                currentEntryIndex++;
            }
        }

        public ColumnVector getArrayVector() {
            return ((ParquetConverters.BaseConverter) converter).getDataColumnVector(currentEntryIndex);
        }
    }
}
