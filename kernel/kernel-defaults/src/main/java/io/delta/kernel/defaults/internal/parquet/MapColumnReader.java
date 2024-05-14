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

import java.util.Optional;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.GroupType;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.MapType;

import io.delta.kernel.defaults.internal.data.vector.DefaultMapVector;
import static io.delta.kernel.defaults.internal.parquet.ParquetColumnReaders.createConverter;

/**
 * Map column readers for materializing the column values from Parquet files into Kernels
 * {@link ColumnVector}.
 */
class MapColumnReader extends RepeatedValueConverter {
    private final MapType typeFromClient;

    MapColumnReader(int initialBatchSize, MapType typeFromClient, GroupType typeFromFile) {
        super(
            initialBatchSize,
            createElementConverters(initialBatchSize, typeFromClient, typeFromFile));
        this.typeFromClient = typeFromClient;
    }

    @Override
    public ColumnVector getDataColumnVector(int batchSize) {
        ColumnVector[] elementVectors = getElementDataVectors();
        ColumnVector mapVector = new DefaultMapVector(
            batchSize,
            typeFromClient,
            Optional.of(getNullability()),
            getOffsets(),
            elementVectors[0],
            elementVectors[1]
        );
        resetWorkingState();
        return mapVector;
    }

    private static Converter[] createElementConverters(
            int initialBatchSize,
            MapType typeFromClient,
            GroupType typeFromFile) {
        // Repeated element can be any name. Late Parquet versions use "key_value" as the name,
        // but legacy versions can use any arbitary name for the repeated group.
        // See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps for details
        final GroupType innerMapType = (GroupType) typeFromFile.getType(0);
        Converter[] elemConverters = new Converter[2];
        elemConverters[0] = createConverter(
            initialBatchSize,
            typeFromClient.getKeyType(),
            innerMapType.getType("key"));
        elemConverters[1] = createConverter(
            initialBatchSize,
            typeFromClient.getValueType(),
            innerMapType.getType("value"));

        return elemConverters;
    }
}
