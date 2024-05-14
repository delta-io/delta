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
import org.apache.parquet.schema.Type;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.ArrayType;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.defaults.internal.data.vector.DefaultArrayVector;
import static io.delta.kernel.defaults.internal.parquet.ParquetColumnReaders.createConverter;

/**
 * Array column reader for materializing the column values from Parquet files into Kernels
 * {@link ColumnVector}.
 */
class ArrayColumnReader extends RepeatedValueConverter {
    private final ArrayType typeFromClient;

    ArrayColumnReader(int initialBatchSize, ArrayType typeFromClient, GroupType typeFromFile) {
        super(
            initialBatchSize,
            createElementConverter(initialBatchSize, typeFromClient, typeFromFile));
        this.typeFromClient = typeFromClient;
    }

    @Override
    public ColumnVector getDataColumnVector(int batchSize) {
        ColumnVector arrayVector = new DefaultArrayVector(
            batchSize,
            typeFromClient,
            Optional.of(getNullability()),
            getOffsets(),
            getElementDataVectors()[0]);
        resetWorkingState();
        return arrayVector;
    }

    /**
     * Currently, support for 3-level nested arrays only.
     * <p>
     * optional group readerFeatures (LIST) {
     *   repeated group list {
     *     optional binary element (STRING);
     *   }
     * }
     * <p>
     * optional group readerFeatures (LIST) {
     *   repeated group bag {
     *     optional binary array (STRING);
     *   }
     * }
     *
     * TODO: Add support for 2-level nested arrays.
     */
    private static Converter createElementConverter(
        int initialBatchSize,
        ArrayType typeFromClient,
        GroupType typeFromFile) {

        checkArgument(
                typeFromFile.getFieldCount() == 1, "Expected exactly one field in the array type");
        GroupType repeatedGroup = typeFromFile.getType(0).asGroupType();

        // TODO: handle the legacy 2-level list physical format
        checkArgument(repeatedGroup.getFieldCount() == 1,
                "Expected exactly one field in the repeated group");

        Type elmentType = repeatedGroup.getType(0);

        return createConverter(initialBatchSize, typeFromClient.getElementType(), elmentType);
    }
}
