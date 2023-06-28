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
package io.delta.kernel.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

public class DefaultColumnarBatch
    implements ColumnarBatch
{
    private final int size;
    private final StructType schema;
    private final List<ColumnVector> columnVectors;

    public DefaultColumnarBatch(
        int size,
        StructType schema,
        ColumnVector[] columnVectors
    )
    {
        this.schema = schema;
        this.size = size;
        this.columnVectors = Collections.unmodifiableList(Arrays.asList(columnVectors));
    }

    @Override
    public StructType getSchema()
    {
        return schema;
    }

    @Override
    public ColumnVector getColumnVector(int ordinal)
    {
        checkColumnOrdinal(ordinal);
        return columnVectors.get(ordinal);
    }

    @Override
    public ColumnarBatch withNewColumn(int ordinal, StructField structField,
        ColumnVector columnVector)
    {
        if (ordinal < 0 || ordinal > columnVectors.size()) {
            throw new IllegalArgumentException("Invalid ordinal: " + ordinal);
        }

        if (columnVector == null || columnVector.getSize() != size) {
            throw new IllegalArgumentException(
                "given vector size is not matching the current batch size");
        }

        // Update the schema
        ArrayList<StructField> newStructFields = new ArrayList<>(schema.fields());
        newStructFields.ensureCapacity(schema.length() + 1);
        newStructFields.add(ordinal, structField);
        StructType newSchema = new StructType(newStructFields);

        // Update the vectors
        ArrayList<ColumnVector> newColumnVectors = new ArrayList<>(columnVectors);
        newColumnVectors.ensureCapacity(columnVectors.size() + 1);
        newColumnVectors.add(ordinal, columnVector);

        return new DefaultColumnarBatch(
            size, newSchema, newColumnVectors.toArray(new ColumnVector[0]));
    }

    @Override
    public ColumnarBatch withDeletedColumnAt(int ordinal)
    {
        if (ordinal < 0 || ordinal > columnVectors.size()) {
            throw new IllegalArgumentException("Invalid ordinal: " + ordinal);
        }

        // Update the schema
        ArrayList<StructField> newStructFields = new ArrayList<>(schema.fields());
        newStructFields.remove(ordinal);
        StructType newSchema = new StructType(newStructFields);

        // Update the vectors
        ArrayList<ColumnVector> newColumnVectors = new ArrayList<>(columnVectors);
        newColumnVectors.remove(ordinal);

        return new DefaultColumnarBatch(
            size, newSchema, newColumnVectors.toArray(new ColumnVector[0]));
    }

    @Override
    public ColumnarBatch withNewSchema(StructType newSchema)
    {
        if (!schema.equivalent(newSchema)) {
            throw new IllegalArgumentException
                ("Given new schema data type is not same as the existing schema");
        }

        return new DefaultColumnarBatch(
            size, newSchema, columnVectors.toArray(new ColumnVector[0]));
    }

    @Override
    public int getSize()
    {
        return size;
    }

    private void checkColumnOrdinal(int ordinal)
    {
        if (ordinal < 0 || ordinal >= columnVectors.size()) {
            throw new IllegalArgumentException("invalid column ordinal");
        }
    }
}
