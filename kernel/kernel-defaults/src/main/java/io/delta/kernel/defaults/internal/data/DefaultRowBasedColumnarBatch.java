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
package io.delta.kernel.defaults.internal.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import io.delta.kernel.defaults.internal.data.vector.DefaultSubFieldVector;

/**
 * {@link ColumnarBatch} wrapper around list of {@link Row} objects.
 *  TODO: We should change the {@link io.delta.kernel.defaults.client.DefaultJsonHandler} to
 *  generate data in true columnar format than wrapping a set of rows with a columnar batch
 *  interface.
 */
public class DefaultRowBasedColumnarBatch implements ColumnarBatch {
    private final StructType schema;
    private final List<Row> rows;

    /**
     * Holds the actual ColumnVectors, once the rows have been parsed for that column.
     * <p>
     * Uses lazy initialization, i.e. a value of Optional.empty() at an ordinal means we have not
     * parsed the rows for that column yet.
     */
    private final List<Optional<ColumnVector>> columnVectors;

    public DefaultRowBasedColumnarBatch(StructType schema, List<Row> rows) {
        this.schema = schema;
        this.rows = rows;
        this.columnVectors = new ArrayList<>(schema.length());
        for (int i = 0; i < schema.length(); i++) {
            columnVectors.add(Optional.empty());
        }
    }

    @Override
    public StructType getSchema() {
        return schema;
    }

    @Override
    public int getSize() {
        return rows.size();
    }

    @Override
    public ColumnVector getColumnVector(int ordinal) {
        if (ordinal < 0 || ordinal >= columnVectors.size()) {
            throw new IllegalArgumentException("Invalid ordinal: " + ordinal);
        }

        if (!columnVectors.get(ordinal).isPresent()) {
            final StructField field = schema.at(ordinal);
            final ColumnVector vector = new DefaultSubFieldVector(
                getSize(),
                field.getDataType(),
                ordinal,
                (rowId) -> rows.get(rowId));
            columnVectors.set(ordinal, Optional.of(vector));
        }

        return columnVectors.get(ordinal).get();
    }

    /**
     * TODO this implementation sucks
     */
    @Override
    public ColumnarBatch withDeletedColumnAt(int ordinal) {
        if (ordinal < 0 || ordinal >= columnVectors.size()) {
            throw new IllegalArgumentException("Invalid ordinal: " + ordinal);
        }

        // Update the schema
        final List<StructField> newStructFields = new ArrayList<>(schema.fields());
        newStructFields.remove(ordinal);
        final StructType newSchema = new StructType(newStructFields);

        // Fill all the vectors, except the one being deleted
        for (int i = 0; i < columnVectors.size(); i++) {
            if (i == ordinal) {
                continue;
            }
            getColumnVector(i);
        }

        // Delete the vector at the target ordinal
        final List<Optional<ColumnVector>> newColumnVectors = new ArrayList<>(columnVectors);
        newColumnVectors.remove(ordinal);

        // Fill the new array
        ColumnVector[] newColumnVectorArr = new ColumnVector[newColumnVectors.size()];
        for (int i = 0; i < newColumnVectorArr.length; i++) {
            newColumnVectorArr[i] = newColumnVectors.get(i).get();
        }

        return new DefaultColumnarBatch(
            getSize(), // # of rows hasn't changed
            newSchema,
            newColumnVectorArr);
    }
}
