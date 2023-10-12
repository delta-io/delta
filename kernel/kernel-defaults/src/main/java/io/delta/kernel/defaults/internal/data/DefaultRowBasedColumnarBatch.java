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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.*;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import static io.delta.kernel.defaults.internal.Preconditions.checkArgument;

/**
 * {@link ColumnarBatch} wrapper around list of {@link Row} objects.
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
            final ColumnVector vector = new SubFieldColumnVector(
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

    /**
     * {@link ColumnVector} wrapper on top of {@link Row} objects. This wrapper allows referncing
     * any nested level column vector from a set of rows.
     * TODO: We should change the {@link io.delta.kernel.defaults.client.DefaultJsonHandler} to
     * generate data in true columnar format than wrapping a set of rows with a columnar batch
     * interface.
     */
    private static class SubFieldColumnVector implements ColumnVector {
        private final int size;
        private final DataType dataType;
        private final int columnOrdinal;
        private final Function<Integer, Row> rowIdToRowAccessor;

        /**
         * Create an instance of {@link SubFieldColumnVector}
         *
         * @param size               Number of elements in the vector
         * @param dataType           Datatype of the vector
         * @param columnOrdinal      Ordinal of the column represented by this vector in the rows
         *                           returned by {@link #rowIdToRowAccessor}
         * @param rowIdToRowAccessor {@link Function} that returns a {@link Row} object for given
         *                           rowId
         */
        SubFieldColumnVector(
            int size,
            DataType dataType,
            int columnOrdinal,
            Function<Integer, Row> rowIdToRowAccessor) {
            checkArgument(size >= 0, "invalid size: %s", size);
            this.size = size;
            checkArgument(columnOrdinal >= 0, "invalid column ordinal: %s", columnOrdinal);
            this.columnOrdinal = columnOrdinal;
            this.rowIdToRowAccessor =
                requireNonNull(rowIdToRowAccessor, "rowIdToRowAccessor is null");
            this.dataType = requireNonNull(dataType, "dataType is null");
        }

        @Override
        public DataType getDataType() {
            return dataType;
        }

        @Override
        public int getSize() {
            return size;
        }

        @Override
        public void close() { /* nothing to close */ }

        @Override
        public boolean isNullAt(int rowId) {
            assertValidRowId(rowId);
            Row row = rowIdToRowAccessor.apply(rowId);
            return row == null || row.isNullAt(columnOrdinal);
        }

        @Override
        public boolean getBoolean(int rowId) {
            assertValidRowId(rowId);
            return rowIdToRowAccessor.apply(rowId).getBoolean(columnOrdinal);
        }

        @Override
        public byte getByte(int rowId) {
            assertValidRowId(rowId);
            return rowIdToRowAccessor.apply(rowId).getByte(columnOrdinal);
        }

        @Override
        public short getShort(int rowId) {
            assertValidRowId(rowId);
            return rowIdToRowAccessor.apply(rowId).getShort(columnOrdinal);
        }

        @Override
        public int getInt(int rowId) {
            assertValidRowId(rowId);
            return rowIdToRowAccessor.apply(rowId).getInt(columnOrdinal);
        }

        @Override
        public long getLong(int rowId) {
            assertValidRowId(rowId);
            return rowIdToRowAccessor.apply(rowId).getLong(columnOrdinal);
        }

        @Override
        public float getFloat(int rowId) {
            assertValidRowId(rowId);
            return rowIdToRowAccessor.apply(rowId).getFloat(columnOrdinal);
        }

        @Override
        public double getDouble(int rowId) {
            assertValidRowId(rowId);
            return rowIdToRowAccessor.apply(rowId).getDouble(columnOrdinal);
        }

        @Override
        public byte[] getBinary(int rowId) {
            assertValidRowId(rowId);
            return rowIdToRowAccessor.apply(rowId).getBinary(columnOrdinal);
        }

        @Override
        public String getString(int rowId) {
            assertValidRowId(rowId);
            return rowIdToRowAccessor.apply(rowId).getString(columnOrdinal);
        }

        @Override
        public BigDecimal getDecimal(int rowId) {
            assertValidRowId(rowId);
            return rowIdToRowAccessor.apply(rowId).getDecimal(columnOrdinal);
        }

        @Override
        public MapValue getMap(int rowId) {
            assertValidRowId(rowId);
            return rowIdToRowAccessor.apply(rowId).getMap(columnOrdinal);
        }

        @Override
        public Row getStruct(int rowId) {
            assertValidRowId(rowId);
            return rowIdToRowAccessor.apply(rowId).getStruct(columnOrdinal);
        }

        @Override
        public ArrayValue getArray(int rowId) {
            assertValidRowId(rowId);
            return rowIdToRowAccessor.apply(rowId).getArray(columnOrdinal);
        }

        @Override
        public ColumnVector getChild(int childOrdinal) {
            StructType structType = (StructType) dataType;
            StructField childField = structType.at(childOrdinal);
            return new SubFieldColumnVector(
                size,
                childField.getDataType(),
                childOrdinal,
                (rowId) -> (rowIdToRowAccessor.apply(rowId).getStruct(columnOrdinal)));
        }

        private void assertValidRowId(int rowId) {
            checkArgument(rowId < size,
                "Invalid rowId: " + rowId + ", max allowed rowId is: " + (size - 1));
        }
    }
}
