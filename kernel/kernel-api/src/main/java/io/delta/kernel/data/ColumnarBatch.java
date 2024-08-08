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

import java.util.NoSuchElementException;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

import io.delta.kernel.internal.data.ColumnarBatchRow;

/**
 * Represents zero or more rows of records with same schema type.
 *
 * @since 3.0.0
 */
@Evolving
public interface ColumnarBatch {
    /**
     * @return the schema of the data in this batch.
     */
    StructType getSchema();

    /**
     * Return the {@link ColumnVector} for the given ordinal in the columnar batch. If the ordinal
     * is not valid throws error.
     *
     * @param ordinal the ordinal of the column to retrieve
     * @return the {@link ColumnVector} for the given ordinal in the columnar batch
     */
    ColumnVector getColumnVector(int ordinal);

    /**
     * @return the number of rows/records in the columnar batch
     */
    int getSize();

    /**
     * Return a copy of the {@link ColumnarBatch} with given new column vector inserted at the
     * given {@code columnVector} at given {@code ordinal}. Shift the existing
     * {@link ColumnVector}s located at from {@code ordinal} to the end by one position.
     * The schema of the new {@link ColumnarBatch} will also be changed to reflect the newly
     * inserted vector.
     *
     * @param ordinal
     * @param columnSchema Column name and schema details of the new column vector.
     * @param columnVector
     * @return {@link ColumnarBatch} with new vector inserted.
     * @throws IllegalArgumentException If the ordinal is not valid (ie less than zero or
     *                                  greater than the current number of vectors).
     */
    default ColumnarBatch withNewColumn(int ordinal, StructField columnSchema,
                                        ColumnVector columnVector) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Return a copy of this {@link ColumnarBatch} with the column at given {@code ordinal}
     * removed. All columns after the {@code ordinal} will be shifted to left by one position.
     *
     * @param ordinal Column ordinal to delete.
     * @return {@link ColumnarBatch} with a column vector deleted.
     */
    default ColumnarBatch withDeletedColumnAt(int ordinal) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Generate a copy of this {@link ColumnarBatch} with the given {@code newSchema}. The data
     * types of elements in the given new schema and existing schema should be the same. Rest of
     * the details such as name of the column or column metadata could be different.
     *
     * @param newSchema
     * @return {@link ColumnarBatch} with given new schema.
     */
    default ColumnarBatch withNewSchema(StructType newSchema) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Return a slice of the current batch.
     *
     * @param start Starting record index to include in the returned columnar batch
     * @param end   Ending record index (exclusive) to include in the returned columnar batch
     * @return a columnar batch containing the records between [start, end)
     */
    default ColumnarBatch slice(int start, int end) {
        throw new UnsupportedOperationException("Not yet implemented!");
    }

    /**
     * Return a copy of this {@link ColumnarBatch} with the column at given {@code ordinal}
     * replaced with {@code newVector} and the schema field at given {@code ordinal} replaced
     * with {@code newColumnSchema}.
     *
     * @param ordinal Ordinal of the column vector to replace.
     * @param newColumnSchema The schema field of the new column.
     * @param newVector New column vector that will replace the column vector at the given
     *                  {@code ordinal}.
     * @return {@link ColumnarBatch} with a new column vector at the given ordinal.
     */
    default ColumnarBatch withReplacedColumnVector(int ordinal, StructField newColumnSchema,
                                                   ColumnVector newVector) {
        throw new UnsupportedOperationException("Not yet implemented!");
    }

    /**
     * @return iterator of {@link Row}s in this batch
     */
    default CloseableIterator<Row> getRows() {
        final ColumnarBatch batch = this;
        return new CloseableIterator<Row>() {
            int rowId = 0;
            int maxRowId = getSize();

            @Override
            public boolean hasNext() {
                return rowId < maxRowId;
            }

            @Override
            public Row next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Row row = new ColumnarBatchRow(batch, rowId);
                rowId += 1;
                return row;
            }

            @Override
            public void close() {}
        };
    }
}
