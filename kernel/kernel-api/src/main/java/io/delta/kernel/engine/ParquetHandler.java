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

package io.delta.kernel.engine;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;
import java.util.Optional;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.data.*;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.*;

/**
 * Provides Parquet file related functionalities to Delta Kernel. Connectors can leverage this
 * interface to provide their own custom implementation of Parquet data file functionalities to
 * Delta Kernel.
 *
 * @since 3.0.0
 */
@Evolving
public interface ParquetHandler {
    /**
     * Read the Parquet format files at the given locations and return the data as a
     * {@link ColumnarBatch} with the columns requested by {@code physicalSchema}.
     * <p>
     * If {@code physicalSchema} has a {@link StructField} with column name
     * {@link StructField#METADATA_ROW_INDEX_COLUMN_NAME} and the field is a metadata column
     * {@link StructField#isMetadataColumn()} the column must be populated with the file row index.
     * <p>
     * How does a column in {@code physicalSchema} match to the column in the Parquet file? If the
     * {@link StructField} has a field id in the {@code metadata} with key `parquet.field.id` the
     * column is attempted to match by id. If the column is not found by id, the column is matched
     * by name. When trying to find the column in Parquet by name, first case-sensitive match is
     * used. If not found then a case-insensitive match is attempted.
     *
     * @param fileIter       Iterator of files to read data from.
     * @param physicalSchema Select list of columns to read from the Parquet file.
     * @param predicate      Optional predicate which the  Parquet reader can optionally use to
     *                       prune rows that don't satisfy the predicate. Because pruning is
     *                       optional and may be incomplete, caller is still responsible apply the
     *                       predicate on the data returned by this method.
     * @return an iterator of {@link ColumnarBatch}s containing the data in columnar format. It is
     * the responsibility of the caller to close the iterator. The data returned is in the same as
     * the order of files given in {@code scanFileIter}.
     * @throws IOException if an I/O error occurs during the read.
     */
    CloseableIterator<ColumnarBatch> readParquetFiles(
            CloseableIterator<FileStatus> fileIter,
            StructType physicalSchema,
            Optional<Predicate> predicate) throws IOException;

    /**
     * Write the given data batches to a Parquet files. Try to keep the Parquet file size to given
     * size. If the current file exceeds this size close the current file and start writing to a new
     * file.
     * <p>
     *
     * @param directoryPath Location where the data files should be written.
     * @param dataIter      Iterator of data batches to write. It is the responsibility of the calle
     *                      to close the iterator.
     * @param statsColumns  List of columns to collect statistics for. The statistics collection is
     *                      optional. If the implementation does not support statistics collection,
     *                      it is ok to return no statistics.
     * @return an iterator of {@link DataFileStatus} containing the status of the written files.
     * Each status contains the file path and the optionally collected statistics for the file
     * It is the responsibility of the caller to close the iterator.
     *
     * @throws IOException if an I/O error occurs during the file writing. This may leave some files
     *                     already written in the directory. It is the responsibility of the caller
     *                     to clean up.
     * @since 3.2.0
     */
    CloseableIterator<DataFileStatus> writeParquetFiles(
            String directoryPath,
            CloseableIterator<FilteredColumnarBatch> dataIter,
            List<Column> statsColumns) throws IOException;

    /**
     * Write the given data as a Parquet file. This call either succeeds in creating the file with
     * given contents or no file is created at all. It won't leave behind a partially written file.
     * <p>
     * @param filePath  Fully qualified destination file path
     * @param data      Iterator of {@link FilteredColumnarBatch}
     * @throws FileAlreadyExistsException if the file already exists and {@code overwrite} is false.
     * @throws IOException                if any other I/O error occurs.
     */
    void writeParquetFileAtomically(
            String filePath,
            CloseableIterator<FilteredColumnarBatch> data) throws IOException;
}
