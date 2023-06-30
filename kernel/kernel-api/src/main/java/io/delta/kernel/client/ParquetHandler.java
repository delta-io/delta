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

package io.delta.kernel.client;

import java.io.IOException;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FileDataReadResult;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Provides Parquet file related functionalities to Delta Kernel. Connectors can leverage this
 * interface to provide their own custom implementation of Parquet data file functionalities to
 * Delta Kernel.
 */
public interface ParquetHandler
    extends FileHandler
{
    /**
     * Read the Parquet format files at the given locations and return the data as a
     * {@link ColumnarBatch} with the columns requested by {@code physicalSchema}.
     *
     * If {@code physicalSchema} has a {@link StructField} with column name
     * {@link StructField#ROW_INDEX_COLUMN_NAME} and the field is a metadata column
     * {@link StructField#isMetadataColumn()} the column must be populated with the file row index.
     *
     * @param fileIter Iterator of {@link FileReadContext} objects to read data from.
     * @param physicalSchema Select list of columns to read from the Parquet file.
     * @return an iterator of {@link FileDataReadResult}s containing the data in columnar format
     *         and the corresponding scan file information. It is the responsibility of the caller
     *         to close the iterator. The data returned is in the same as the order of files given
     *         in <i>fileIter</i>.
     * @throws IOException if an error occurs during the read.
     */
    CloseableIterator<FileDataReadResult> readParquetFiles(
            CloseableIterator<FileReadContext> fileIter,
            StructType physicalSchema) throws IOException;
}
