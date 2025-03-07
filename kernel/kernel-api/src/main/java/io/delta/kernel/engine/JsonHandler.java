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

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.data.*;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Optional;

/**
 * Provides JSON handling functionality to Delta Kernel. Delta Kernel can use this client to parse
 * JSON strings into {@link ColumnarBatch} or read content from JSON files. Connectors can leverage
 * this interface to provide their best implementation of the JSON parsing capability to Delta
 * Kernel.
 *
 * @since 3.0.0
 */
@Evolving
public interface JsonHandler {
  /**
   * Parse the given <i>json</i> strings and return the fields requested by {@code outputSchema} as
   * columns in a {@link ColumnarBatch}.
   *
   * <p>There are a couple special cases that should be handled for specific data types:
   *
   * <ul>
   *   <li><b>FloatType and DoubleType:</b> handle non-numeric numbers encoded as strings
   *       <ul>
   *         <li>NaN: <code>"NaN"</code>
   *         <li>Positive infinity: <code>"+INF", "Infinity", "+Infinity"</code>
   *         <li>Negative infinity: <code>"-INF", "-Infinity""</code>
   *       </ul>
   *   <li><b>DateType:</b> handle dates encoded as strings in the format <code>"yyyy-MM-dd"</code>
   *   <li><b>TimestampType:</b> handle timestamps encoded as strings in the format <code>
   *       "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"</code>
   * </ul>
   *
   * @param jsonStringVector String {@link ColumnVector} of valid JSON strings.
   * @param outputSchema Schema of the data to return from the parsed JSON. If any requested fields
   *     are missing in the JSON string, a <i>null</i> is returned for that particular field in the
   *     returned {@link Row}. The type for each given field is expected to match the type in the
   *     JSON.
   * @param selectionVector Optional selection vector indicating which rows to parse the JSON. If
   *     present, only the selected rows should be parsed. Unselected rows should be all null in the
   *     returned batch.
   * @return a {@link ColumnarBatch} of schema {@code outputSchema} with one row for each entry in
   *     {@code jsonStringVector}
   */
  ColumnarBatch parseJson(
      ColumnVector jsonStringVector,
      StructType outputSchema,
      Optional<ColumnVector> selectionVector);

  /**
   * Read and parse the JSON format file at given locations and return the data as a {@link
   * ColumnarBatch} with the columns requested by {@code physicalSchema}.
   *
   * @param fileIter Iterator of files to read data from.
   * @param physicalSchema Select list of columns to read from the JSON file.
   * @param predicate Optional predicate which the JSON reader can optionally use to prune rows that
   *     don't satisfy the predicate. Because pruning is optional and may be incomplete, caller is
   *     still responsible apply the predicate on the data returned by this method.
   * @return an iterator of {@link ColumnarBatch}s containing the data in columnar format. It is the
   *     responsibility of the caller to close the iterator. The data returned is in the same as the
   *     order of files given in {@code scanFileIter}
   * @throws IOException if an I/O error occurs during the read.
   */
  CloseableIterator<ColumnarBatch> readJsonFiles(
      CloseableIterator<FileStatus> fileIter,
      StructType physicalSchema,
      Optional<Predicate> predicate)
      throws IOException;

  /**
   * Serialize each {@code Row} in the iterator as JSON and write as a separate line in destination
   * file. This call either succeeds in creating the file with given contents or no file is created
   * at all. It won't leave behind a partially written file.
   *
   * <p>Following are the supported data types and their serialization rules. At a high-level, the
   * JSON serialization is similar to that of {@code jackson} JSON serializer.
   *
   * <ul>
   *   <li>Primitive types: @code boolean, byte, short, int, long, float, double, string}
   *   <li>{@code struct}: any element whose value is null is not written to file
   *   <li>{@code map}: only a {@code map} with {@code string} key type is supported. If an entry
   *       value is {@code null}, it should be written to the file.
   *   <li>{@code array}: {@code null} value elements are written to file
   * </ul>
   *
   * @param filePath Fully qualified destination file path
   * @param data Iterator of {@link Row} objects where each row should be serialized as JSON and
   *     written as separate line in the destination file.
   * @param overwrite If {@code true}, the file is overwritten if it already exists. If {@code
   *     false} and a file exists {@link FileAlreadyExistsException} is thrown.
   * @throws FileAlreadyExistsException if the file already exists and {@code overwrite} is false.
   * @throws IOException if any other I/O error occurs.
   */
  void writeJsonFileAtomically(String filePath, CloseableIterator<Row> data, boolean overwrite)
      throws IOException;
}
