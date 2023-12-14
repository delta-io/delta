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
import java.util.Optional;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Provides JSON handling functionality to Delta Kernel. Delta Kernel can use this client to
 * parse JSON strings into {@link io.delta.kernel.data.Row} or read content from JSON files.
 * Connectors can leverage this interface to provide their best implementation of the JSON parsing
 * capability to Delta Kernel.
 *
 * @since 3.0.0
 */
@Evolving
public interface JsonHandler {
    /**
     * Parse the given <i>json</i> strings and return the fields requested by {@code outputSchema}
     * as columns in a {@link ColumnarBatch}.
     *
     * @param jsonStringVector String {@link ColumnVector} of valid JSON strings.
     * @param outputSchema     Schema of the data to return from the parsed JSON. If any requested
     *                         fields are missing in the JSON string, a <i>null</i> is returned
     *                         for that
     *                         particular field in the returned {@link Row}. The type for each given
     *                         field is expected to match the type in the JSON.
     * @return a {@link ColumnarBatch} of schema {@code outputSchema} with one row for each entry
     * in {@code jsonStringVector}
     */
    ColumnarBatch parseJson(ColumnVector jsonStringVector, StructType outputSchema);

    /**
     * Deserialize the Delta schema from {@code structTypeJson} according to the Delta Protocol
     * <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types">
     *    schema serialization rules </a>.
     *
     * @param structTypeJson the JSON formatted schema string to parse
     * @return the parsed {@link StructType}
     */
    StructType deserializeStructType(String structTypeJson);

    /**
     * Read and parse the JSON format file at given locations and return the data as a
     * {@link ColumnarBatch} with the columns requested by {@code physicalSchema}.
     *
     * @param scanFileIter   Iterator of scan file {@link Row} objects to read data from.
     * @param physicalSchema Select list of columns to read from the JSON file.
     * @param predicate      Optional predicate which the JSON reader can use to prune the rows
     *                       that don't satisfy the predicate. Result could still contain rows that
     *                       don't satisfy the predicate. Caller should still need to apply the
     *                       predicate on the data returned by this method to completely prune rows
     *                       that don't satisfy the predicate.
     * @return an iterator of {@link ColumnarBatch}s containing the data in columnar format.
     * It is the responsibility of the caller to close the iterator. The data returned is in
     * the same as the order of files given in {@code scanFileIter}
     * @throws IOException if an I/O error occurs during the read.
     */
    CloseableIterator<ColumnarBatch> readJsonFiles(
        CloseableIterator<Row> scanFileIter,
        StructType physicalSchema,
        Optional<Predicate> predicate) throws IOException;
}
