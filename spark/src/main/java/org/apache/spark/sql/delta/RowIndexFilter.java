/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta;

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

/**
 * Provides filtering information for each row index within given range.
 * Specific filters are implemented in subclasses.
 */
public interface RowIndexFilter {

    /**
     * Materialize filtering information for all rows in the range [start, end)
     * by filling a boolean column vector batch.
     *
     * @param start  Beginning index of the filtering range (inclusive)
     * @param end    End index of the filtering range (exclusive)
     * @param batch  The column vector for the current batch to materialize the range into
     */
    void materializeIntoVector(long start, long end, WritableColumnVector batch);

    /**
     * Value that must be materialised for a row to be kept after filtering.
     */
    public static final byte KEEP_ROW_VALUE = 0;
    /**
     * Value that must be materialised for a row to be dropped during filtering.
     */
    public static final byte DROP_ROW_VALUE = 1;
}
