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
package io.delta.kernel.utils;

import java.util.Collections;
import java.util.Map;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;

/**
 * Statistics about data file in a Delta Lake table.
 */
public class DataFileStatistics {
    private final long numRecords;
    private final Map<Column, Literal> minValues;
    private final Map<Column, Literal> maxValues;
    private final Map<Column, Long> nullCounts;

    /**
     * Create a new instance of {@link DataFileStatistics}.
     *
     * @param numRecords Number of records in the data file.
     * @param minValues  Map of column to minimum value of it in the data file. If the data file has
     *                   all nulls for the column, the value will be null or not present in the
     *                   map.
     * @param maxValues  Map of column to maximum value of it in the data file. If the data file has
     *                   all nulls for the column, the value will be null or not present in the
     *                   map.
     * @param nullCounts Map of column to number of nulls in the data file.
     */
    public DataFileStatistics(
            long numRecords,
            Map<Column, Literal> minValues,
            Map<Column, Literal> maxValues,
            Map<Column, Long> nullCounts) {
        this.numRecords = numRecords;
        this.minValues = Collections.unmodifiableMap(minValues);
        this.maxValues = Collections.unmodifiableMap(maxValues);
        this.nullCounts = Collections.unmodifiableMap(nullCounts);
    }

    /**
     * Get the number of records in the data file.
     *
     * @return Number of records in the data file.
     */
    public long getNumRecords() {
        return numRecords;
    }

    /**
     * Get the minimum values of the columns in the data file. The map may contain statistics for
     * only a subset of columns in the data file.
     *
     * @return Map of column to minimum value of it in the data file.
     */
    public Map<Column, Literal> getMinValues() {
        return minValues;
    }

    /**
     * Get the maximum values of the columns in the data file. The map may contain statistics for
     * only a subset of columns in the data file.
     *
     * @return Map of column to minimum value of it in the data file.
     */
    public Map<Column, Literal> getMaxValues() {
        return maxValues;
    }

    /**
     * Get the number of nulls of columns in the data file. The map may contain statistics for only
     * a subset of columns in the data file.
     *
     * @return Map of column to number of nulls in the data file.
     */
    public Map<Column, Long> getNullCounts() {
        return nullCounts;
    }

    public String serializeAsJson() {
        // TODO: implement this
        return "{}";
    }
}
