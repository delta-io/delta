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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.util.JsonUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/** Statistics about data file in a Delta Lake table. */
public class DataFileStatistics {
  private final long numRecords;
  private final Map<Column, Literal> minValues;
  private final Map<Column, Literal> maxValues;
  private final Map<Column, Long> nullCounts;

  /**
   * Create a new instance of {@link DataFileStatistics}.
   *
   * @param numRecords Number of records in the data file.
   * @param minValues Map of column to minimum value of it in the data file. If the data file has
   *     all nulls for the column, the value will be null or not present in the map.
   * @param maxValues Map of column to maximum value of it in the data file. If the data file has
   *     all nulls for the column, the value will be null or not present in the map.
   * @param nullCounts Map of column to number of nulls in the data file.
   */
  public DataFileStatistics(
      long numRecords,
      Map<Column, Literal> minValues,
      Map<Column, Literal> maxValues,
      Map<Column, Long> nullCounts) {
    checkArgument(numRecords >= 0, "numRecords should be non-negative");
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
   * Get the minimum values of the columns in the data file. The map may contain statistics for only
   * a subset of columns in the data file.
   *
   * @return Map of column to minimum value of it in the data file.
   */
  public Map<Column, Literal> getMinValues() {
    return minValues;
  }

  /**
   * Get the maximum values of the columns in the data file. The map may contain statistics for only
   * a subset of columns in the data file.
   *
   * @return Map of column to minimum value of it in the data file.
   */
  public Map<Column, Literal> getMaxValues() {
    return maxValues;
  }

  /**
   * Get the number of nulls of columns in the data file. The map may contain statistics for only a
   * subset of columns in the data file.
   *
   * @return Map of column to number of nulls in the data file.
   */
  public Map<Column, Long> getNullCounts() {
    return nullCounts;
  }

  @Override
  public String toString() {
    return serializeAsJson();
  }

  public String serializeAsJson() {
    // TODO: implement this. Full statistics serialization will be added as part of
    // https://github.com/delta-io/delta/pull/3342. The PR is pending on a decision.
    // For now just serialize the number of records.
    return "{\"numRecords\":" + numRecords + "}";
  }

  /**
   * Deserialize the statistics from a JSON string. For now only the number of records is
   * deserialized, the rest of the statistics are not supported yet.
   *
   * @param json Data statistics JSON string to deserialize.
   * @return An {@link Optional} containing the deserialized {@link DataFileStatistics} if present.
   */
  public static Optional<DataFileStatistics> deserializeFromJson(String json) {
    Map<String, String> keyValueMap = JsonUtils.parseJSONKeyValueMap(json);

    // For now just deserialize the number of records
    String numRecordsStr = keyValueMap.get("numRecords");
    if (numRecordsStr == null) {
      return Optional.empty();
    }
    long numRecords = Long.parseLong(numRecordsStr);
    DataFileStatistics stats =
        new DataFileStatistics(
            numRecords, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    return Optional.of(stats);
  }
}
