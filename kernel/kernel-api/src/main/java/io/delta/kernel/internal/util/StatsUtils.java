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
package io.delta.kernel.internal.util;

import com.fasterxml.jackson.databind.JsonNode;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.statistics.DataFileStatistics;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

/** Encapsulates various utility methods for statistics related operations. */
public class StatsUtils {

  private StatsUtils() {}

  /**
   * Utility method to deserialize statistics from a JSON string. NOTE: Currently, this method only
   * deserializes the numRecords field and ignores other statistics (minValues, maxValues,
   * nullCount). Full deserialization support for all statistics will be added in a future update.
   *
   * @param json Data statistics JSON string to deserialize.
   * @return An {@link Optional} containing the deserialized {@link DataFileStatistics} if present.
   * @throws KernelException if JSON parsing fails
   */
  public static Optional<DataFileStatistics> deserializeFromJson(String json) {
    JsonNode root;
    try {
      root = JsonUtils.mapper().readTree(json);
    } catch (IOException e) {
      throw new KernelException(String.format("Failed to parse JSON string: %s", json), e);
    }

    JsonNode numRecordsNode = root.get("numRecords");
    if (numRecordsNode == null || !numRecordsNode.isNumber()) {
      return Optional.empty();
    }

    long numRecords = numRecordsNode.asLong();
    return Optional.of(
        new DataFileStatistics(
            numRecords, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()));
  }
}
