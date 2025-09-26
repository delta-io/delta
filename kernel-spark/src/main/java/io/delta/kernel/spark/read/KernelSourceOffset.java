/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.spark.read;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.connector.read.streaming.Offset;

/**
 * Kernel-native offset implementation for Delta streaming. This is a simplified version that
 * doesn't depend on the main Delta Lake classes.
 */
public class KernelSourceOffset extends Offset {

  public static final long BASE_INDEX = -1L;
  public static final long END_INDEX = Long.MAX_VALUE - 100;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String tableId;
  private final long version;
  private final long index;
  private final boolean isInitialSnapshot;

  @JsonCreator
  public KernelSourceOffset(
      @JsonProperty("tableId") String tableId,
      @JsonProperty("version") long version,
      @JsonProperty("index") long index,
      @JsonProperty("isInitialSnapshot") boolean isInitialSnapshot) {
    this.tableId = tableId;
    this.version = version;
    this.index = index;
    this.isInitialSnapshot = isInitialSnapshot;
  }

  public static KernelSourceOffset apply(
      String tableId, long version, long index, boolean isInitialSnapshot) {
    return new KernelSourceOffset(tableId, version, index, isInitialSnapshot);
  }

  public String getTableId() {
    return tableId;
  }

  public long getVersion() {
    return version;
  }

  public long getIndex() {
    return index;
  }

  public boolean isInitialSnapshot() {
    return isInitialSnapshot;
  }

  @Override
  public String json() {
    try {
      return MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize offset", e);
    }
  }

  public static KernelSourceOffset fromJson(String tableId, String json) {
    try {
      KernelSourceOffset offset = MAPPER.readValue(json, KernelSourceOffset.class);
      if (!offset.tableId.equals(tableId)) {
        throw new IllegalArgumentException("Table ID mismatch in offset");
      }
      return offset;
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize offset", e);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof KernelSourceOffset)) return false;

    KernelSourceOffset that = (KernelSourceOffset) obj;
    return version == that.version
        && index == that.index
        && isInitialSnapshot == that.isInitialSnapshot
        && tableId.equals(that.tableId);
  }

  @Override
  public int hashCode() {
    int result = tableId.hashCode();
    result = 31 * result + Long.hashCode(version);
    result = 31 * result + Long.hashCode(index);
    result = 31 * result + Boolean.hashCode(isInitialSnapshot);
    return result;
  }

  @Override
  public String toString() {
    return String.format(
        "KernelSourceOffset(tableId=%s, version=%d, index=%d, isInitialSnapshot=%s)",
        tableId, version, index, isInitialSnapshot);
  }
}
