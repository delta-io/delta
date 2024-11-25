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
package io.delta.kernel.internal.checkpoints;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;
import java.util.*;

public class CheckpointMetaData {
  public static CheckpointMetaData fromRow(Row row) {
    return new CheckpointMetaData(
        row.getLong(0),
        row.getLong(1),
        row.isNullAt(2) ? Optional.empty() : Optional.of(row.getLong(2)));
  }

  public static StructType READ_SCHEMA =
      new StructType()
          .add("version", LongType.LONG, false /* nullable */)
          .add("size", LongType.LONG, false /* nullable */)
          .add("parts", LongType.LONG);

  public final long version;
  public final long size;
  public final Optional<Long> parts;

  public CheckpointMetaData(long version, long size, Optional<Long> parts) {
    this.version = version;
    this.size = size;
    this.parts = parts;
  }

  public Row toRow() {
    Map<Integer, Object> dataMap = new HashMap<>();
    dataMap.put(0, version);
    dataMap.put(1, size);
    parts.ifPresent(aLong -> dataMap.put(2, aLong));

    return new GenericRow(READ_SCHEMA, dataMap);
  }

  @Override
  public String toString() {
    return "CheckpointMetaData{"
        + "version="
        + version
        + ", size="
        + size
        + ", parts="
        + parts
        + '}';
  }
}
