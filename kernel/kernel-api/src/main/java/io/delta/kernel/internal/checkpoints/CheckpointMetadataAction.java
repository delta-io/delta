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

package io.delta.kernel.internal.checkpoints;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.HashMap;
import java.util.Map;

/** Action representing a checkpointMetadata action in a top-level V2 checkpoint file */
public class CheckpointMetadataAction {

  public static final StructType FULL_SCHEMA =
      new StructType()
          .add("version", LongType.LONG, false /* nullable */)
          .add("tags", new MapType(StringType.STRING, StringType.STRING, false /* nullable */));

  private final long version;
  private final Map<String, String> tags;

  public CheckpointMetadataAction(long version, Map<String, String> tags) {
    this.version = version;
    this.tags = tags;
  }

  public long getVersion() {
    return version;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public Row toRow() {
    Map<Integer, Object> contentMap = new HashMap<>();
    contentMap.put(0, version);
    contentMap.put(1, VectorUtils.stringStringMapValue(tags));
    return new GenericRow(FULL_SCHEMA, contentMap);
  }
}
