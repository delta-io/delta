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
package io.delta.spark.internal.v2.read;

import io.delta.kernel.data.MapValue;
import java.util.Objects;

/**
 * Wrapper class holding AddCDCFile data extracted from kernel.
 *
 * <p>This class mirrors the structure of kernel's AddCDCFile action, containing the path, partition
 * values, and size of a CDC file from the _change_data directory.
 */
public class CDCFileInfo {
  private final String path;
  private final MapValue partitionValues;
  private final long size;

  public CDCFileInfo(String path, MapValue partitionValues, long size) {
    this.path = Objects.requireNonNull(path, "path is null");
    this.partitionValues = Objects.requireNonNull(partitionValues, "partitionValues is null");
    this.size = size;
  }

  public String getPath() {
    return path;
  }

  public MapValue getPartitionValues() {
    return partitionValues;
  }

  public long getSize() {
    return size;
  }

  @Override
  public String toString() {
    return "CDCFileInfo{" + "path='" + path + '\'' + ", size=" + size + '}';
  }
}
