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
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import java.util.Objects;
import java.util.Optional;

/** Narrow descriptor for a Delta file selected by a DSv2 scan. */
public final class DeltaScanFile {

  private final String path;
  private final MapValue partitionValues;
  private final long size;
  private final Optional<Long> baseRowId;
  private final Optional<Long> defaultRowCommitVersion;
  private final Optional<DeletionVectorDescriptor> deletionVector;

  DeltaScanFile(AddFile addFile) {
    this.path = Objects.requireNonNull(addFile, "addFile is null").getPath();
    this.partitionValues = addFile.getPartitionValues();
    this.size = addFile.getSize();
    this.baseRowId = addFile.getBaseRowId();
    this.defaultRowCommitVersion = addFile.getDefaultRowCommitVersion();
    this.deletionVector = addFile.getDeletionVector();
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

  public Optional<Long> getBaseRowId() {
    return baseRowId;
  }

  public Optional<Long> getDefaultRowCommitVersion() {
    return defaultRowCommitVersion;
  }

  public Optional<DeletionVectorDescriptor> getDeletionVector() {
    return deletionVector;
  }
}
