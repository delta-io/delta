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

import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.spark.internal.v2.utils.PartitionUtils;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Narrow descriptor for a Delta file selected by a DSv2 scan. */
public final class DeltaScanFile {

  private final String path;
  private final Map<String, String> partitionValuesMap;
  private final long size;
  private final long modificationTime;
  private final Optional<Long> baseRowId;
  private final Optional<Long> defaultRowCommitVersion;
  private final Optional<DeletionVectorDescriptor> deletionVector;

  DeltaScanFile(AddFile addFile) {
    this(
        Objects.requireNonNull(addFile, "addFile is null").getPath(),
        PartitionUtils.buildPartitionValuesMap(addFile.getPartitionValues()),
        addFile.getSize(),
        addFile.getModificationTime(),
        addFile.getBaseRowId(),
        addFile.getDefaultRowCommitVersion(),
        addFile.getDeletionVector());
  }

  private DeltaScanFile(
      String path,
      Map<String, String> partitionValuesMap,
      long size,
      long modificationTime,
      Optional<Long> baseRowId,
      Optional<Long> defaultRowCommitVersion,
      Optional<DeletionVectorDescriptor> deletionVector) {
    this.path = path;
    this.partitionValuesMap = partitionValuesMap;
    this.size = size;
    this.modificationTime = modificationTime;
    this.baseRowId = baseRowId;
    this.defaultRowCommitVersion = defaultRowCommitVersion;
    this.deletionVector = deletionVector;
  }

  /**
   * Builds a descriptor from a V1 AddFile selected by V1 data skipping rather than Kernel's {@code
   * getScanFiles}. The V1 fields are copied into the same durable representation used for
   * Kernel-selected files.
   */
  static DeltaScanFile fromV1AddFile(org.apache.spark.sql.delta.actions.AddFile v1AddFile) {
    Objects.requireNonNull(v1AddFile, "v1AddFile is null");
    Map<String, String> partitionValues =
        PartitionUtils.buildPartitionValuesMap(
            scala.jdk.javaapi.CollectionConverters.asJava(v1AddFile.partitionValues()));
    Optional<DeletionVectorDescriptor> dv =
        Optional.ofNullable(v1AddFile.deletionVector())
            .map(
                v1Dv ->
                    new DeletionVectorDescriptor(
                        v1Dv.storageType(),
                        v1Dv.pathOrInlineDv(),
                        toJavaInt(v1Dv.offset()),
                        v1Dv.sizeInBytes(),
                        v1Dv.cardinality()));
    return new DeltaScanFile(
        v1AddFile.path(),
        partitionValues,
        v1AddFile.size(),
        v1AddFile.modificationTime(),
        toJavaLong(v1AddFile.baseRowId()),
        toJavaLong(v1AddFile.defaultRowCommitVersion()),
        dv);
  }

  private static Optional<Long> toJavaLong(scala.Option<Object> opt) {
    return opt.isDefined() ? Optional.of(((Number) opt.get()).longValue()) : Optional.empty();
  }

  private static Optional<Integer> toJavaInt(scala.Option<Object> opt) {
    return opt.isDefined() ? Optional.of(((Number) opt.get()).intValue()) : Optional.empty();
  }

  public String getPath() {
    return path;
  }

  /** Returns partition values copied out of the selected file action. */
  public Map<String, String> getPartitionValuesMap() {
    return partitionValuesMap;
  }

  public long getSize() {
    return size;
  }

  public long getModificationTime() {
    return modificationTime;
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
