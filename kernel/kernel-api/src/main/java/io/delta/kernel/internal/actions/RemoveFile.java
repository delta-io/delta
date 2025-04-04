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
package io.delta.kernel.internal.actions;

import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.util.StatsUtils;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.*;
import java.util.Optional;

/** Metadata about {@code remove} action in the Delta Log. */
public class RemoveFile extends RowBackedAction {
  /** Full schema of the {@code remove} action in the Delta Log. */
  public static final StructType FULL_SCHEMA =
      new StructType()
          .add("path", StringType.STRING, false /* nullable */)
          .add("deletionTimestamp", LongType.LONG, true /* nullable */)
          .add("dataChange", BooleanType.BOOLEAN, false /* nullable*/)
          .add("extendedFileMetadata", BooleanType.BOOLEAN, true /* nullable */)
          .add(
              "partitionValues",
              new MapType(StringType.STRING, StringType.STRING, true),
              true /* nullable*/)
          .add("size", LongType.LONG, true /* nullable*/)
          .add("stats", StringType.STRING, true /* nullable */)
          .add("tags", new MapType(StringType.STRING, StringType.STRING, true), true /* nullable */)
          .add("deletionVector", DeletionVectorDescriptor.READ_SCHEMA, true /* nullable */)
          .add("baseRowId", LongType.LONG, true /* nullable */)
          .add("defaultRowCommitVersion", LongType.LONG, true /* nullable */);
  // TODO: Currently Kernel doesn't support RemoveFile actions when rowTracking is enabled (or have
  //   any public API for generating RemoveFile actions). Once we
  //   do this we need to ensure that the baseRowId and defaultRowCommitVersion fields are correctly
  //   populated to match the corresponding AddFile actions

  /** Constructs an {@link RemoveFile} action from the given 'RemoveFile' {@link Row}. */
  public RemoveFile(Row row) {
    super(row);
  }

  public String getPath() {
    return row.getString(getFieldIndex("path"));
  }

  public Optional<Long> getDeletionTimestamp() {
    return row.isNullAt(getFieldIndex("deletionTimestamp"))
        ? Optional.empty()
        : Optional.of(row.getLong(getFieldIndex("deletionTimestamp")));
  }

  public boolean getDataChange() {
    return row.getBoolean(getFieldIndex("dataChange"));
  }

  public Optional<Boolean> getExtendedFileMetadata() {
    return row.isNullAt(getFieldIndex("extendedFileMetadata"))
        ? Optional.empty()
        : Optional.of(row.getBoolean(getFieldIndex("extendedFileMetadata")));
  }

  public Optional<MapValue> getPartitionValues() {
    return row.isNullAt(getFieldIndex("partitionValues"))
        ? Optional.empty()
        : Optional.of(row.getMap(getFieldIndex("partitionValues")));
  }

  public Optional<Long> getSize() {
    return row.isNullAt(getFieldIndex("size"))
        ? Optional.empty()
        : Optional.of(row.getLong(getFieldIndex("size")));
  }

  public Optional<DataFileStatistics> getStats() {
    return getFieldIndexOpt("stats")
        .flatMap(
            index ->
                row.isNullAt(index)
                    ? Optional.empty()
                    : StatsUtils.deserializeFromJson(row.getString(index)));
  }

  public Optional<MapValue> getTags() {
    int index = getFieldIndex("tags");
    return Optional.ofNullable(row.isNullAt(index) ? null : row.getMap(index));
  }

  public Optional<DeletionVectorDescriptor> getDeletionVector() {
    int index = getFieldIndex("deletionVector");
    return Optional.ofNullable(
        row.isNullAt(index) ? null : DeletionVectorDescriptor.fromRow(row.getStruct(index)));
  }

  public Optional<Long> getBaseRowId() {
    int index = getFieldIndex("baseRowId");
    return Optional.ofNullable(row.isNullAt(index) ? null : row.getLong(index));
  }

  public Optional<Long> getDefaultRowCommitVersion() {
    int index = getFieldIndex("defaultRowCommitVersion");
    return Optional.ofNullable(row.isNullAt(index) ? null : row.getLong(index));
  }

  @Override
  public String toString() {
    // No specific ordering is guaranteed for partitionValues and tags in the returned string
    StringBuilder sb = new StringBuilder();
    sb.append("RemoveFile{");
    sb.append("path='").append(getPath()).append('\'');
    sb.append(", deletionTimestamp=").append(getDeletionTimestamp());
    sb.append(", dataChange=").append(getDataChange());
    sb.append(", extendedFileMetadata=").append(getExtendedFileMetadata());
    sb.append(", partitionValues=").append(getPartitionValues().map(VectorUtils::toJavaMap));
    sb.append(", size=").append(getSize());
    sb.append(", stats=").append(getStats().map(d -> d.serializeAsJson(null)).orElse(""));
    sb.append(", tags=").append(getTags().map(VectorUtils::toJavaMap));
    sb.append(", deletionVector=").append(getDeletionVector());
    sb.append(", baseRowId=").append(getBaseRowId());
    sb.append(", defaultRowCommitVersion=").append(getDefaultRowCommitVersion());
    sb.append('}');
    return sb.toString();
  }
}
