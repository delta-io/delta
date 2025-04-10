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

import static io.delta.kernel.internal.util.InternalUtils.relativizePath;
import static io.delta.kernel.internal.util.PartitionUtils.serializePartitionMap;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.VectorUtils.toJavaMap;

import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.StatsUtils;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.DataFileStatus;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Delta log action representing an `AddFile` */
public class AddFile extends RowBackedAction {

  /* We conditionally read this field based on the query filter */
  private static final StructField JSON_STATS_FIELD =
      new StructField("stats", StringType.STRING, true /* nullable */);

  /**
   * Schema of the {@code add} action in the Delta Log without stats. Used for constructing table
   * snapshot to read data from the table.
   */
  public static final StructType SCHEMA_WITHOUT_STATS =
      new StructType()
          .add("path", StringType.STRING, false /* nullable */)
          .add(
              "partitionValues",
              new MapType(StringType.STRING, StringType.STRING, true),
              false /* nullable*/)
          .add("size", LongType.LONG, false /* nullable*/)
          .add("modificationTime", LongType.LONG, false /* nullable*/)
          .add("dataChange", BooleanType.BOOLEAN, false /* nullable*/)
          .add("deletionVector", DeletionVectorDescriptor.READ_SCHEMA, true /* nullable */)
          .add(
              "tags",
              new MapType(StringType.STRING, StringType.STRING, true /* valueContainsNull */),
              true /* nullable */)
          .add("baseRowId", LongType.LONG, true /* nullable */)
          .add("defaultRowCommitVersion", LongType.LONG, true /* nullable */);

  public static final StructType SCHEMA_WITH_STATS = SCHEMA_WITHOUT_STATS.add(JSON_STATS_FIELD);

  /** Full schema of the {@code add} action in the Delta Log. */
  public static final StructType FULL_SCHEMA = SCHEMA_WITH_STATS;

  // There are more fields which are added when row-id tracking and clustering is enabled.
  // When Kernel starts supporting row-ids and clustering, we should add those fields here.

  /**
   * Utility to generate {@link AddFile} action instance from the given {@link DataFileStatus} and
   * partition values.
   */
  public static AddFile convertDataFileStatus(
      StructType physicalSchema,
      URI tableRoot,
      DataFileStatus dataFileStatus,
      Map<String, Literal> partitionValues,
      boolean dataChange) {
    Row row =
        createAddFileRow(
            physicalSchema,
            relativizePath(new Path(dataFileStatus.getPath()), tableRoot).toUri().toString(),
            serializePartitionMap(partitionValues),
            dataFileStatus.getSize(),
            dataFileStatus.getModificationTime(),
            dataChange,
            dataFileStatus.getDeletionVectorDescriptor(), // deletionVector
            Optional.empty(), // tags
            Optional.empty(), // baseRowId
            Optional.empty(), // defaultRowCommitVersion
            dataFileStatus.getStatistics());

    return new AddFile(row);
  }

  /** Utility to generate an 'AddFile' row from the given fields. */
  public static Row createAddFileRow(
      StructType physicalSchema,
      String path,
      MapValue partitionValues,
      long size,
      long modificationTime,
      boolean dataChange,
      Optional<DeletionVectorDescriptor> deletionVector,
      Optional<MapValue> tags,
      Optional<Long> baseRowId,
      Optional<Long> defaultRowCommitVersion,
      Optional<DataFileStatistics> stats) {

    checkArgument(path != null, "path is not nullable");
    checkArgument(partitionValues != null, "partitionValues is not nullable");

    Map<Integer, Object> fieldMap = new HashMap<>();
    fieldMap.put(FULL_SCHEMA.indexOf("path"), path);
    fieldMap.put(FULL_SCHEMA.indexOf("partitionValues"), partitionValues);
    fieldMap.put(FULL_SCHEMA.indexOf("size"), size);
    fieldMap.put(FULL_SCHEMA.indexOf("modificationTime"), modificationTime);
    fieldMap.put(FULL_SCHEMA.indexOf("dataChange"), dataChange);
    tags.ifPresent(tag -> fieldMap.put(FULL_SCHEMA.indexOf("tags"), tag));
    baseRowId.ifPresent(id -> fieldMap.put(FULL_SCHEMA.indexOf("baseRowId"), id));
    defaultRowCommitVersion.ifPresent(
        version -> fieldMap.put(FULL_SCHEMA.indexOf("defaultRowCommitVersion"), version));
    stats.ifPresent(
        stat -> fieldMap.put(FULL_SCHEMA.indexOf("stats"), stat.serializeAsJson(physicalSchema)));
    deletionVector.ifPresent(
        dv -> {
          Map<Integer, Object> dvFieldMap = new HashMap<>();
          StructType dvSchema = DeletionVectorDescriptor.READ_SCHEMA;

          dvFieldMap.put(dvSchema.indexOf("storageType"), dv.getStorageType());
          dvFieldMap.put(dvSchema.indexOf("pathOrInlineDv"), dv.getPathOrInlineDv());
          dv.getOffset().ifPresent(offset -> dvFieldMap.put(dvSchema.indexOf("offset"), offset));
          dvFieldMap.put(dvSchema.indexOf("sizeInBytes"), dv.getSizeInBytes());
          dvFieldMap.put(dvSchema.indexOf("cardinality"), dv.getCardinality());

          Row dvRow = new GenericRow(dvSchema, dvFieldMap);

          fieldMap.put(FULL_SCHEMA.indexOf("deletionVector"), dvRow);
        });

    return new GenericRow(FULL_SCHEMA, fieldMap);
  }

  ////////////////////////////////////
  // Constructor and Member Methods //
  ////////////////////////////////////

  /** Constructs an {@link AddFile} action from the given 'AddFile' {@link Row}. */
  public AddFile(Row row) {
    super(row);
  }

  public String getPath() {
    return row.getString(getFieldIndex("path"));
  }

  public MapValue getPartitionValues() {
    return row.getMap(getFieldIndex("partitionValues"));
  }

  public long getSize() {
    return row.getLong(getFieldIndex("size"));
  }

  public long getModificationTime() {
    return row.getLong(getFieldIndex("modificationTime"));
  }

  public boolean getDataChange() {
    return row.getBoolean(getFieldIndex("dataChange"));
  }

  public Optional<DeletionVectorDescriptor> getDeletionVector() {
    int index = getFieldIndex("deletionVector");
    return Optional.ofNullable(
        row.isNullAt(index) ? null : DeletionVectorDescriptor.fromRow(row.getStruct(index)));
  }

  public Optional<MapValue> getTags() {
    int index = getFieldIndex("tags");
    return Optional.ofNullable(row.isNullAt(index) ? null : row.getMap(index));
  }

  public Optional<Long> getBaseRowId() {
    int index = getFieldIndex("baseRowId");
    return Optional.ofNullable(row.isNullAt(index) ? null : row.getLong(index));
  }

  public Optional<Long> getDefaultRowCommitVersion() {
    int index = getFieldIndex("defaultRowCommitVersion");
    return Optional.ofNullable(row.isNullAt(index) ? null : row.getLong(index));
  }

  public Optional<DataFileStatistics> getStats() {
    return getFieldIndexOpt("stats")
        .flatMap(
            index ->
                row.isNullAt(index)
                    ? Optional.empty()
                    : StatsUtils.deserializeFromJson(row.getString(index)));
  }

  public Optional<Long> getNumRecords() {
    return getStats().map(DataFileStatistics::getNumRecords);
  }

  /** Returns a new {@link AddFile} with the provided baseRowId. */
  public AddFile withNewBaseRowId(long baseRowId) {
    return new AddFile(toRowWithOverriddenValue("baseRowId", baseRowId));
  }

  /** Returns a new {@link AddFile} with the provided defaultRowCommitVersion. */
  public AddFile withNewDefaultRowCommitVersion(long defaultRowCommitVersion) {
    return new AddFile(
        toRowWithOverriddenValue("defaultRowCommitVersion", defaultRowCommitVersion));
  }

  @Override
  public String toString() {
    // No specific ordering is guaranteed for partitionValues and tags in the returned string
    StringBuilder sb = new StringBuilder();
    sb.append("AddFile{");
    sb.append("path='").append(getPath()).append('\'');
    sb.append(", partitionValues=").append(toJavaMap(getPartitionValues()));
    sb.append(", size=").append(getSize());
    sb.append(", modificationTime=").append(getModificationTime());
    sb.append(", dataChange=").append(getDataChange());
    sb.append(", deletionVector=").append(getDeletionVector());
    sb.append(", tags=").append(getTags().map(VectorUtils::toJavaMap));
    sb.append(", baseRowId=").append(getBaseRowId());
    sb.append(", defaultRowCommitVersion=").append(getDefaultRowCommitVersion());
    sb.append(", stats=").append(getStats().map(d -> d.serializeAsJson(null)).orElse(""));
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof AddFile)) return false;
    AddFile other = (AddFile) obj;

    // MapValue and DataFileStatistics don't implement equals(), so we need to convert
    // partitionValues and tags to Java Maps, and stats to strings to compare them
    return getSize() == other.getSize()
        && getModificationTime() == other.getModificationTime()
        && getDataChange() == other.getDataChange()
        && Objects.equals(getPath(), other.getPath())
        && Objects.equals(toJavaMap(getPartitionValues()), toJavaMap(other.getPartitionValues()))
        && Objects.equals(getDeletionVector(), other.getDeletionVector())
        && Objects.equals(
            getTags().map(VectorUtils::toJavaMap), other.getTags().map(VectorUtils::toJavaMap))
        && Objects.equals(getBaseRowId(), other.getBaseRowId())
        && Objects.equals(getDefaultRowCommitVersion(), other.getDefaultRowCommitVersion())
        && Objects.equals(getStats(), other.getStats());
  }

  @Override
  public int hashCode() {
    // MapValue and DataFileStatistics don't implement hashCode(), so we need to convert
    // partitionValues and tags to Java Maps, and stats to strings to compute the hash code
    return Objects.hash(
        getPath(),
        toJavaMap(getPartitionValues()),
        getSize(),
        getModificationTime(),
        getDataChange(),
        getDeletionVector(),
        getTags().map(VectorUtils::toJavaMap),
        getBaseRowId(),
        getDefaultRowCommitVersion(),
        getStats());
  }
}
