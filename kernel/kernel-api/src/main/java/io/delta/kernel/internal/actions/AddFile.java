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
import static java.util.stream.Collectors.toMap;

import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.InternalUtils;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.DataFileStatistics;
import io.delta.kernel.utils.DataFileStatus;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

/** Delta log action representing an `AddFile` */
public class AddFile {

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

  private static final Map<String, Integer> COL_NAME_TO_ORDINAL =
      IntStream.range(0, FULL_SCHEMA.length())
          .boxed()
          .collect(toMap(i -> FULL_SCHEMA.at(i).getName(), i -> i));

  /**
   * Utility to generate `AddFile` row from the given {@link DataFileStatus} and partition values.
   */
  public static Row convertDataFileStatus(
      URI tableRoot,
      DataFileStatus dataFileStatus,
      Map<String, Literal> partitionValues,
      boolean dataChange) {
    Path filePath = new Path(dataFileStatus.getPath());
    Map<Integer, Object> valueMap = new HashMap<>();
    valueMap.put(
        COL_NAME_TO_ORDINAL.get("path"), relativizePath(filePath, tableRoot).toUri().toString());
    valueMap.put(
        COL_NAME_TO_ORDINAL.get("partitionValues"), serializePartitionMap(partitionValues));
    valueMap.put(COL_NAME_TO_ORDINAL.get("size"), dataFileStatus.getSize());
    valueMap.put(COL_NAME_TO_ORDINAL.get("modificationTime"), dataFileStatus.getModificationTime());
    valueMap.put(COL_NAME_TO_ORDINAL.get("dataChange"), dataChange);
    if (dataFileStatus.getStatistics().isPresent()) {
      valueMap.put(
          COL_NAME_TO_ORDINAL.get("stats"), dataFileStatus.getStatistics().get().serializeAsJson());
    }
    // any fields not present in the valueMap are considered null
    return new GenericRow(FULL_SCHEMA, valueMap);
  }

  /**
   * Utility to generate an {@link AddFile} action from an 'AddFile' {@link Row}.
   *
   * @throws NullPointerException if row is null
   */
  public static AddFile fromRow(Row row) {
    Objects.requireNonNull(row, "Cannot generate an AddFile action from a null row");

    checkArgument(
        row.getSchema().equals(FULL_SCHEMA),
        "Expected schema: %s, found: %s",
        FULL_SCHEMA,
        row.getSchema());

    return new AddFile(
        InternalUtils.requireNonNull(row, COL_NAME_TO_ORDINAL.get("path"), "path")
            .getString(COL_NAME_TO_ORDINAL.get("path")),
        InternalUtils.requireNonNull(
                row, COL_NAME_TO_ORDINAL.get("partitionValues"), "partitionValues")
            .getMap(COL_NAME_TO_ORDINAL.get("partitionValues")),
        InternalUtils.requireNonNull(row, COL_NAME_TO_ORDINAL.get("size"), "size")
            .getLong(COL_NAME_TO_ORDINAL.get("size")),
        InternalUtils.requireNonNull(
                row, COL_NAME_TO_ORDINAL.get("modificationTime"), "modificationTime")
            .getLong(COL_NAME_TO_ORDINAL.get("modificationTime")),
        InternalUtils.requireNonNull(row, COL_NAME_TO_ORDINAL.get("dataChange"), "dataChange")
            .getBoolean(COL_NAME_TO_ORDINAL.get("dataChange")),
        Optional.ofNullable(
            row.isNullAt(COL_NAME_TO_ORDINAL.get("deletionVector"))
                ? null
                : DeletionVectorDescriptor.fromRow(
                    row.getStruct(COL_NAME_TO_ORDINAL.get("deletionVector")))),
        Optional.ofNullable(
            row.isNullAt(COL_NAME_TO_ORDINAL.get("tags"))
                ? null
                : row.getMap(COL_NAME_TO_ORDINAL.get("tags"))),
        Optional.ofNullable(
            row.isNullAt(COL_NAME_TO_ORDINAL.get("baseRowId"))
                ? null
                : row.getLong(COL_NAME_TO_ORDINAL.get("baseRowId"))),
        Optional.ofNullable(
            row.isNullAt(COL_NAME_TO_ORDINAL.get("defaultRowCommitVersion"))
                ? null
                : row.getLong(COL_NAME_TO_ORDINAL.get("defaultRowCommitVersion"))),
        Optional.ofNullable(
            row.isNullAt(COL_NAME_TO_ORDINAL.get("stats"))
                ? null
                : DataFileStatistics.deserializeFromJson(
                        row.getString(COL_NAME_TO_ORDINAL.get("stats")))
                    .orElse(null)));
  }

  private final String path;
  private final MapValue partitionValues;
  private final long size;
  private final long modificationTime;
  private final boolean dataChange;
  private final Optional<DeletionVectorDescriptor> deletionVector;
  private final Optional<MapValue> tags;
  private final Optional<Long> baseRowId;
  private final Optional<Long> defaultRowCommitVersion;
  private final Optional<DataFileStatistics> stats;

  public AddFile(
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
    this.path = Objects.requireNonNull(path, "path is null");
    this.partitionValues = Objects.requireNonNull(partitionValues, "partitionValues is null");
    this.size = size;
    this.modificationTime = modificationTime;
    this.dataChange = dataChange;
    this.deletionVector = deletionVector;
    this.tags = tags;
    this.baseRowId = baseRowId;
    this.defaultRowCommitVersion = defaultRowCommitVersion;
    this.stats = stats;
  }

  public Row toRow() {
    Map<Integer, Object> valueMap = new HashMap<>();
    valueMap.put(COL_NAME_TO_ORDINAL.get("path"), path);
    valueMap.put(COL_NAME_TO_ORDINAL.get("partitionValues"), partitionValues);
    valueMap.put(COL_NAME_TO_ORDINAL.get("size"), size);
    valueMap.put(COL_NAME_TO_ORDINAL.get("modificationTime"), modificationTime);
    valueMap.put(COL_NAME_TO_ORDINAL.get("dataChange"), dataChange);
    deletionVector.ifPresent(dv -> valueMap.put(COL_NAME_TO_ORDINAL.get("deletionVector"), dv));
    tags.ifPresent(tags -> valueMap.put(COL_NAME_TO_ORDINAL.get("tags"), tags));
    baseRowId.ifPresent(rowId -> valueMap.put(COL_NAME_TO_ORDINAL.get("baseRowId"), rowId));
    defaultRowCommitVersion.ifPresent(
        commitVersion ->
            valueMap.put(COL_NAME_TO_ORDINAL.get("defaultRowCommitVersion"), commitVersion));
    stats.ifPresent(
        stats -> valueMap.put(COL_NAME_TO_ORDINAL.get("stats"), stats.serializeAsJson()));
    return new GenericRow(FULL_SCHEMA, valueMap);
  }

  public Optional<Long> getBaseRowId() {
    return baseRowId;
  }

  public Optional<Long> getDefaultRowCommitVersion() {
    return defaultRowCommitVersion;
  }

  public Optional<DataFileStatistics> getStats() {
    return stats;
  }

  public Optional<Long> getNumRecords() {
    return stats.map(DataFileStatistics::getNumRecords);
  }

  /** Creates a new AddFile instance with the specified base row ID. */
  public AddFile withNewBaseRowId(long baseRowId) {
    return new AddFile(
        path,
        partitionValues,
        size,
        modificationTime,
        dataChange,
        deletionVector,
        tags,
        Optional.of(baseRowId),
        defaultRowCommitVersion,
        stats);
  }

  /** Creates a new AddFile instance with the specified default row commit version. */
  public AddFile withNewDefaultRowCommitVersion(long defaultRowCommitVersion) {
    return new AddFile(
        path,
        partitionValues,
        size,
        modificationTime,
        dataChange,
        deletionVector,
        tags,
        baseRowId,
        Optional.of(defaultRowCommitVersion),
        stats);
  }

  @Override
  public String toString() {
    // Explicitly convert the partitionValues and tags to Java Maps
    Map<String, String> partitionValuesJavaMap = VectorUtils.toJavaMap(this.partitionValues);
    Optional<Map<String, String>> tagsJavaMap = this.tags.map(VectorUtils::toJavaMap);

    StringBuilder sb = new StringBuilder();
    sb.append("AddFile{");
    sb.append("path='").append(path).append('\'');
    sb.append(", partitionValues=").append(partitionValuesJavaMap);
    sb.append(", size=").append(size);
    sb.append(", modificationTime=").append(modificationTime);
    sb.append(", dataChange=").append(dataChange);
    sb.append(", deletionVector=").append(deletionVector);
    sb.append(", tags=").append(tagsJavaMap);
    sb.append(", baseRowId=").append(baseRowId);
    sb.append(", defaultRowCommitVersion=").append(defaultRowCommitVersion);
    sb.append(", stats=").append(stats);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof AddFile)) return false;
    AddFile other = (AddFile) obj;
    return size == other.size
        && modificationTime == other.modificationTime
        && dataChange == other.dataChange
        && Objects.equals(path, other.path)
        && Objects.equals(partitionValues, other.partitionValues)
        && Objects.equals(deletionVector, other.deletionVector)
        && Objects.equals(tags, other.tags)
        && Objects.equals(baseRowId, other.baseRowId)
        && Objects.equals(defaultRowCommitVersion, other.defaultRowCommitVersion)
        && Objects.equals(stats, other.stats);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        path,
        partitionValues,
        size,
        modificationTime,
        dataChange,
        deletionVector,
        tags,
        baseRowId,
        defaultRowCommitVersion,
        stats);
  }
}
