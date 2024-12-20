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
import static java.util.stream.Collectors.toMap;

import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.data.DelegateRow;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.DataFileStatistics;
import io.delta.kernel.utils.DataFileStatus;
import java.net.URI;
import java.util.*;
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
   * The underlying {@link Row} that represents an 'AddFile' action and contains all its field
   * values. Can be either a {@link GenericRow} or a {@link DelegateRow}.
   */
  private final Row row;

  public AddFile(Row row) {
    this.row = row;
  }

  public Row toRow() {
    return row;
  }

  public String getPath() {
    return row.getString(COL_NAME_TO_ORDINAL.get("path"));
  }

  public MapValue getPartitionValues() {
    return row.getMap(COL_NAME_TO_ORDINAL.get("partitionValues"));
  }

  public long getSize() {
    return row.getLong(COL_NAME_TO_ORDINAL.get("size"));
  }

  public long getModificationTime() {
    return row.getLong(COL_NAME_TO_ORDINAL.get("modificationTime"));
  }

  public boolean getDataChange() {
    return row.getBoolean(COL_NAME_TO_ORDINAL.get("dataChange"));
  }

  public Optional<DeletionVectorDescriptor> getDeletionVector() {
    int ordinal = COL_NAME_TO_ORDINAL.get("deletionVector");
    return Optional.ofNullable(
        row.isNullAt(ordinal) ? null : DeletionVectorDescriptor.fromRow(row.getStruct(ordinal)));
  }

  public Optional<MapValue> getTags() {
    int ordinal = COL_NAME_TO_ORDINAL.get("tags");
    return Optional.ofNullable(row.isNullAt(ordinal) ? null : row.getMap(ordinal));
  }

  public Optional<Long> getBaseRowId() {
    int ordinal = COL_NAME_TO_ORDINAL.get("baseRowId");
    return Optional.ofNullable(row.isNullAt(ordinal) ? null : row.getLong(ordinal));
  }

  public Optional<Long> getDefaultRowCommitVersion() {
    int ordinal = COL_NAME_TO_ORDINAL.get("defaultRowCommitVersion");
    return Optional.ofNullable(row.isNullAt(ordinal) ? null : row.getLong(ordinal));
  }

  public Optional<DataFileStatistics> getStats() {
    int ordinal = COL_NAME_TO_ORDINAL.get("stats");
    return Optional.ofNullable(
        row.isNullAt(ordinal)
            ? null
            : DataFileStatistics.deserializeFromJson(row.getString(ordinal)).orElse(null));
  }

  public Optional<Long> getNumRecords() {
    return this.getStats().map(DataFileStatistics::getNumRecords);
  }

  /**
   * Returns a new {@link AddFile} with the provided baseRowId. Under the hood, this is achieved by
   * creating a new {@link DelegateRow} with the baseRowId overridden.
   */
  public AddFile withNewBaseRowId(long baseRowId) {
    Map<Integer, Object> overrides =
        Collections.singletonMap(COL_NAME_TO_ORDINAL.get("baseRowId"), baseRowId);
    return new AddFile(new DelegateRow(row, overrides));
  }

  /**
   * Returns a new {@link AddFile} with the provided defaultRowCommitVersion. Under the hood, this
   * is achieved by creating a new {@link DelegateRow} with the defaultRowCommitVersion overridden.
   */
  public AddFile withNewDefaultRowCommitVersion(long defaultRowCommitVersion) {
    Map<Integer, Object> overrides =
        Collections.singletonMap(
            COL_NAME_TO_ORDINAL.get("defaultRowCommitVersion"), defaultRowCommitVersion);
    return new AddFile(new DelegateRow(row, overrides));
  }

  @Override
  public String toString() {
    // Convert the partitionValues and tags to Java Maps and make them sorted by key.
    Map<String, String> partitionValuesJavaMap = VectorUtils.toJavaMap(getPartitionValues());
    Map<String, String> sortedPartitionValuesJavaMap = new TreeMap<>(partitionValuesJavaMap);

    Optional<Map<String, String>> tagsJavaMap = getTags().map(VectorUtils::toJavaMap);
    Optional<Map<String, String>> sortedTagsJavaMap = tagsJavaMap.map(TreeMap::new);

    StringBuilder sb = new StringBuilder();
    sb.append("AddFile{");
    sb.append("path='").append(getPath()).append('\'');
    sb.append(", partitionValues=").append(sortedPartitionValuesJavaMap);
    sb.append(", size=").append(getSize());
    sb.append(", modificationTime=").append(getModificationTime());
    sb.append(", dataChange=").append(getDataChange());
    sb.append(", deletionVector=").append(getDeletionVector());
    sb.append(", tags=").append(sortedTagsJavaMap);
    sb.append(", baseRowId=").append(getBaseRowId());
    sb.append(", defaultRowCommitVersion=").append(getDefaultRowCommitVersion());
    sb.append(", stats=").append(getStats());
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof AddFile)) return false;
    AddFile other = (AddFile) obj;
    return getSize() == other.getSize()
        && getModificationTime() == other.getModificationTime()
        && getDataChange() == other.getDataChange()
        && Objects.equals(getPath(), other.getPath())
        && Objects.equals(getPartitionValues(), other.getPartitionValues())
        && Objects.equals(getDeletionVector(), other.getDeletionVector())
        && Objects.equals(getTags(), other.getTags())
        && Objects.equals(getBaseRowId(), other.getBaseRowId())
        && Objects.equals(getDefaultRowCommitVersion(), other.getDefaultRowCommitVersion())
        && Objects.equals(getStats(), other.getStats());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getPath(),
        getPartitionValues(),
        getSize(),
        getModificationTime(),
        getDataChange(),
        getDeletionVector(),
        getTags(),
        getBaseRowId(),
        getDefaultRowCommitVersion(),
        getStats());
  }
}
