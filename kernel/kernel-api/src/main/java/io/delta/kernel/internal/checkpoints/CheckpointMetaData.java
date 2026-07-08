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

import static io.delta.kernel.internal.util.VectorUtils.stringStringMapValue;
import static io.delta.kernel.internal.util.VectorUtils.toJavaMap;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.actions.SetTransaction;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.data.StructRow;
import io.delta.kernel.internal.util.JsonUtils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.*;

public class CheckpointMetaData {

  /** Schema of a single entry of {@code v2Checkpoint.nonFileActions}. */
  private static final StructType NON_FILE_ACTION_SCHEMA =
      new StructType()
          .add("txn", SetTransaction.FULL_SCHEMA, true /* nullable */)
          .add("metaData", Metadata.FULL_SCHEMA, true /* nullable */)
          .add("protocol", Protocol.FULL_SCHEMA, true /* nullable */)
          .add("domainMetadata", DomainMetadata.FULL_SCHEMA, true /* nullable */)
          .add("checkpointMetadata", CheckpointMetadataAction.FULL_SCHEMA, true /* nullable */);

  /** Schema of the {@code v2Checkpoint} block. */
  private static final StructType V2_CHECKPOINT_SCHEMA =
      new StructType()
          .add("path", StringType.STRING, false /* nullable */)
          .add("sizeInBytes", LongType.LONG, false /* nullable */)
          .add("modificationTime", LongType.LONG, false /* nullable */)
          .add(
              "nonFileActions",
              new ArrayType(NON_FILE_ACTION_SCHEMA, false /* contains null */),
              true /* nullable */)
          .add(
              "sidecarFiles",
              new ArrayType(SidecarFile.READ_SCHEMA, false /* contains null */),
              true /* nullable */);

  public static StructType READ_SCHEMA =
      new StructType()
          .add("version", LongType.LONG, false /* nullable */)
          .add("size", LongType.LONG, false /* nullable */)
          .add("parts", LongType.LONG, true /* nullable */)
          .add("sizeInBytes", LongType.LONG, true /* nullable */)
          .add("numOfAddFiles", LongType.LONG, true /* nullable */)
          .add("v2Checkpoint", V2_CHECKPOINT_SCHEMA, true /* nullable */)
          .add("checksum", StringType.STRING, true /* nullable */)
          .add(
              "tags",
              new MapType(StringType.STRING, StringType.STRING, false),
              true /* nullable */);

  private static final int VERSION_ORDINAL = 0;
  private static final int SIZE_ORDINAL = 1;
  private static final int PARTS_ORDINAL = 2;
  private static final int SIZE_IN_BYTES_ORDINAL = 3;
  private static final int NUM_OF_ADD_FILES_ORDINAL = 4;
  private static final int V2_CHECKPOINT_ORDINAL = 5;
  private static final int CHECKSUM_ORDINAL = 6;
  private static final int TAGS_ORDINAL = 7;

  public static CheckpointMetaData fromRow(Row row) {
    return new CheckpointMetaData(
        row.getLong(VERSION_ORDINAL),
        row.getLong(SIZE_ORDINAL),
        row.isNullAt(PARTS_ORDINAL) ? Optional.empty() : Optional.of(row.getLong(PARTS_ORDINAL)),
        row.isNullAt(SIZE_IN_BYTES_ORDINAL)
            ? Optional.empty()
            : Optional.of(row.getLong(SIZE_IN_BYTES_ORDINAL)),
        row.isNullAt(NUM_OF_ADD_FILES_ORDINAL)
            ? Optional.empty()
            : Optional.of(row.getLong(NUM_OF_ADD_FILES_ORDINAL)),
        // Deep-copy the v2Checkpoint struct: row.getStruct returns a lazy view backed by the
        // ColumnarBatch, which the caller (loadMetadataFromFile) closes right after this. Detaching
        // it into a GenericRow of plain Java values keeps it valid after the batch is freed.
        row.isNullAt(V2_CHECKPOINT_ORDINAL)
            ? Optional.empty()
            : Optional.of(StructRow.deepCopy(row.getStruct(V2_CHECKPOINT_ORDINAL))),
        row.isNullAt(CHECKSUM_ORDINAL)
            ? Optional.empty()
            : Optional.of(row.getString(CHECKSUM_ORDINAL)),
        row.isNullAt(TAGS_ORDINAL) ? Map.of() : toJavaMap(row.getMap(TAGS_ORDINAL)));
  }

  public final long version;
  public final long size;
  public final Optional<Long> parts;
  public final Optional<Long> sizeInBytes;
  public final Optional<Long> numOfAddFiles;
  public final Optional<Row> v2Checkpoint;
  public final Optional<String> checksum;
  public final Map<String, String> tags;

  public CheckpointMetaData(long version, long size, Optional<Long> parts) {
    this(
        version,
        size,
        parts,
        Optional.empty() /* sizeInBytes */,
        Optional.empty() /* numOfAddFiles */,
        Optional.empty() /* v2Checkpoint */,
        Optional.empty() /* checksum */,
        Map.of() /* tags */);
  }

  public CheckpointMetaData(
      long version, long size, Optional<Long> parts, Map<String, String> tags) {
    this(
        version,
        size,
        parts,
        Optional.empty() /* sizeInBytes */,
        Optional.empty() /* numOfAddFiles */,
        Optional.empty() /* v2Checkpoint */,
        Optional.empty() /* checksum */,
        tags);
  }

  public CheckpointMetaData(
      long version,
      long size,
      Optional<Long> parts,
      Optional<Long> sizeInBytes,
      Optional<Long> numOfAddFiles,
      Optional<Row> v2Checkpoint,
      Optional<String> checksum,
      Map<String, String> tags) {
    this.version = version;
    this.size = size;
    this.parts = parts;
    this.sizeInBytes = sizeInBytes;
    this.numOfAddFiles = numOfAddFiles;
    this.v2Checkpoint = v2Checkpoint;
    this.checksum = checksum;
    this.tags = tags;
  }

  public Row toRow() {
    Map<Integer, Object> dataMap = new HashMap<>();
    dataMap.put(VERSION_ORDINAL, version);
    dataMap.put(SIZE_ORDINAL, size);
    parts.ifPresent(aLong -> dataMap.put(PARTS_ORDINAL, aLong));
    sizeInBytes.ifPresent(aLong -> dataMap.put(SIZE_IN_BYTES_ORDINAL, aLong));
    numOfAddFiles.ifPresent(aLong -> dataMap.put(NUM_OF_ADD_FILES_ORDINAL, aLong));
    v2Checkpoint.ifPresent(row -> dataMap.put(V2_CHECKPOINT_ORDINAL, row));
    checksum.ifPresent(str -> dataMap.put(CHECKSUM_ORDINAL, str));
    if (!tags.isEmpty()) {
      dataMap.put(TAGS_ORDINAL, stringStringMapValue(tags));
    }

    return new GenericRow(READ_SCHEMA, dataMap);
  }

  public String toJson() {
    return JsonUtils.rowToJson(toRow());
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
        + ", sizeInBytes="
        + sizeInBytes
        + ", numOfAddFiles="
        + numOfAddFiles
        + ", v2Checkpoint="
        + (v2Checkpoint.isPresent() ? "present" : "empty")
        + ", checksum="
        + checksum
        + ", tags="
        + tags
        + '}';
  }
}
