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
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.DataFileStatus;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Metadata about {@code remove} action in the Delta Log. */
// extends RowBackedAction
public class RemoveFile {
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
  // TODO: Currently, Kernel doesn't create RemoveFile actions internally, nor provides APIs for
  //  connectors to generate and commit them. Once we have the need for this, we should ensure
  //  that the baseRowId and defaultRowCommitVersion fields of RemoveFile actions are correctly
  //  populated to match the corresponding AddFile actions.

  public static Row createRemoveFileRowWithExtendedFileMetadata(
      String path,
      long deletionTimestamp,
      boolean dataChange,
      MapValue partitionValues,
      long size,
      Optional<DataFileStatistics> stats,
      StructType physicalSchema) {
    Map<Integer, Object> fieldMap = new HashMap<>();
    fieldMap.put(FULL_SCHEMA.indexOf("path"), requireNonNull(path));
    fieldMap.put(FULL_SCHEMA.indexOf("deletionTimestamp"), deletionTimestamp);
    fieldMap.put(FULL_SCHEMA.indexOf("dataChange"), dataChange);
    fieldMap.put(FULL_SCHEMA.indexOf("extendedFileMetadata"), true);
    fieldMap.put(FULL_SCHEMA.indexOf("partitionValues"), requireNonNull(partitionValues));
    fieldMap.put(FULL_SCHEMA.indexOf("size"), size);
    stats.ifPresent(
        stat -> fieldMap.put(FULL_SCHEMA.indexOf("stats"), stat.serializeAsJson(physicalSchema)));
    return new GenericRow(FULL_SCHEMA, fieldMap);
  }

  // TODO docs about when this should be used
  public static Row convertDataFileStatus(
      StructType physicalSchema,
      URI tableRoot,
      DataFileStatus dataFileStatus,
      Map<String, Literal> partitionValues,
      boolean dataChange) {
    return createRemoveFileRowWithExtendedFileMetadata(
        relativizePath(new Path(dataFileStatus.getPath()), tableRoot).toUri().toString(),
        dataFileStatus.getModificationTime(),
        dataChange,
        serializePartitionMap(partitionValues),
        dataFileStatus.getSize(),
        dataFileStatus.getStatistics(),
        physicalSchema);
  }

  // TODO re-org
  /*
  public static Row createRemoveFileRow(
      String path,
      Optional<Long> deletionTimestamp,
      boolean dataChange,
      Optional<Boolean> extendedFileMetadata,
      Optional<MapValue> partitionValues,
      Optional<Long>
  )
   */
}
