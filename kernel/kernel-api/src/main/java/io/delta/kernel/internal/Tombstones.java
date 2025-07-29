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

package io.delta.kernel.internal;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.defaults.internal.data.vector.DefaultBooleanVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.internal.replay.LogReplayUtils;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

// TODO: rename to distributed replay state
public class Tombstones {
  public Set<LogReplayUtils.UniqueFileActionTuple> removeFileSet;
  public Set<LogReplayUtils.UniqueFileActionTuple> alreadyReturnedSet;

  static StructType schema =
      new StructType()
          .add("fileURI", StringType.STRING)
          .add("deletionVectorId", StringType.STRING)
          .add("hasDeletionVector", BooleanType.BOOLEAN)
          .add("isRemoved", BooleanType.BOOLEAN);

  public Tombstones(
      Set<LogReplayUtils.UniqueFileActionTuple> removeFileSet,
      Set<LogReplayUtils.UniqueFileActionTuple> alreadyReturnedSet) {
    this.alreadyReturnedSet = alreadyReturnedSet;
    this.removeFileSet = removeFileSet;
  }

  public String toSerialize() {
    //    return "{"
    //        + "\"removeFileSet\":"
    //        + serializeSet(removeFileSet)
    //        + ",\"alreadyReturnedSet\":"
    //        + serializeSet(alreadyReturnedSet)
    //        + "}";
    return JsonUtils.columnarBatchToJSON(tombstonesToColumnarBatch(this));
  }

  private String serializeSet(Set<LogReplayUtils.UniqueFileActionTuple> set) {
    return "["
        + set.stream()
            .map(
                tuple ->
                    "{\"fileURI\":\""
                        + tuple._1.toString()
                        + "\","
                        + "\"deletionVectorId\":\""
                        + tuple._2.orElse("")
                        + "\"}")
            .collect(Collectors.joining(","))
        + "]";
  }

  public static Tombstones deserializeTombstone(String serializedTombstone) {
    // return new Tombstones(new HashSet<>(), new HashSet<>());
    return tombstonesFromColumnarBatch(
        JsonUtils.columnarBatchFromJson(serializedTombstone, schema));
  }

  public static ColumnarBatch tombstonesToColumnarBatch(Tombstones tombstones) {
    List<LogReplayUtils.UniqueFileActionTuple> allTuples = new ArrayList<>();
    List<Boolean> isRemoved = new ArrayList<>();

    for (LogReplayUtils.UniqueFileActionTuple t : tombstones.removeFileSet) {
      allTuples.add(t);
      isRemoved.add(true);
    }

    for (LogReplayUtils.UniqueFileActionTuple t : tombstones.alreadyReturnedSet) {
      allTuples.add(t);
      isRemoved.add(false);
    }

    int size = allTuples.size();

    String[] fileURIs = new String[size];
    String[] deletionVectorIds = new String[size];
    boolean[] hasDVs = new boolean[size];
    boolean[] isRemovedArr = new boolean[size];

    for (int i = 0; i < size; i++) {
      LogReplayUtils.UniqueFileActionTuple t = allTuples.get(i);
      fileURIs[i] = t._1.toString();
      deletionVectorIds[i] = t._2.orElse("");
      hasDVs[i] = t._2.isPresent();
      isRemovedArr[i] = isRemoved.get(i);
    }

    ColumnVector[] vectors =
        new ColumnVector[] {
          DefaultGenericVector.fromArray(StringType.STRING, fileURIs),
          DefaultGenericVector.fromArray(StringType.STRING, deletionVectorIds),
          new DefaultBooleanVector(size, Optional.empty(), hasDVs),
          new DefaultBooleanVector(size, Optional.empty(), isRemovedArr)
        };

    return new DefaultColumnarBatch(size, schema, vectors);
  }

  public static Tombstones tombstonesFromColumnarBatch(ColumnarBatch batch) {
    ColumnVector fileURICol = batch.getColumnVector(0);
    ColumnVector dvIdCol = batch.getColumnVector(1);
    ColumnVector isRemovedCol = batch.getColumnVector(3); // skip hasDeletionVector

    Set<LogReplayUtils.UniqueFileActionTuple> removeSet = new HashSet<>();
    Set<LogReplayUtils.UniqueFileActionTuple> alreadyReturnedSet = new HashSet<>();

    for (int i = 0; i < batch.getSize(); i++) {
      String fileUri = fileURICol.getString(i);
      String dvId = dvIdCol.getString(i);
      boolean isRemoved = isRemovedCol.getBoolean(i);

      LogReplayUtils.UniqueFileActionTuple tuple =
          new LogReplayUtils.UniqueFileActionTuple(
              URI.create(fileUri), dvId.isEmpty() ? Optional.empty() : Optional.of(dvId));

      if (isRemoved) {
        removeSet.add(tuple);
      } else {
        alreadyReturnedSet.add(tuple);
      }
    }

    return new Tombstones(removeSet, alreadyReturnedSet);
  }
}
