/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.flink.sink.dynamic;

import io.delta.flink.sink.WriterResultContext;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.util.Preconditions;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * The write result for a single checkpoint interval from one table within a {@link
 * DeltaDynamicSinkWriter}.
 *
 * <p>Extends the single-table writer result by carrying a table name, enabling per-table grouping
 * in the pre-commit aggregator.
 */
public class DeltaDynamicWriterResult implements Serializable {

  private final String tableName;
  private final WriterResultContext context;
  // transient: serialized separately via the inner Serializer
  private final transient List<Row> deltaActions;

  public DeltaDynamicWriterResult(
      String tableName, List<Row> deltaActions, WriterResultContext context) {
    this.tableName = tableName;
    this.deltaActions = deltaActions;
    this.context = context;
  }

  /**
   * Returns the target table identifier.
   *
   * @return table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Returns the writer context containing watermark information.
   *
   * @return writer result context
   */
  public WriterResultContext getContext() {
    return context;
  }

  /**
   * Returns the Delta actions produced by this write (typically {@code AddFile} actions).
   *
   * @return Delta actions
   */
  public List<Row> getDeltaActions() {
    return deltaActions;
  }

  /**
   * Merges {@code another} result into this one. Both must refer to the same table.
   *
   * @param another result to merge; must carry the same table name
   * @return this instance after merging
   */
  public DeltaDynamicWriterResult merge(DeltaDynamicWriterResult another) {
    Preconditions.checkArgument(
        Objects.equals(this.tableName, another.tableName),
        "Cannot merge writer results for different tables");
    this.deltaActions.addAll(another.deltaActions);
    this.context.merge(another.context);
    return this;
  }

  @Override
  public String toString() {
    return "DeltaDynamicWriterResult{"
        + "tableName='"
        + tableName
        + '\''
        + ", context="
        + context
        + ", deltaActions="
        + deltaActions.stream().map(JsonUtils::rowToJson).collect(Collectors.joining(","))
        + "}";
  }

  static class Serializer implements SimpleVersionedSerializer<DeltaDynamicWriterResult> {

    @Override
    public int getVersion() {
      return 2;
    }

    @Override
    public byte[] serialize(DeltaDynamicWriterResult obj) throws IOException {
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
          ObjectOutputStream out = new ObjectOutputStream(bos)) {
        out.writeUTF(obj.tableName);
        out.writeObject(obj.context);
        out.writeInt(obj.deltaActions.size());
        for (Row row : obj.deltaActions) {
          Preconditions.checkArgument(
              row.getSchema().equivalent(SingleAction.FULL_SCHEMA), "Need to be an action");
          out.writeUTF(JsonUtils.rowToJson(row));
        }
        out.flush();
        return bos.toByteArray();
      }
    }

    @Override
    public DeltaDynamicWriterResult deserialize(int version, byte[] serialized) throws IOException {
      try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
          ObjectInputStream in = new ObjectInputStream(bis)) {
        String tableName = in.readUTF();
        WriterResultContext context = (WriterResultContext) in.readObject();
        int numActions = in.readInt();
        List<Row> actions = new ArrayList<>(numActions);
        for (int i = 0; i < numActions; i++) {
          actions.add(JsonUtils.rowFromJson(in.readUTF(), SingleAction.FULL_SCHEMA));
        }
        return new DeltaDynamicWriterResult(tableName, actions, context);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
