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
import java.util.stream.Collectors;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * A per-table unit of pending work produced by {@link DeltaDynamicWriterResultAggregator}, ready to
 * be committed to a specific Delta table.
 *
 * <p>Carries a table name for routing in addition to the checkpoint and action fields of a
 * single-table committable.
 *
 * <p>Committing the same {@code DeltaDynamicCommittable} multiple times is safe: the Delta
 * transaction identifier stored in the table log deduplicates replayed committables on recovery.
 */
public class DeltaDynamicCommittable {

  private final String jobId;
  private final String operatorId;
  private final long checkpointId;
  private final String tableName;
  private final List<Row> deltaActions;
  private final WriterResultContext context;

  public DeltaDynamicCommittable(
      String jobId,
      String operatorId,
      long checkpointId,
      String tableName,
      List<Row> deltaActions,
      WriterResultContext context) {
    this.jobId = jobId;
    this.operatorId = operatorId;
    this.checkpointId = checkpointId;
    this.tableName = tableName;
    this.deltaActions = deltaActions;
    this.context = context;
  }

  public String getJobId() {
    return jobId;
  }

  public String getOperatorId() {
    return operatorId;
  }

  public long getCheckpointId() {
    return checkpointId;
  }

  /**
   * Returns the target table identifier.
   *
   * @return table name
   */
  public String getTableName() {
    return tableName;
  }

  public List<Row> getDeltaActions() {
    return deltaActions;
  }

  public WriterResultContext getContext() {
    return context;
  }

  @Override
  public String toString() {
    return "DeltaDynamicCommittable{"
        + "jobId='"
        + jobId
        + '\''
        + ", operatorId='"
        + operatorId
        + '\''
        + ", checkpointId="
        + checkpointId
        + ", tableName='"
        + tableName
        + '\''
        + ", context="
        + context
        + ", deltaActions="
        + deltaActions.stream().map(JsonUtils::rowToJson).collect(Collectors.joining(","))
        + '}';
  }

  static class Serializer implements SimpleVersionedSerializer<DeltaDynamicCommittable> {

    @Override
    public int getVersion() {
      return 2;
    }

    @Override
    public byte[] serialize(DeltaDynamicCommittable obj) throws IOException {
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
          ObjectOutputStream out = new ObjectOutputStream(bos)) {
        out.writeUTF(obj.jobId);
        out.writeUTF(obj.operatorId);
        out.writeLong(obj.checkpointId);
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
    public DeltaDynamicCommittable deserialize(int version, byte[] serialized) throws IOException {
      try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
          ObjectInputStream in = new ObjectInputStream(bis)) {
        String jobId = in.readUTF();
        String operatorId = in.readUTF();
        long checkpointId = in.readLong();
        String tableName = in.readUTF();
        WriterResultContext context = (WriterResultContext) in.readObject();
        int numActions = in.readInt();
        List<Row> actions = new ArrayList<>(numActions);
        for (int i = 0; i < numActions; i++) {
          actions.add(JsonUtils.rowFromJson(in.readUTF(), SingleAction.FULL_SCHEMA));
        }
        return new DeltaDynamicCommittable(
            jobId, operatorId, checkpointId, tableName, actions, context);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
