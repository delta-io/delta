/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink.sink;

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
 * A {@code DeltaCommittable} represents a unit of pending work produced by a Delta sink writer that
 * is ready to be committed to a Delta table.
 *
 * <p>{@code DeltaCommittable} instances are emitted by {@link
 * org.apache.flink.api.connector.sink.SinkWriter} implementations during checkpointing and are
 * later consumed by a {@link DeltaCommitter} or global committer to finalize changes in the Delta
 * transaction log.
 *
 * <p>Each committable encapsulates:
 *
 * <ul>
 *   <li>one or more Delta actions (e.g., {@code AddFile} actions) produced by a writer,
 *   <li>checkpoint-scoped context that allows the commit process to be retried safely.
 * </ul>
 *
 * <p>During recovery or retries, the same {@code DeltaCommittable} may be delivered multiple times
 * to the committer. Implementations must therefore ensure that committing a committable is either
 * idempotent or protected by higher-level deduplication mechanisms (for example, checkpoint
 * tracking stored in the Delta table metadata).
 *
 * <p>{@code DeltaCommittable} is a transport object only; it does not perform I/O or commit
 * operations itself. All side effects are applied by the corresponding committer.
 *
 * <p>This class is typically serialized and checkpointed by Flink and must therefore remain stable
 * and backward-compatible across versions of the connector.
 */
public class DeltaCommittable {
  private final String jobId;
  private final String operatorId;
  private final long checkpointId;
  private final List<Row> deltaActions;
  private final WriterResultContext context;

  public DeltaCommittable(
      String jobId,
      String operatorId,
      long checkpointId,
      List<Row> deltaActions,
      WriterResultContext context) {
    this.jobId = jobId;
    this.operatorId = operatorId;
    this.checkpointId = checkpointId;
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

  public List<Row> getDeltaActions() {
    return deltaActions;
  }

  public WriterResultContext getContext() {
    return context;
  }

  @Override
  public String toString() {
    return "DeltaCommittable{"
        + "jobId='"
        + jobId
        + '\''
        + ", operatorId='"
        + operatorId
        + '\''
        + ", checkpointId="
        + checkpointId
        + ", context="
        + context
        + ", deltaActions="
        + deltaActions.stream().map(JsonUtils::rowToJson).collect(Collectors.joining(","))
        + '}';
  }

  static class Serializer implements SimpleVersionedSerializer<DeltaCommittable> {
    @Override
    public int getVersion() {
      return 1;
    }

    @Override
    public byte[] serialize(DeltaCommittable obj) throws IOException {
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
          ObjectOutputStream out = new ObjectOutputStream(bos)) {
        out.writeUTF(obj.getJobId());
        out.writeUTF(obj.getOperatorId());
        out.writeLong(obj.getCheckpointId());
        out.writeObject(obj.getContext());
        out.writeInt(obj.getDeltaActions().size());
        for (Row row : obj.getDeltaActions()) {
          Preconditions.checkArgument(
              row.getSchema().equivalent(SingleAction.FULL_SCHEMA), "Need to be an action");
          out.writeUTF(JsonUtils.rowToJson(row));
        }
        out.flush();
        out.close();
        return bos.toByteArray();
      }
    }

    @Override
    public DeltaCommittable deserialize(int version, byte[] serialized) throws IOException {
      try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
          ObjectInputStream in = new ObjectInputStream(bis)) {
        final String jobId = in.readUTF();
        final String operatorId = in.readUTF();
        final long checkpointId = in.readLong();
        final WriterResultContext context = (WriterResultContext) in.readObject();
        final int numActions = in.readInt();
        List<Row> actions = new ArrayList<>(numActions);
        for (int i = 0; i < numActions; i++) {
          final String actionJson = in.readUTF();
          actions.add(JsonUtils.rowFromJson(actionJson, SingleAction.FULL_SCHEMA));
        }
        return new DeltaCommittable(jobId, operatorId, checkpointId, actions, context);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
