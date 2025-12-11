package io.delta.flink.sink;

import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.util.Preconditions;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DeltaCommittable {
  private final String jobId;
  private final String operatorId;
  private final long checkpointId;
  private final List<Row> deltaActions;

  public DeltaCommittable(
      String jobId, String operatorId, long checkpointId, List<Row> deltaActions) {
    this.jobId = jobId;
    this.operatorId = operatorId;
    this.checkpointId = checkpointId;
    this.deltaActions = deltaActions;
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
        final int numActions = in.readInt();
        List<Row> actions = new ArrayList<>(numActions);
        for (int i = 0; i < numActions; i++) {
          final String actionJson = in.readUTF();
          actions.add(JsonUtils.rowFromJson(actionJson, SingleAction.FULL_SCHEMA));
        }
        return new DeltaCommittable(jobId, operatorId, checkpointId, actions);
      }
    }
  }
}
