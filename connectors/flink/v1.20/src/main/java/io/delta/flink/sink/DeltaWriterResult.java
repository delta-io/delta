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
 * The WriterResult emitted by DeltaSinkWriter. It includes multiple actions, each containing data
 * in a partition.
 */
public class DeltaWriterResult implements Serializable {
  private final List<Row> deltaActions;

  public DeltaWriterResult(List<Row> deltaActions) {
    this.deltaActions = deltaActions;
  }

  public List<Row> getDeltaActions() {
    return deltaActions;
  }

  @Override
  public String toString() {
    return "DeltaWriterResult{"
        + "deltaActions="
        + deltaActions.stream().map(JsonUtils::rowToJson).collect(Collectors.joining(","))
        + '}';
  }

  static class Serializer implements SimpleVersionedSerializer<DeltaWriterResult> {
    @Override
    public int getVersion() {
      return 1;
    }

    @Override
    public byte[] serialize(DeltaWriterResult obj) throws IOException {
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
          ObjectOutputStream out = new ObjectOutputStream(bos)) {
        out.writeInt(obj.getDeltaActions().size());
        for (Row row : obj.getDeltaActions()) {
          Preconditions.checkArgument(
              row.getSchema().equivalent(SingleAction.FULL_SCHEMA), "Need to be an action");
          out.writeUTF(JsonUtils.rowToJson(row));
        }
        out.flush();
        return bos.toByteArray();
      }
    }

    @Override
    public DeltaWriterResult deserialize(int version, byte[] serialized) throws IOException {
      try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
          ObjectInputStream in = new ObjectInputStream(bis)) {
        final int numActions = in.readInt();
        List<Row> actions = new ArrayList<>(numActions);
        for (int i = 0; i < numActions; i++) {
          final String actionJson = in.readUTF();
          actions.add(JsonUtils.rowFromJson(actionJson, SingleAction.FULL_SCHEMA));
        }
        return new DeltaWriterResult(actions);
      }
    }
  }
}
