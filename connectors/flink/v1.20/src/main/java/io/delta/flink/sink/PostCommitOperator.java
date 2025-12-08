package io.delta.flink.sink;

import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostCommitOperator
    extends ProcessFunction<CommittableMessage<DeltaCommittable>, Void> {

  private static final Logger LOG = LoggerFactory.getLogger(PostCommitOperator.class);

  public PostCommitOperator() {}

  @Override
  public void processElement(
      CommittableMessage<DeltaCommittable> value,
      ProcessFunction<CommittableMessage<DeltaCommittable>, Void>.Context ctx,
      Collector<Void> out)
      throws Exception {
    if (value instanceof CommittableWithLineage) {
      CommittableWithLineage<DeltaCommittable> committableWithLineage =
          (CommittableWithLineage<DeltaCommittable>) value;
      LOG.debug("Received deltaCommittable {}", committableWithLineage.getCommittable());
    }
    if (value instanceof CommittableSummary) {
      CommittableSummary<DeltaCommittable> committableSummary =
          (CommittableSummary<DeltaCommittable>) value;
      LOG.debug(
          "Received committableSummary: {}, # committables: {}, checkpointId: {}",
          committableSummary,
          committableSummary.getNumberOfCommittables(),
          committableSummary.getCheckpointId());
    }
  }
}
