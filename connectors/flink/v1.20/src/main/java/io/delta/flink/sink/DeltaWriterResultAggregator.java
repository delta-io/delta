package io.delta.flink.sink;

import io.delta.kernel.internal.util.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operator which aggregates the individual {@link DeltaWriterResult} objects) to a single {@link
 * DeltaCommittable} per checkpoint.
 */
class DeltaWriterResultAggregator
    extends AbstractStreamOperator<CommittableMessage<DeltaCommittable>>
    implements OneInputStreamOperator<
        CommittableMessage<DeltaWriterResult>, CommittableMessage<DeltaCommittable>> {
  private static final Logger LOG = LoggerFactory.getLogger(DeltaWriterResultAggregator.class);
  private final Collection<DeltaWriterResult> results;

  DeltaWriterResultAggregator() {
    results = new ArrayList<>();
  }

  @Override
  public void open() throws Exception {
    String flinkJobId = getContainingTask().getEnvironment().getJobID().toString();
    String operatorId = getOperatorID().toString();
    int subTaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    Preconditions.checkArgument(
        subTaskId == 0, "The subTaskId must be zero in the IcebergWriteAggregator");
    int attemptId = getRuntimeContext().getTaskInfo().getAttemptNumber();
  }

  @Override
  public void finish() throws IOException {
    LOG.debug("Finishing");
    prepareSnapshotPreBarrier(Long.MAX_VALUE);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws IOException {
    LOG.debug("Aggregating on checkpoint {}", checkpointId);
    DeltaCommittable committable =
        new DeltaCommittable(
            getContainingTask().getEnvironment().getJobID().toString(),
            getRuntimeContext().getOperatorUniqueID(),
            checkpointId,
            results.stream()
                .flatMap(writerResult -> writerResult.getDeltaActions().stream())
                .collect(Collectors.toList()));
    CommittableMessage<DeltaCommittable> summary =
        new CommittableSummary<>(0, 1, checkpointId, 1, 1, 0);
    output.collect(new StreamRecord<>(summary));
    CommittableMessage<DeltaCommittable> message =
        new CommittableWithLineage<>(committable, checkpointId, 0);
    output.collect(new StreamRecord<>(message));

    LOG.debug(
        "Emitted commit message to downstream committer operator on checkpoint {} containing {} rows",
        checkpointId,
        committable.getDeltaActions().size());
    results.clear();
  }

  @Override
  public void processElement(StreamRecord<CommittableMessage<DeltaWriterResult>> element)
      throws Exception {
    if (element.isRecord() && element.getValue() instanceof CommittableWithLineage) {
      LOG.debug("Received writerResult: {}", element.getValue());
      results.add(
          ((CommittableWithLineage<DeltaWriterResult>) element.getValue()).getCommittable());
    }
  }
}
