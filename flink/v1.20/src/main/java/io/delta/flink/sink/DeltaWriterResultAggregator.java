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

import io.delta.kernel.internal.util.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code DeltaWriterResultAggregator} is a pre-commit operator in the Flink Sink V2 topology that
 * aggregates individual {@link DeltaWriterResult} instances into a single {@link DeltaCommittable}
 * per checkpoint.
 *
 * <p>Each {@link DeltaSinkWriter} emits {@code DeltaWriterResult}s independently, from parallel
 * subtasks as data is written. This operator collects and merges those per-writer results, grouping
 * all actions belonging to the same checkpoint into a unified committable unit.
 *
 * <p>The aggregated {@link DeltaCommittable} represents the complete set of table changes produced
 * by the sink for a given checkpoint and is passed downstream to the commit phase, where it is
 * atomically applied to the Delta table.
 *
 * <p>This operator does not perform any I/O or table mutations itself; it is responsible only for
 * logical aggregation and checkpoint scoping of writer results. Exactly-once guarantees are
 * achieved by ensuring that at most one {@code DeltaCommittable} is emitted per checkpoint.
 */
public class DeltaWriterResultAggregator
    extends AbstractStreamOperator<CommittableMessage<DeltaCommittable>>
    implements OneInputStreamOperator<
        CommittableMessage<DeltaWriterResult>, CommittableMessage<DeltaCommittable>> {
  private static final Logger LOG = LoggerFactory.getLogger(DeltaWriterResultAggregator.class);
  private final Collection<DeltaWriterResult> results;

  public DeltaWriterResultAggregator() {
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

    DeltaWriterResult merged =
        results.stream()
            .collect(
                DeltaWriterResult::new,
                (buffer, toMerge) -> buffer.merge(toMerge),
                (buffer1, buffer2) -> buffer1.merge(buffer2));

    DeltaCommittable committable =
        new DeltaCommittable(
            getContainingTask().getEnvironment().getJobID().toString(),
            getRuntimeContext().getOperatorUniqueID(),
            checkpointId,
            merged.getDeltaActions(),
            merged.getContext());
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
    // TODO control buffer size
    if (element.isRecord() && element.getValue() instanceof CommittableWithLineage) {
      LOG.debug("Received writerResult: {}", element.getValue());
      results.add(
          ((CommittableWithLineage<DeltaWriterResult>) element.getValue()).getCommittable());
    }
  }
}
