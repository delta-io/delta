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

import java.io.IOException;
import java.util.*;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pre-commit operator that aggregates {@link DeltaDynamicWriterResult}s and emits one {@link
 * DeltaDynamicCommittable} per distinct table per checkpoint.
 *
 * <p>The upstream topology keys committables by table name and runs this operator at parallelism
 * {@code P}, so each subtask merges only the tables mapped to it. At each checkpoint a subtask
 * emits a {@link CommittableSummary} with its own subtask id and {@code P} as {@link
 * CommittableSummary#getNumberOfSubtasks()}, plus one {@link CommittableWithLineage} per table
 * handled on that subtask, preserving Flink sink v2 bookkeeping.
 */
public class DeltaDynamicWriterResultAggregator
    extends AbstractStreamOperator<CommittableMessage<DeltaDynamicCommittable>>
    implements OneInputStreamOperator<
        CommittableMessage<DeltaDynamicWriterResult>, CommittableMessage<DeltaDynamicCommittable>> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DeltaDynamicWriterResultAggregator.class);

  /** Accumulated results from this checkpoint interval, keyed by table name. */
  private final Map<String, DeltaDynamicWriterResult> resultsByTable;

  public DeltaDynamicWriterResultAggregator() {
    resultsByTable = new LinkedHashMap<>();
  }

  @Override
  public void finish() throws IOException {
    LOG.debug("Finishing");
    prepareSnapshotPreBarrier(Long.MAX_VALUE);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws IOException {
    LOG.debug("Aggregating {} tables on checkpoint {}", resultsByTable.size(), checkpointId);

    String jobId = getContainingTask().getEnvironment().getJobID().toString();
    String operatorId = getRuntimeContext().getOperatorUniqueID();
    int numTables = resultsByTable.size();
    int subtaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    int numSubtasks = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();

    CommittableMessage<DeltaDynamicCommittable> summary =
        new CommittableSummary<>(subtaskId, numSubtasks, checkpointId, numTables, 0);
    output.collect(new StreamRecord<>(summary));

    for (DeltaDynamicWriterResult tableResult : resultsByTable.values()) {
      DeltaDynamicCommittable committable =
          new DeltaDynamicCommittable(
              jobId,
              operatorId,
              checkpointId,
              tableResult.getTableName(),
              tableResult.getDeltaActions(),
              tableResult.getContext());
      output.collect(
          new StreamRecord<>(new CommittableWithLineage<>(committable, checkpointId, subtaskId)));
      LOG.debug(
          "Emitted committable for table '{}' on checkpoint {} with {} actions",
          tableResult.getTableName(),
          checkpointId,
          tableResult.getDeltaActions().size());
    }

    resultsByTable.clear();
  }

  @Override
  public void processElement(StreamRecord<CommittableMessage<DeltaDynamicWriterResult>> element)
      throws Exception {
    if (!(element.isRecord() && element.getValue() instanceof CommittableWithLineage)) {
      return;
    }
    DeltaDynamicWriterResult incoming =
        ((CommittableWithLineage<DeltaDynamicWriterResult>) element.getValue()).getCommittable();
    LOG.debug("Received writer result: {}", incoming);

    String key = incoming.getTableName();
    resultsByTable.merge(
        key,
        new DeltaDynamicWriterResult(
            incoming.getTableName(),
            new ArrayList<>(incoming.getDeltaActions()),
            incoming.getContext()),
        DeltaDynamicWriterResult::merge);
  }
}
