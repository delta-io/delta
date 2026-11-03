/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class DeltaWriterResultAggregatorTest {

  @Test
  void finishPreservesIncomingCheckpointId() throws Exception {
    DeltaWriterResultAggregator aggregator = new DeltaWriterResultAggregator();
    OneInputStreamOperatorTestHarness<
            CommittableMessage<DeltaWriterResult>, CommittableMessage<DeltaCommittable>>
        harness = new OneInputStreamOperatorTestHarness<>(aggregator);
    try {
      harness.open();
      long checkpointId = 42;
      harness.processElement(
          new StreamRecord<>(new CommittableSummary<>(0, 1, checkpointId, 1, 1)));
      harness.processElement(
          new StreamRecord<>(
              new CommittableWithLineage<>(new DeltaWriterResult(), checkpointId, 0)));

      aggregator.finish();

      List<CommittableMessage<DeltaCommittable>> output = harness.extractOutputValues();
      assertEquals(2, output.size());
      assertInstanceOf(CommittableSummary.class, output.get(0));
      assertEquals(checkpointId, output.get(0).getCheckpointId());
      CommittableWithLineage<?> committable =
          assertInstanceOf(CommittableWithLineage.class, output.get(1));
      assertEquals(checkpointId, committable.getCheckpointId());
      DeltaCommittable deltaCommittable =
          assertInstanceOf(DeltaCommittable.class, committable.getCommittable());
      assertEquals(checkpointId, deltaCommittable.getCheckpointId());
    } finally {
      harness.close();
    }
  }

  @Test
  void rejectsMixedCheckpointIds() throws Exception {
    DeltaWriterResultAggregator aggregator = new DeltaWriterResultAggregator();
    OneInputStreamOperatorTestHarness<
            CommittableMessage<DeltaWriterResult>, CommittableMessage<DeltaCommittable>>
        harness = new OneInputStreamOperatorTestHarness<>(aggregator);
    try {
      harness.open();
      harness.processElement(new StreamRecord<>(new CommittableSummary<>(0, 1, 42, 1, 1)));

      IllegalStateException exception =
          assertThrows(
              IllegalStateException.class,
              () ->
                  harness.processElement(
                      new StreamRecord<>(
                          new CommittableWithLineage<>(new DeltaWriterResult(), 43, 0))));
      assertEquals(
          "Received results from checkpoints 42 and 43 in one aggregation", exception.getMessage());
    } finally {
      harness.close();
    }
  }

  @Test
  void rejectsMismatchedCheckpointBarrier() throws Exception {
    DeltaWriterResultAggregator aggregator = new DeltaWriterResultAggregator();
    OneInputStreamOperatorTestHarness<
            CommittableMessage<DeltaWriterResult>, CommittableMessage<DeltaCommittable>>
        harness = new OneInputStreamOperatorTestHarness<>(aggregator);
    try {
      harness.open();
      harness.processElement(new StreamRecord<>(new CommittableSummary<>(0, 1, 42, 1, 1)));

      IllegalStateException exception =
          assertThrows(IllegalStateException.class, () -> aggregator.prepareSnapshotPreBarrier(43));
      assertEquals(
          "Cannot aggregate results from checkpoint 42 into checkpoint 43", exception.getMessage());
    } finally {
      harness.close();
    }
  }
}
