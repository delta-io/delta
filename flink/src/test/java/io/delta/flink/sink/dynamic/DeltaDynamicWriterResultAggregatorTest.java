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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.delta.flink.TestHelper;
import io.delta.flink.sink.WriterResultContext;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

/** JUnit tests for {@link DeltaDynamicWriterResultAggregator}. */
class DeltaDynamicWriterResultAggregatorTest extends TestHelper {

  @Test
  void testCheckpointWithNoInputOnlyEmitsSummary() throws Exception {
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<DeltaDynamicWriterResult>,
            CommittableMessage<DeltaDynamicCommittable>>
        harness = createHarness()) {
      harness.open();
      harness.prepareSnapshotPreBarrier(1L);
      List<CommittableMessage<DeltaDynamicCommittable>> out = extractCommittableMessages(harness);
      assertEquals(1, out.size());
      assertInstanceOf(CommittableSummary.class, out.get(0));
      CommittableSummary<?> summary = (CommittableSummary<?>) out.get(0);
      assertEquals(1L, summary.getCheckpointId());
      assertEquals(0, summary.getNumberOfCommittables());
      assertEquals(0, summary.getSubtaskId());
      assertEquals(1, summary.getNumberOfSubtasks());
    }
  }

  @Test
  void testCheckpointEmitsSummaryAndOneCommittablePerTable() throws Exception {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
    Row a = dummyAddFileRow(schema, 10, Map.of("part", Literal.ofString("east")));
    Row b = dummyAddFileRow(schema, 11, Map.of("part", Literal.ofString("west")));

    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<DeltaDynamicWriterResult>,
            CommittableMessage<DeltaDynamicCommittable>>
        harness = createHarness()) {
      harness.open();
      harness.processElement(
          new CommittableWithLineage<>(
              new DeltaDynamicWriterResult(
                  "cat.sch.t1", new ArrayList<>(List.of(a)), new WriterResultContext()),
              0L,
              0),
          0L);
      harness.processElement(
          new CommittableWithLineage<>(
              new DeltaDynamicWriterResult(
                  "cat.sch.t2", new ArrayList<>(List.of(b)), new WriterResultContext()),
              0L,
              0),
          0L);
      harness.prepareSnapshotPreBarrier(42L);

      List<CommittableMessage<DeltaDynamicCommittable>> out = extractCommittableMessages(harness);
      assertEquals(3, out.size());
      assertInstanceOf(CommittableSummary.class, out.get(0));
      CommittableSummary<?> summary = (CommittableSummary<?>) out.get(0);
      assertEquals(42L, summary.getCheckpointId());
      assertEquals(2, summary.getNumberOfCommittables());
      assertEquals(0, summary.getSubtaskId());
      assertEquals(1, summary.getNumberOfSubtasks());

      CommittableWithLineage<DeltaDynamicCommittable> line1 =
          (CommittableWithLineage<DeltaDynamicCommittable>) out.get(1);
      CommittableWithLineage<DeltaDynamicCommittable> line2 =
          (CommittableWithLineage<DeltaDynamicCommittable>) out.get(2);
      assertEquals(0, line1.getSubtaskId());
      assertEquals(0, line2.getSubtaskId());

      DeltaDynamicCommittable c1 = line1.getCommittable();
      DeltaDynamicCommittable c2 = line2.getCommittable();
      assertEquals("cat.sch.t1", c1.getTableName());
      assertEquals("cat.sch.t2", c2.getTableName());
      assertEquals(42L, c1.getCheckpointId());
      assertEquals(42L, c2.getCheckpointId());
      assertEquals(1, c1.getDeltaActions().size());
      assertEquals(1, c2.getDeltaActions().size());
      assertNotEquals(
          c1.getDeltaActions().get(0).hashCode(), c2.getDeltaActions().get(0).hashCode());
    }
  }

  @Test
  void testSameTableMergedIntoSingleCommittable() throws Exception {
    StructType schema = new StructType().add("id", IntegerType.INTEGER);
    Row r1 = dummyAddFileRow(schema, 1, Map.of());
    Row r2 = dummyAddFileRow(schema, 2, Map.of());

    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<DeltaDynamicWriterResult>,
            CommittableMessage<DeltaDynamicCommittable>>
        harness = createHarness()) {
      harness.open();
      harness.processElement(
          new CommittableWithLineage<>(
              new DeltaDynamicWriterResult(
                  "tbl", new ArrayList<>(List.of(r1)), new WriterResultContext(10L, 20L)),
              0L,
              0),
          0L);
      harness.processElement(
          new CommittableWithLineage<>(
              new DeltaDynamicWriterResult(
                  "tbl", new ArrayList<>(List.of(r2)), new WriterResultContext(5L, 30L)),
              0L,
              0),
          0L);
      harness.prepareSnapshotPreBarrier(7L);

      List<CommittableMessage<DeltaDynamicCommittable>> out = extractCommittableMessages(harness);
      assertEquals(2, out.size());
      CommittableSummary<?> summary = (CommittableSummary<?>) out.get(0);
      assertEquals(1, summary.getNumberOfCommittables());

      DeltaDynamicCommittable c =
          ((CommittableWithLineage<DeltaDynamicCommittable>) out.get(1)).getCommittable();
      assertEquals("tbl", c.getTableName());
      assertEquals(7L, c.getCheckpointId());
      assertEquals(2, c.getDeltaActions().size());
      assertEquals(5L, c.getContext().getLowWatermark());
      assertEquals(30L, c.getContext().getHighWatermark());
      assertNotEquals(c.getDeltaActions().get(0).hashCode(), c.getDeltaActions().get(1).hashCode());
      assertEquals(0, ((CommittableSummary<?>) out.get(0)).getSubtaskId());
      assertEquals(1, ((CommittableSummary<?>) out.get(0)).getNumberOfSubtasks());
      assertEquals(
          0, ((CommittableWithLineage<DeltaDynamicCommittable>) out.get(1)).getSubtaskId());
    }
  }

  @Test
  void testParallelAggregatorSubtasksUseDistinctSummaryIds() throws Exception {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
    Row r0 = dummyAddFileRow(schema, 1, Map.of("part", Literal.ofString("x")));

    DeltaDynamicWriterResultAggregator agg0 = new DeltaDynamicWriterResultAggregator();
    DeltaDynamicWriterResultAggregator agg1 = new DeltaDynamicWriterResultAggregator();
    try (OneInputStreamOperatorTestHarness<
                CommittableMessage<DeltaDynamicWriterResult>,
                CommittableMessage<DeltaDynamicCommittable>>
            h0 = new OneInputStreamOperatorTestHarness<>(agg0, 2, 2, 0);
        OneInputStreamOperatorTestHarness<
                CommittableMessage<DeltaDynamicWriterResult>,
                CommittableMessage<DeltaDynamicCommittable>>
            h1 = new OneInputStreamOperatorTestHarness<>(agg1, 2, 2, 1)) {
      h0.setup();
      h1.setup();
      h0.open();
      h1.open();

      h0.processElement(
          new CommittableWithLineage<>(
              new DeltaDynamicWriterResult(
                  "cat.sch.ta", new ArrayList<>(List.of(r0)), new WriterResultContext()),
              0L,
              0),
          0L);
      h1.processElement(
          new CommittableWithLineage<>(
              new DeltaDynamicWriterResult(
                  "cat.sch.tb", new ArrayList<>(List.of(r0)), new WriterResultContext()),
              0L,
              0),
          0L);

      h0.prepareSnapshotPreBarrier(100L);
      h1.prepareSnapshotPreBarrier(100L);

      List<CommittableMessage<DeltaDynamicCommittable>> out0 = extractCommittableMessages(h0);
      List<CommittableMessage<DeltaDynamicCommittable>> out1 = extractCommittableMessages(h1);
      CommittableSummary<?> s0 = (CommittableSummary<?>) out0.get(0);
      CommittableSummary<?> s1 = (CommittableSummary<?>) out1.get(0);

      assertEquals(0, s0.getSubtaskId());
      assertEquals(1, s1.getSubtaskId());
      assertEquals(2, s0.getNumberOfSubtasks());
      assertEquals(2, s1.getNumberOfSubtasks());
    }
  }

  @Test
  void testSecondCheckpointStartsFreshAfterEmit() throws Exception {
    StructType schema = new StructType().add("id", IntegerType.INTEGER);
    Row r = dummyAddFileRow(schema, 1, Map.of());

    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<DeltaDynamicWriterResult>,
            CommittableMessage<DeltaDynamicCommittable>>
        harness = createHarness()) {
      harness.open();
      harness.processElement(
          new CommittableWithLineage<>(
              new DeltaDynamicWriterResult(
                  "only", new ArrayList<>(List.of(r)), new WriterResultContext()),
              0L,
              0),
          0L);
      harness.prepareSnapshotPreBarrier(1L);
      List<CommittableMessage<DeltaDynamicCommittable>> first = extractCommittableMessages(harness);
      assertEquals(2, first.size());

      harness.prepareSnapshotPreBarrier(2L);
      List<CommittableMessage<DeltaDynamicCommittable>> all = extractCommittableMessages(harness);

      assertEquals(3, all.size());
      assertInstanceOf(CommittableSummary.class, all.get(2));
      CommittableSummary<?> secondSummary = (CommittableSummary<?>) all.get(2);
      assertEquals(2L, secondSummary.getCheckpointId());
      assertEquals(0, secondSummary.getNumberOfCommittables());
    }
  }

  private static OneInputStreamOperatorTestHarness<
          CommittableMessage<DeltaDynamicWriterResult>, CommittableMessage<DeltaDynamicCommittable>>
      createHarness() throws Exception {
    DeltaDynamicWriterResultAggregator aggregator = new DeltaDynamicWriterResultAggregator();
    OneInputStreamOperatorTestHarness<
            CommittableMessage<DeltaDynamicWriterResult>,
            CommittableMessage<DeltaDynamicCommittable>>
        harness = new OneInputStreamOperatorTestHarness<>(aggregator, 1, 1, 0);
    harness.setup();
    return harness;
  }

  /**
   * Normalized view of operator output: {@link OneInputStreamOperatorTestHarness} may return {@link
   * StreamRecord} wrappers or bare {@link CommittableMessage} values depending on version.
   */
  @SuppressWarnings("unchecked")
  private static List<CommittableMessage<DeltaDynamicCommittable>> extractCommittableMessages(
      OneInputStreamOperatorTestHarness<
              CommittableMessage<DeltaDynamicWriterResult>,
              CommittableMessage<DeltaDynamicCommittable>>
          harness) {
    List<CommittableMessage<DeltaDynamicCommittable>> out = new ArrayList<>();
    for (Object o : harness.extractOutputValues()) {
      if (o instanceof StreamRecord) {
        Object v = ((StreamRecord<?>) o).getValue();
        out.add((CommittableMessage<DeltaDynamicCommittable>) v);
      } else {
        out.add((CommittableMessage<DeltaDynamicCommittable>) o);
      }
    }
    return out;
  }
}
