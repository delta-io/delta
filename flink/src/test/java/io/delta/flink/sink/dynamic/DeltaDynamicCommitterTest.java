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
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.flink.TestHelper;
import io.delta.flink.sink.WriterResultContext;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.mocks.MockCommitRequest;
import org.apache.flink.runtime.metrics.groups.InternalSinkCommitterMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.junit.jupiter.api.Test;

/** JUnit tests for {@link DeltaDynamicCommitter}. */
class DeltaDynamicCommitterTest extends TestHelper {

  private static final String JOB = "dynamic-test-job";
  private static final String OPR = "dynamic-test-opr";
  private static final int TASK_INDEX = 2;

  private final InternalSinkCommitterMetricGroup metricGroup =
      InternalSinkCommitterMetricGroup.wrap(
          UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup());

  @Test
  void testCommitSingleTableSingleCheckpoint() throws Exception {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
    String tableName = "warehouse.schema.t_one";
    ProviderFixture fx = new ProviderFixture(schema);
    DeltaDynamicCommitter committer = buildCommitter(fx.provider());

    List<Committer.CommitRequest<DeltaDynamicCommittable>> requests =
        List.of(
            mockRequest(
                new DeltaDynamicCommittable(
                    JOB,
                    OPR,
                    1000L,
                    tableName,
                    IntStream.range(0, 5)
                        .mapToObj(
                            i ->
                                dummyAddFileRow(
                                    schema, 10 + i, Map.of("part", Literal.ofString("p" + i))))
                        .collect(Collectors.toList()),
                    new WriterResultContext())));

    committer.commit(requests);
    committer.close();

    StubDeltaTable table = fx.stubFor(tableName);
    assertEquals(1, table.commitRecords.size());
    assertEquals(1, table.getRefreshCallCount());
    StubDeltaTable.CommitRecord inv = table.commitRecords.get(0);
    assertEquals(5, inv.getActionCount());
    assertEquals(1000L, inv.getTxnId());
    assertEquals(committerAppId(tableName), inv.getAppId());
    assertEquals(1, fx.creates.get());
  }

  @Test
  void testCommitMultipleCheckpointsSortedByCheckpointId() throws Exception {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
    String tableName = "t.multi.cp";
    ProviderFixture fx = new ProviderFixture(schema);
    DeltaDynamicCommitter committer = buildCommitter(fx.provider());

    List<Committer.CommitRequest<DeltaDynamicCommittable>> requests =
        List.of(
            mockRequest(singleFileCommittable(schema, tableName, 2L, "y2")),
            mockRequest(singleFileCommittable(schema, tableName, 0L, "y0")),
            mockRequest(singleFileCommittable(schema, tableName, 1L, "y1")));

    committer.commit(requests);
    committer.close();

    StubDeltaTable table = fx.stubFor(tableName);
    assertEquals(3, table.commitRecords.size());
    assertEquals(3, table.getRefreshCallCount());
    assertEquals(0L, table.commitRecords.get(0).getTxnId());
    assertEquals(1L, table.commitRecords.get(1).getTxnId());
    assertEquals(2L, table.commitRecords.get(2).getTxnId());
  }

  @Test
  void testCommitTwoTablesRoutesToSeparateTableInstances() throws Exception {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
    String nameA = "cat.sch.a";
    String nameB = "cat.sch.b";
    ProviderFixture fx = new ProviderFixture(schema);
    DeltaDynamicCommitter committer = buildCommitter(fx.provider());

    committer.commit(
        List.of(
            mockRequest(singleFileCommittable(schema, nameA, 1L, "onlyA")),
            mockRequest(singleFileCommittable(schema, nameB, 1L, "onlyB"))));
    committer.close();

    assertEquals(1, fx.stubFor(nameA).commitRecords.size());
    assertEquals(1, fx.stubFor(nameB).commitRecords.size());
    assertEquals(1, fx.stubFor(nameA).commitRecords.get(0).getActionCount());
    assertEquals(1, fx.stubFor(nameB).commitRecords.get(0).getActionCount());
    assertEquals(2, fx.creates.get());
  }

  @Test
  void testCommitMergesWatermarksForRequestsInSameCheckpoint() throws Exception {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
    String tableName = "wm.merge";
    ProviderFixture fx = new ProviderFixture(schema);
    DeltaDynamicCommitter committer = buildCommitter(fx.provider());

    committer.commit(
        List.of(
            mockRequest(
                new DeltaDynamicCommittable(
                    JOB,
                    OPR,
                    7L,
                    tableName,
                    List.of(dummyAddFileRow(schema, 10, Map.of("part", Literal.ofString("x")))),
                    new WriterResultContext(100, 200))),
            mockRequest(
                new DeltaDynamicCommittable(
                    JOB,
                    OPR,
                    7L,
                    tableName,
                    List.of(dummyAddFileRow(schema, 11, Map.of("part", Literal.ofString("y")))),
                    new WriterResultContext(50, 250)))));
    committer.close();

    StubDeltaTable table = fx.stubFor(tableName);
    assertEquals(1, table.commitRecords.size());
    StubDeltaTable.CommitRecord inv = table.commitRecords.get(0);
    assertEquals(2, inv.getActionCount());
    assertEquals(
        Map.of("flink.low-watermark", "50", "flink.high-watermark", "250"), inv.getProperties());
  }

  @Test
  void testTableProviderInvokedOncePerTableWithCache() throws Exception {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
    String tableName = "cache.once";
    ProviderFixture fx = new ProviderFixture(schema);
    DeltaDynamicCommitter committer = buildCommitter(fx.provider());

    committer.commit(List.of(mockRequest(singleFileCommittable(schema, tableName, 1L, "p1"))));
    committer.commit(List.of(mockRequest(singleFileCommittable(schema, tableName, 2L, "p2"))));

    assertEquals(1, fx.creates.get());
    assertEquals(2, fx.stubFor(tableName).commitRecords.size());
    committer.close();
  }

  @Test
  void testCommitEmptyBatchDoesNotLoadTable() throws Exception {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
    ProviderFixture fx = new ProviderFixture(schema);
    DeltaDynamicCommitter committer = buildCommitter(fx.provider());
    committer.commit(Collections.emptyList());
    committer.close();
    assertEquals(0, fx.creates.get());
  }

  @Test
  void testBuildRequiresJobId() {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
    assertThrows(
        NullPointerException.class,
        () ->
            new DeltaDynamicCommitter.Builder()
                .withTaskIndex(0)
                .withTableProvider(new ProviderFixture(schema).provider())
                .withMetricGroup(metricGroup)
                .build());
  }

  private static String committerAppId(String tableName) {
    return String.format("%s-%d-%s", JOB, TASK_INDEX, tableName);
  }

  private DeltaDynamicCommitter buildCommitter(DeltaTableProvider provider) {
    return new DeltaDynamicCommitter.Builder()
        .withJobId(JOB)
        .withTaskIndex(TASK_INDEX)
        .withTableProvider(provider)
        .withMetricGroup(metricGroup)
        .build();
  }

  private DeltaDynamicCommittable singleFileCommittable(
      StructType schema, String tableName, long checkpointId, String partValue) {
    return new DeltaDynamicCommittable(
        JOB,
        OPR,
        checkpointId,
        tableName,
        List.of(dummyAddFileRow(schema, 10, Map.of("part", Literal.ofString(partValue)))),
        new WriterResultContext());
  }

  private static MockCommitRequest<DeltaDynamicCommittable> mockRequest(DeltaDynamicCommittable c) {
    return new MockCommitRequest<>(c);
  }
}
