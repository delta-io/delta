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

import static org.junit.jupiter.api.Assertions.*;

import io.delta.flink.TestHelper;
import io.delta.flink.table.HadoopTable;
import io.delta.flink.table.LocalFileSystemTable;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.mocks.MockCommitRequest;
import org.apache.flink.runtime.metrics.groups.InternalSinkCommitterMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** JUnit test suite for {@link DeltaCommitter}. */
class DeltaCommitterTest extends TestHelper {

  private final InternalSinkCommitterMetricGroup metricGroup =
      InternalSinkCommitterMetricGroup.wrap(
          UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup());

  @Test
  void testCommitWithSingleCheckpointToAnEmptyTable() {
    withTempDir(
        dir -> {
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

          HadoopTable table =
              new HadoopTable(dir.toURI(), Collections.emptyMap(), schema, List.of("part"));
          table.open();

          DeltaCommitter committer =
              new DeltaCommitter.Builder()
                  .withDeltaTable(table)
                  .withJobId("test-job")
                  .withMetricGroup(metricGroup)
                  .withConf(new DeltaSinkConf(schema, Collections.emptyMap()))
                  .build();

          // By the way we direct the stream traffic, we should receive only one committable.
          List<Committer.CommitRequest<DeltaCommittable>> commitMessages =
              List.of(
                  new MockCommitRequest<>(
                      new DeltaCommittable(
                          "test-job",
                          "test-opr",
                          1000L,
                          IntStream.range(0, 5)
                              .mapToObj(
                                  i ->
                                      dummyAddFileRow(
                                          schema,
                                          10 + i,
                                          Map.of(
                                              "part",
                                              io.delta.kernel.expressions.Literal.ofString(
                                                  "p" + i))))
                              .collect(Collectors.toList()),
                          new WriterResultContext())));

          committer.commit(commitMessages);

          // The target table should have one version
          verifyTableContent(
              dir.toString(),
              (version, actions, props) -> {
                assertEquals(1L, version);
                // There should be 5 files to scan
                List<AddFile> actionList = new ArrayList<>();
                actions.iterator().forEachRemaining(actionList::add);
                assertEquals(5, actionList.size());
                Set<String> partitions =
                    actionList.stream()
                        .map(a -> a.getPartitionValues().getValues().getString(0))
                        .collect(Collectors.toSet());
                assertEquals(Set.of("p0", "p1", "p2", "p3", "p4"), partitions);
                assertEquals(60, actionList.stream().mapToLong(a -> a.getNumRecords().get()).sum());
              });
        });
  }

  @ParameterizedTest
  @CsvSource({"false, false", "true, false", "true, true"})
  void testRestoreWithNewJobId(boolean alreadyCommitted, boolean removed) {
    withTempDir(
        dir -> {
          StructType schema = new StructType().add("id", IntegerType.INTEGER);
          HadoopTable table = new HadoopTable(dir.toURI(), Map.of(), schema, List.of());
          table.open();
          try {
            DeltaSinkConf conf = new DeltaSinkConf(schema, Map.of());
            DeltaCommitter original =
                new DeltaCommitter.Builder()
                    .withDeltaTable(table)
                    .withTaskIndex(3)
                    .withJobId("original-job")
                    .withMetricGroup(metricGroup)
                    .withConf(conf)
                    .build();
            DeltaCommittable pending =
                new DeltaCommittable(
                    "original-job",
                    "test-operator",
                    1L,
                    List.of(dummyAddFileRow(schema, 1, Map.of())),
                    new WriterResultContext());
            DeltaCommittable.Serializer serializer = new DeltaCommittable.Serializer();
            byte[] checkpoint = serializer.serialize(pending);
            if (alreadyCommitted) {
              original.commit(List.of(new MockCommitRequest<>(pending)));
            }
            if (removed) {
              List<Row> removes = new ArrayList<>();
              verifyTableContent(
                  dir.toString(),
                  (version, files, props) ->
                      files.forEach(
                          file ->
                              removes.add(
                                  SingleAction.createRemoveFileSingleAction(
                                      file.toRemoveFileRow(true, Optional.empty())))));
              table.commit(
                  CloseableIterable.inMemoryIterable(Utils.toCloseableIterator(removes.iterator())),
                  "maintenance",
                  1L,
                  Map.of());
            }

            DeltaCommitter restored =
                new DeltaCommitter.Builder()
                    .withDeltaTable(table)
                    .withTaskIndex(3)
                    .withMetricGroup(metricGroup)
                    .withConf(conf)
                    .build();
            DeltaCommittable recovered =
                serializer.deserialize(serializer.getVersion(), checkpoint);
            restored.commit(List.of(new MockCommitRequest<>(recovered)));
            SnapshotImpl snapshot =
                (SnapshotImpl) TableManager.loadSnapshot(dir.toString()).build(table.getEngine());
            assertEquals(
                Optional.of(1L),
                snapshot.getLatestTransactionVersion(table.getEngine(), "original-job-3"));
            assertEquals(
                Optional.empty(),
                snapshot.getLatestTransactionVersion(table.getEngine(), "restored-job-3"));
            long expectedVersion = removed ? 2L : 1L;
            int expectedFiles = removed ? 0 : 1;
            verifyTableContent(
                dir.toString(),
                (version, files, props) -> {
                  assertEquals(expectedVersion, version);
                  assertEquals(
                      expectedFiles, StreamSupport.stream(files.spliterator(), false).count());
                });

            // Defensive coverage: submit different job IDs with the same checkpoint number.
            // Flink 2.3 normally commits each checkpoint manager separately.
            DeltaCommittable fresh =
                new DeltaCommittable(
                    "restored-job",
                    "test-operator",
                    1L,
                    List.of(dummyAddFileRow(schema, 1, Map.of())),
                    new WriterResultContext());
            for (int attempt = 0; attempt < 2; attempt++) {
              restored.commit(
                  List.of(new MockCommitRequest<>(recovered), new MockCommitRequest<>(fresh)));
              snapshot =
                  (SnapshotImpl) TableManager.loadSnapshot(dir.toString()).build(table.getEngine());
              assertEquals(
                  Optional.of(1L),
                  snapshot.getLatestTransactionVersion(table.getEngine(), "original-job-3"));
              assertEquals(
                  Optional.of(1L),
                  snapshot.getLatestTransactionVersion(table.getEngine(), "restored-job-3"));
              verifyTableContent(
                  dir.toString(),
                  (version, files, props) -> {
                    assertEquals(expectedVersion + 1, version);
                    assertEquals(
                        expectedFiles + 1,
                        StreamSupport.stream(files.spliterator(), false).count());
                  });
            }
          } finally {
            table.close();
          }
        });
  }

  @Test
  void testCommitWithMultipleCheckpoints() {
    withTempDir(
        dir -> {
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

          HadoopTable table =
              new HadoopTable(
                  dir.toPath().toUri(), Collections.emptyMap(), schema, List.of("part"));
          table.open();

          DeltaCommitter committer =
              new DeltaCommitter.Builder()
                  .withDeltaTable(table)
                  .withJobId("test-job")
                  .withMetricGroup(metricGroup)
                  .withConf(new DeltaSinkConf(schema, Collections.emptyMap()))
                  .build();

          // Three checkpoints, each contains 5 add files
          List<Committer.CommitRequest<DeltaCommittable>> commitMessages =
              IntStream.range(0, 3)
                  .mapToObj(
                      i ->
                          new MockCommitRequest<>(
                              new DeltaCommittable(
                                  "test-job",
                                  "test-opr",
                                  i,
                                  IntStream.range(0, 5)
                                      .mapToObj(
                                          j ->
                                              dummyAddFileRow(
                                                  schema,
                                                  j + 10,
                                                  Map.of(
                                                      "part",
                                                      io.delta.kernel.expressions.Literal.ofString(
                                                          "p" + j))))
                                      .collect(Collectors.toList()),
                                  new WriterResultContext())))
                  .collect(Collectors.toList());

          committer.commit(commitMessages);

          verifyTableContent(
              dir.toString(),
              (version, actions, props) -> {
                assertEquals(3L, version);
                // There should be 15 files to scan
                List<AddFile> actionList = new ArrayList<>();
                actions.iterator().forEachRemaining(actionList::add);
                assertEquals(15, actionList.size());
                Set<String> partitions =
                    actionList.stream()
                        .map(a -> a.getPartitionValues().getValues().getString(0))
                        .collect(Collectors.toSet());
                assertEquals(Set.of("p0", "p1", "p2", "p3", "p4"), partitions);
                assertEquals(
                    180, actionList.stream().mapToLong(a -> a.getNumRecords().get()).sum());
              });
        });
  }

  @Test
  void testCommitWithNoSchemaEvolutionPolicy() {
    withTempDir(
        dir -> {
          DefaultEngine engine = DefaultEngine.create(new Configuration());
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
          StructType anotherSchema =
              new StructType().add("v1", StringType.STRING).add("v2", StringType.STRING);
          createNonEmptyTable(
              engine, dir.getAbsolutePath(), anotherSchema, List.of("v1"), Collections.emptyMap());

          HadoopTable table =
              new HadoopTable(dir.toURI(), Collections.emptyMap(), schema, List.of("part"));
          table.open();

          DeltaCommitter committer =
              new DeltaCommitter.Builder()
                  .withDeltaTable(table)
                  .withJobId("test-job")
                  .withMetricGroup(metricGroup)
                  .withConf(new DeltaSinkConf(schema, Collections.emptyMap()))
                  .build();

          List<Committer.CommitRequest<DeltaCommittable>> commitMessages =
              List.of(
                  new MockCommitRequest<>(
                      new DeltaCommittable(
                          "test-job",
                          "test-opr",
                          1000L,
                          IntStream.range(0, 5)
                              .mapToObj(
                                  i ->
                                      dummyAddFileRow(
                                          schema,
                                          i + 10,
                                          Map.of(
                                              "part",
                                              io.delta.kernel.expressions.Literal.ofString(
                                                  "p" + i))))
                              .collect(Collectors.toList()),
                          new WriterResultContext())));

          IllegalStateException e =
              assertThrows(IllegalStateException.class, () -> committer.commit(commitMessages));
          assertTrue(
              e.getMessage().contains("Invalid schema evolution observed, aborting committing"));
        });
  }

  @Test
  void testCommitWithNewColumnSchemaEvolutionPolicy() {
    withTempDir(
        dir -> {
          DefaultEngine engine = DefaultEngine.create(new Configuration());
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
          StructType anotherSchema =
              new StructType()
                  .add("id", IntegerType.INTEGER)
                  .add("part", StringType.STRING)
                  .add("another", StringType.STRING);

          createNonEmptyTable(
              engine,
              dir.getAbsolutePath(),
              anotherSchema,
              Collections.emptyList(),
              Collections.emptyMap());

          LocalFileSystemTable table =
              new LocalFileSystemTable(
                  dir.toURI(), Collections.emptyMap(), anotherSchema, Collections.emptyList());
          table.open();

          DeltaCommitter committer =
              new DeltaCommitter.Builder()
                  .withDeltaTable(table)
                  .withJobId("test-job")
                  .withMetricGroup(metricGroup)
                  .withConf(new DeltaSinkConf(schema, Map.of("schema_evolution.mode", "newcolumn")))
                  .build();

          List<Committer.CommitRequest<DeltaCommittable>> commitMessages =
              List.of(
                  new MockCommitRequest<>(
                      new DeltaCommittable(
                          "test-job",
                          "test-opr",
                          1000L,
                          IntStream.range(0, 5)
                              .mapToObj(
                                  i ->
                                      dummyAddFileRow(
                                          schema,
                                          i + 10,
                                          Map.of(
                                              "part",
                                              io.delta.kernel.expressions.Literal.ofString(
                                                  "p" + i))))
                              .collect(Collectors.toList()),
                          new WriterResultContext())));

          committer.commit(commitMessages);
        });
  }

  @Test
  void testCommitMetricsOnSuccess() {
    withTempDir(
        dir -> {
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

          HadoopTable table =
              new HadoopTable(dir.toURI(), Collections.emptyMap(), schema, List.of("part"));
          table.open();

          TestSinkCommitterMetricGroup freshMetrics = new TestSinkCommitterMetricGroup();

          DeltaCommitter committer =
              new DeltaCommitter.Builder()
                  .withDeltaTable(table)
                  .withJobId("test-job")
                  .withMetricGroup(freshMetrics)
                  .withConf(new DeltaSinkConf(schema, Collections.emptyMap()))
                  .build();

          List<Committer.CommitRequest<DeltaCommittable>> commitMessages =
              List.of(
                  new MockCommitRequest<>(
                      new DeltaCommittable(
                          "test-job",
                          "test-opr",
                          1000L,
                          IntStream.range(0, 5)
                              .mapToObj(
                                  i ->
                                      dummyAddFileRow(
                                          schema,
                                          10 + i,
                                          Map.of(
                                              "part",
                                              io.delta.kernel.expressions.Literal.ofString(
                                                  "p" + i))))
                              .collect(Collectors.toList()),
                          new WriterResultContext())));

          committer.commit(commitMessages);

          assertEquals(1, freshMetrics.getNumCommittablesTotalCounter().getCount());
          assertEquals(1, freshMetrics.getNumCommittablesSuccessCounter().getCount());
          assertEquals(0, freshMetrics.getNumCommittablesFailureCounter().getCount());
          assertEquals(5, freshMetrics.counter("numActionsCommitted").getCount());
        });
  }

  @Test
  void testCommitMetricsOnSchemaEvolutionFailure() {
    withTempDir(
        dir -> {
          DefaultEngine engine = DefaultEngine.create(new Configuration());
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
          StructType anotherSchema =
              new StructType().add("v1", StringType.STRING).add("v2", StringType.STRING);
          createNonEmptyTable(
              engine, dir.getAbsolutePath(), anotherSchema, List.of("v1"), Collections.emptyMap());

          HadoopTable table =
              new HadoopTable(dir.toURI(), Collections.emptyMap(), schema, List.of("part"));
          table.open();

          TestSinkCommitterMetricGroup freshMetrics = new TestSinkCommitterMetricGroup();

          DeltaCommitter committer =
              new DeltaCommitter.Builder()
                  .withDeltaTable(table)
                  .withJobId("test-job")
                  .withMetricGroup(freshMetrics)
                  .withConf(new DeltaSinkConf(schema, Collections.emptyMap()))
                  .build();

          List<Committer.CommitRequest<DeltaCommittable>> commitMessages =
              List.of(
                  new MockCommitRequest<>(
                      new DeltaCommittable(
                          "test-job",
                          "test-opr",
                          1000L,
                          IntStream.range(0, 5)
                              .mapToObj(
                                  i ->
                                      dummyAddFileRow(
                                          schema,
                                          i + 10,
                                          Map.of(
                                              "part",
                                              io.delta.kernel.expressions.Literal.ofString(
                                                  "p" + i))))
                              .collect(Collectors.toList()),
                          new WriterResultContext())));

          assertThrows(IllegalStateException.class, () -> committer.commit(commitMessages));

          assertEquals(1, freshMetrics.getNumCommittablesTotalCounter().getCount());
          assertEquals(0, freshMetrics.getNumCommittablesSuccessCounter().getCount());
          assertEquals(1, freshMetrics.getNumCommittablesFailureCounter().getCount());
        });
  }

  @Test
  void testCommitWritesWatermarks() {
    withTempDir(
        dir -> {
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

          HadoopTable table =
              new HadoopTable(dir.toURI(), Collections.emptyMap(), schema, List.of("part"));
          table.open();

          DeltaCommitter committer =
              new DeltaCommitter.Builder()
                  .withDeltaTable(table)
                  .withJobId("test-job")
                  .withMetricGroup(metricGroup)
                  .withConf(new DeltaSinkConf(schema, Collections.emptyMap()))
                  .build();

          // By the way we direct the stream traffic, we should receive only one committable.
          List<Committer.CommitRequest<DeltaCommittable>> commitMessages =
              List.of(
                  new MockCommitRequest<>(
                      new DeltaCommittable(
                          "test-job",
                          "test-opr",
                          1000L,
                          IntStream.range(0, 5)
                              .mapToObj(
                                  i ->
                                      dummyAddFileRow(
                                          schema,
                                          10 + i,
                                          Map.of(
                                              "part",
                                              io.delta.kernel.expressions.Literal.ofString(
                                                  "p" + i))))
                              .collect(Collectors.toList()),
                          new WriterResultContext(100, 200))));

          committer.commit(commitMessages);

          // The target table should have one version
          verifyTableContent(
              dir.toString(),
              (version, actions, props) -> {
                assertEquals(1L, version);
                // There should be 5 files to scan
                List<AddFile> actionList = new ArrayList<>();
                actions.iterator().forEachRemaining(actionList::add);
                assertEquals(5, actionList.size());
                Set<String> partitions =
                    actionList.stream()
                        .map(a -> a.getPartitionValues().getValues().getString(0))
                        .collect(Collectors.toSet());
                assertEquals(Set.of("p0", "p1", "p2", "p3", "p4"), partitions);
                assertEquals(60, actionList.stream().mapToLong(a -> a.getNumRecords().get()).sum());
                assertEquals(
                    Map.of("flink.high-watermark", "200", "flink.low-watermark", "100"), props);
              });
        });
  }
}
