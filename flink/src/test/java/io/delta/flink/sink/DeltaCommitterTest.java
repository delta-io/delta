/*
 *  Copyright (2021) The Delta Lake Project Authors.
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

import static org.junit.jupiter.api.Assertions.*;

import io.delta.flink.TestHelper;
import io.delta.flink.table.HadoopTable;
import io.delta.flink.table.LocalFileSystemTable;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.types.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.mocks.MockCommitRequest;
import org.apache.flink.runtime.metrics.groups.InternalSinkCommitterMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/** JUnit 6 test suite for {@link DeltaCommitter}. */
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
