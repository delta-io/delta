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

package io.delta.flink.kernel;

import static io.delta.flink.kernel.CheckpointWriter.TAG_SIDECAR_COUNT;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static org.junit.jupiter.api.Assertions.*;

import io.delta.flink.TestHelper;
import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.checkpoints.CheckpointMetaData;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.kernel.utils.FileStatus;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/** JUnit test suite for checkpoint creation and validation. */
class CheckpointWriterTest extends TestHelper {

  /**
   * Helper method to create a table and write commits with a callback on each commit.
   *
   * @param engine Delta engine
   * @param tablePath path to the table
   * @param schema table schema
   * @param partitionCols partition columns
   * @param numCommits number of commits to write
   * @param callback callback invoked after each commit with the commit index (0-based) and snapshot
   */
  private void createTableAndWriteCommits(
      Engine engine,
      String tablePath,
      StructType schema,
      List<String> partitionCols,
      int numCommits,
      CommitCallback callback) {
    Map<String, String> properties = Map.of("delta.feature.v2Checkpoint", "supported");

    createNonEmptyTable(engine, tablePath, schema, partitionCols, properties);

    for (int i = 0; i < numCommits; i++) {
      Optional<Snapshot> snapshot = writeTable(engine, tablePath, schema, partitionCols);
      if (snapshot.isPresent() && callback != null) {
        callback.onCommit(i, snapshot.get());
      }
    }
  }

  @FunctionalInterface
  interface CommitCallback {
    void onCommit(int commitIndex, Snapshot snapshot);
  }

  private Optional<Snapshot> writeRemoveFile(
      Engine engine, String tablePath, StructType schema, List<String> partitionCols) {
    Map<String, Literal> partitionMap = new HashMap<>();
    for (String colName : partitionCols) {
      partitionMap.put(colName, dummyRandomLiteral(schema.get(colName).getDataType()));
    }

    // Prepare some dummy AddFile
    AddFile dummyAddFile =
        AddFile.convertDataFileStatus(
            schema,
            URI.create(tablePath),
            new DataFileStatus(UUID.randomUUID().toString(), 1000L, 2000L, Optional.empty()),
            partitionMap,
            true,
            Collections.emptyMap(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    var txn =
        TableManager.loadSnapshot(tablePath)
            .build(engine)
            .buildUpdateTableTransaction("dummy", Operation.WRITE)
            .build(engine);

    txn.commit(
            engine,
            CloseableIterable.inMemoryIterable(
                Utils.singletonCloseableIterator(
                    SingleAction.createAddFileSingleAction(dummyAddFile.toRow()))))
        .getPostCommitSnapshot();

    txn =
        TableManager.loadSnapshot(tablePath)
            .build(engine)
            .buildUpdateTableTransaction("dummy", Operation.WRITE)
            .build(engine);

    return txn.commit(
            engine,
            CloseableIterable.inMemoryIterable(
                Utils.singletonCloseableIterator(
                    SingleAction.createRemoveFileSingleAction(
                        dummyAddFile.toRemoveFileRow(true, Optional.empty())))))
        .getPostCommitSnapshot();
  }

  private void assertSnapshotRead(
      Engine engine, String tablePath, long version, int numSidecars, int numActions) {
    SnapshotImpl latest = (SnapshotImpl) TableManager.loadSnapshot(tablePath).build(engine);
    assertSnapshotRead(engine, latest, version, numSidecars, numActions);
  }

  private void assertSnapshotRead(
      Engine engine, SnapshotImpl snapshot, long version, int numSidecars, int numActions) {
    if (version >= 0) {
      assertEquals(version, snapshot.getSnapshotReport().getCheckpointVersion().orElse(-1L));
    }

    var files = snapshot.getScanBuilder().build().getScanFiles(engine).toInMemoryList();
    if (numSidecars >= 0) {
      long sidecarCount =
          files.stream()
              .map(FilteredColumnarBatch::getFilePath)
              .filter(path -> path.orElse("").contains("_sidecar"))
              .count();
      assertEquals(numSidecars, sidecarCount);
    }

    if (numActions >= 0) {
      List<AddFile> actions =
          files.stream()
              .flatMap(scanFile -> scanFile.getRows().toInMemoryList().stream())
              .map(row -> new AddFile(row.getStruct(0)))
              .collect(Collectors.toList());

      assertEquals(numActions, actions.size());
    }
  }

  /**
   * Reads and verifies the _last_checkpoint file.
   *
   * @param engine Delta engine
   * @param snapshot snapshot to get table path from
   * @param expectedVersion expected checkpoint version (-1 to skip verification)
   * @param expectedNumSidecars expected number of sidecars (-1 to skip verification)
   */
  private void assertLastCheckpointFile(
      Engine engine, Snapshot snapshot, int expectedVersion, int expectedNumSidecars) {
    String tablePath = snapshot.getPath();
    String lastCheckpointPath = tablePath + "/_delta_log/_last_checkpoint";

    try {
      var content =
          engine
              .getJsonHandler()
              .readJsonFiles(
                  singletonCloseableIterator(FileStatus.of(lastCheckpointPath)),
                  CheckpointMetaData.READ_SCHEMA,
                  Optional.empty())
              .flatMap(ColumnarBatch::getRows)
              .toInMemoryList()
              .get(0);

      CheckpointMetaData metadata = CheckpointMetaData.fromRow(content);

      // Verify version
      if (expectedVersion >= 0) {
        assertEquals(expectedVersion, metadata.version, "Checkpoint version mismatch");
      }

      // Verify tags contain TAG_FLINK_DELTASINK_CHECKPOINT
      assertTrue(
          Boolean.parseBoolean(
              metadata.tags.getOrDefault(CheckpointWriter.TAG_DELTASINK_CHECKPOINT, "false")),
          "Expected TAG_DELTASINK_CHECKPOINT to be true in _last_checkpoint");

      // Verify number of sidecars if parts are present
      if (expectedNumSidecars >= 0 && metadata.parts.isPresent()) {
        assertEquals(
            expectedNumSidecars,
            Integer.parseInt(metadata.tags.getOrDefault(TAG_SIDECAR_COUNT, "0")),
            "Number of sidecars mismatch in _last_checkpoint");
      }
    } catch (Exception e) {
      throw new AssertionError("Failed to read or verify _last_checkpoint", e);
    }
  }

  @Test
  void testCreateIncrementalCheckpoint() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          Engine engine = DefaultEngine.create(new Configuration());
          StructType schema = new StructType().add("id", IntegerType.INTEGER);

          createTableAndWriteCommits(
              engine,
              tablePath,
              schema,
              Collections.emptyList(),
              25,
              (i, snapshot) -> {
                if (i % 7 == 6) {
                  TestHelper.<Snapshot>wrap(s -> new CheckpointWriter(engine, s).write())
                      .accept(snapshot);
                  assertLastCheckpointFile(
                      engine, snapshot, /* version */ i + 1, /* numSidecar */ (i + 1) / 7);
                }
              });

          assertSnapshotRead(engine, tablePath, 21, 3, 26);
        });
  }

  @Test
  void testIgnoreNotMyCheckpoints() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          Engine engine = DefaultEngine.create(new Configuration());
          StructType schema = new StructType().add("id", IntegerType.INTEGER);

          createTableAndWriteCommits(
              engine,
              tablePath,
              schema,
              Collections.emptyList(),
              25,
              (i, snapshot) -> {
                if (i % 7 == 6) {
                  TestHelper.<Snapshot>wrap(s -> s.writeCheckpoint(engine)).accept(snapshot);
                }
                if (i == 24) {
                  TestHelper.<Snapshot>wrap(s -> new CheckpointWriter(engine, s).write())
                      .accept(snapshot);
                }
              });

          // 1 checkpoint, 1 sidecars
          assertSnapshotRead(engine, tablePath, 25, 1, 26);
        });
  }

  @Test
  void testCreateOnOlderSnapshots() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          Engine engine = DefaultEngine.create(new Configuration());
          StructType schema = new StructType().add("id", IntegerType.INTEGER);

          createTableAndWriteCommits(engine, tablePath, schema, Collections.emptyList(), 25, null);

          SnapshotImpl snapshot = (SnapshotImpl) TableManager.loadSnapshot(tablePath).build(engine);
          snapshot.writeCheckpoint(engine);

          SnapshotImpl oldSnapshot =
              (SnapshotImpl) TableManager.loadSnapshot(tablePath).atVersion(20).build(engine);
          new CheckpointWriter(engine, oldSnapshot).write();

          SnapshotImpl oldSnapshotAgain =
              (SnapshotImpl) TableManager.loadSnapshot(tablePath).atVersion(20).build(engine);

          assertEquals(20, oldSnapshotAgain.getSnapshotReport().getCheckpointVersion().orElse(-1L));

          assertSnapshotRead(engine, oldSnapshotAgain, -1, 1, 21);

          // Make sure _last_checkpoint is not changed
          String fileName = tablePath + "/_delta_log/_last_checkpoint";
          var content =
              engine
                  .getJsonHandler()
                  .readJsonFiles(
                      singletonCloseableIterator(FileStatus.of(fileName)),
                      CheckpointMetaData.READ_SCHEMA,
                      Optional.empty())
                  .toInMemoryList();

          List<CheckpointMetaData> checkpoints =
              content.stream()
                  .flatMap(columnarBatch -> columnarBatch.getRows().toInMemoryList().stream())
                  .map(CheckpointMetaData::fromRow)
                  .collect(Collectors.toList());

          assertEquals(25, checkpoints.get(0).version);
        });
  }

  @Test
  void testFallbackOnRemoveFiles() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          Engine engine = DefaultEngine.create(new Configuration());
          StructType schema = new StructType().add("id", IntegerType.INTEGER);

          createTableAndWriteCommits(
              engine,
              tablePath,
              schema,
              Collections.emptyList(),
              25,
              (i, snapshot) -> {
                if (i % 7 == 6) {
                  TestHelper.<Snapshot>wrap(s -> new CheckpointWriter(engine, s).write())
                      .accept(snapshot);
                }
              });

          var s = writeRemoveFile(engine, tablePath, schema, Collections.emptyList());
          s.ifPresent(wrap(sn -> new CheckpointWriter(engine, sn).write()));

          assertSnapshotRead(engine, tablePath, 27, 1, 26);
        });
  }

  @Test
  void testMergeManySidecars() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          Engine engine = DefaultEngine.create(new Configuration());
          StructType schema = new StructType().add("id", IntegerType.INTEGER);

          // Create multiple checkpoints to accumulate 5 sidecars
          createTableAndWriteCommits(
              engine,
              tablePath,
              schema,
              Collections.emptyList(),
              40,
              (i, snapshot) -> {
                if (i % 7 == 6) {
                  TestHelper.<Snapshot>wrap(s -> new CheckpointWriter(engine, s).write())
                      .accept(snapshot);
                }
              });

          // Verify we have accumulated the expected sidecars
          assertSnapshotRead(engine, tablePath, -1L, 5, -1);

          // Write one more checkpoint - this should merge the 5 old sidecars
          SnapshotImpl finalSnapshot =
              (SnapshotImpl) TableManager.loadSnapshot(tablePath).build(engine);
          new CheckpointWriter(engine, finalSnapshot, 5).write();

          // Load the snapshot again to get the latest checkpoint
          SnapshotImpl snapshotAfter =
              (SnapshotImpl) TableManager.loadSnapshot(tablePath).build(engine);

          assertSnapshotRead(engine, snapshotAfter, -1, 2, 41);
        });
  }

  @Test
  public void testBlockTableWithDomainMetadata() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          Engine engine = DefaultEngine.create(new Configuration());
          StructType schema = new StructType().add("id", IntegerType.INTEGER);
          Map<String, String> properties = Map.of("delta.feature.domainMetadata", "supported");
          Optional<Snapshot> snapshot =
              createNonEmptyTable(engine, tablePath, schema, List.of(), properties);

          try {
            new CheckpointWriter(engine, snapshot.get()).write();
            fail();
          } catch (IllegalArgumentException ignore) {

          }
        });
  }
}
