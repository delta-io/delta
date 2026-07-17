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
package io.delta.spark.internal.v2.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Snapshot;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.InternalRowTestUtils;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import java.io.File;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link DeltaV2StreamingWrite}; the streaming counterpart of DeltaV2BatchWriteTest.
 */
public class DeltaV2StreamingWriteTest extends DeltaV2TestBase {

  private static final StructType TABLE_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {
            DataTypes.createStructField("id", DataTypes.IntegerType, true),
            DataTypes.createStructField("name", DataTypes.StringType, true)
          });

  @Test
  public void testUseCommitCoordinator_isFalse(@TempDir File tempDir) {
    DeltaV2StreamingWrite write = newWrite(createTable(tempDir, "streaming_no_coordinator"));
    assertFalse(write.useCommitCoordinator());
  }

  @Test
  public void testCreateStreamingWriterFactory_returnsDeltaV2DataWriterFactory(
      @TempDir File tempDir) {
    DeltaV2StreamingWrite write = newWrite(createTable(tempDir, "streaming_factory_type"));
    StreamingDataWriterFactory factory =
        write.createStreamingWriterFactory(WriteTestUtils.physicalWriteInfo(1));
    assertTrue(factory instanceof DeltaV2DataWriterFactory);
  }

  /**
   * Distinct epochs each append: epoch 0 and epoch 1 accumulate, and the rows are the ones written.
   */
  @Test
  public void testCommit_distinctEpochsAppend(@TempDir File tempDir) throws Exception {
    String path = createTable(tempDir, "streaming_distinct_epochs");
    DeltaV2StreamingWrite write = newWrite(path);

    write.commit(0L, new WriterCommitMessage[] {writeEpoch(write, 0L, 1, "Alice", 2, "Bob")});
    write.commit(1L, new WriterCommitMessage[] {writeEpoch(write, 1L, 3, "Carol", 4, "Dave")});

    List<Row> rows = spark.read().format("delta").load(path).orderBy("id").collectAsList();
    assertEquals(4, rows.size(), "distinct epochs must accumulate");
    assertEquals("Alice", rows.get(0).getString(1));
    assertEquals("Bob", rows.get(1).getString(1));
    assertEquals("Carol", rows.get(2).getString(1));
    assertEquals("Dave", rows.get(3).getString(1));
  }

  /**
   * A concurrent data-only append must not break the stream: the per-epoch snapshot refresh picks
   * it up and this epoch commits on top of it rather than clobbering it.
   */
  @Test
  public void testCommit_toleratesConcurrentDataAppend(@TempDir File tempDir) throws Exception {
    String path = createTable(tempDir, "streaming_concurrent_append");
    DeltaV2StreamingWrite write = newWrite(path);

    // A concurrent writer appends to the same table after the streaming write was constructed.
    spark.sql("INSERT INTO delta.`" + path + "` VALUES (99, 'Concurrent')");

    write.commit(0L, new WriterCommitMessage[] {writeEpoch(write, 0L, 1, "Alice")});

    List<Row> rows = spark.read().format("delta").load(path).orderBy("id").collectAsList();
    assertEquals(2, rows.size(), "epoch must commit on top of the concurrent append, not clobber");
    assertEquals("Alice", rows.get(0).getString(1));
    assertEquals("Concurrent", rows.get(1).getString(1));
  }

  /**
   * A schema change after the stream started must fail the epoch loudly (the guard), not silently
   * append files written against the stale schema.
   */
  @Test
  public void testCommit_failsOnConcurrentSchemaChange(@TempDir File tempDir) throws Exception {
    String path = createTable(tempDir, "streaming_schema_change");
    DeltaV2StreamingWrite write = newWrite(path);
    WriterCommitMessage[] messages = {writeEpoch(write, 0L, 1, "Alice", 2, "Bob")};

    // Concurrent column add, after the write's schema was frozen at construction.
    spark.sql("ALTER TABLE delta.`" + path + "` ADD COLUMN (extra STRING)");

    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> write.commit(0L, messages));
    assertTrue(
        e.getMessage().contains("schema changed"),
        "guard should report the schema change: " + e.getMessage());
    // Nothing was appended: the guard fired before the commit.
    assertEquals(0L, spark.read().format("delta").load(path).count());
  }

  /**
   * A protocol change after the stream started must fail the epoch loudly (the guard). Enabling row
   * tracking adds a writer feature (a protocol change Kernel can still read) without touching the
   * schema, so only the protocol branch can fire.
   */
  @Test
  public void testCommit_failsOnConcurrentProtocolChange(@TempDir File tempDir) throws Exception {
    String path = createTable(tempDir, "streaming_protocol_change");
    DeltaV2StreamingWrite write = newWrite(path);
    WriterCommitMessage[] messages = {writeEpoch(write, 0L, 1, "Alice", 2, "Bob")};

    // Concurrent protocol upgrade (adds the rowTracking writer feature), after the write's protocol
    // was frozen at construction.
    spark.sql(
        "ALTER TABLE delta.`" + path + "` SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')");

    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> write.commit(0L, messages));
    assertTrue(
        e.getMessage().contains("protocol changed"),
        "guard should report the protocol change: " + e.getMessage());
    // Nothing was appended: the guard fired before the commit.
    assertEquals(0L, spark.read().format("delta").load(path).count());
  }

  /**
   * Replaying an already-committed epoch must be an idempotent no-op: the data is committed once,
   * and re-committing the same epoch does not duplicate it (the {@code withTransactionId} guard).
   */
  @Test
  public void testCommit_isIdempotentOnEpochReplay(@TempDir File tempDir) throws Exception {
    String path = createTable(tempDir, "streaming_idempotent_replay");
    DeltaV2StreamingWrite write = newWrite(path);

    write.commit(0L, new WriterCommitMessage[] {writeEpoch(write, 0L, 1, "Alice", 2, "Bob")});
    long rowsAfterFirstCommit = spark.read().format("delta").load(path).count();
    long versionsAfterFirstCommit = spark.sql("DESCRIBE HISTORY delta.`" + path + "`").count();

    // Replay epoch 0 with a FRESH executor write (new UUID file paths, as a real retry produces).
    // Replaying the same commit message would keep the row count at 2 via Delta's path dedup even
    // if the guard never fired; fresh paths make the row-count assertion itself prove the skip.
    write.commit(0L, new WriterCommitMessage[] {writeEpoch(write, 0L, 3, "Carol", 4, "Dave")});
    long rowsAfterReplay = spark.read().format("delta").load(path).count();

    assertEquals(2L, rowsAfterFirstCommit);
    assertEquals(
        2L,
        rowsAfterReplay,
        "replaying an already-committed epoch (with freshly written files) must not add data");
    // Also assert no new table version: a redundant (even empty) re-commit would advance it, which
    // row count alone can't detect.
    assertEquals(
        versionsAfterFirstCommit,
        spark.sql("DESCRIBE HISTORY delta.`" + path + "`").count(),
        "replaying an already-committed epoch must not create a new table version");
  }

  /**
   * Aborting an epoch commits nothing: the written files are left orphaned, not added to the table.
   */
  @Test
  public void testAbort_doesNotCommit(@TempDir File tempDir) throws Exception {
    String path = createTable(tempDir, "streaming_abort");
    DeltaV2StreamingWrite write = newWrite(path);
    // Stage a real file so the abort has data to discard, not a trivial no-op.
    WriterCommitMessage[] messages = {writeEpoch(write, 0L, 1, "Alice", 2, "Bob")};
    long versionsBefore = spark.sql("DESCRIBE HISTORY delta.`" + path + "`").count();

    write.abort(0L, messages);

    assertEquals(0L, spark.read().format("delta").load(path).count(), "abort must not commit data");
    // Also assert no new table version (row count is 0 either way; matches DeltaV2BatchWriteTest).
    assertEquals(
        versionsBefore,
        spark.sql("DESCRIBE HISTORY delta.`" + path + "`").count(),
        "abort must not create a new table version");
  }

  /**
   * Runs the executor-side writer for one epoch and returns its commit message. {@code idNamePairs}
   * is a flat list of (int id, String name) values.
   */
  private WriterCommitMessage writeEpoch(
      DeltaV2StreamingWrite write, long epochId, Object... idNamePairs) throws Exception {
    DataWriter<InternalRow> writer =
        write
            .createStreamingWriterFactory(WriteTestUtils.physicalWriteInfo(1))
            .createWriter(0, 0L, epochId);
    for (int i = 0; i < idNamePairs.length; i += 2) {
      writer.write(InternalRowTestUtils.row(idNamePairs[i], idNamePairs[i + 1]));
    }
    return writer.commit();
  }

  private String createTable(File tempDir, String tableName) {
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'", tableName, path));
    return path;
  }

  private DeltaV2StreamingWrite newWrite(String path) {
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(path, spark.sessionState().newHadoopConf());
    Snapshot snapshot = snapshotManager.loadLatestSnapshot();
    LogicalWriteInfo info =
        WriteTestUtils.logicalWriteInfo(TABLE_SCHEMA, CaseInsensitiveStringMap.empty());
    DeltaV2Write write =
        new DeltaV2Write(
            defaultEngine,
            spark.sessionState().newHadoopConf(),
            path,
            snapshot,
            snapshotManager,
            TABLE_SCHEMA,
            info);
    return (DeltaV2StreamingWrite) write.toStreaming();
  }
}
