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
package io.delta.spark.internal.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;

/**
 * Integration tests for DSv2 streaming reads on row-tracking-enabled Delta tables.
 *
 * <p>Mirrors {@link V2RowTrackingReadTest} (which is batch-only) but exercises the streaming
 * micro-batch path. Each test launches a streaming query against {@code dsv2.delta.`<tablePath>`},
 * projects {@code _metadata.row_id} / {@code _metadata.row_commit_version} where relevant, and
 * validates that the row-tracking metadata reaches the consumer.
 *
 * <p>Failures here indicate bugs in DSv2 streaming row-tracking integration. Tests are
 * intentionally thin so each one isolates a single hypothesis.
 */
public class V2StreamingRowTrackingTest extends V2TestBase {

  // ---------------------------------------------------------------------------
  // Case 1: stream from row-tracked table, basic — verify rows arrive
  // ---------------------------------------------------------------------------

  @Test
  public void testStreamFromRowTrackedTableBasic(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createRowTrackedTable(tablePath);
    insert(tablePath, "(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    assertTrue(streamingDF.isStreaming());

    List<Row> rows = processStreamingQuery(streamingDF, "rt_basic");
    assertEquals(3, rows.size());
    Set<Long> ids = new HashSet<>();
    for (Row r : rows) {
      ids.add(r.getLong(0));
    }
    assertEquals(Set.of(1L, 2L, 3L), ids);
  }

  // ---------------------------------------------------------------------------
  // Case 2: project _metadata.row_id — verify ids stable across batches
  // ---------------------------------------------------------------------------

  @Test
  public void testStreamProjectsRowId(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createRowTrackedTable(tablePath);
    insert(tablePath, "(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().table(dsv2TableRef).selectExpr("id", "_metadata.row_id AS row_id");

    List<Row> rows = processStreamingQuery(streamingDF, "rt_row_id");
    assertEquals(3, rows.size());
    // Map id -> row_id
    Map<Long, Long> idToRowId = new HashMap<>();
    for (Row r : rows) {
      idToRowId.put(r.getLong(0), r.getLong(1));
    }
    assertEquals(Set.of(0L, 1L, 2L), new HashSet<>(idToRowId.values()), "Expected row_ids 0,1,2");
  }

  // ---------------------------------------------------------------------------
  // Case 3: project _metadata.row_commit_version — verify monotonic
  // ---------------------------------------------------------------------------

  @Test
  public void testStreamProjectsRowCommitVersionMonotonic(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createRowTrackedTable(tablePath);
    insert(tablePath, "(1, 'Alice')");
    insert(tablePath, "(2, 'Bob')");
    insert(tablePath, "(3, 'Charlie')");

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .table(dsv2TableRef)
            .selectExpr("id", "_metadata.row_commit_version AS rcv");

    List<Row> rows = processStreamingQuery(streamingDF, "rt_rcv_monotonic");
    assertEquals(3, rows.size());
    Map<Long, Long> idToRcv = new HashMap<>();
    for (Row r : rows) {
      idToRcv.put(r.getLong(0), r.getLong(1));
    }
    // Each insert is its own commit (1, 2, 3). Row tracking commit version should reflect that.
    assertEquals(1L, idToRcv.get(1L), "Alice was inserted in commit 1");
    assertEquals(2L, idToRcv.get(2L), "Bob was inserted in commit 2");
    assertEquals(3L, idToRcv.get(3L), "Charlie was inserted in commit 3");
  }

  // ---------------------------------------------------------------------------
  // Case 4: row tracking × Trigger.AvailableNow
  // ---------------------------------------------------------------------------

  @Test
  public void testRowTrackingWithAvailableNowTrigger(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createRowTrackedTable(tablePath);
    insert(tablePath, "(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    File checkpointDir = new File(tempDir, "_checkpoint");
    String memoryName = "rt_avail_now";

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .table(dsv2TableRef)
            .selectExpr("id", "_metadata.row_id AS row_id", "_metadata.row_commit_version AS rcv");

    StreamingQuery query =
        streamingDF
            .writeStream()
            .format("memory")
            .queryName(memoryName)
            .outputMode("append")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .trigger(Trigger.AvailableNow())
            .start();
    try {
      query.awaitTermination();
    } finally {
      query.stop();
      DeltaLog.clearCache();
    }

    List<Row> rows = spark.sql("SELECT * FROM " + memoryName).collectAsList();
    assertEquals(3, rows.size(), "AvailableNow should drain all available rows");
    Set<Long> rowIds = new HashSet<>();
    for (Row r : rows) {
      rowIds.add(r.getLong(1));
    }
    assertEquals(Set.of(0L, 1L, 2L), rowIds);
  }

  // ---------------------------------------------------------------------------
  // Case 5: row tracking × restart — start, stop, append, restart, verify row_id consistency
  // ---------------------------------------------------------------------------

  @Test
  public void testRowTrackingAcrossStreamRestart(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createRowTrackedTable(tablePath);
    insert(tablePath, "(1, 'Alice'), (2, 'Bob')");

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    File checkpointDir = new File(tempDir, "_checkpoint");
    File outputDir = new File(tempDir, "_out");

    // Parquet sink (instead of memory sink) so checkpoint recovery is supported across restart.
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .table(dsv2TableRef)
            .selectExpr("id", "_metadata.row_id AS row_id", "_metadata.row_commit_version AS rcv");

    StreamingQuery q1 =
        streamingDF
            .writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q1.processAllAvailable();
    } finally {
      q1.stop();
    }

    // Append 2 more rows, restart the same stream from checkpoint.
    insert(tablePath, "(3, 'Charlie'), (4, 'Dave')");

    StreamingQuery q2 =
        streamingDF
            .writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q2.processAllAvailable();
    } finally {
      q2.stop();
      DeltaLog.clearCache();
    }

    List<Row> rows = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    assertEquals(4, rows.size(), "All 4 rows should arrive across restart");
    Map<Long, Long> idToRowId = new HashMap<>();
    for (Row r : rows) {
      idToRowId.put(r.getLong(0), r.getLong(1));
    }
    // Row tracking row_ids should be unique and continue from high watermark across restart.
    assertEquals(0L, idToRowId.get(1L));
    assertEquals(1L, idToRowId.get(2L));
    assertEquals(2L, idToRowId.get(3L));
    assertEquals(3L, idToRowId.get(4L));
  }

  // Case 5b: row tracking x maxFilesPerTrigger - row_ids stable when admission splits commits
  // across batches; row_ids unique across all batches; total row count correct.

  @Test
  public void testRowTrackingWithMaxFilesPerTrigger(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createRowTrackedTable(tablePath);
    // 3 separate commits of 2 rows each: each commit produces 1 file, so maxFilesPerTrigger=1
    // forces admission to split work into 3 batches.
    insert(tablePath, "(1, 'Alice'), (2, 'Bob')");
    insert(tablePath, "(3, 'Charlie'), (4, 'Dave')");
    insert(tablePath, "(5, 'Eve'), (6, 'Frank')");

    // Capture canonical id -> row_id mapping via a batch read; row_ids must match this in the
    // streaming output regardless of how admission splits the commits.
    List<Row> beforeRows =
        spark
            .sql(
                str(
                    "SELECT id, _metadata.row_id AS row_id FROM dsv2.delta.`%s` ORDER BY id",
                    tablePath))
            .collectAsList();
    Map<Long, Long> expectedIdToRowId = new HashMap<>();
    for (Row r : beforeRows) {
      expectedIdToRowId.put(r.getLong(0), r.getLong(1));
    }
    assertEquals(6, expectedIdToRowId.size(), "Expected 6 unique row_ids from batch read");

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    File checkpointDir = new File(tempDir, "_checkpoint");
    String memoryName = "rt_mfpt";

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("maxFilesPerTrigger", "1")
            .table(dsv2TableRef)
            .selectExpr("id", "_metadata.row_id AS row_id");

    StreamingQuery query =
        streamingDF
            .writeStream()
            .format("memory")
            .queryName(memoryName)
            .outputMode("append")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      query.processAllAvailable();
    } finally {
      query.stop();
      DeltaLog.clearCache();
    }

    // Stream produced multiple non-empty batches (commit split across batches).
    StreamingQueryProgress[] progresses = query.recentProgress();
    int nonEmptyCount = 0;
    for (StreamingQueryProgress p : progresses) {
      if (p.numInputRows() > 0) {
        nonEmptyCount++;
      }
    }
    final int finalNonEmpty = nonEmptyCount;
    assertTrue(
        finalNonEmpty >= 2,
        () ->
            "Expected admission to produce multiple batches with maxFilesPerTrigger=1; got "
                + "non-empty batches="
                + finalNonEmpty);

    List<Row> rows = spark.sql("SELECT * FROM " + memoryName).collectAsList();
    assertEquals(6, rows.size(), "All 6 rows must surface across batches");

    Map<Long, Long> streamIdToRowId = new HashMap<>();
    for (Row r : rows) {
      streamIdToRowId.put(r.getLong(0), r.getLong(1));
    }
    // row_ids are stable: each id sees the same row_id as the batch read.
    assertEquals(
        expectedIdToRowId,
        streamIdToRowId,
        "row_ids must remain stable when admission splits commits across batches");
    // row_ids are unique across batches.
    assertEquals(
        6,
        new HashSet<>(streamIdToRowId.values()).size(),
        "row_ids must be unique across all batches");
  }

  // ---------------------------------------------------------------------------
  // Case 6: row tracking × DV — DELETE rows, stream, verify surviving row_ids unchanged
  // ---------------------------------------------------------------------------

  @Test
  public void testRowTrackingWithDeletionVectorsPreservesIds(@TempDir File tempDir)
      throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta TBLPROPERTIES "
                + "('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark
        .range(1000)
        .selectExpr("id", "cast(id as string) as name")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));

    DeltaLog deltaLog = DeltaLog.forTable(spark, tablePath);
    long numDVs =
        (long)
            deltaLog
                .update(false, Option.empty(), Option.empty())
                .numDeletionVectorsOpt()
                .getOrElse(() -> 0L);
    assertTrue(numDVs > 0, "Expected DVs to be created");

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().table(dsv2TableRef).selectExpr("id", "_metadata.row_id AS row_id");

    List<Row> rows = processStreamingQuery(streamingDF, "rt_dv");
    assertEquals(500, rows.size(), "Expected only odd ids (DV-filtered)");
    for (Row r : rows) {
      long id = r.getLong(0);
      long rowId = r.getLong(1);
      assertEquals(1L, id % 2, "Only odd IDs should survive deletion");
      // With stable physical row positions row_id == id for the initial single-file insert.
      assertEquals(id, rowId, "row_id should be preserved across DV-filtered streaming reads");
    }
  }

  // ---------------------------------------------------------------------------
  // Case 7: row tracking × column mapping (combine top-2 cross products)
  // ---------------------------------------------------------------------------

  @Test
  public void testRowTrackingWithColumnMapping(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta TBLPROPERTIES "
                + "('delta.enableRowTracking' = 'true', 'delta.columnMapping.mode' = 'name', "
                + "'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')",
            tablePath));
    insert(tablePath, "(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .table(dsv2TableRef)
            .selectExpr("id", "name", "_metadata.row_id AS row_id");

    List<Row> rows = processStreamingQuery(streamingDF, "rt_cm");
    assertEquals(3, rows.size());
    Map<Long, Long> idToRowId = new HashMap<>();
    for (Row r : rows) {
      idToRowId.put(r.getLong(0), r.getLong(2));
    }
    assertEquals(Set.of(0L, 1L, 2L), new HashSet<>(idToRowId.values()));
  }

  // ---------------------------------------------------------------------------
  // Case 8: row tracking × INSERT OVERWRITE — should row_ids change for rewritten rows?
  //
  // Per Delta semantics, INSERT OVERWRITE is a logical replacement. We make a streaming
  // query consume the table after the overwrite and verify the rows produced by the stream
  // (an initial snapshot read) carry the post-overwrite row_ids from the high watermark.
  // ---------------------------------------------------------------------------

  @Test
  public void testRowTrackingWithInsertOverwrite(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createRowTrackedTable(tablePath);
    insert(tablePath, "(1, 'Alice'), (2, 'Bob')");
    // Overwrite — replaces all data; new rows should get fresh row_ids continuing from watermark.
    spark.sql(str("INSERT OVERWRITE TABLE delta.`%s` VALUES (10, 'X'), (20, 'Y')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().table(dsv2TableRef).selectExpr("id", "_metadata.row_id AS row_id");

    List<Row> rows = processStreamingQuery(streamingDF, "rt_overwrite");
    assertEquals(2, rows.size(), "Stream should see only post-overwrite rows");
    Set<Long> ids = new HashSet<>();
    Set<Long> rowIds = new HashSet<>();
    for (Row r : rows) {
      ids.add(r.getLong(0));
      rowIds.add(r.getLong(1));
    }
    assertEquals(Set.of(10L, 20L), ids, "Only overwritten ids should be visible");
    assertEquals(2, rowIds.size(), "row_ids should be unique per surviving row");
    // Row IDs after overwrite should continue past the original 2 rows: the first watermark
    // was {0, 1} so the new rows must have ids >= 2 (any 2 ids from {2, 3}).
    for (long rid : rowIds) {
      assertTrue(rid >= 2L, "Expected row_id >= 2 after overwrite; got " + rid);
    }
  }

  // ---------------------------------------------------------------------------
  // Case 9: row tracking × MERGE → ignoreChanges, verify row_id preserved across reused files
  // ---------------------------------------------------------------------------

  @Test
  public void testRowTrackingWithMergeIgnoreChanges(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createRowTrackedTable(tablePath);
    insert(tablePath, "(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

    // Capture pre-MERGE row_ids via a batch query.
    List<Row> beforeRows =
        spark
            .sql(
                str(
                    "SELECT id, _metadata.row_id AS row_id FROM dsv2.delta.`%s` ORDER BY id",
                    tablePath))
            .collectAsList();
    Map<Long, Long> beforeIdToRowId = new HashMap<>();
    for (Row r : beforeRows) {
      beforeIdToRowId.put(r.getLong(0), r.getLong(1));
    }

    // Set up a source for MERGE.
    spark.sql("DROP VIEW IF EXISTS rt_merge_src");
    spark
        .sql("SELECT 1L AS id, 'ALICE' AS name UNION ALL SELECT 99L AS id, 'New' AS name")
        .createOrReplaceTempView("rt_merge_src");

    spark.sql(
        str(
            "MERGE INTO delta.`%s` t USING rt_merge_src s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET name = s.name "
                + "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
            tablePath));

    // ignoreChanges allows streaming over a table with non-append commits.
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("ignoreChanges", "true")
            .table(dsv2TableRef)
            .selectExpr("id", "_metadata.row_id AS row_id");

    List<Row> rows = processStreamingQuery(streamingDF, "rt_merge_ignore");
    Map<Long, Long> afterIdToRowId = new HashMap<>();
    for (Row r : rows) {
      afterIdToRowId.put(r.getLong(0), r.getLong(1));
    }
    // Rows id=2 and id=3 were untouched in MERGE (Bob, Charlie). If MERGE rewrites the file
    // (file-rewrite path), row tracking must preserve their original row_ids.
    assertTrue(afterIdToRowId.containsKey(2L), "Bob (id=2) should still be present");
    assertTrue(afterIdToRowId.containsKey(3L), "Charlie (id=3) should still be present");
    assertEquals(
        beforeIdToRowId.get(2L),
        afterIdToRowId.get(2L),
        "Bob's row_id should be preserved across MERGE rewrites");
    assertEquals(
        beforeIdToRowId.get(3L),
        afterIdToRowId.get(3L),
        "Charlie's row_id should be preserved across MERGE rewrites");
  }

  // ---------------------------------------------------------------------------
  // Case 10: row tracking on a snapshot started without it (enabled later)
  // ---------------------------------------------------------------------------

  @Test
  public void testRowTrackingEnabledAfterTableCreate(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta", tablePath));
    insert(tablePath, "(1, 'Alice'), (2, 'Bob')");
    // Enable row tracking after data is already present.
    spark.sql(
        str(
            "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')",
            tablePath));
    insert(tablePath, "(3, 'Charlie')");

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().table(dsv2TableRef).selectExpr("id", "_metadata.row_id AS row_id");

    List<Row> rows = processStreamingQuery(streamingDF, "rt_late_enable");
    assertEquals(3, rows.size(), "All rows should arrive even though RT was enabled mid-life");
    Set<Long> rowIds = new HashSet<>();
    for (Row r : rows) {
      rowIds.add(r.getLong(1));
    }
    assertEquals(3, rowIds.size(), "row_ids should be unique across the whole table");
  }

  // ---------------------------------------------------------------------------
  // Sanity: project _metadata struct on a non-RT table through DSv2 streaming
  // ---------------------------------------------------------------------------

  @Test
  public void testStreamMetadataStructOnNonRowTrackedTable(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta", tablePath));
    insert(tablePath, "(1, 'Alice'), (2, 'Bob')");

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    // Accessing _metadata.row_id on a non-RT table must surface a clear analysis error.
    AnalysisException ex =
        assertThrows(
            AnalysisException.class,
            () -> spark.readStream().table(dsv2TableRef).selectExpr("_metadata.row_id AS row_id"));
    assertTrue(
        ex.getMessage().toLowerCase().contains("row_id")
            || ex.getMessage().toLowerCase().contains("_metadata"),
        "Expected analysis error mentioning row_id or _metadata; got: " + ex.getMessage());
  }

  // Case 11: row tracking x ignoreDeletes. Partition the table so DELETE WHERE part=<x> removes
  // whole files (no rewrite), the case ignoreDeletes=true is designed to skip. Surviving rows
  // must keep their original row_ids and the stream must not error on the DELETE commit.
  @Test
  public void testRowTrackingWithIgnoreDeletes(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING, part INT) USING delta "
                + "PARTITIONED BY (part) "
                + "TBLPROPERTIES ('delta.enableRowTracking' = 'true')",
            tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES (1, 'Alice', 0), (2, 'Bob', 0), "
                + "(3, 'Charlie', 1), (4, 'Dave', 1)",
            tablePath));

    // Capture canonical pre-DELETE row_ids for the rows in partition 1 (the survivors).
    List<Row> beforeRows =
        spark
            .sql(
                str(
                    "SELECT id, _metadata.row_id AS row_id FROM dsv2.delta.`%s` "
                        + "WHERE part = 1 ORDER BY id",
                    tablePath))
            .collectAsList();
    Map<Long, Long> beforeIdToRowId = new HashMap<>();
    for (Row r : beforeRows) {
      beforeIdToRowId.put(r.getLong(0), r.getLong(1));
    }
    assertEquals(2, beforeIdToRowId.size(), "Partition 1 should have 2 rows pre-DELETE");

    // Whole-partition DELETE - removes the partition=0 file(s) without rewriting partition=1.
    spark.sql(str("DELETE FROM delta.`%s` WHERE part = 0", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("ignoreDeletes", "true")
            .table(dsv2TableRef)
            .selectExpr("id", "_metadata.row_id AS row_id");

    List<Row> rows = processStreamingQuery(streamingDF, "rt_ignore_deletes");
    // Initial snapshot reads the post-DELETE table state: 2 surviving rows from partition 1.
    assertEquals(2, rows.size(), "Only partition=1 rows should survive whole-partition DELETE");
    Map<Long, Long> afterIdToRowId = new HashMap<>();
    for (Row r : rows) {
      afterIdToRowId.put(r.getLong(0), r.getLong(1));
    }
    assertEquals(
        Set.of(3L, 4L), afterIdToRowId.keySet(), "Only ids 3 and 4 (partition=1) should remain");
    // row_ids of the survivors must be stable across the whole-file DELETE.
    assertEquals(
        beforeIdToRowId.get(3L),
        afterIdToRowId.get(3L),
        "Charlie's row_id must be stable across whole-file DELETE");
    assertEquals(
        beforeIdToRowId.get(4L),
        afterIdToRowId.get(4L),
        "Dave's row_id must be stable across whole-file DELETE");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private void createRowTrackedTable(String path) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableRowTracking' = 'true')",
            path));
  }

  private void insert(String path, String values) {
    spark.sql(str("INSERT INTO delta.`%s` VALUES %s", path, values));
  }

  // S15-S22 cross-product tests: row tracking x remaining scenario columns.
  // Helpers below are scoped to these tests (parquet sink, log-state mutation,
  // checkpoint, concurrent appender) and mirror the patterns used by
  // V2StreamingLifecycleExtendedScenariosTest.

  /**
   * Writes a streaming query to a parquet sink + checkpoint, drains all available data, then stops.
   * Parquet sink supports checkpoint recovery across restarts; memory sink does not.
   */
  private List<Row> runWithParquetSink(
      Dataset<Row> streamingDF, File outputDir, File checkpointDir, Trigger trigger)
      throws Exception {
    StreamingQuery query = null;
    try {
      DataStreamWriter<Row> writer =
          streamingDF
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath());
      if (trigger != null) {
        writer = writer.trigger(trigger);
      }
      query = writer.start();
      query.processAllAvailable();
      if (trigger != null) {
        query.awaitTermination(60_000L);
      }
    } finally {
      if (query != null) {
        query.stop();
      }
    }
    return spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
  }

  /** Force a checkpoint so a pruned commit JSON does not block snapshot reconstruction. */
  @SuppressWarnings("deprecation")
  private void checkpoint(String tablePath) {
    DeltaLog.forTable(spark, tablePath).checkpoint();
  }

  /** Delete the commit JSON (and its CRC sibling) for the given version to simulate prune. */
  private void pruneCommitJson(String tablePath, long version) throws Exception {
    Path json = Paths.get(tablePath, "_delta_log", String.format("%020d.json", version));
    Files.delete(json);
    Path crc = Paths.get(tablePath, "_delta_log", String.format("%020d.crc", version));
    if (Files.exists(crc)) {
      Files.delete(crc);
    }
    DeltaLog.clearCache();
  }

  // S15: row tracking x ADD COLUMN. Row tracking must survive an additive
  // schema evolution: row_ids remain stable for pre-ADD rows and continue from
  // the high watermark for post-ADD rows.
  @Test
  public void testRowTrackingWithAddColumn(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableRowTracking' = 'true')",
            tablePath));
    insert(tablePath, "(1, 'a'), (2, 'b')");

    // Capture canonical id -> row_id before the schema change.
    List<Row> beforeRows =
        spark
            .sql(
                str(
                    "SELECT id, _metadata.row_id AS row_id FROM dsv2.delta.`%s` ORDER BY id",
                    tablePath))
            .collectAsList();
    Map<Integer, Long> expectedIdToRowId = new HashMap<>();
    for (Row r : beforeRows) {
      expectedIdToRowId.put(r.getInt(0), r.getLong(1));
    }
    assertEquals(2, expectedIdToRowId.size(), "pre-ADD batch read should have 2 row_ids");

    // Additive schema evolution: ADD COLUMN, then insert post-ADD rows.
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra INT", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c', 30), (4, 'd', 40)", tablePath));

    // Capture canonical row_ids for the new rows from the same batch read; they should be unique
    // and continue past the pre-ADD watermark.
    List<Row> postRows =
        spark
            .sql(
                str(
                    "SELECT id, _metadata.row_id AS row_id FROM dsv2.delta.`%s` "
                        + "WHERE id IN (3, 4) ORDER BY id",
                    tablePath))
            .collectAsList();
    for (Row r : postRows) {
      expectedIdToRowId.put(r.getInt(0), r.getLong(1));
    }
    assertEquals(4, expectedIdToRowId.size(), "post-ADD batch read should add 2 more row_ids");

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().table(dsv2TableRef).selectExpr("id", "_metadata.row_id AS row_id");

    List<Row> rows = processStreamingQuery(streamingDF, "rt_add_column");
    assertEquals(4, rows.size(), "stream should see all 4 rows across the ADD COLUMN");
    Map<Integer, Long> streamIdToRowId = new HashMap<>();
    for (Row r : rows) {
      streamIdToRowId.put(r.getInt(0), r.getLong(1));
    }
    assertEquals(
        expectedIdToRowId,
        streamIdToRowId,
        "row_ids must remain stable across ADD COLUMN and match the canonical batch read");
    assertEquals(
        4,
        new HashSet<>(streamIdToRowId.values()).size(),
        "row_ids must be unique across pre- and post-ADD rows");
  }

  // S16: row tracking x RENAME COLUMN. Non-additive evolution requires CM
  // (rename uses column mapping) + schemaTrackingLocation +
  // allowSourceColumnRename=always. Row tracking must thread through:
  // post-rename rows surface under the new column name; row_ids stay stable.
  @Test
  public void testRowTrackingWithRenameColumn(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    File checkpointDir = new File(tempDir, "_checkpoint");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");
    File outputDir = new File(tempDir, "_output");
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    // CM-name + row tracking. RENAME requires column mapping.
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableRowTracking' = 'true', "
                + "               'delta.columnMapping.mode' = 'name', "
                + "               'delta.minReaderVersion' = '2', "
                + "               'delta.minWriterVersion' = '5')",
            tablePath));
    insert(tablePath, "(1, 'a'), (2, 'b')");

    // Capture canonical id -> row_id under the original schema.
    List<Row> beforeRows =
        spark
            .sql(
                str(
                    "SELECT id, _metadata.row_id AS row_id FROM dsv2.delta.`%s` ORDER BY id",
                    tablePath))
            .collectAsList();
    Map<Long, Long> beforeIdToRowId = new HashMap<>();
    for (Row r : beforeRows) {
      beforeIdToRowId.put(r.getLong(0), r.getLong(1));
    }
    assertEquals(2, beforeIdToRowId.size(), "pre-rename batch read should have 2 row_ids");

    // First run: drain the 2 pre-rename rows under the original schema.
    Dataset<Row> df1 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2TableRef)
            .selectExpr("id", "value", "_metadata.row_id AS row_id");
    List<Row> firstRun = runWithParquetSink(df1, outputDir, checkpointDir, Trigger.AvailableNow());
    assertEquals(2, firstRun.size(), "first run should emit 2 pre-rename rows");
    DeltaLog.clearCache();

    // Non-additive change between runs: RENAME `value` -> `val`, then write post-rename rows.
    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN value TO val", tablePath));
    insert(tablePath, "(3, 'c'), (4, 'd')");

    // Second run: restart from checkpoint with allowSourceColumnRename=always; project the
    // renamed column `val` so we can verify it threads through.
    Dataset<Row> df2 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .option("allowSourceColumnRename", "always")
            .table(dsv2TableRef)
            .selectExpr("id", "val", "_metadata.row_id AS row_id");
    List<Row> secondRun = runWithParquetSink(df2, outputDir, checkpointDir, Trigger.AvailableNow());
    DeltaLog.clearCache();

    assertEquals(
        2, secondRun.size(), () -> "second run should emit 2 post-rename rows: " + secondRun);

    // First-run rows must still carry their original row_ids.
    Map<Long, Long> firstIdToRowId = new HashMap<>();
    for (Row r : firstRun) {
      firstIdToRowId.put(r.getLong(0), r.getLong(2));
    }
    assertEquals(
        beforeIdToRowId, firstIdToRowId, "pre-rename row_ids must match the canonical batch read");

    // Post-rename rows surface the renamed column at ordinal 1 and have unique row_ids.
    Set<Long> allRowIds = new HashSet<>(firstIdToRowId.values());
    for (Row r : secondRun) {
      long id = r.getLong(0);
      String val = r.getString(1);
      long rowId = r.getLong(2);
      assertTrue(
          id == 3L || id == 4L, () -> "second run should only contain post-rename ids: " + id);
      assertTrue(
          "c".equals(val) || "d".equals(val),
          () -> "renamed column `val` should carry the inserted value, got: " + val);
      allRowIds.add(rowId);
    }
    assertEquals(4, allRowIds.size(), "row_ids must be unique across the rename");
  }

  // S17: row tracking x nullability toggle. Drop NOT NULL on a column, insert a
  // NULL row, restart with schemaTrackingLocation. Row tracking must not break:
  // the NULL-id row arrives and all row_ids are unique.
  @Test
  public void testRowTrackingWithNullabilityToggle(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    File checkpointDir = new File(tempDir, "_checkpoint");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");
    File outputDir = new File(tempDir, "_output");
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    // CM-name + row tracking with a NOT NULL column.
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT NOT NULL, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableRowTracking' = 'true', "
                + "               'delta.columnMapping.mode' = 'name', "
                + "               'delta.minReaderVersion' = '2', "
                + "               'delta.minWriterVersion' = '5')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));

    // First run: drain pre-toggle rows.
    Dataset<Row> df1 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2TableRef)
            .selectExpr("id", "value", "_metadata.row_id AS row_id");
    List<Row> firstRun = runWithParquetSink(df1, outputDir, checkpointDir, Trigger.AvailableNow());
    assertEquals(2, firstRun.size(), "first run should emit 2 pre-toggle rows");
    DeltaLog.clearCache();

    // Drop NOT NULL, then insert a NULL-id row plus a normal row.
    spark.sql(str("ALTER TABLE delta.`%s` ALTER COLUMN id DROP NOT NULL", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c'), (null, 'd')", tablePath));

    // Second run: restart from checkpoint; the schema evolution handler should adopt the new
    // nullability and surface the NULL-id row.
    Dataset<Row> df2 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2TableRef)
            .selectExpr("id", "value", "_metadata.row_id AS row_id");
    List<Row> secondRun = runWithParquetSink(df2, outputDir, checkpointDir, Trigger.AvailableNow());
    DeltaLog.clearCache();

    assertEquals(2, secondRun.size(), "second run should emit 2 post-toggle rows");
    assertTrue(
        secondRun.stream().anyMatch(r -> r.isNullAt(0)),
        () -> "post-toggle NULL-id row must surface, got: " + secondRun);

    // Combine across both runs and verify all 4 row_ids are unique.
    Set<Long> allRowIds = new HashSet<>();
    for (Row r : firstRun) {
      allRowIds.add(r.getLong(2));
    }
    for (Row r : secondRun) {
      allRowIds.add(r.getLong(2));
    }
    assertEquals(4, allRowIds.size(), "row_ids must be unique across the nullability toggle");
  }

  // S21: row tracking x log retention. Force a checkpoint, then delete an early
  // commit JSON to simulate the snapshot reconstruction path under log-retention
  // pruning. With failOnDataLoss=false the stream must drain the surviving rows
  // and still expose row_ids.
  @Test
  public void testRowTrackingWithLogRetention(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createRowTrackedTable(tablePath);
    // 4 single-row commits -> versions 1..4.
    insert(tablePath, "(1, 'a')");
    insert(tablePath, "(2, 'b')");
    insert(tablePath, "(3, 'c')");
    insert(tablePath, "(4, 'd')");

    // Checkpoint at the latest snapshot so reconstruction does not require commit v1's JSON.
    checkpoint(tablePath);
    // Simulate log-retention prune: delete v=1's JSON.
    pruneCommitJson(tablePath, /* version= */ 1L);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("startingVersion", "0")
            .table(dsv2TableRef)
            .selectExpr("id", "_metadata.row_id AS row_id");

    List<Row> rows = processStreamingQuery(streamingDF, "rt_log_retention");
    // All 4 rows survive in the reconstructed snapshot.
    assertEquals(
        4,
        rows.size(),
        () -> "failOnDataLoss=false + pruned commit should surface all 4 rows, got: " + rows);
    Set<Long> ids = new HashSet<>();
    Set<Long> rowIds = new HashSet<>();
    for (Row r : rows) {
      ids.add(r.getLong(0));
      rowIds.add(r.getLong(1));
    }
    assertEquals(
        Set.of(1L, 2L, 3L, 4L), ids, "all original ids should survive log-retention prune");
    assertEquals(4, rowIds.size(), "row_ids must be unique across the pruned-log replay");
  }

  // S22: row tracking x concurrent writer. A streaming reader projects row_id
  // while a background thread keeps appending. The stream must drain all
  // surfaced rows without error and every row must carry a unique row_id.
  @Test
  public void testRowTrackingWithConcurrentWriter(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    File checkpointDir = new File(tempDir, "_ckpt");
    File outputDir = new File(tempDir, "_out");

    createRowTrackedTable(tablePath);
    insert(tablePath, "(0, 'seed')");

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().table(dsv2TableRef).selectExpr("id", "_metadata.row_id AS row_id");

    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService writer = Executors.newSingleThreadExecutor();
    StreamingQuery query = null;
    Throwable caught = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .start();
      writer.submit(
          () -> {
            int i = 1;
            while (!stop.get() && i <= 10) {
              try {
                spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, 'c%d')", tablePath, i + 100, i));
                Thread.sleep(50);
                i++;
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              } catch (Exception ignored) {
                // Concurrent commit may transiently fail; keep going.
              }
            }
          });
      Thread.sleep(2_000L);
      stop.set(true);
      query.processAllAvailable();
    } catch (Throwable t) {
      caught = t;
    } finally {
      stop.set(true);
      writer.shutdownNow();
      writer.awaitTermination(5, TimeUnit.SECONDS);
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }
    final Throwable finalCaught = caught;
    assertNull(
        finalCaught,
        () ->
            "concurrent writer + row-tracking stream should not surface an error: " + finalCaught);

    // Sink size must not exceed source row count, and every row_id in the sink must be unique.
    long sourceCount = spark.read().format("delta").load(tablePath).count();
    List<Row> sink = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    assertTrue(
        sink.size() <= sourceCount,
        () -> "sink size " + sink.size() + " must not exceed source " + sourceCount);
    assertTrue(sink.size() >= 1, () -> "sink should contain at least the seed row, got: " + sink);
    Set<Long> rowIds = new HashSet<>();
    for (Row r : sink) {
      rowIds.add(r.getLong(1));
    }
    assertEquals(
        sink.size(),
        rowIds.size(),
        "every emitted row must carry a unique row_id across the concurrent writer");
  }
}
