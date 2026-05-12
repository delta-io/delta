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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.StreamingQuery;
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
 * <p>Each test ALSO runs the same scenario through DSv1 streaming ({@code
 * spark.readStream().format("delta").load(path)}) and asserts that the rows produced by V1 and V2
 * match. V1 is the oracle for parity; divergence here indicates a DSv2 streaming bug.
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

    // V1 vs V2 streaming parity: same projection, sort by id, compare.
    assertV1V2StreamingParity(tablePath, "rt_basic", /* projection= */ null);
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

    assertV1V2StreamingParity(
        tablePath, "rt_row_id", df -> df.selectExpr("id", "_metadata.row_id AS row_id"));
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

    assertV1V2StreamingParity(
        tablePath,
        "rt_rcv_monotonic",
        df -> df.selectExpr("id", "_metadata.row_commit_version AS rcv"));
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

    // V1 parity: run the same AvailableNow stream through DSv1 with a separate checkpoint,
    // then compare the collected rows (sorted by id).
    File v1CheckpointDir = new File(tempDir, "_checkpoint_v1");
    String v1MemoryName = "rt_avail_now_v1";
    Dataset<Row> v1StreamingDF =
        spark
            .readStream()
            .format("delta")
            .load(tablePath)
            .selectExpr("id", "_metadata.row_id AS row_id", "_metadata.row_commit_version AS rcv");
    StreamingQuery v1Query =
        v1StreamingDF
            .writeStream()
            .format("memory")
            .queryName(v1MemoryName)
            .outputMode("append")
            .option("checkpointLocation", v1CheckpointDir.getAbsolutePath())
            .trigger(Trigger.AvailableNow())
            .start();
    try {
      v1Query.awaitTermination();
    } finally {
      v1Query.stop();
      DeltaLog.clearCache();
    }
    List<Row> v1Rows = spark.sql("SELECT * FROM " + v1MemoryName).collectAsList();
    assertRowsEqualSortedByFirstCol(v1Rows, rows, "rt_avail_now");
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

    // V1 parity: drive a separate DSv1 stream with its own checkpoint + output dir, restart it
    // across the same gap, then assert V1's full output equals V2's full output.
    File v1CheckpointDir = new File(tempDir, "_checkpoint_v1");
    File v1OutputDir = new File(tempDir, "_out_v1");
    Dataset<Row> v1StreamingDF =
        spark
            .readStream()
            .format("delta")
            .load(tablePath)
            .selectExpr("id", "_metadata.row_id AS row_id", "_metadata.row_commit_version AS rcv");
    // V1 first run: should consume rows present at start of stream. Because we already appended
    // (3,4) above, V1's "first run" will see all 4 rows in the first batch. That is fine - we
    // only assert the FINAL union after both runs.
    StreamingQuery v1q1 =
        v1StreamingDF
            .writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", v1OutputDir.getAbsolutePath())
            .option("checkpointLocation", v1CheckpointDir.getAbsolutePath())
            .start();
    try {
      v1q1.processAllAvailable();
    } finally {
      v1q1.stop();
    }
    // Restart against the same checkpoint - there is nothing more to consume, so this is a no-op
    // that exercises the checkpoint recovery code path.
    StreamingQuery v1q2 =
        v1StreamingDF
            .writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", v1OutputDir.getAbsolutePath())
            .option("checkpointLocation", v1CheckpointDir.getAbsolutePath())
            .start();
    try {
      v1q2.processAllAvailable();
    } finally {
      v1q2.stop();
      DeltaLog.clearCache();
    }
    List<Row> v1Rows = spark.read().parquet(v1OutputDir.getAbsolutePath()).collectAsList();
    assertRowsEqualSortedByFirstCol(v1Rows, rows, "rt_restart");
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

    // V1 parity: DELETE produces a non-append commit, so V1 streaming requires ignoreChanges
    // (or ignoreDeletes) to consume past it. The initial snapshot read still sees only surviving
    // rows; we project the same columns and compare.
    Dataset<Row> v1StreamingDF =
        spark
            .readStream()
            .format("delta")
            .option("ignoreDeletes", "true")
            .load(tablePath)
            .selectExpr("id", "_metadata.row_id AS row_id");
    List<Row> v1Rows = processStreamingQuery(v1StreamingDF, "rt_dv_v1");
    assertRowsEqualSortedByFirstCol(v1Rows, rows, "rt_dv");
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

    assertV1V2StreamingParity(
        tablePath, "rt_cm", df -> df.selectExpr("id", "name", "_metadata.row_id AS row_id"));
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

    // V1 parity: INSERT OVERWRITE is a non-append commit; DSv1 streaming requires
    // ignoreChanges to start a fresh stream after such a commit. Snapshot read sees only the
    // post-overwrite rows.
    Dataset<Row> v1StreamingDF =
        spark
            .readStream()
            .format("delta")
            .option("ignoreChanges", "true")
            .load(tablePath)
            .selectExpr("id", "_metadata.row_id AS row_id");
    List<Row> v1Rows = processStreamingQuery(v1StreamingDF, "rt_overwrite_v1");
    assertRowsEqualSortedByFirstCol(v1Rows, rows, "rt_overwrite");
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

    // V1 parity: same option + same projection.
    Dataset<Row> v1StreamingDF =
        spark
            .readStream()
            .format("delta")
            .option("ignoreChanges", "true")
            .load(tablePath)
            .selectExpr("id", "_metadata.row_id AS row_id");
    List<Row> v1Rows = processStreamingQuery(v1StreamingDF, "rt_merge_ignore_v1");
    assertRowsEqualSortedByFirstCol(v1Rows, rows, "rt_merge_ignore");
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

    // V1 parity: ALTER TABLE produces a non-append (metadata) commit; use ignoreChanges so
    // V1 streaming can replay past it.
    Dataset<Row> v1StreamingDF =
        spark
            .readStream()
            .format("delta")
            .option("ignoreChanges", "true")
            .load(tablePath)
            .selectExpr("id", "_metadata.row_id AS row_id");
    List<Row> v1Rows = processStreamingQuery(v1StreamingDF, "rt_late_enable_v1");
    assertRowsEqualSortedByFirstCol(v1Rows, rows, "rt_late_enable");
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

    // V1 parity: DSv1 streaming must also reject _metadata.row_id on a non-row-tracked table.
    AnalysisException v1Ex =
        assertThrows(
            AnalysisException.class,
            () ->
                spark
                    .readStream()
                    .format("delta")
                    .load(tablePath)
                    .selectExpr("_metadata.row_id AS row_id"));
    assertTrue(
        v1Ex.getMessage().toLowerCase().contains("row_id")
            || v1Ex.getMessage().toLowerCase().contains("_metadata"),
        "V1: expected analysis error mentioning row_id or _metadata; got: " + v1Ex.getMessage());
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

  /**
   * Runs the same streaming projection against DSv1 (file path) and DSv2 (catalog table) and
   * asserts the produced rows match (sorted by the first column's toString).
   *
   * <p>Use this for tests whose underlying table has only append commits - i.e., V1 streaming can
   * consume it without {@code ignoreChanges}/{@code ignoreDeletes}. For non-append-only tables
   * (DV/overwrite/merge/alter), inline a V1 stream with the appropriate option instead.
   */
  private void assertV1V2StreamingParity(
      String tablePath, String tag, Function<Dataset<Row>, Dataset<Row>> projection)
      throws Exception {
    Dataset<Row> v1 = spark.readStream().format("delta").load(tablePath);
    if (projection != null) v1 = projection.apply(v1);
    List<Row> v1Rows = processStreamingQuery(v1, tag + "_v1");

    Dataset<Row> v2 = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    if (projection != null) v2 = projection.apply(v2);
    List<Row> v2Rows = processStreamingQuery(v2, tag + "_v2");

    assertRowsEqualSortedByFirstCol(v1Rows, v2Rows, tag);
  }

  /** Sorts both lists by the first column's toString and asserts equality. */
  private static void assertRowsEqualSortedByFirstCol(
      List<Row> v1Rows, List<Row> v2Rows, String tag) {
    List<Row> v1Sorted = new ArrayList<>(v1Rows);
    List<Row> v2Sorted = new ArrayList<>(v2Rows);
    Comparator<Row> byFirstCol = Comparator.comparing(r -> String.valueOf(r.get(0)));
    v1Sorted.sort(byFirstCol);
    v2Sorted.sort(byFirstCol);
    assertEquals(
        v1Sorted.toString(), v2Sorted.toString(), tag + ": V1 vs V2 streaming row mismatch");
  }
}
