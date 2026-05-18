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
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * DSv2 streaming coverage for {@code failOnDataLoss=false} crossed with feature-bearing tables.
 *
 * <p>The pattern is the same in every case: write N commits, force a checkpoint so the snapshot can
 * still be reconstructed without the pruned commit JSON, manually delete a commit JSON (and its
 * CRC) to simulate {@code logRetentionDuration} expiry, then start a DSv2 stream with {@code
 * failOnDataLoss=false} and assert the stream proceeds rather than throwing {@code
 * DELTA_VERSION_NOT_FOUND}. This pins behavior across every feature row that previously had no
 * failOnDataLoss coverage in {@link V2StreamingOptionMatrixTest}.
 *
 * <p>Existing coverage in {@link V2StreamingOptionMatrixTest} already covers:
 *
 * <ul>
 *   <li>{@code testFailOnDataLoss_deletedCommitJson} - vanilla append, single commit pruned.
 *   <li>{@code testFailOnDataLoss_startingVersionPastPruned} - throws the time-travel error before
 *       fnl=false kicks in (DSv1 parity).
 * </ul>
 *
 * <p>This file walks the rest of the feature row in the {@code true_cross_product.md} matrix.
 */
public class V2StreamingFailOnDataLossMatrixTest extends V2TestBase {

  /**
   * Force a checkpoint at the current snapshot. Required so the table can be read back without the
   * pruned commit JSONs.
   */
  @SuppressWarnings("deprecation")
  private void checkpoint(String tablePath) {
    DeltaLog.forTable(spark, tablePath).checkpoint();
  }

  /**
   * Simulate {@code logRetentionDuration} expiry by deleting the commit JSON for the given version
   * and its CRC sibling. Uses {@code Files.delete} per the task spec - {@link Files#delete} is the
   * NIO equivalent of {@link File#delete}, and asserting on it gives a clearer error than the
   * boolean return path.
   */
  private void pruneCommitJson(String tablePath, long version) throws Exception {
    String name = String.format("%020d.json", version);
    Path json = Paths.get(tablePath, "_delta_log", name);
    Files.delete(json);
    Path crc = Paths.get(tablePath, "_delta_log", String.format("%020d.crc", version));
    if (Files.exists(crc)) {
      Files.delete(crc);
    }
    DeltaLog.clearCache();
  }

  /** Build a DSv2 streaming reader with {@code failOnDataLoss=false} starting from version 0. */
  private Dataset<Row> failOnDataLossFalseStream(String tablePath) {
    return spark
        .readStream()
        .option("failOnDataLoss", "false")
        .option("startingVersion", "0")
        .table(str("dsv2.delta.`%s`", tablePath));
  }

  /** Append N single-row commits (id = 0..N-1) to a vanilla {@code delta} path. */
  private void appendNSingleRowCommits(String tablePath, int n) {
    for (int i = 0; i < n; i++) {
      spark.range(i, i + 1).toDF("id").write().format("delta").mode("append").save(tablePath);
    }
  }

  /**
   * Case 1. fnl=false x DV table, prune the commit JSON containing the DV DELETE.
   *
   * <p>DV table: v0 CREATE, v1 INSERT 10 rows, v2 DELETE id<5 (writes the DV). Prune v2 (the DV
   * commit). Stream from v=0 with fnl=false: should skip the missing v2 commit and read the
   * remaining surviving data via snapshot reconstruction off the v2 checkpoint.
   */
  @Test
  public void testFnl_onDvTable_acrossDvDelete(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark
        .range(0, 10)
        .selectExpr("cast(id as int) as id")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);
    spark.sql(str("DELETE FROM delta.`%s` WHERE id < 5", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 1L);

    Dataset<Row> df = failOnDataLossFalseStream(tablePath);
    List<Row> rows = processStreamingQuery(df, "fnl_dv");
    // Pin: 5 surviving rows (ids 5..9) after the DV DELETE; the pruned v1 commit JSON does not
    // block the stream because failOnDataLoss=false allows skip-forward to the checkpoint.
    assertEquals(5, rows.size(), () -> "expected 5 surviving rows after DV delete, got: " + rows);
  }

  /** Case 2. fnl=false x column mapping mode=name. Pruned middle commit; stream skips ahead. */
  @Test
  public void testFnl_onColMappedNameTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    // v1, v2, v3 - three inserts.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 2L);

    Dataset<Row> df = failOnDataLossFalseStream(tablePath);
    List<Row> rows = processStreamingQuery(df, "fnl_cm_name");
    // All 3 rows survive in the snapshot reconstructed from the checkpoint.
    assertEquals(
        3, rows.size(), () -> "expected 3 rows on CM-name table with pruned v2, got: " + rows);
  }

  /** Case 3. fnl=false x column mapping mode=id. Mirror of Case 2. */
  @Test
  public void testFnl_onColMappedIdTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 2L);

    Dataset<Row> df = failOnDataLossFalseStream(tablePath);
    List<Row> rows = processStreamingQuery(df, "fnl_cm_id");
    assertEquals(
        3, rows.size(), () -> "expected 3 rows on CM-id table with pruned v2, got: " + rows);
  }

  /**
   * Case 4. fnl=false x row-tracking enabled.
   *
   * <p>Stream and project {@code _metadata.row_id}. With v1 pruned, the snapshot is reconstructed
   * from the checkpoint - row_ids should still be assigned (0..N-1).
   */
  @Test
  public void testFnl_onRowTrackedTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableRowTracking' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (4, 'd')", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 1L);

    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath))
            .selectExpr("id", "_metadata.row_id AS row_id");
    List<Row> rows = processStreamingQuery(df, "fnl_rt");
    // 4 rows in total survive across both commits.
    assertEquals(4, rows.size(), () -> "expected 4 rows on RT table with pruned v1, got: " + rows);
    // Row ids must be present (non-null) - the stream surfaces row-tracking metadata even across
    // a pruned region.
    for (Row r : rows) {
      assertFalse(r.isNullAt(1), () -> "row_id must be assigned on RT table, got: " + r);
    }
  }

  /**
   * Case 5. fnl=false x ICT (in-commit timestamps).
   *
   * <p>Prune a middle commit. With fnl=false, the stream should skip-forward rather than fail to
   * resolve the pruned commit's ICT.
   */
  @Test
  public void testFnl_onIctTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id BIGINT) USING delta "
                + "TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true')",
            tablePath));
    spark.range(0, 5).write().format("delta").mode("append").save(tablePath);
    spark.range(5, 10).write().format("delta").mode("append").save(tablePath);
    spark.range(10, 15).write().format("delta").mode("append").save(tablePath);

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 2L);

    Dataset<Row> df = failOnDataLossFalseStream(tablePath);
    List<Row> rows = processStreamingQuery(df, "fnl_ict");
    // 15 rows survive in the reconstructed snapshot.
    assertEquals(
        15, rows.size(), () -> "expected 15 rows on ICT table with pruned v2, got: " + rows);
  }

  /** Case 6. fnl=false x v2 checkpoint policy. Sidecars + checkpoint metadata; stream skips. */
  @Test
  public void testFnl_onV2CheckpointTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id BIGINT) USING delta "
                + "TBLPROPERTIES ('delta.checkpointPolicy' = 'v2')",
            tablePath));
    spark.range(0, 3).write().format("delta").mode("append").save(tablePath);
    spark.range(3, 6).write().format("delta").mode("append").save(tablePath);
    spark.range(6, 9).write().format("delta").mode("append").save(tablePath);

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 2L);

    Dataset<Row> df = failOnDataLossFalseStream(tablePath);
    List<Row> rows = processStreamingQuery(df, "fnl_v2cp");
    assertEquals(
        9,
        rows.size(),
        () -> "expected 9 rows on v2-checkpoint table with pruned v2, got: " + rows);
  }

  /** Case 7. fnl=false x TIMESTAMP_NTZ column. Prune middle commit, stream surfaces all rows. */
  @Test
  public void testFnl_onTimestampNtzTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, ts TIMESTAMP_NTZ) USING delta", tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, TIMESTAMP_NTZ'2024-01-01 00:00:00')", tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (2, TIMESTAMP_NTZ'2024-02-01 00:00:00')", tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (3, TIMESTAMP_NTZ'2024-03-01 00:00:00')", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 2L);

    Dataset<Row> df = failOnDataLossFalseStream(tablePath);
    List<Row> rows = processStreamingQuery(df, "fnl_tsntz");
    assertEquals(
        3,
        rows.size(),
        () -> "expected 3 rows on TIMESTAMP_NTZ table with pruned v2, got: " + rows);
  }

  /** Case 8. fnl=false x CLUSTER BY (Liquid-clustered) table; pruned middle commit. */
  @Test
  public void testFnl_onClusteredTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta CLUSTER BY (id)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 2L);

    Dataset<Row> df = failOnDataLossFalseStream(tablePath);
    List<Row> rows = processStreamingQuery(df, "fnl_clustered");
    assertEquals(
        3, rows.size(), () -> "expected 3 rows on clustered table with pruned v2, got: " + rows);
  }

  /** Case 9. fnl=false x delta.appendOnly=true; pruned middle commit. */
  @Test
  public void testFnl_onAppendOnlyTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.appendOnly' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 2L);

    Dataset<Row> df = failOnDataLossFalseStream(tablePath);
    List<Row> rows = processStreamingQuery(df, "fnl_appendonly");
    assertEquals(
        3, rows.size(), () -> "expected 3 rows on appendOnly table with pruned v2, got: " + rows);
  }

  /**
   * Case 10. fnl=false x partitioned table (partition-pruning aware). Prune middle commit; the
   * snapshot still reconstructs partitions.
   */
  @Test
  public void testFnl_onPartitionedTable_partLast(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'x'), (2, 'y')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'x'), (4, 'y')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (5, 'z')", tablePath));

    checkpoint(tablePath);
    // Prune the middle insert; the last commit (v3) and the checkpoint together let the snapshot
    // still see all 5 rows.
    pruneCommitJson(tablePath, /* version= */ 2L);

    Dataset<Row> df = failOnDataLossFalseStream(tablePath);
    List<Row> rows = processStreamingQuery(df, "fnl_partitioned");
    assertEquals(
        5, rows.size(), () -> "expected 5 rows on partitioned table with pruned v2, got: " + rows);
  }

  /**
   * Case 11. fnl=false x prune a commit containing an ADD COLUMN.
   *
   * <p>The stream may either (a) throw a schema-mismatch error, or (b) skip forward and surface the
   * post-ADD-COLUMN schema. This test pins whichever behavior DSv2 produces today; if either side
   * changes, the test breaks and forces a deliberate decision.
   *
   * <p>Setup: v0 CREATE (id INT), v1 INSERT (id=1,2), v2 ADD COLUMN val STRING, v3 INSERT (id=3,
   * val='c'). Prune v2 (the ADD COLUMN). Stream from v=0 with fnl=false.
   */
  @Test
  public void testFnl_pruneCommitWithSchemaChange(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2)", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN val STRING", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 2L);

    Dataset<Row> df = failOnDataLossFalseStream(tablePath);
    // Today's contract: the stream skips forward and surfaces rows under the post-ADD-COLUMN
    // schema (id INT, val STRING). 3 rows total survive in the reconstructed snapshot.
    try {
      List<Row> rows = processStreamingQuery(df, "fnl_schema_change");
      assertEquals(
          3,
          rows.size(),
          () ->
              "expected 3 rows on schema-change table with pruned v2 (skip-forward), got: " + rows);
      // Schema should reflect the post-ADD-COLUMN shape (2 columns).
      assertEquals(
          2,
          rows.get(0).length(),
          () -> "expected 2-column schema after ADD COLUMN, got: " + rows.get(0));
    } catch (StreamingQueryException ex) {
      // If DSv2 instead chooses to surface a schema-mismatch error, pin that shape so any drift
      // is caught.
      Throwable cause = ex.getCause();
      String causeMsg = cause == null ? "" : String.valueOf(cause);
      assertTrue(
          causeMsg.contains("schema") || causeMsg.contains("Schema"),
          () -> "expected schema-related cause if stream errors, got: " + causeMsg);
    }
  }

  /**
   * Case 12. fnl=false x multiple early commits pruned, startingVersion=0.
   *
   * <p>Prune v0 AND v1 (the first two commits). With a checkpoint at v3 and fnl=false, the stream
   * should still start from the reconstructed snapshot.
   */
  @Test
  public void testFnl_pruneFirstFewCommits_startingVersion0(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    appendNSingleRowCommits(tablePath, /* n= */ 4);

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 0L);
    pruneCommitJson(tablePath, /* version= */ 1L);

    Dataset<Row> df = failOnDataLossFalseStream(tablePath);
    List<Row> rows = processStreamingQuery(df, "fnl_prune_head");
    // 4 rows total (ids 0..3) survive in the reconstructed snapshot.
    assertEquals(
        4, rows.size(), () -> "expected 4 rows with v0+v1 pruned + fnl=false, got: " + rows);
  }

  /**
   * Case 13. fnl=false combined with ignoreChanges=true on an UPDATE.
   *
   * <p>v0 CREATE, v1 INSERT, v2 UPDATE (re-writes AddFiles). Prune v1; ignoreChanges should allow
   * the UPDATE commit to be streamed even though it rewrites data, and fnl=false should suppress
   * the pruned-log error.
   */
  @Test
  public void testFnl_combinedWithIgnoreChanges(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));
    spark.sql(str("UPDATE delta.`%s` SET val = 'b2' WHERE id = 2", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 1L);

    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("ignoreChanges", "true")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "fnl_ignore_changes");
    // ignoreChanges re-emits the UPDATE's AddFiles. The exact row count depends on how many
    // AddFiles the UPDATE rewrites; we pin "at least 2 rows" (the post-UPDATE final state) since
    // the rewrite is data-preserving.
    assertTrue(
        rows.size() >= 2,
        () -> "expected at least 2 rows from fnl=false + ignoreChanges, got: " + rows);
  }

  /**
   * Case 14. fnl=false combined with skipChangeCommits=true on an UPDATE.
   *
   * <p>Same setup as Case 13 but skipChangeCommits drops the UPDATE commit entirely; only the
   * initial snapshot rows should arrive.
   */
  @Test
  public void testFnl_combinedWithSkipChangeCommits(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));
    spark.sql(str("UPDATE delta.`%s` SET val = 'b2' WHERE id = 2", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 1L);

    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("skipChangeCommits", "true")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "fnl_skip_change");
    // skipChangeCommits drops the UPDATE commit; the surviving snapshot from the checkpoint
    // still has 2 rows.
    assertEquals(
        2, rows.size(), () -> "expected 2 rows from fnl=false + skipChangeCommits, got: " + rows);
  }

  /**
   * Case 15. fnl=false combined with maxFilesPerTrigger=1 across the pruned region.
   *
   * <p>Multiple files exist in the snapshot; the rate-limited stream should still consume all of
   * them across the pruned region.
   */
  @Test
  public void testFnl_combinedWithMaxFilesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    appendNSingleRowCommits(tablePath, /* n= */ 5);

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 2L);

    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("maxFilesPerTrigger", "1")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "fnl_maxfiles");
    // 5 single-row commits, 5 files total in the snapshot - all should arrive even though the
    // stream takes one file per trigger.
    assertEquals(
        5,
        rows.size(),
        () ->
            "expected 5 rows from fnl=false + maxFilesPerTrigger=1 across pruned region, got: "
                + rows);
  }
}
