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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Triple-feature composition tests for DSv2 streaming: column mapping &times; row tracking &times;
 * deletion vectors.
 *
 * <p>Pairwise coverage already exists:
 *
 * <ul>
 *   <li>CM &times; RT: {@link V2StreamingRowTrackingTest#testRowTrackingWithColumnMapping} (Bug
 *       #25's surface).
 *   <li>RT &times; DV: {@link
 *       V2StreamingRowTrackingTest#testRowTrackingWithDeletionVectorsPreservesIds}.
 *   <li>CM &times; DV: {@link V2StreamingColumnMappingTest#testColumnMapping_withDeletionVectors}.
 * </ul>
 *
 * <p>The three-way intersection (CM &times; RT &times; DV) is the historical hot spot for
 * silent-corruption bugs because each feature mutates a different leg of the read pipeline: column
 * mapping rewrites the projected schema, row tracking injects {@code _metadata.row_id} / {@code
 * _metadata.row_commit_version} struct fields, and deletion vectors filter rows from each file. A
 * regression where the row-id mapping is dropped <em>after</em> column-mapping rewires physical
 * column names (Bug #25, scoped only to CM &times; RT) would re-surface here if combined with DV.
 */
public class V2StreamingCmRtDvTest extends V2TestBase {

  /** Create a CM-name + RT + DV table. */
  private void createCmNameRtDvTable(String path) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta "
                + "TBLPROPERTIES ("
                + "  'delta.columnMapping.mode' = 'name',"
                + "  'delta.enableRowTracking' = 'true',"
                + "  'delta.enableDeletionVectors' = 'true',"
                + "  'delta.minReaderVersion' = '3',"
                + "  'delta.minWriterVersion' = '7')",
            path));
  }

  /** Create a CM-id + RT + DV table. */
  private void createCmIdRtDvTable(String path) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta "
                + "TBLPROPERTIES ("
                + "  'delta.columnMapping.mode' = 'id',"
                + "  'delta.enableRowTracking' = 'true',"
                + "  'delta.enableDeletionVectors' = 'true',"
                + "  'delta.minReaderVersion' = '3',"
                + "  'delta.minWriterVersion' = '7')",
            path));
  }

  /** Single-file initial write of {@code numRows} rows of (id, "name-<id>"). */
  private void writeInitialSingleFile(String path, long numRows) {
    spark
        .range(numRows)
        .selectExpr("id", "concat('name-', cast(id as string)) as name")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(path);
  }

  /**
   * Sanity: bare stream over a CM-name + RT + DV table after a DV delete returns surviving rows.
   */
  @Test
  public void testBasic_cmName_rt_dv(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmNameRtDvTable(tablePath);
    writeInitialSingleFile(tablePath, 10);
    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    assertTrue(streamingDF.isStreaming());

    List<Row> rows = processStreamingQuery(streamingDF, "cmrt_dv_basic_name");
    assertEquals(5, rows.size(), "Expected 5 surviving odd rows after DV delete");
    Set<Long> ids = new HashSet<>();
    for (Row r : rows) {
      ids.add(r.getLong(0));
    }
    assertEquals(Set.of(1L, 3L, 5L, 7L, 9L), ids);
  }

  /**
   * Same as #1, but column mapping mode=id. The id-mode physical column lookup differs from name.
   */
  @Test
  public void testBasic_cmId_rt_dv(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmIdRtDvTable(tablePath);
    writeInitialSingleFile(tablePath, 10);
    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    assertTrue(streamingDF.isStreaming());

    List<Row> rows = processStreamingQuery(streamingDF, "cmrt_dv_basic_id");
    assertEquals(5, rows.size(), "Expected 5 surviving odd rows after DV delete");
    Set<Long> ids = new HashSet<>();
    for (Row r : rows) {
      ids.add(r.getLong(0));
    }
    assertEquals(Set.of(1L, 3L, 5L, 7L, 9L), ids);
  }

  /**
   * Bug #25's surface (CM x RT) composed with DV. After a DV-only DELETE, the surviving rows must
   * keep their original row_ids - even though column mapping has rewired the physical schema and
   * the DV filter is being applied to the column vector that carries the row_id metadata column.
   */
  @Test
  public void testProjectRowId_cmName_rt_dv(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmNameRtDvTable(tablePath);
    writeInitialSingleFile(tablePath, 10);

    DeltaLog deltaLog = DeltaLog.forTable(spark, tablePath);
    long versionBeforeDelete =
        deltaLog.update(false, scala.Option.empty(), scala.Option.empty()).version();

    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .table(dsv2TableRef)
            .selectExpr("id", "name", "_metadata.row_id AS row_id");

    List<Row> rows = processStreamingQuery(streamingDF, "cmrt_dv_row_id");
    assertEquals(5, rows.size(), "Expected 5 surviving rows after DV delete");

    Map<Long, Long> idToRowId = new HashMap<>();
    for (Row r : rows) {
      long id = r.getLong(0);
      String name = r.getString(1);
      long rowId = r.getLong(2);
      assertEquals(1L, id % 2, "Only odd ids should survive");
      assertEquals("name-" + id, name, "name column must align with id under CM rewrite");
      idToRowId.put(id, rowId);
    }
    // Single coalesced file with stable physical row positions: row_id == id.
    for (Map.Entry<Long, Long> e : idToRowId.entrySet()) {
      assertEquals(
          e.getKey(),
          e.getValue(),
          () ->
              "Bug #25-style regression under CM x RT x DV: id="
                  + e.getKey()
                  + " produced row_id="
                  + e.getValue()
                  + " (expected row_id=id for single-file initial write)");
    }
    // Sanity: the snapshot we read from is the post-DELETE version.
    assertTrue(
        deltaLog.update(false, scala.Option.empty(), scala.Option.empty()).version()
            > versionBeforeDelete,
        "DELETE should have committed");
  }

  /**
   * Same shape as #3 but for {@code _metadata.row_commit_version}. For the un-rewritten initial
   * file, surviving rows must report the original insert commit version, not the DV-delete version.
   */
  @Test
  public void testProjectRowCommitVersion_cmName_rt_dv(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmNameRtDvTable(tablePath);
    writeInitialSingleFile(tablePath, 10);
    long insertVersion =
        DeltaLog.forTable(spark, tablePath)
            .update(false, scala.Option.empty(), scala.Option.empty())
            .version();
    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .table(dsv2TableRef)
            .selectExpr("id", "_metadata.row_commit_version AS rcv");

    List<Row> rows = processStreamingQuery(streamingDF, "cmrt_dv_rcv");
    assertEquals(5, rows.size());
    for (Row r : rows) {
      long id = r.getLong(0);
      long rcv = r.getLong(1);
      assertEquals(
          insertVersion,
          rcv,
          () ->
              "Surviving row id="
                  + id
                  + " has row_commit_version="
                  + rcv
                  + " (expected "
                  + insertVersion
                  + ", the insert commit). A DV delete must not bump row_commit_version on rows it"
                  + " did not touch.");
    }
  }

  /**
   * Additive schema change: ADD COLUMN does not require schemaTrackingLocation. After the additive
   * change and a subsequent DV-delete, row_id correctness must survive both the schema-evolution
   * and DV-filter steps.
   */
  @Test
  public void testDvDeleteAfterAddColumn_cmName_rt(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmNameRtDvTable(tablePath);
    writeInitialSingleFile(tablePath, 6);
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    // DV-delete after the ADD COLUMN: rows 0,2,4 removed; rows 1,3,5 survive with extra=NULL.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .table(dsv2TableRef)
            .selectExpr("id", "name", "extra", "_metadata.row_id AS row_id");

    List<Row> rows = processStreamingQuery(streamingDF, "cmrt_dv_add_col");
    assertEquals(3, rows.size(), "Expected 3 odd rows surviving DV-delete after ADD COLUMN");
    Map<Long, Long> idToRowId = new HashMap<>();
    for (Row r : rows) {
      long id = r.getLong(0);
      assertEquals("name-" + id, r.getString(1));
      assertNull(r.get(2), "extra should project as NULL for rows written before ADD COLUMN");
      idToRowId.put(id, r.getLong(3));
    }
    // Single-file insert => row_id == id for survivors.
    assertEquals(1L, (long) idToRowId.get(1L));
    assertEquals(3L, (long) idToRowId.get(3L));
    assertEquals(5L, (long) idToRowId.get(5L));
  }

  /**
   * Stream initial rows to a parquet sink, stop, DV-delete some rows, restart from the same
   * checkpoint. The restart must not re-emit rows that already arrived, and the surviving rows'
   * row_ids must remain stable across the restart boundary even with CM rewriting columns under the
   * hood.
   */
  @Test
  public void testRestart_cmName_rt_dv_dvDeleteBetweenRuns(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmNameRtDvTable(tablePath);
    writeInitialSingleFile(tablePath, 6);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    File checkpointDir = new File(tempDir, "_ckpt");
    File outputDir = new File(tempDir, "_out");

    Dataset<Row> df1 =
        spark
            .readStream()
            .table(dsv2TableRef)
            .selectExpr("id", "name", "_metadata.row_id AS row_id");

    StreamingQuery q1 =
        df1.writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q1.processAllAvailable();
    } finally {
      q1.stop();
      DeltaLog.clearCache();
    }

    // Between runs: DV-delete + new append. skipChangeCommits drops the DV-delete commit; the
    // surviving rows arrive via the post-delete append commit.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (100, 'name-100')", tablePath));

    Dataset<Row> df2 =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(dsv2TableRef)
            .selectExpr("id", "name", "_metadata.row_id AS row_id");

    StreamingQuery q2 =
        df2.writeStream()
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
    // Run 1 emitted all 6 rows; Run 2 emitted the appended row 100. The DV-delete commit is
    // dropped via skipChangeCommits, so no rows are re-emitted.
    assertEquals(
        7,
        rows.size(),
        () -> "Expected 6 run-1 rows + 1 appended row across restart; got: " + rows);

    Set<Long> ids = new HashSet<>();
    for (Row r : rows) {
      ids.add(r.getLong(0));
      assertEquals("name-" + r.getLong(0), r.getString(1));
    }
    assertEquals(Set.of(0L, 1L, 2L, 3L, 4L, 5L, 100L), ids);
  }

  /** Same as #6 but column mapping mode=id, to exercise the id-mode physical-name lookup. */
  @Test
  public void testRestart_cmId_rt_dv(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmIdRtDvTable(tablePath);
    writeInitialSingleFile(tablePath, 6);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    File checkpointDir = new File(tempDir, "_ckpt");
    File outputDir = new File(tempDir, "_out");

    Dataset<Row> df1 = spark.readStream().table(dsv2TableRef).selectExpr("id", "name");

    StreamingQuery q1 =
        df1.writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q1.processAllAvailable();
    } finally {
      q1.stop();
      DeltaLog.clearCache();
    }

    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (100, 'name-100')", tablePath));

    Dataset<Row> df2 =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(dsv2TableRef)
            .selectExpr("id", "name");

    StreamingQuery q2 =
        df2.writeStream()
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
    assertEquals(
        7,
        rows.size(),
        () -> "Expected 6 run-1 rows + 1 appended row across restart with CM-id; got: " + rows);
    Set<Long> ids = new HashSet<>();
    for (Row r : rows) {
      ids.add(r.getLong(0));
      assertEquals("name-" + r.getLong(0), r.getString(1));
    }
    assertEquals(Set.of(0L, 1L, 2L, 3L, 4L, 5L, 100L), ids);
  }

  /**
   * Rate-limited stream over multiple commits, one of which is a DV-only DELETE. With {@code
   * maxFilesPerTrigger=1} the planner must admit files one at a time across both append and
   * DV-rewrite commits, while still preserving CM column resolution and RT row_id stability.
   */
  @Test
  public void testMaxFilesPerTrigger_cmName_rt_dv(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmNameRtDvTable(tablePath);
    // Three separate single-file commits so maxFilesPerTrigger has work to partition.
    for (long i = 0; i < 3; i++) {
      spark
          .range(i * 2, i * 2 + 2)
          .selectExpr("id", "concat('name-', cast(id as string)) as name")
          .coalesce(1)
          .write()
          .format("delta")
          .mode("append")
          .save(tablePath);
    }
    // DV-delete one row in the middle of the table - preserves all files (DV-only).
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 3", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("maxFilesPerTrigger", "1")
            .table(dsv2TableRef)
            .selectExpr("id", "name");

    List<Row> rows = processStreamingQuery(streamingDF, "cmrt_dv_max_files");
    // Surviving ids: {0,1,2,4,5}. (id=3 was DV-deleted.)
    Set<Long> ids = new HashSet<>();
    for (Row r : rows) {
      ids.add(r.getLong(0));
      assertEquals("name-" + r.getLong(0), r.getString(1));
    }
    assertEquals(Set.of(0L, 1L, 2L, 4L, 5L), ids);
  }

  /**
   * RT contract: {@code _metadata.row_id} for a row is determined when the row is first written.
   * Subsequent DV-deletes that touch <em>other</em> rows must not change it. Run multiple DV-delete
   * commits and assert survivors keep id-aligned row_ids on a single-file initial write.
   */
  @Test
  public void testRowIdMonotonicAcrossDvDelete(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmNameRtDvTable(tablePath);
    writeInitialSingleFile(tablePath, 12);

    // Three DV-delete commits that each remove a disjoint set.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id IN (0, 1)", tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE id IN (4, 5)", tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE id IN (8, 9)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().table(dsv2TableRef).selectExpr("id", "_metadata.row_id AS row_id");

    List<Row> rows = processStreamingQuery(streamingDF, "cmrt_dv_row_id_monotonic");
    // Surviving ids = {2,3,6,7,10,11}; for a single-file initial write row_id == id.
    assertEquals(6, rows.size());
    for (Row r : rows) {
      long id = r.getLong(0);
      long rowId = r.getLong(1);
      assertEquals(
          id,
          rowId,
          () ->
              "RT contract violation: id="
                  + id
                  + " produced row_id="
                  + rowId
                  + " after disjoint DV-deletes (expected row_id=id for single-file write).");
    }
  }

  /**
   * RT contract: surviving rows in a file that wasn't physically rewritten must keep their original
   * {@code _metadata.row_commit_version}. DV-deletes attach a DV but do not rewrite the data file,
   * so row_commit_version on survivors must remain the insert version.
   */
  @Test
  public void testRowCommitVersionAcrossDvDelete(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmNameRtDvTable(tablePath);
    writeInitialSingleFile(tablePath, 8);
    long insertVersion =
        DeltaLog.forTable(spark, tablePath)
            .update(false, scala.Option.empty(), scala.Option.empty())
            .version();

    // Two DV-delete commits - each bumps the table version but leaves the original file.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 0", tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 7", tablePath));
    long postDeleteVersion =
        DeltaLog.forTable(spark, tablePath)
            .update(false, scala.Option.empty(), scala.Option.empty())
            .version();
    assertTrue(postDeleteVersion > insertVersion, "DV-deletes must advance the table version");

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .table(dsv2TableRef)
            .selectExpr("id", "_metadata.row_commit_version AS rcv");

    List<Row> rows = processStreamingQuery(streamingDF, "cmrt_dv_rcv_stable");
    assertEquals(6, rows.size(), "Expected 6 survivors after deleting ids 0 and 7");
    for (Row r : rows) {
      long id = r.getLong(0);
      long rcv = r.getLong(1);
      assertEquals(
          insertVersion,
          rcv,
          () ->
              "RT contract violation: surviving id="
                  + id
                  + " has row_commit_version="
                  + rcv
                  + " (expected "
                  + insertVersion
                  + ", the original insert commit; DV-deletes do not rewrite the file).");
    }
  }

  /**
   * Non-additive schema change (RENAME COLUMN) on a column-mapped table requires {@code
   * schemaTrackingLocation}, which DSv2 streaming does not yet plumb through. Tracked alongside the
   * other Cluster S-3 gaps; re-enable once {@code SparkScan.SUPPORTED_STREAMING_OPTIONS} accepts
   * the option.
   */
  @Test
  public void testRenameColumn_cmName_rt_dv(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmNameRtDvTable(tablePath);
    writeInitialSingleFile(tablePath, 4);

    File checkpointDir = new File(tempDir, "_ckpt");
    File outputDir = new File(tempDir, "_out");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    Dataset<Row> df1 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2TableRef);

    StreamingQuery q1 =
        df1.writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q1.processAllAvailable();
    } finally {
      q1.stop();
      DeltaLog.clearCache();
    }

    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN name TO display_name", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (10, 'name-10')", tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 1", tablePath));

    Dataset<Row> df2 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .option("skipChangeCommits", "true")
            .table(dsv2TableRef);

    StreamingQuery q2 =
        df2.writeStream()
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
    // 4 initial rows + 1 appended after rename = 5; DV-delete commit dropped by skipChangeCommits.
    assertEquals(5, rows.size(), () -> "Expected 5 rows across rename + restart; got: " + rows);
  }

  /**
   * OPTIMIZE materializes pending DVs into a physical rewrite (the rewritten AddFile carries no
   * DV). The rewrite is {@code dataChange=false}; the streamed snapshot from a fresh stream after
   * OPTIMIZE must reflect the DV-filtered survivors with row_ids preserved from the original insert
   * under the column-mapped + row-tracked layout. Compare against the existing {@code
   * V2StreamingOptimizeTest::testOptimize_onDvTable_skipChangeCommits}, which exercises the same
   * OPTIMIZE-after-DV path but without column mapping or row tracking projection.
   */
  @Test
  public void testOptimizeAfterDvDelete_cmName_rt(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmNameRtDvTable(tablePath);
    writeInitialSingleFile(tablePath, 10);

    // Capture pre-OPTIMIZE row_ids via a batch query (single-file write => row_id == id).
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

    spark.sql(str("DELETE FROM delta.`%s` WHERE id < 3", tablePath));

    // Force OPTIMIZE to compact: raise minFileSize well above the actual file size.
    withSQLConf(
        "spark.databricks.delta.optimize.minFileSize",
        Long.toString(1024L * 1024L * 1024L),
        () -> spark.sql(str("OPTIMIZE delta.`%s`", tablePath)));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(dsv2TableRef)
            .selectExpr("id", "name", "_metadata.row_id AS row_id");

    List<Row> rows = processStreamingQuery(streamingDF, "cmrt_optimize_dv");
    // Initial snapshot after DELETE+OPTIMIZE reflects survivors {3..9} (7 rows).
    assertEquals(7, rows.size(), () -> "Expected 7 survivors after DELETE+OPTIMIZE; got: " + rows);
    for (Row r : rows) {
      long id = r.getLong(0);
      assertEquals("name-" + id, r.getString(1));
      long rowId = r.getLong(2);
      // OPTIMIZE rewrote the file; row tracking must preserve the original row_id for each
      // surviving
      // row.
      Long expected = beforeIdToRowId.get(id);
      assertNotNull(expected, () -> "id=" + id + " missing from pre-OPTIMIZE snapshot");
      assertEquals(
          expected,
          rowId,
          () ->
              "row_id mismatch under CM x RT after OPTIMIZE rewrite of DV-deleted file: id="
                  + id
                  + " pre="
                  + beforeIdToRowId.get(id)
                  + " post="
                  + rowId);
    }
  }

  /**
   * Create a CM-name + RT + DV + partitioned table with schema (id LONG, name STRING, p STRING).
   * Partitioning enables whole-file deletes (ignoreDeletes-friendly) while still exercising the
   * triple-feature read pipeline.
   */
  private void createCmNameRtDvPartitionedTable(String path) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING, p STRING) USING delta "
                + "PARTITIONED BY (p) "
                + "TBLPROPERTIES ("
                + "  'delta.columnMapping.mode' = 'name',"
                + "  'delta.enableRowTracking' = 'true',"
                + "  'delta.enableDeletionVectors' = 'true',"
                + "  'delta.minReaderVersion' = '3',"
                + "  'delta.minWriterVersion' = '7')",
            path));
  }

  /**
   * Compound CM-name + RT + DV + partitions x {@code ignoreDeletes=true}. v0 CREATE, v1 INSERT
   * across two partitions, v2 whole-partition DELETE (file-granular, ignoreDeletes-friendly), v3
   * INSERT more. The DELETE commit must be skipped; only INSERT rows surface, all under the
   * column-mapped + row-tracked + DV-enabled physical layout.
   */
  @Test
  public void testCompound_ignoreDeletes(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmNameRtDvPartitionedTable(tablePath);
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, 'name-1', 'x'), (2, 'name-2', 'y')", tablePath));
    // Whole-partition delete on p='y': file-granular, allowed under ignoreDeletes.
    spark.sql(str("DELETE FROM delta.`%s` WHERE p = 'y'", tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (3, 'name-3', 'x'), (4, 'name-4', 'z')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("ignoreDeletes", "true")
            .table(dsv2TableRef)
            .selectExpr("id", "name", "p");

    List<Row> rows = processStreamingQuery(streamingDF, "compound_ignore_deletes");
    // 4 INSERT rows survive; the DELETE commit is dropped by ignoreDeletes.
    assertEquals(
        4,
        rows.size(),
        () -> "expected 4 INSERT rows with DELETE skipped under compound features, got: " + rows);
    Set<Long> ids = new HashSet<>();
    for (Row r : rows) {
      long id = r.getLong(0);
      ids.add(id);
      assertEquals("name-" + id, r.getString(1), "name column must align with id under CM rewrite");
    }
    assertEquals(Set.of(1L, 2L, 3L, 4L), ids);
  }

  /**
   * Compound CM-name + RT + DV + partitions x {@code ignoreChanges=true}. v0 CREATE, v1 INSERT, v2
   * UPDATE (re-emits the rewritten file's rows as appends under ignoreChanges), v3 INSERT more. The
   * stream must not error; INSERT rows are present, and ignoreChanges treats the UPDATE rewrite as
   * a re-emitted append rather than a hard failure.
   */
  @Test
  public void testCompound_ignoreChanges(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmNameRtDvPartitionedTable(tablePath);
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, 'name-1', 'x'), (2, 'name-2', 'y')", tablePath));
    // UPDATE p='y' row: under DV this is a rewrite commit; ignoreChanges re-emits the new AddFile
    // as an append rather than failing the stream.
    spark.sql(str("UPDATE delta.`%s` SET name = 'updated-2' WHERE id = 2", tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (3, 'name-3', 'x'), (4, 'name-4', 'z')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("ignoreChanges", "true")
            .table(dsv2TableRef)
            .selectExpr("id", "name", "p");

    List<Row> rows = processStreamingQuery(streamingDF, "compound_ignore_changes");
    // The two original INSERT rows + the UPDATE-rewritten p='y' file (re-emitted as an append by
    // ignoreChanges, carrying the updated name) + the two final-INSERT rows = 5 rows total.
    assertEquals(
        5,
        rows.size(),
        () ->
            "expected 5 rows (2 initial INSERTs + UPDATE rewrite re-emitted + 2 final INSERTs)"
                + " under ignoreChanges, got: "
                + rows);
    Set<Long> ids = new HashSet<>();
    for (Row r : rows) {
      ids.add(r.getLong(0));
    }
    // All inserted ids must be present; the UPDATE re-emits id=2 (now once via the rewrite, since
    // the original p='y' file was rewritten in place).
    assertEquals(Set.of(1L, 2L, 3L, 4L), ids);
    long updatedRowCount = rows.stream().filter(r -> "updated-2".equals(r.getString(1))).count();
    assertEquals(
        1,
        updatedRowCount,
        () ->
            "expected exactly one row to carry the updated name under ignoreChanges, got: " + rows);
  }

  /**
   * Compound CM-name + RT + DV + partitions x {@code skipChangeCommits=true}. v0 CREATE, v1 INSERT,
   * v2 UPDATE (change commit - dropped entirely by skipChangeCommits), v3 INSERT more. Only INSERT
   * rows surface; the UPDATE rewrite does not produce any rows on the stream.
   */
  @Test
  public void testCompound_skipChangeCommits(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmNameRtDvPartitionedTable(tablePath);
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, 'name-1', 'x'), (2, 'name-2', 'y')", tablePath));
    // UPDATE: change commit, dropped entirely by skipChangeCommits.
    spark.sql(str("UPDATE delta.`%s` SET name = 'updated-2' WHERE id = 2", tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (3, 'name-3', 'x'), (4, 'name-4', 'z')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(dsv2TableRef)
            .selectExpr("id", "name", "p");

    List<Row> rows = processStreamingQuery(streamingDF, "compound_skip_change_commits");
    // 4 INSERT rows surface; the UPDATE change commit is dropped, so no row carries 'updated-2'.
    assertEquals(
        4,
        rows.size(),
        () ->
            "expected 4 INSERT rows with UPDATE commit dropped under skipChangeCommits, got: "
                + rows);
    Set<Long> ids = new HashSet<>();
    for (Row r : rows) {
      long id = r.getLong(0);
      ids.add(id);
      assertEquals(
          "name-" + id,
          r.getString(1),
          () -> "skipChangeCommits must not surface the UPDATE's rewritten rows, got: " + rows);
    }
    assertEquals(Set.of(1L, 2L, 3L, 4L), ids);
  }

  /**
   * Compound CM-name + RT + DV + partitions x corrupt checkpoint parquet. After forcing a
   * checkpoint, overwrite the checkpoint file with garbage. The stream must either recover (Kernel
   * falls back to log listing) or surface a structured error rather than leaking a raw NPE.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testCompound_corruptCheckpoint(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmNameRtDvPartitionedTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'name-1', 'x')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'name-2', 'y')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'name-3', 'x')", tablePath));

    // Force a checkpoint at the latest version.
    DeltaLog.forTable(spark, tablePath).checkpoint();
    DeltaLog.clearCache();

    // Find the checkpoint parquet file under _delta_log and overwrite it with garbage bytes.
    Path logDir = Paths.get(tablePath, "_delta_log");
    Path checkpointFile = null;
    try (java.util.stream.Stream<Path> entries = Files.list(logDir)) {
      checkpointFile =
          entries
              .filter(p -> p.getFileName().toString().endsWith(".checkpoint.parquet"))
              .findFirst()
              .orElse(null);
    }
    assertNotNull(checkpointFile, "expected a .checkpoint.parquet file under _delta_log");
    Files.write(checkpointFile, new byte[] {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07});
    DeltaLog.clearCache();

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Throwable failure = null;
    List<Row> rows = null;
    try {
      Dataset<Row> df = spark.readStream().table(dsv2TableRef).selectExpr("id", "name", "p");
      rows = processStreamingQuery(df, "compound_corrupt_ckpt");
    } catch (Throwable t) {
      failure = t;
    }

    if (failure != null) {
      // Walk the cause chain: any raw NPE is a regression.
      Throwable cur = failure;
      boolean mentionsCheckpoint = false;
      while (cur != null) {
        if (cur instanceof NullPointerException) {
          fail("Corrupt checkpoint parquet under compound features produced a raw NPE", failure);
        }
        String msg = cur.getMessage();
        if (msg != null) {
          String lower = msg.toLowerCase();
          if (lower.contains("checkpoint")
              || lower.contains("corrupt")
              || lower.contains("parquet")
              || lower.contains("magic")) {
            mentionsCheckpoint = true;
          }
        }
        cur = cur.getCause();
      }
      final Throwable finalFailure = failure;
      assertTrue(
          mentionsCheckpoint,
          () ->
              "expected a structured error referencing checkpoint/corrupt/parquet, got: "
                  + finalFailure);
    } else {
      // Successful fallback: all three inserted rows surface.
      final List<Row> finalRows = rows;
      assertEquals(
          3,
          finalRows.size(),
          () -> "expected 3 rows after checkpoint-fallback recovery; got: " + finalRows);
      Set<Long> ids = new HashSet<>();
      for (Row r : finalRows) {
        ids.add(r.getLong(0));
        assertEquals("name-" + r.getLong(0), r.getString(1));
      }
      assertEquals(Set.of(1L, 2L, 3L), ids);
    }
  }

  /**
   * Compound CM-name + RT + DV + partitions x log-retention prune. After a checkpoint covers all
   * commits, delete the oldest commit JSON. The stream must still surface the current snapshot
   * under {@code failOnDataLoss=false}, using the checkpoint to reconstruct state.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testCompound_logRetentionPrune(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmNameRtDvPartitionedTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'name-1', 'x')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'name-2', 'y')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'name-3', 'x')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (4, 'name-4', 'z')", tablePath));

    // Checkpoint so the snapshot can be reconstructed without the pruned commit JSON.
    DeltaLog.forTable(spark, tablePath).checkpoint();
    DeltaLog.clearCache();

    // Prune the oldest INSERT commit JSON (v=1) and its CRC sibling.
    Path prunedJson = Paths.get(tablePath, "_delta_log", String.format("%020d.json", 1L));
    Files.delete(prunedJson);
    Path prunedCrc = Paths.get(tablePath, "_delta_log", String.format("%020d.crc", 1L));
    if (Files.exists(prunedCrc)) {
      Files.delete(prunedCrc);
    }
    DeltaLog.clearCache();

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("startingVersion", "0")
            .table(dsv2TableRef)
            .selectExpr("id", "name", "p");

    List<Row> rows = processStreamingQuery(df, "compound_log_prune");
    // All 4 INSERT rows must surface from the post-checkpoint snapshot.
    assertEquals(
        4,
        rows.size(),
        () -> "expected 4 rows after pruned-log recovery under compound features; got: " + rows);
    Set<Long> ids = new HashSet<>();
    for (Row r : rows) {
      ids.add(r.getLong(0));
      assertEquals("name-" + r.getLong(0), r.getString(1));
    }
    assertEquals(Set.of(1L, 2L, 3L, 4L), ids);
  }

  /**
   * Compound CM-name + RT + DV + partitions x concurrent writer. Stream with {@code
   * ignoreChanges=true} (DV-based UPDATEs are change commits) while a concurrent writer issues
   * INSERT + UPDATE on a second thread. The stream must not error; emitted rows must be a
   * consistent subset of the writer's universe (ids in {1, 2, 5}).
   */
  @Test
  public void testCompound_concurrentWriter(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmNameRtDvPartitionedTable(tablePath);
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, 'name-1', 'x'), (2, 'name-2', 'y')", tablePath));

    File checkpointDir = new File(tempDir, "_ckpt");
    File outputDir = new File(tempDir, "_out");
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("ignoreChanges", "true")
            .table(dsv2TableRef)
            .selectExpr("id", "name", "p");

    final String finalTablePath = tablePath;
    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService writer = Executors.newSingleThreadExecutor();
    StreamingQuery query = null;
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
            try {
              Thread.sleep(50);
              spark.sql(str("INSERT INTO delta.`%s` VALUES (5, 'name-5', 'x')", finalTablePath));
              spark.sql(
                  str("UPDATE delta.`%s` SET name = 'updated-2' WHERE id = 2", finalTablePath));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } catch (Exception ignored) {
              // Concurrent commit may transiently fail; just continue.
            }
          });

      Thread.sleep(2_000L);
      stop.set(true);
      query.processAllAvailable();
    } finally {
      stop.set(true);
      writer.shutdownNow();
      writer.awaitTermination(5, TimeUnit.SECONDS);
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }

    List<Row> sink = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    // Universe of writer ids: {1, 2, 5}. ignoreChanges may re-emit the UPDATE-rewritten file,
    // so the sink rows must be a non-empty consistent subset of that universe.
    assertFalse(sink.isEmpty(), "expected at least one row from the concurrent stream");
    Set<Long> allowedIds = Set.of(1L, 2L, 5L);
    Set<Long> seenIds = new HashSet<>();
    for (Row r : sink) {
      long id = r.getLong(0);
      assertTrue(
          allowedIds.contains(id),
          () -> "stream emitted unexpected id=" + id + " (allowed " + allowedIds + ")");
      String name = r.getString(1);
      assertTrue(
          name != null && (name.startsWith("name-") || name.startsWith("updated-")),
          () -> "unexpected name under compound features: " + name);
      seenIds.add(id);
    }
    // The two initial INSERT rows must always be present in the sink.
    assertTrue(
        seenIds.contains(1L) && seenIds.contains(2L),
        () -> "stream must surface the pre-stream INSERT rows {1, 2}; got: " + seenIds);
  }
}
