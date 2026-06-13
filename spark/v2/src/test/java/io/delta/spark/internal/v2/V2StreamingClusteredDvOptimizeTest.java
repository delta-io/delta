/*
 * Copyright (2025) The Delta Lake Project Authors.
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
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for V2 streaming reads on the Clustered x DV x OPTIMIZE triple-feature composition.
 *
 * <p>Each scenario exercises a Liquid-clustered table with deletion-vector deletes plus an OPTIMIZE
 * that materializes the DV into a physical rewrite. The streaming source must consume the OPTIMIZE
 * rewrite as a {@code dataChange=false} no-op (no re-emission of compacted/materialized rows),
 * while still respecting the initial snapshot's DV-filtered row set.
 *
 * <ol>
 *   <li>Basic clustered + DV + OPTIMIZE, no re-emission.
 *   <li>Multi-commit row-count oracle across OPTIMIZE.
 *   <li>Restart across OPTIMIZE on clustered + DV.
 *   <li>Trigger.AvailableNow post-OPTIMIZE.
 *   <li>maxFilesPerTrigger across OPTIMIZE + DV materialization.
 *   <li>ALTER CLUSTER BY -> DV-delete -> OPTIMIZE.
 *   <li>CLUSTER BY NONE mid-stream with DV present.
 *   <li>Multi-column CLUSTER BY + DV + OPTIMIZE.
 *   <li>OPTIMIZE ... ZORDER BY on a clustered + DV table (Delta rejects ZORDER on clustered).
 * </ol>
 */
public class V2StreamingClusteredDvOptimizeTest extends V2TestBase {

  /** 1 GiB - guarantees small per-commit files are below the OPTIMIZE compaction threshold. */
  private static final long ONE_GIB = 1024L * 1024L * 1024L;

  /**
   * Runs a streaming query against a parquet sink + checkpoint, processes all available data, then
   * stops. Returns the rows by reading the parquet output. Parquet sink supports checkpoint
   * recovery on restart, unlike memory sink.
   */
  private List<Row> runWithParquetSink(
      Dataset<Row> streamingDF, File outputDir, File checkpointDir, Trigger trigger)
      throws Exception {
    StreamingQuery query = null;
    try {
      org.apache.spark.sql.streaming.DataStreamWriter<Row> writer =
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
        query.awaitTermination(60_000);
      }
    } finally {
      if (query != null) {
        query.stop();
      }
    }
    return spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
  }

  /**
   * Forces OPTIMIZE to compact even tiny files. Without raising {@code minFileSize}, OPTIMIZE on a
   * few small commits may produce no rewrite at all, silently masking the test's intent.
   */
  private void runOptimize(String tablePath) {
    withSQLConf(
        "spark.databricks.delta.optimize.minFileSize",
        Long.toString(ONE_GIB),
        () -> spark.sql(str("OPTIMIZE delta.`%s`", tablePath)));
  }

  /**
   * 1. Clustered table + DV-DELETE + OPTIMIZE (materializes the DV into a clean rewrite,
   * dataChange=false). Stream must not re-emit the materialized rows.
   */
  @Test
  public void testBasic_clustered_dv_optimize_noReEmission(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (value INT) USING delta CLUSTER BY (value) "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark
        .range(10)
        .selectExpr("cast(id as int) as value")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);
    spark.sql(str("DELETE FROM delta.`%s` WHERE value < 3", tablePath));

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File outputDir = new File(deltaTablePath, "_out");
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);

    runOptimize(tablePath);

    try {
      List<Row> actualRows =
          runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);
      List<Row> expectedRows = new ArrayList<>();
      for (int i = 3; i < 10; i++) {
        expectedRows.add(RowFactory.create(i));
      }
      assertDataEquals(actualRows, expectedRows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 2. N independent commits to a clustered + DV table, DV-delete a subset, OPTIMIZE, then stream.
   * Assert exact (N - deleted) row count is emitted - no re-emission, no double-counting from the
   * DV materialization rewrite.
   */
  @Test
  public void testStreamRowCount_acrossOptimize_clustered_dv(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (value INT) USING delta CLUSTER BY (value) "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    int numCommits = 6;
    for (int i = 0; i < numCommits; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d)", tablePath, i));
    }
    // Delete two rows via DV.
    spark.sql(str("DELETE FROM delta.`%s` WHERE value IN (1, 4)", tablePath));
    runOptimize(tablePath);

    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "cluster_dv_opt_count");

    List<Row> expectedRows = new ArrayList<>();
    for (int i = 0; i < numCommits; i++) {
      if (i != 1 && i != 4) {
        expectedRows.add(RowFactory.create(i));
      }
    }
    assertEquals(numCommits - 2, actualRows.size());
    assertDataEquals(actualRows, expectedRows);
  }

  /**
   * 3. Stream, drain, DV-delete, OPTIMIZE, restart, finish. Parquet sink with checkpoint recovery.
   * Uses {@code skipChangeCommits=true} so the post-drain DV-delete (a data-change commit) is
   * skipped; the OPTIMIZE rewrite is dataChange=false and must not re-emit. Final cumulative row
   * count must equal the original drain plus any post-OPTIMIZE inserts - the one-shot oracle on the
   * final table state.
   */
  @Test
  public void testRestart_clustered_dv_optimizeBetweenRuns(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (value INT) USING delta CLUSTER BY (value) "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark
        .range(8)
        .selectExpr("cast(id as int) as value")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File outputDir = new File(deltaTablePath, "_out");
    Dataset<Row> streamingDF =
        spark.readStream().option("skipChangeCommits", "true").table(dsv2TableRef);
    // First drain: consume the initial 8 rows.
    List<Row> firstDrain =
        runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);
    assertEquals(8, firstDrain.size());

    // DV-delete some rows (skipped by skipChangeCommits), then OPTIMIZE to materialize the DV.
    spark.sql(str("DELETE FROM delta.`%s` WHERE value IN (2, 5)", tablePath));
    runOptimize(tablePath);

    // Add a real data commit after OPTIMIZE so the restart has new rows to emit.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (100), (101)", tablePath));

    try {
      List<Row> actualRows =
          runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);

      // One-shot oracle: a fresh stream over the final table state with skipChangeCommits=true.
      // From a fresh start the initial snapshot reflects the DV (rows {2,5} dropped) plus the
      // post-OPTIMIZE inserts {100,101}.
      List<Row> oracleRows =
          processStreamingQuery(
              spark.readStream().option("skipChangeCommits", "true").table(dsv2TableRef),
              "cluster_dv_opt_restart_oracle");

      // The restart should yield the original-8 rows from the first drain PLUS the (100,101)
      // commit emitted on resume. The DV-delete and OPTIMIZE contribute nothing.
      assertEquals(firstDrain.size() + 2, actualRows.size());
      List<Row> expected = new ArrayList<>(firstDrain);
      expected.add(RowFactory.create(100));
      expected.add(RowFactory.create(101));
      assertDataEquals(actualRows, expected);

      // Sanity: oracle on the final state has the post-DV survivors plus the two inserts.
      assertEquals(8 - 2 + 2, oracleRows.size());
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** 4. Trigger.AvailableNow over a clustered + DV table after OPTIMIZE - terminates cleanly. */
  @Test
  public void testAvailableNow_clustered_dv_optimize(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (value INT) USING delta CLUSTER BY (value) "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    for (int i = 0; i < 5; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d)", tablePath, i));
    }
    spark.sql(str("DELETE FROM delta.`%s` WHERE value = 2", tablePath));
    runOptimize(tablePath);

    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File outputDir = new File(deltaTablePath, "_out");
    try {
      List<Row> actualRows =
          runWithParquetSink(streamingDF, outputDir, checkpointDir, Trigger.AvailableNow());
      List<Row> expectedRows =
          Arrays.asList(
              RowFactory.create(0),
              RowFactory.create(1),
              RowFactory.create(3),
              RowFactory.create(4));
      assertDataEquals(actualRows, expectedRows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 5. maxFilesPerTrigger across the OPTIMIZE rewrite + DV materialization. The rate limit must not
   * cause the dataChange=false rewrite files to be exposed as additional batches.
   */
  @Test
  public void testMaxFilesPerTrigger_clustered_dv_optimize(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (value INT) USING delta CLUSTER BY (value) "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    // Several single-row commits -> several AddFiles for OPTIMIZE to compact.
    for (int i = 0; i < 5; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d)", tablePath, i));
    }
    spark.sql(str("DELETE FROM delta.`%s` WHERE value = 3", tablePath));
    runOptimize(tablePath);

    Dataset<Row> streamingDF =
        spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "cluster_dv_opt_mfpt");

    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(0), RowFactory.create(1), RowFactory.create(2), RowFactory.create(4));
    assertDataEquals(actualRows, expectedRows);
  }

  /**
   * 6. Start clustered by one column, ALTER CLUSTER BY a different column, DV-delete, OPTIMIZE,
   * then stream. The clustering-column change is a metadata-only commit; the stream should sail
   * through.
   */
  @Test
  public void testAlterClusterBy_then_dvDelete_then_optimize(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (col1 INT, col2 INT) USING delta CLUSTER BY (col1) "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 10), (2, 20), (3, 30), (4, 40)", tablePath));

    spark.sql(str("ALTER TABLE delta.`%s` CLUSTER BY (col2)", tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE col1 = 2", tablePath));
    runOptimize(tablePath);

    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "alter_cb_dv_opt");

    List<Row> expectedRows =
        Arrays.asList(RowFactory.create(1, 10), RowFactory.create(3, 30), RowFactory.create(4, 40));
    assertDataEquals(actualRows, expectedRows);
  }

  /**
   * 7. CLUSTER BY NONE on a DV-enabled table, then DV-delete + OPTIMIZE all before the stream
   * starts. The clustering-removal commit is metadata-only; the subsequent OPTIMIZE materializes
   * the DV into a clean rewrite. The stream must produce the post-DV row set without any
   * schema-mismatch from the metadata-level clustering change.
   */
  @Test
  public void testDropClustering_with_dv(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (value INT) USING delta CLUSTER BY (value) "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3), (4)", tablePath));

    // Drop clustering, DV-delete, OPTIMIZE - all before stream startup so the initial snapshot
    // reflects the post-DV state and no change-commit handling is required.
    spark.sql(str("ALTER TABLE delta.`%s` CLUSTER BY NONE", tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE value = 2", tablePath));
    runOptimize(tablePath);

    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "drop_cluster_dv_opt");

    List<Row> expectedRows =
        Arrays.asList(RowFactory.create(1), RowFactory.create(3), RowFactory.create(4));
    assertDataEquals(actualRows, expectedRows);
  }

  /** 8. Multi-column CLUSTER BY + DV-delete + OPTIMIZE. */
  @Test
  public void testMultiColumnClusterBy_dv_optimize(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (col1 INT, col2 STRING) USING delta "
                + "CLUSTER BY (col1, col2) "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D')", tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE col1 = 2", tablePath));
    runOptimize(tablePath);

    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "multi_cluster_dv_opt");

    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(1, "A"), RowFactory.create(3, "C"), RowFactory.create(4, "D"));
    assertDataEquals(actualRows, expectedRows);
  }

  /**
   * 9. OPTIMIZE ... ZORDER BY on a clustered + DV table. Delta rejects ZORDER on clustered tables
   * with {@code DELTA_CLUSTERING_WITH_ZORDER_BY} - this test pins that behavior. If the underlying
   * Delta version ever relaxes this, the test will fail loudly and force a re-evaluation of whether
   * the streaming source needs to handle the new code path.
   */
  @Test
  public void testRowCount_optimize_zorder_on_clustered_dv(@TempDir File deltaTablePath) {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (value INT) USING delta CLUSTER BY (value) "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3), (4)", tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE value = 2", tablePath));

    AnalysisException thrown =
        assertThrows(
            AnalysisException.class,
            () ->
                withSQLConf(
                    "spark.databricks.delta.optimize.minFileSize",
                    Long.toString(ONE_GIB),
                    () -> spark.sql(str("OPTIMIZE delta.`%s` ZORDER BY (value)", tablePath))));
    // Delta surfaces ZORDER-on-clustered as DELTA_CLUSTERING_WITH_ZORDER_BY.
    assertTrue(
        thrown.getMessage() != null
            && (thrown.getMessage().contains("DELTA_CLUSTERING_WITH_ZORDER_BY")
                || thrown.getMessage().toLowerCase().contains("zorder")),
        () -> "Unexpected error message: " + thrown.getMessage());
  }
}
