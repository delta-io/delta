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
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * DSv2 streaming Trigger.Once / Trigger.ProcessingTime("0s") x feature matrix.
 *
 * <p>Phase-0 re-baseline scenario-column gap #2: ~33 features uncovered for these two triggers.
 * {@code Trigger.Once()} is deprecated in Spark 3.5 but is still wired to {@code
 * MicroBatchExecution}, so it exercises the same {@code latestOffset}/{@code planInputPartitions}
 * paths as a one-shot. {@code Trigger.ProcessingTime("0s")} fires continuously until the source
 * returns empty offsets, exercising offset stability across many micro-batches.
 *
 * <p>For every feature row, the test does:
 *
 * <ol>
 *   <li>Build the table.
 *   <li>Run a single {@code Trigger.AvailableNow} oracle on a sibling stream to compute the
 *       expected total row count.
 *   <li>Run with {@code Trigger.Once()}; assert sink count == oracle.
 *   <li>Run with {@code Trigger.ProcessingTime("0s")}; assert sink count == oracle and query
 *       terminates cleanly via {@code processAllAvailable} + {@code stop}.
 * </ol>
 */
public class V2StreamingTriggerOnceMatrixTest extends V2TestBase {

  /** Run a one-shot {@code Trigger.AvailableNow} stream and return the sink row count. */
  private long oracleCount(String dsv2Ref, String queryName) throws Exception {
    StreamingQuery q =
        spark
            .readStream()
            .table(dsv2Ref)
            .writeStream()
            .format("memory")
            .queryName(queryName)
            .trigger(Trigger.AvailableNow())
            .start();
    try {
      assertTrue(q.awaitTermination(60_000), "AvailableNow oracle did not terminate in 60s");
      return spark.sql("SELECT COUNT(*) FROM " + queryName).collectAsList().get(0).getLong(0);
    } finally {
      q.stop();
    }
  }

  /** Run a one-shot {@code Trigger.Once()} stream and return the sink row count. */
  @SuppressWarnings("deprecation")
  private long triggerOnceCount(String dsv2Ref, String queryName) throws Exception {
    StreamingQuery q =
        spark
            .readStream()
            .table(dsv2Ref)
            .writeStream()
            .format("memory")
            .queryName(queryName)
            .trigger(Trigger.Once())
            .start();
    try {
      assertTrue(q.awaitTermination(60_000), "Trigger.Once did not terminate in 60s");
      return spark.sql("SELECT COUNT(*) FROM " + queryName).collectAsList().get(0).getLong(0);
    } finally {
      q.stop();
    }
  }

  /**
   * Run a {@code Trigger.ProcessingTime("0s")} stream until source is drained and return the sink
   * row count. {@code processAllAvailable} blocks until no more input is available; we then stop
   * the query explicitly because {@code ProcessingTime} never self-terminates.
   */
  private long processingTimeZeroCount(String dsv2Ref, String queryName) throws Exception {
    StreamingQuery q =
        spark
            .readStream()
            .table(dsv2Ref)
            .writeStream()
            .format("memory")
            .queryName(queryName)
            .trigger(Trigger.ProcessingTime(0, TimeUnit.SECONDS))
            .start();
    try {
      q.processAllAvailable();
      return spark.sql("SELECT COUNT(*) FROM " + queryName).collectAsList().get(0).getLong(0);
    } finally {
      // ProcessingTime never self-terminates; we MUST stop the query explicitly to avoid an
      // infinite-loop test hang. stop() also lets @TempDir cleanup release file handles.
      q.stop();
    }
  }

  /**
   * Run the Trigger.Once and Trigger.ProcessingTime("0s") legs against {@code dsv2Ref} and assert
   * both match the AvailableNow oracle. {@code tag} disambiguates memory-sink names so each test
   * uses a fresh table.
   */
  private void runMatrix(String dsv2Ref, String tag) throws Exception {
    String suffix = UUID.randomUUID().toString().replace('-', '_');
    long oracle = oracleCount(dsv2Ref, "oracle_" + tag + "_" + suffix);
    assertTrue(oracle > 0, () -> tag + ": oracle row count must be positive (got " + oracle + ")");

    long onceRows = triggerOnceCount(dsv2Ref, "once_" + tag + "_" + suffix);
    assertEquals(
        oracle, onceRows, () -> tag + ": Trigger.Once row count must match oracle " + oracle);

    long ptRows = processingTimeZeroCount(dsv2Ref, "pt0_" + tag + "_" + suffix);
    assertEquals(
        oracle,
        ptRows,
        () -> tag + ": Trigger.ProcessingTime(0s) row count must match oracle " + oracle);
  }

  /** Append 5 single-row commits with id = base..base+4 to a (id INT) table. */
  private void appendFiveIntCommits(String tablePath, int base) {
    for (int i = 0; i < 5; i++) {
      spark
          .range(base + i, base + i + 1)
          .selectExpr("cast(id as int) as id")
          .write()
          .format("delta")
          .mode("append")
          .save(tablePath);
    }
  }

  /** 1. DV table. */
  @Test
  public void testTriggerOnceMatrix_deletionVectorTable(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta TBLPROPERTIES ("
                + "'delta.enableDeletionVectors' = 'true')",
            tablePath));
    appendFiveIntCommits(tablePath, 0);
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 2", tablePath));

    try {
      runMatrix(str("dsv2.delta.`%s`", tablePath), "dv");
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** 2. Column mapping = name. */
  @Test
  public void testTriggerOnceMatrix_columnMappingNameTable(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta TBLPROPERTIES ("
                + "'delta.columnMapping.mode' = 'name', "
                + "'delta.minReaderVersion' = '2', "
                + "'delta.minWriterVersion' = '5')",
            tablePath));
    for (int i = 0; i < 5; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, '%s')", tablePath, i, "n" + i));
    }

    try {
      runMatrix(str("dsv2.delta.`%s`", tablePath), "cm_name");
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** 3. Column mapping = id. */
  @Test
  public void testTriggerOnceMatrix_columnMappingIdTable(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta TBLPROPERTIES ("
                + "'delta.columnMapping.mode' = 'id', "
                + "'delta.minReaderVersion' = '2', "
                + "'delta.minWriterVersion' = '5')",
            tablePath));
    for (int i = 0; i < 5; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, '%s')", tablePath, i, "n" + i));
    }

    try {
      runMatrix(str("dsv2.delta.`%s`", tablePath), "cm_id");
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** 4. RowTracking table. */
  @Test
  public void testTriggerOnceMatrix_rowTrackingTable(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta TBLPROPERTIES ("
                + "'delta.enableRowTracking' = 'true')",
            tablePath));
    appendFiveIntCommits(tablePath, 0);

    try {
      runMatrix(str("dsv2.delta.`%s`", tablePath), "row_tracking");
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** 5. In-Commit-Timestamp (ICT) table. */
  @Test
  public void testTriggerOnceMatrix_ictTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta TBLPROPERTIES ("
                + "'delta.enableInCommitTimestamps' = 'true')",
            tablePath));
    appendFiveIntCommits(tablePath, 0);

    try {
      runMatrix(str("dsv2.delta.`%s`", tablePath), "ict");
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** 6. v2Checkpoint table. */
  @Test
  public void testTriggerOnceMatrix_v2CheckpointTable(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta TBLPROPERTIES ("
                + "'delta.checkpointPolicy' = 'v2')",
            tablePath));
    appendFiveIntCommits(tablePath, 0);

    try {
      runMatrix(str("dsv2.delta.`%s`", tablePath), "v2_checkpoint");
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** 7. TIMESTAMP_NTZ table. */
  @Test
  public void testTriggerOnceMatrix_timestampNtzTable(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, ts TIMESTAMP_NTZ) USING delta", tablePath));
    for (int i = 0; i < 5; i++) {
      spark.sql(
          str(
              "INSERT INTO delta.`%s` VALUES (%d, TIMESTAMP_NTZ'2024-0%d-01 00:00:00')",
              tablePath, i, i + 1));
    }

    try {
      runMatrix(str("dsv2.delta.`%s`", tablePath), "ts_ntz");
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** 8. Liquid-clustered table (CLUSTER BY). */
  @Test
  public void testTriggerOnceMatrix_clusteredTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta CLUSTER BY (id)",
            tablePath));
    for (int i = 0; i < 5; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, '%s')", tablePath, i, "n" + i));
    }

    try {
      runMatrix(str("dsv2.delta.`%s`", tablePath), "clustered");
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** 9. appendOnly = true. */
  @Test
  public void testTriggerOnceMatrix_appendOnlyTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta TBLPROPERTIES ("
                + "'delta.appendOnly' = 'true')",
            tablePath));
    appendFiveIntCommits(tablePath, 0);

    try {
      runMatrix(str("dsv2.delta.`%s`", tablePath), "append_only");
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** 10. Partitioned table - partition column declared LAST in DDL (Spark normalizes anyway). */
  @Test
  public void testTriggerOnceMatrix_partitionedTable_partLast(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));
    String[] parts = {"a", "b", "c", "d", "e"};
    for (int i = 0; i < 5; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, '%s')", tablePath, i, parts[i]));
    }

    try {
      runMatrix(str("dsv2.delta.`%s`", tablePath), "part_last");
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 11. Partitioned table - partition column FIRST via DataFrameWriter.partitionBy. KNOWN-GAP: Bug
   * #2 / #21 from PR #6609. V2StreamingSchemaReorder reorders partition columns to the trailing
   * positions; partition-first DDL forms surface known issues.
   */
  @Test
  public void testTriggerOnceMatrix_partitionedTable_partFirst(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    StructType schema =
        new StructType().add("p", DataTypes.StringType).add("id", DataTypes.IntegerType);
    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create("a", 0),
                RowFactory.create("b", 1),
                RowFactory.create("c", 2),
                RowFactory.create("d", 3),
                RowFactory.create("e", 4)),
            schema)
        .write()
        .format("delta")
        .partitionBy("p")
        .save(tablePath);

    try {
      runMatrix(str("dsv2.delta.`%s`", tablePath), "part_first");
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** 12. VARIANT column table. */
  @Test
  public void testTriggerOnceMatrix_variantTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, v VARIANT) USING delta", tablePath));
    for (int i = 0; i < 5; i++) {
      spark
          .range(i, i + 1)
          .selectExpr("cast(id as int) as id", "parse_json(concat('{\"row\":', id, '}')) as v")
          .coalesce(1)
          .write()
          .format("delta")
          .mode("append")
          .save(tablePath);
    }

    try {
      runMatrix(str("dsv2.delta.`%s`", tablePath), "variant");
    } finally {
      DeltaLog.clearCache();
    }
  }
}
