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
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.util.FileNames;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * DSv2 streaming option-matrix coverage tests for the three highest-uncovered options identified in
 * {@code true_cross_product.md}: {@code failOnDataLoss=false} (92% uncovered), {@code Trigger.Once}
 * / {@code Trigger.ProcessingTime} (89% uncovered), and {@code excludeRegex} (86% uncovered).
 *
 * <p>Each option is here exercised in combination with deletion vectors, column mapping, partition
 * paths, malformed regex, etc. - combinations that DSv1 covers only on a vanilla append-only table.
 *
 * <p>This file deliberately does <strong>not</strong> attempt to fix bugs found. Failures are
 * captured for the test-gap report.
 */
public class V2StreamingOptionMatrixTest extends V2TestBase {

  /**
   * Case 1. fnl=false x deleted commit JSON v=1 (pruned by retention). The initial snapshot at v=3
   * is reconstructable from the checkpoint, so the stream reads all 4 surviving rows. DSv1 parity
   * (DeltaSourceSuite:2208,2245).
   */
  @Test
  public void testFailOnDataLoss_deletedCommitJson(@TempDir File baseDir) throws Exception {
    File inputDir = new File(baseDir, "input");
    String tablePath = inputDir.getAbsolutePath();
    inputDir.mkdirs();

    // 4 single-row commits (v=0..3): ids 0, 10, 20, 30
    for (int i = 0; i < 4; i++) {
      spark
          .range(i * 10L, i * 10L + 1)
          .toDF("id")
          .write()
          .format("delta")
          .mode("append")
          .save(tablePath);
    }
    DeltaLog srcLog = DeltaLog.forTable(spark, tablePath);
    srcLog.checkpoint();

    // Delete commit JSON v=1; checkpoint at v=3 still allows snapshot reconstruction.
    boolean deleted = new File(FileNames.unsafeDeltaFile(srcLog.logPath(), 1L).toUri()).delete();
    assertTrue(deleted, "expected to delete _delta_log/00..01.json");
    DeltaLog.clearCache();

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("failOnDataLoss", "false").table(dsv2TableRef);
    List<Row> rows = processStreamingQuery(df, "fnl_deleted_commit");
    List<Row> expected =
        Arrays.asList(
            RowFactory.create(0L),
            RowFactory.create(10L),
            RowFactory.create(20L),
            RowFactory.create(30L));
    assertDataEquals(rows, expected);
  }

  /** Case 2. fnl=false x excludeRegex matches all files. Verify both options compose. */
  @Test
  public void testFailOnDataLoss_excludeRegexMatchesAll(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.range(0, 5).toDF("id").write().format("delta").mode("append").save(tablePath);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("excludeRegex", ".*\\.parquet$")
            .table(dsv2TableRef);

    // We expect either (a) UnsupportedOperationException about failOnDataLoss
    // or (b) successful zero-row stream once the option is accepted.
    try {
      List<Row> rows = processStreamingQuery(df, "fnl_excl_all");
      assertEquals(0, rows.size(), "regex matched all parquet -> empty");
    } catch (Exception e) {
      // record failure shape
      assertTrue(
          e.toString().contains("failOnDataLoss") || e.toString().contains("not supported"),
          () -> "unexpected failure shape: " + e);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** Case 3. fnl=false x DV table - does DSv2 suppress legitimate data-loss errors? */
  @Test
  public void testFailOnDataLoss_withDeletionVectors(@TempDir File deltaTablePath)
      throws Exception {
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

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("failOnDataLoss", "false").table(dsv2TableRef);
    try {
      List<Row> rows = processStreamingQuery(df, "fnl_dv");
      assertEquals(5, rows.size(), "expected 5 surviving rows after DV");
    } catch (Exception e) {
      // Expect: option rejected by DSv2 with UnsupportedOperationException
      assertTrue(
          e.toString().contains("failOnDataLoss") || e.toString().contains("not supported"),
          () -> "unexpected failure shape: " + e);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** Case 4. fnl=false x column mapping rename - surface or suppress schema-change error? */
  @Test
  public void testFailOnDataLoss_columnMappingRename(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN value TO val2", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("failOnDataLoss", "false").table(dsv2TableRef);
    try {
      List<Row> rows = processStreamingQuery(df, "fnl_cm_rename");
      // record what we got
      assertNotNull(rows);
    } catch (Exception e) {
      // Expect option rejected by DSv2.
      assertTrue(
          e.toString().contains("failOnDataLoss") || e.toString().contains("not supported"),
          () -> "unexpected failure shape: " + e);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * Case 5. fnl=false x startingVersion past pruned region. Both DSv1 and DSv2 throw
   * VersionNotFoundException from the time-travel validation in getStartingVersion (DeltaSource
   * line 1250 / SparkMicroBatchStream line 609); failOnDataLoss=false does not suppress it.
   */
  @Test
  public void testFailOnDataLoss_startingVersionPastPruned(@TempDir File baseDir) throws Exception {
    File inputDir = new File(baseDir, "input");
    String tablePath = inputDir.getAbsolutePath();
    inputDir.mkdirs();
    for (int i = 0; i < 4; i++) {
      spark.range(i, i + 1).toDF("id").write().format("delta").mode("append").save(tablePath);
    }
    DeltaLog srcLog = DeltaLog.forTable(spark, tablePath);
    srcLog.checkpoint();
    // Delete commit JSONs 0 and 1 (pruned region).
    new File(FileNames.unsafeDeltaFile(srcLog.logPath(), 0L).toUri()).delete();
    new File(FileNames.unsafeDeltaFile(srcLog.logPath(), 1L).toUri()).delete();
    DeltaLog.clearCache();

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("startingVersion", "0")
            .table(dsv2TableRef);
    StreamingQueryException ex =
        assertThrows(
            StreamingQueryException.class, () -> processStreamingQuery(df, "fnl_starting_pruned"));
    String message = ex.toString();
    Throwable cause = ex.getCause();
    if (cause != null && cause.getMessage() != null) {
      message = message + " | cause: " + cause.getMessage();
    }
    String finalMessage = message;
    assertTrue(
        finalMessage.contains("Cannot time travel") && finalMessage.contains("Available versions"),
        () -> "expected time-travel error mentioning available versions, got: " + finalMessage);
    DeltaLog.clearCache();
  }

  /** Case 6. Trigger.Once x DV table - single batch consumes DV history. */
  @SuppressWarnings("deprecation")
  @Test
  public void testTriggerOnce_deletionVectorTable(@TempDir File deltaTablePath) throws Exception {
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
    spark.sql(str("DELETE FROM delta.`%s` WHERE id < 3", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    // memory sink + checkpointLocation is rejected by Spark; mirror DSv1
    // DeltaSourceSuite:458 which omits checkpointLocation for memory sink.
    StreamingQuery q1 =
        spark
            .readStream()
            .table(dsv2TableRef)
            .writeStream()
            .format("memory")
            .queryName("once_dv_run1")
            .trigger(Trigger.Once())
            .start();
    try {
      assertTrue(q1.awaitTermination(60_000));
      List<Row> rows = spark.sql("SELECT id FROM once_dv_run1 ORDER BY id").collectAsList();
      // Surviving ids: 3..9 (7 rows).
      assertEquals(7, rows.size(), () -> "expected 7 surviving rows, got: " + rows);
    } finally {
      q1.stop();
      DeltaLog.clearCache();
    }
  }

  /** Case 7. Trigger.Once x column mapping. Schema resolution inside one batch. */
  @SuppressWarnings("deprecation")
  @Test
  public void testTriggerOnce_columnMapping(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    StreamingQuery q =
        spark
            .readStream()
            .table(dsv2TableRef)
            .writeStream()
            .format("memory")
            .queryName("once_cm")
            .trigger(Trigger.Once())
            .start();
    try {
      assertTrue(q.awaitTermination(60_000));
      List<Row> rows = spark.sql("SELECT id, name FROM once_cm ORDER BY id").collectAsList();
      List<Row> expected =
          Arrays.asList(
              RowFactory.create(1, "a"), RowFactory.create(2, "b"), RowFactory.create(3, "c"));
      assertDataEquals(rows, expected);
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /** Case 8. Trigger.Once x multi-version table starting from v=3. */
  @SuppressWarnings("deprecation")
  @Test
  public void testTriggerOnce_startingVersion3(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    for (int i = 0; i < 6; i++) {
      spark.range(i, i + 1).toDF("id").write().format("delta").mode("append").save(tablePath);
    }

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    StreamingQuery q =
        spark
            .readStream()
            .option("startingVersion", "3")
            .table(dsv2TableRef)
            .writeStream()
            .format("memory")
            .queryName("once_v3")
            .trigger(Trigger.Once())
            .start();
    try {
      assertTrue(q.awaitTermination(60_000));
      List<Row> rows = spark.sql("SELECT id FROM once_v3 ORDER BY id").collectAsList();
      // Versions 3, 4, 5 each contributed one row (id=3,4,5).
      assertEquals(3, rows.size(), () -> "expected 3 rows from v=3..5, got: " + rows);
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * Case 9. Trigger.Once + maxFilesPerTrigger ignored (DSv1 parity at DeltaSourceSuite:458).
   * Already covered by V2RateLimitStreamingTest.testMaxFilesPerTrigger_triggerOnce; this case
   * additionally checks the partitioned-table variant.
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testTriggerOnce_maxFilesIgnored_partitioned(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1,'x'),(2,'y'),(3,'z'),(4,'w'),(5,'v')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    StreamingQuery q =
        spark
            .readStream()
            .option("maxFilesPerTrigger", "1")
            .table(dsv2TableRef)
            .writeStream()
            .format("memory")
            .queryName("once_part_max")
            .trigger(Trigger.Once())
            .start();
    try {
      assertTrue(q.awaitTermination(60_000));
      long batches = Arrays.stream(q.recentProgress()).filter(p -> p.numInputRows() != 0L).count();
      assertEquals(1L, batches, "Trigger.Once must consume all in 1 non-empty batch");
      assertEquals(
          5,
          spark.sql("SELECT * FROM once_part_max").collectAsList().size(),
          "Trigger.Once must read all 5 rows ignoring maxFilesPerTrigger");
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /** Case 10. ProcessingTime("0s") - degenerate trigger storm. Should terminate, no duplicates. */
  @Test
  public void testProcessingTime_zeroSeconds(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.range(0, 5).toDF("id").write().format("delta").mode("append").save(tablePath);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    StreamingQuery q =
        spark
            .readStream()
            .table(dsv2TableRef)
            .writeStream()
            .format("memory")
            .queryName("pt0")
            .trigger(Trigger.ProcessingTime(0, TimeUnit.SECONDS))
            .start();
    try {
      q.processAllAvailable();
      List<Row> rows = spark.sql("SELECT id FROM pt0 ORDER BY id").collectAsList();
      // No duplicates: 5 distinct rows.
      assertEquals(5, rows.size(), () -> "expected 5 unique rows, got: " + rows);
      assertEquals(
          5L, spark.sql("SELECT COUNT(DISTINCT id) FROM pt0").collectAsList().get(0).getLong(0));
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * Case 11. excludeRegex against partition path component. Decoded vs encoded form question. DSv2
   * task M case 13 noted URL-encoding quirks with partition values.
   */
  @Test
  public void testExcludeRegex_partitionPathComponent(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1,'foo'),(2,'foo'),(3,'bar')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("excludeRegex", "p=foo").table(dsv2TableRef);
    List<Row> rows = processStreamingQuery(df, "excl_part");
    // Only id=3 (in p=bar) should remain if regex matches the path
    assertEquals(
        1,
        rows.size(),
        () ->
            "expected 1 row (id=3 in p=bar) if 'p=foo' regex matched both foo files; got: " + rows);
  }

  /** Case 12. excludeRegex on a DV-bearing table - files-with-DVs match the regex. */
  @Test
  public void testExcludeRegex_withDeletionVectors(@TempDir File deltaTablePath) throws Exception {
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
    spark.sql(str("DELETE FROM delta.`%s` WHERE id < 3", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    // Match every parquet file -> exclude everything; result must be empty.
    Dataset<Row> df =
        spark.readStream().option("excludeRegex", ".*\\.parquet$").table(dsv2TableRef);
    List<Row> rows = processStreamingQuery(df, "excl_dv");
    assertEquals(0, rows.size(), () -> "expected 0 rows after excluding all files; got: " + rows);
  }

  /** Case 13. excludeRegex with a malformed pattern - DSv1 parity (DeltaSourceSuite:925). */
  @Test
  public void testExcludeRegex_malformedPattern(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (value STRING) USING delta", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    StreamingQueryException ex =
        assertThrows(
            StreamingQueryException.class,
            () -> {
              StreamingQuery q =
                  spark
                      .readStream()
                      .option("excludeRegex", "[abc")
                      .table(dsv2TableRef)
                      .writeStream()
                      .format("noop")
                      .start();
              try {
                q.processAllAvailable();
              } finally {
                q.stop();
              }
            });
    Throwable cause = ex.getCause();
    assertNotNull(cause, "expected non-null cause");
    // DSv1 parity: cause should be IllegalArgumentException mentioning excludeRegex
    assertTrue(
        cause instanceof IllegalArgumentException,
        () -> "expected IllegalArgumentException, got: " + cause);
    assertTrue(
        cause.getMessage() != null && cause.getMessage().contains("excludeRegex"),
        () -> "expected message mentioning excludeRegex, got: " + cause.getMessage());
    DeltaLog.clearCache();
  }

  /** Case 14. excludeRegex matching no files - should be no-op. */
  @Test
  public void testExcludeRegex_matchesNoFiles(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.range(0, 5).toDF("id").write().format("delta").mode("append").save(tablePath);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df =
        spark
            .readStream()
            .option("excludeRegex", "this-pattern-cannot-match-any-real-file-name-xyz")
            .table(dsv2TableRef);
    List<Row> rows = processStreamingQuery(df, "excl_none");
    assertEquals(5, rows.size(), () -> "expected all 5 rows (no exclusion); got: " + rows);
  }

  /**
   * Case 15. excludeRegex on column-mapped physical filenames ({@code col-<uuid>.parquet}). Tests
   * whether the regex sees logical or physical paths.
   */
  @Test
  public void testExcludeRegex_columnMappingPhysicalNames(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1,'a'),(2,'b')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    // Column-mapped tables typically write files with "part-" prefixed names; exclude all parquet.
    Dataset<Row> df =
        spark.readStream().option("excludeRegex", ".*\\.parquet$").table(dsv2TableRef);
    List<Row> rows = processStreamingQuery(df, "excl_cm_phys");
    assertEquals(
        0,
        rows.size(),
        () ->
            "expected 0 rows after excluding all parquet files (regex sees physical path); got: "
                + rows);
  }
}
