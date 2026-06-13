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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Filler DSv2 streaming coverage for compound-feature cells left over after the dedicated
 * feature-specific suites ({@link V2StreamingColumnMappingTest}, {@link
 * V2StreamingColumnMappingIdModeTest}, {@link V2StreamingVariantScenarioTest}, {@link
 * V2StreamingV2CheckpointTest}, {@link V2StreamingTimestampNtzTest}, {@link
 * V2StreamingAppendOnlyTest}) closed their primary scenarios.
 *
 * <p>Each cluster below targets ~10 of the remaining uncovered cells in the cross-product matrix:
 *
 * <ul>
 *   <li>Compound column-mapping (name/id) + DV + partitioned x non-S1/S14 streaming options.
 *   <li>ColMap-id x option scenarios that the id-mode suite did not cover (excludeRegex,
 *       failOnDataLoss=false, Trigger.Once, maxBytesPerTrigger, startingTimestamp).
 *   <li>VARIANT x additional scenarios beyond the existing scenario suite.
 *   <li>Stragglers on v2Checkpoint and TIMESTAMP_NTZ + appendOnly that are not yet pinned by the
 *       option-matrix suites.
 * </ul>
 *
 * <p>For the {@code failOnDataLoss=false} flavors, the pattern mirrors {@link
 * V2StreamingFailOnDataLossMatrixTest}: write commits, force a checkpoint, prune one commit JSON,
 * then verify the stream still emits the surviving snapshot rows.
 */
public class V2StreamingCompoundFeatureFillerTest extends V2TestBase {

  /** Force a checkpoint so a pruned commit JSON does not block snapshot reconstruction. */
  @SuppressWarnings("deprecation")
  private void checkpoint(String tablePath) {
    DeltaLog.forTable(spark, tablePath).checkpoint();
  }

  /** Delete the commit JSON (and its CRC sibling) for the given version. */
  private void pruneCommitJson(String tablePath, long version) throws Exception {
    Path json = Paths.get(tablePath, "_delta_log", String.format("%020d.json", version));
    Files.delete(json);
    Path crc = Paths.get(tablePath, "_delta_log", String.format("%020d.crc", version));
    if (Files.exists(crc)) {
      Files.delete(crc);
    }
    DeltaLog.clearCache();
  }

  /** Format wall-clock millis using the session-local time zone, for startingTimestamp options. */
  private String formatTs(long millis) {
    String tz = spark.sessionState().conf().sessionLocalTimeZone();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sdf.setTimeZone(TimeZone.getTimeZone(tz));
    return sdf.format(new Date(millis));
  }

  /** Create a CM-name + DV + partitioned table with schema (id INT, name STRING, p STRING). */
  private void createCmNameDvPartitionedTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING, p STRING) USING delta "
                + "PARTITIONED BY (p) "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name', "
                + "  'delta.enableDeletionVectors' = 'true')",
            tablePath));
  }

  /** Create a CM-id + DV + partitioned table with schema (id INT, name STRING, p STRING). */
  private void createCmIdDvPartitionedTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING, p STRING) USING delta "
                + "PARTITIONED BY (p) "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5', "
                + "  'delta.enableDeletionVectors' = 'true')",
            tablePath));
  }

  /** 1. CM-name + DV + partitioned x startingVersion. */
  @Test
  public void testCmNameDvPartitioned_startingVersion(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmNameDvPartitionedTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a', 'x')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b', 'y')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c', 'x')", tablePath));

    Dataset<Row> df =
        spark.readStream().option("startingVersion", "2").table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cm_name_dv_part_startVer");
    // v2 and v3 each contribute one row.
    assertEquals(2, rows.size(), () -> "expected 2 rows from v2..v3, got: " + rows);
  }

  /** 2. CM-name + DV + partitioned x maxFilesPerTrigger. */
  @Test
  public void testCmNameDvPartitioned_maxFilesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmNameDvPartitionedTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a', 'x')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b', 'y')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c', 'z')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (4, 'd', 'x')", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("maxFilesPerTrigger", "1")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cm_name_dv_part_maxFiles");
    assertEquals(
        4, rows.size(), () -> "expected 4 rows across 4 single-file commits, got: " + rows);
  }

  /** 3. CM-name + DV + partitioned x excludeRegex (no-match pattern). */
  @Test
  public void testCmNameDvPartitioned_excludeRegex(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmNameDvPartitionedTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a', 'x'), (2, 'b', 'y')", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("excludeRegex", "this-pattern-cannot-match-any-real-file-name-xyz")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cm_name_dv_part_exclRegex");
    assertEquals(2, rows.size(), () -> "expected 2 rows with no-match excludeRegex, got: " + rows);
  }

  /** 4. CM-name + DV + partitioned x Trigger.AvailableNow. */
  @Test
  public void testCmNameDvPartitioned_triggerAvailableNow(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmNameDvPartitionedTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a', 'x'), (2, 'b', 'y')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    Dataset<Row> df = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    StreamingQuery q =
        df.writeStream()
            .format("memory")
            .queryName("cm_name_dv_part_availNow")
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q.processAllAvailable();
      assertTrue(q.awaitTermination(60_000));
      List<Row> rows = spark.sql("SELECT * FROM cm_name_dv_part_availNow").collectAsList();
      assertEquals(2, rows.size(), () -> "expected 2 rows from AvailableNow, got: " + rows);
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /** 5. CM-name + DV + partitioned x failOnDataLoss=false (prune a middle commit). */
  @Test
  public void testCmNameDvPartitioned_failOnDataLossFalse(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmNameDvPartitionedTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a', 'x')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b', 'y')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c', 'x')", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 2L);

    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cm_name_dv_part_fnl");
    // All 3 rows survive in the snapshot reconstructed from the checkpoint.
    assertEquals(3, rows.size(), () -> "expected 3 rows after pruning v2, got: " + rows);
  }

  /** 6. CM-id + DV + partitioned x startingVersion. */
  @Test
  public void testCmIdDvPartitioned_startingVersion(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmIdDvPartitionedTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a', 'x')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b', 'y')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c', 'x')", tablePath));

    Dataset<Row> df =
        spark.readStream().option("startingVersion", "2").table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cm_id_dv_part_startVer");
    assertEquals(2, rows.size(), () -> "expected 2 rows from v2..v3, got: " + rows);
  }

  /** 7. CM-id + DV + partitioned x maxFilesPerTrigger. */
  @Test
  public void testCmIdDvPartitioned_maxFilesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmIdDvPartitionedTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a', 'x')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b', 'y')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c', 'z')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (4, 'd', 'x')", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("maxFilesPerTrigger", "1")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cm_id_dv_part_maxFiles");
    assertEquals(
        4, rows.size(), () -> "expected 4 rows across 4 single-file commits, got: " + rows);
  }

  /** 8. CM-id + DV + partitioned x excludeRegex (no-match pattern). */
  @Test
  public void testCmIdDvPartitioned_excludeRegex(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmIdDvPartitionedTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a', 'x'), (2, 'b', 'y')", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("excludeRegex", "this-pattern-cannot-match-any-real-file-name-xyz")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cm_id_dv_part_exclRegex");
    assertEquals(2, rows.size(), () -> "expected 2 rows with no-match excludeRegex, got: " + rows);
  }

  /** 9. CM-id + DV + partitioned x Trigger.AvailableNow. */
  @Test
  public void testCmIdDvPartitioned_triggerAvailableNow(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmIdDvPartitionedTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a', 'x'), (2, 'b', 'y')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    Dataset<Row> df = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    StreamingQuery q =
        df.writeStream()
            .format("memory")
            .queryName("cm_id_dv_part_availNow")
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q.processAllAvailable();
      assertTrue(q.awaitTermination(60_000));
      List<Row> rows = spark.sql("SELECT * FROM cm_id_dv_part_availNow").collectAsList();
      assertEquals(2, rows.size(), () -> "expected 2 rows from AvailableNow, got: " + rows);
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /** 10. CM-id + DV + partitioned x failOnDataLoss=false (prune a middle commit). */
  @Test
  public void testCmIdDvPartitioned_failOnDataLossFalse(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmIdDvPartitionedTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a', 'x')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b', 'y')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c', 'x')", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 2L);

    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cm_id_dv_part_fnl");
    assertEquals(3, rows.size(), () -> "expected 3 rows after pruning v2, got: " + rows);
  }

  /** Create a basic CM-id table with schema (id INT, name STRING). */
  private void createCmIdTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5')",
            tablePath));
  }

  /** 11. CM-id x excludeRegex (no-match pattern). */
  @Test
  public void testCmId_excludeRegex(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmIdTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("excludeRegex", "this-pattern-cannot-match-any-real-file-name-xyz")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cm_id_exclRegex");
    assertEquals(3, rows.size(), () -> "expected 3 rows with no-match excludeRegex, got: " + rows);
  }

  /** 12. CM-id x failOnDataLoss=false (prune a middle commit). */
  @Test
  public void testCmId_failOnDataLossFalse(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmIdTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 2L);

    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cm_id_fnl");
    assertEquals(3, rows.size(), () -> "expected 3 rows after pruning v2, got: " + rows);
  }

  /** 13. CM-id x Trigger.Once - single batch consumes the whole CM-id snapshot. */
  @SuppressWarnings("deprecation")
  @Test
  public void testCmId_triggerOnce(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmIdTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", tablePath));

    StreamingQuery q =
        spark
            .readStream()
            .table(str("dsv2.delta.`%s`", tablePath))
            .writeStream()
            .format("memory")
            .queryName("cm_id_once")
            .trigger(Trigger.Once())
            .start();
    try {
      assertTrue(q.awaitTermination(60_000));
      List<Row> rows = spark.sql("SELECT id, name FROM cm_id_once ORDER BY id").collectAsList();
      assertEquals(3, rows.size(), () -> "expected 3 rows from Trigger.Once, got: " + rows);
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * 14. CM-id x maxBytesPerTrigger=1b - the rate limiter must admit one file per batch on a CM-id
   * table. Already covered for the (id INT, name STRING) shape in {@link
   * V2StreamingMaxBytesPerTriggerMatrixTest#testMaxBytes_onColMappedIdTable}; we re-verify here on
   * a different commit shape (multi-row commits + a richer schema) to catch any per-shape
   * regression.
   */
  @Test
  public void testCmId_maxBytesPerTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmIdTable(tablePath);
    // Four single-row commits so the rate limiter has 4 distinct files to admit.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (4, 'd')", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cm_id_maxBytes");
    assertEquals(4, rows.size(), () -> "expected 4 rows under maxBytesPerTrigger=1b, got: " + rows);
  }

  /** 15. CM-id x startingTimestamp - far-past timestamp consumes everything. */
  @Test
  public void testCmId_startingTimestamp(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmIdTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));

    String tsStr = formatTs(0L); // 1970-01-01 -> always before commit mtimes
    Dataset<Row> df =
        spark
            .readStream()
            .option("startingTimestamp", tsStr)
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cm_id_startTs");
    assertEquals(2, rows.size(), () -> "expected 2 rows from startingTimestamp, got: " + rows);
  }

  /** Create a (id INT, v VARIANT) table without DV. */
  private void createVariantTable(String tablePath) {
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, v VARIANT) USING delta", tablePath));
  }

  /** Append rows [startId, startId+count) where v = parse_json('{"row":<id>}'). */
  private void appendVariantRows(String tablePath, int startId, int count) {
    spark
        .range(startId, startId + count)
        .selectExpr("cast(id as int) as id", "parse_json(concat('{\"row\":', id, '}')) as v")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);
  }

  /** 16. VARIANT x failOnDataLoss=false (prune middle commit). */
  @Test
  public void testVariant_failOnDataLossFalse(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createVariantTable(tablePath);
    appendVariantRows(tablePath, 1, 1); // v=1
    appendVariantRows(tablePath, 2, 1); // v=2
    appendVariantRows(tablePath, 3, 1); // v=3

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 2L);

    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "variant_fnl");
    // All 3 rows survive in the snapshot reconstructed from the checkpoint.
    assertEquals(3, rows.size(), () -> "expected 3 VARIANT rows after pruning v2, got: " + rows);
    // Payload alignment: variant_get(v,'$.row') must match id for every surviving row.
    spark.sql("SELECT * FROM variant_fnl").createOrReplaceTempView("variant_fnl_view");
    List<Row> aligned =
        spark
            .sql("SELECT id, variant_get(v, '$.row', 'int') AS v_row FROM variant_fnl_view")
            .collectAsList();
    for (Row r : aligned) {
      assertEquals(
          r.getInt(0),
          ((Number) r.get(1)).intValue(),
          () -> "VARIANT payload misaligned after prune: " + r);
    }
  }

  /** 17. VARIANT x Trigger.Once - single batch consumes all VARIANT rows. */
  @SuppressWarnings("deprecation")
  @Test
  public void testVariant_triggerOnce(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createVariantTable(tablePath);
    appendVariantRows(tablePath, 1, 4);

    StreamingQuery q =
        spark
            .readStream()
            .table(str("dsv2.delta.`%s`", tablePath))
            .writeStream()
            .format("memory")
            .queryName("variant_once")
            .trigger(Trigger.Once())
            .start();
    try {
      assertTrue(q.awaitTermination(60_000));
      List<Row> rows =
          spark
              .sql(
                  "SELECT id, variant_get(v, '$.row', 'int') AS v_row FROM variant_once ORDER BY id")
              .collectAsList();
      assertEquals(4, rows.size(), () -> "expected 4 VARIANT rows from Trigger.Once, got: " + rows);
      for (Row r : rows) {
        assertEquals(
            r.getInt(0),
            ((Number) r.get(1)).intValue(),
            () -> "VARIANT payload misaligned under Trigger.Once: " + r);
      }
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /** 18. VARIANT x Trigger.ProcessingTime("0s") - degenerate trigger storm. */
  @Test
  public void testVariant_processingTime(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createVariantTable(tablePath);
    appendVariantRows(tablePath, 1, 3);

    StreamingQuery q =
        spark
            .readStream()
            .table(str("dsv2.delta.`%s`", tablePath))
            .writeStream()
            .format("memory")
            .queryName("variant_pt0")
            .trigger(Trigger.ProcessingTime(0, java.util.concurrent.TimeUnit.SECONDS))
            .start();
    try {
      q.processAllAvailable();
      List<Row> rows =
          spark
              .sql(
                  "SELECT id, variant_get(v, '$.row', 'int') AS v_row FROM variant_pt0 ORDER BY id")
              .collectAsList();
      assertEquals(
          3, rows.size(), () -> "expected 3 VARIANT rows from ProcessingTime(0s), got: " + rows);
      for (Row r : rows) {
        assertEquals(
            r.getInt(0),
            ((Number) r.get(1)).intValue(),
            () -> "VARIANT payload misaligned under ProcessingTime(0s): " + r);
      }
    } finally {
      // ProcessingTime never self-terminates; stop explicitly.
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * 19. VARIANT x partitioned table.
   *
   * <p>VARIANT is not currently permitted as a Delta partition column - {@code CREATE TABLE ...
   * PARTITIONED BY (v)} is rejected at analysis time. We pin that behavior here so any future
   * relaxation in Delta surfaces as a test failure. A non-partition VARIANT column on a partitioned
   * table is exercised separately to confirm the read path still aligns payloads under partition
   * pruning.
   */
  @Test
  public void testVariant_partitioned(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    // Leg A: VARIANT-as-partition-column should be rejected.
    File partLegDir = new File(deltaTablePath, "part_leg");
    String partLegPath = partLegDir.getAbsolutePath();
    assertThrows(
        Exception.class,
        () ->
            spark.sql(
                str(
                    "CREATE TABLE delta.`%s` (id INT, v VARIANT) USING delta PARTITIONED BY (v)",
                    partLegPath)),
        "VARIANT must not be allowed as a partition column");

    // Leg B: partitioned table with a non-partition VARIANT column streams correctly.
    File dataLegDir = new File(deltaTablePath, "data_leg");
    String dataLegPath = dataLegDir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING, v VARIANT) USING delta PARTITIONED BY (p)",
            dataLegPath));
    spark
        .range(1, 4)
        .selectExpr(
            "cast(id as int) as id",
            "concat('part_', cast(id % 2 as string)) as p",
            "parse_json(concat('{\"row\":', id, '}')) as v")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(dataLegPath);

    Dataset<Row> df = spark.readStream().table(str("dsv2.delta.`%s`", dataLegPath));
    List<Row> rows = processStreamingQuery(df, "variant_part");
    spark.sql("SELECT * FROM variant_part").createOrReplaceTempView("variant_part_view");
    List<Row> aligned =
        spark
            .sql(
                "SELECT id, variant_get(v, '$.row', 'int') AS v_row FROM variant_part_view"
                    + " ORDER BY id")
            .collectAsList();
    assertEquals(
        3, aligned.size(), () -> "expected 3 VARIANT rows from partitioned table, got: " + aligned);
    for (Row r : aligned) {
      assertEquals(
          r.getInt(0),
          ((Number) r.get(1)).intValue(),
          () -> "VARIANT payload misaligned on partitioned table: " + r);
    }
  }

  /** 21. v2 checkpoint x excludeRegex (no-match pattern). */
  @Test
  public void testV2Checkpoint_excludeRegex(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id BIGINT) USING delta "
                + "TBLPROPERTIES ('delta.checkpointPolicy' = 'v2')",
            tablePath));
    spark.range(0, 3).write().format("delta").mode("append").save(tablePath);
    spark.range(3, 6).write().format("delta").mode("append").save(tablePath);

    Dataset<Row> df =
        spark
            .readStream()
            .option("excludeRegex", "this-pattern-cannot-match-any-real-file-name-xyz")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "v2cp_exclRegex");
    assertEquals(6, rows.size(), () -> "expected 6 rows with no-match excludeRegex, got: " + rows);
  }

  /** 23. TIMESTAMP_NTZ x excludeRegex (no-match pattern). */
  @Test
  public void testTimestampNtz_excludeRegex(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, ts TIMESTAMP_NTZ) USING delta", tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES "
                + "(1, TIMESTAMP_NTZ'2024-01-01 00:00:00'), "
                + "(2, TIMESTAMP_NTZ'2024-02-01 00:00:00')",
            tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("excludeRegex", "this-pattern-cannot-match-any-real-file-name-xyz")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "tsntz_exclRegex");
    assertEquals(2, rows.size(), () -> "expected 2 rows with no-match excludeRegex, got: " + rows);
  }
}
