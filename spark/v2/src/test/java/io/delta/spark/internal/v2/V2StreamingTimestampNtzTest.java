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
import java.time.LocalDateTime;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end DSv2 streaming tests for tables with TIMESTAMP_NTZ columns.
 *
 * <p>Per the cross-product matrix in {@code true_cross_product.md}, timestampNtz has 100% uncovered
 * cells across streaming scenarios. TIMESTAMP_NTZ requires a protocol bump
 * (TimestampNTZTableFeature, minReader=3 / minWriter=7) so any reader/writer mismatch surfaces
 * here.
 *
 * <p>Cases:
 *
 * <ol>
 *   <li>Basic stream with TIMESTAMP_NTZ values that include DST-boundary local times.
 *   <li>timestampNtz x DV (DELETE + DV).
 *   <li>timestampNtz x column mapping name.
 *   <li>timestampNtz partition column.
 *   <li>timestampNtz x startingTimestamp option (orthogonal: source-side commit timestamp).
 *   <li>timestampNtz x restart from checkpoint.
 *   <li>Year-9999 boundary timestamp_ntz value.
 *   <li>NULL timestamp_ntz value.
 * </ol>
 */
public class V2StreamingTimestampNtzTest extends V2TestBase {

  /** 1. Basic: TIMESTAMP_NTZ values across DST boundaries. */
  @Test
  public void case1_basic(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, ts TIMESTAMP_NTZ) USING delta", tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES "
                + "(1, TIMESTAMP_NTZ'2024-03-10 02:30:00'), " // does not exist in PDT (DST jump)
                + "(2, TIMESTAMP_NTZ'2024-11-03 01:30:00'), " // ambiguous in PDT (DST fallback)
                + "(3, TIMESTAMP_NTZ'2024-06-15 12:00:00.123456')",
            tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().table(dsv2Ref);
    assertTrue(df.isStreaming());

    List<Row> rows = processStreamingQuery(df, "tsntz_case1");
    assertEquals(3, rows.size(), () -> "Expected 3 rows, got " + rows);
    // Verify per-id values are decoded as the exact LocalDateTime, regardless of session TZ.
    for (Row r : rows) {
      int id = r.getInt(0);
      LocalDateTime ts = r.getAs(1);
      LocalDateTime expected;
      if (id == 1) expected = LocalDateTime.of(2024, 3, 10, 2, 30, 0);
      else if (id == 2) expected = LocalDateTime.of(2024, 11, 3, 1, 30, 0);
      else expected = LocalDateTime.of(2024, 6, 15, 12, 0, 0, 123456000);
      assertEquals(expected, ts, () -> "Mismatch for id=" + id + " row=" + r);
    }
  }

  /** 2. timestampNtz x DV (DELETE + DV). */
  @Test
  public void case2_dv(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, ts TIMESTAMP_NTZ) USING delta TBLPROPERTIES ("
                + "'delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES "
                + "(1, TIMESTAMP_NTZ'2024-01-01 00:00:00'), "
                + "(2, TIMESTAMP_NTZ'2024-02-01 00:00:00'), "
                + "(3, TIMESTAMP_NTZ'2024-03-01 00:00:00'), "
                + "(4, TIMESTAMP_NTZ'2024-04-01 00:00:00')",
            tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE id <= 2", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().table(dsv2Ref);
    List<Row> rows = processStreamingQuery(df, "tsntz_case2");
    assertEquals(2, rows.size(), () -> "Expected 2 rows after DV; got " + rows);
    for (Row r : rows) {
      int id = r.getInt(0);
      LocalDateTime ts = r.getAs(1);
      LocalDateTime expected;
      if (id == 3) expected = LocalDateTime.of(2024, 3, 1, 0, 0, 0);
      else expected = LocalDateTime.of(2024, 4, 1, 0, 0, 0);
      assertEquals(expected, ts);
    }
  }

  /** 3. timestampNtz x column mapping (name). */
  @Test
  public void case3_columnMapping(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, event_time TIMESTAMP_NTZ) USING delta TBLPROPERTIES ("
                + "'delta.columnMapping.mode' = 'name', "
                + "'delta.minReaderVersion' = '3', "
                + "'delta.minWriterVersion' = '7')",
            tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES "
                + "(1, TIMESTAMP_NTZ'2024-05-01 10:00:00'), "
                + "(2, TIMESTAMP_NTZ'2024-05-02 10:00:00')",
            tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().table(dsv2Ref);
    List<Row> rows = processStreamingQuery(df, "tsntz_case3");
    assertEquals(2, rows.size(), () -> "Expected 2 rows, got " + rows);
  }

  /** 4. timestampNtz partition column. */
  @Test
  public void case4_partitionColumn(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, ts TIMESTAMP_NTZ) USING delta PARTITIONED BY (ts)",
            tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES "
                + "(1, TIMESTAMP_NTZ'2024-01-01 12:00:00'), "
                + "(2, TIMESTAMP_NTZ'2024-01-02 12:00:00')",
            tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().table(dsv2Ref);
    List<Row> rows = processStreamingQuery(df, "tsntz_case4");
    assertEquals(2, rows.size(), () -> "Expected 2 rows, got " + rows);
    for (Row r : rows) {
      int id = r.getInt(0);
      LocalDateTime ts = r.getAs(1);
      LocalDateTime expected =
          (id == 1)
              ? LocalDateTime.of(2024, 1, 1, 12, 0, 0)
              : LocalDateTime.of(2024, 1, 2, 12, 0, 0);
      assertEquals(expected, ts, () -> "Partition value mismatch for id=" + id);
    }
  }

  /**
   * 5. timestampNtz x startingTimestamp on the source. Orthogonal: startingTimestamp resolves
   * commit timestamps (wall clock), while data columns are TIMESTAMP_NTZ. Verify it doesn't get
   * confused.
   */
  @Test
  public void case5_startingTimestamp(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, ts TIMESTAMP_NTZ) USING delta", tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, TIMESTAMP_NTZ'2024-01-01 00:00:00')", tablePath));

    // Use a far-past startingTimestamp so the stream consumes everything.
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df =
        spark.readStream().option("startingTimestamp", "1970-01-01 00:00:00").table(dsv2Ref);
    List<Row> rows = processStreamingQuery(df, "tsntz_case5");
    assertEquals(1, rows.size(), () -> "Expected 1 row from startingTimestamp; got " + rows);
  }

  /** 6. timestampNtz x restart from checkpoint. */
  @Test
  public void case6_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, ts TIMESTAMP_NTZ) USING delta", tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, TIMESTAMP_NTZ'2024-01-01 00:00:00')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    org.apache.spark.sql.streaming.StreamingQuery q1 =
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

    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (2, TIMESTAMP_NTZ'2024-06-15 12:00:00')", tablePath));

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    org.apache.spark.sql.streaming.StreamingQuery q2 =
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

    List<Row> all = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    assertEquals(2, all.size(), () -> "Expected 2 rows total after restart; got " + all);
  }

  /** 7. Year-9999 boundary timestamp_ntz value. */
  @Test
  public void case7_year9999Boundary(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, ts TIMESTAMP_NTZ) USING delta", tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES (1, TIMESTAMP_NTZ'9999-12-31 23:59:59.999999')",
            tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().table(dsv2Ref);
    List<Row> rows = processStreamingQuery(df, "tsntz_case7");
    assertEquals(1, rows.size(), () -> "Expected 1 boundary row; got " + rows);
    LocalDateTime ts = rows.get(0).getAs(1);
    LocalDateTime expected = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999000);
    assertEquals(expected, ts, () -> "Year-9999 boundary mismatch: " + ts);
  }

  /** 8. NULL timestamp_ntz value. */
  @Test
  public void case8_nullValue(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, ts TIMESTAMP_NTZ) USING delta", tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES "
                + "(1, NULL), "
                + "(2, TIMESTAMP_NTZ'2024-01-01 00:00:00')",
            tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().table(dsv2Ref);
    List<Row> rows = processStreamingQuery(df, "tsntz_case8");
    assertEquals(2, rows.size(), () -> "Expected 2 rows; got " + rows);
    boolean sawNull = false;
    boolean sawNonNull = false;
    for (Row r : rows) {
      int id = r.getInt(0);
      Object ts = r.get(1);
      if (id == 1) {
        assertNull(ts, () -> "Expected NULL ts for id=1, got " + ts);
        sawNull = true;
      } else if (id == 2) {
        assertNotNull(ts, () -> "Expected non-null ts for id=2, got null");
        assertEquals(LocalDateTime.of(2024, 1, 1, 0, 0, 0), ts);
        sawNonNull = true;
      }
    }
    assertTrue(sawNull && sawNonNull, "Both null and non-null cases must have been observed");
  }
}
