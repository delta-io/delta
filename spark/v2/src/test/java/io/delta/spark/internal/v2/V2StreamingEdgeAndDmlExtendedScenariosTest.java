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
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Closeout coverage for the two largest remaining holes in the DSv2 streaming cross-product matrix:
 *
 * <ul>
 *   <li>Edge-data rows (all-null, boolean+null, complex types, special strings, null/special-char
 *       partitions, single-row, duplicate-first-column) x scenarios S3 ({@code
 *       startingVersion=latest}), S4 ({@code startingTimestamp}), S6 ({@code maxBytesPerTrigger}),
 *       S8 ({@code Trigger.Once} / {@code Trigger.ProcessingTime}). See {@link
 *       V2StreamingEdgeDataReadTest} for the S1 baseline and {@link
 *       V2StreamingEdgeDataOptionMatrixTest} for the S2/S5/S7/S13/S14 fill.
 *   <li>Table-op rows (DELETE / UPDATE / MERGE / INSERT OVERWRITE / REPLACE WHERE) x scenarios S3
 *       ({@code startingVersion=latest}), S17 (log-pruning related), S19 (concurrent writer), S20
 *       (corrupt {@code _last_checkpoint}), S21 (log-retention prune), S22 (mixed flavors). See
 *       {@link V2StreamingDmlOptionMatrixTest} for the S2/S5/S6/S13/S14 fill and {@link
 *       V2StreamingFailOnDataLossMatrixTest#pruneCommitJson(String, long)} for the prune helper.
 * </ul>
 *
 * <p>The tests are deliberately data-shape focused: each asserts that an edge value or a DML
 * rewrite composes with the scenario option without losing rows, mangling types, or throwing on the
 * option-handling path. Per-row patterns mirror their S1 baseline tests.
 */
public class V2StreamingEdgeAndDmlExtendedScenariosTest extends V2TestBase {

  /** Force a checkpoint so a pruned commit JSON does not block snapshot reconstruction. */
  @SuppressWarnings("deprecation")
  private void checkpoint(String tablePath) {
    DeltaLog.forTable(spark, tablePath).checkpoint();
  }

  /** Delete the commit JSON (and its CRC sibling) for {@code version}. */
  private void pruneCommitJson(String tablePath, long version) throws Exception {
    Path json = Paths.get(tablePath, "_delta_log", String.format("%020d.json", version));
    Files.delete(json);
    Path crc = Paths.get(tablePath, "_delta_log", String.format("%020d.crc", version));
    if (Files.exists(crc)) {
      Files.delete(crc);
    }
    DeltaLog.clearCache();
  }

  /** Truncate the {@code _last_checkpoint} file to roughly half its size, corrupting the JSON. */
  private void corruptLastCheckpoint(String tablePath) throws Exception {
    Path lastCkpt = Paths.get(tablePath, "_delta_log", "_last_checkpoint");
    long size = Files.size(lastCkpt);
    long newSize = Math.max(0L, size / 2L);
    try (FileChannel ch = FileChannel.open(lastCkpt, StandardOpenOption.WRITE)) {
      ch.truncate(newSize);
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

  /** Run a one-shot {@code Trigger.Once()} stream into a memory sink and return its contents. */
  @SuppressWarnings("deprecation")
  private List<Row> runWithTriggerOnce(Dataset<Row> streamingDF, String queryName)
      throws Exception {
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.Once())
              .start();
      assertTrue(query.awaitTermination(60_000L), "Trigger.Once did not terminate in 60s");
      return spark.sql("SELECT * FROM " + queryName).collectAsList();
    } finally {
      if (query != null) {
        query.stop();
        DeltaLog.clearCache();
      }
    }
  }

  // Cluster 1: edge-data x scenarios S3/S4/S6/S8.

  // Row 1: all-null INT column.

  /** S3 startingVersion=latest: nothing surfaces from history; only post-start commits emit. */
  @Test
  public void testAllNullInt_startingVersionLatest(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (v INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (NULL), (NULL)", tablePath)); // history

    Dataset<Row> df = spark.readStream().option("startingVersion", "latest").table(dsv2Ref);
    String queryName = "edge_all_null_int_sv_latest";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      // No new commits yet -> no rows.
      query.processAllAvailable();
      List<Row> beforeAppend = spark.sql("SELECT * FROM " + queryName).collectAsList();
      assertEquals(
          0, beforeAppend.size(), () -> "expected 0 rows pre-append; got: " + beforeAppend);

      spark.sql(str("INSERT INTO delta.`%s` VALUES (NULL)", tablePath));
      query.processAllAvailable();
      List<Row> rows = spark.sql("SELECT * FROM " + queryName).collectAsList();
      assertEquals(1, rows.size(), () -> "expected 1 NULL row from new commit; got: " + rows);
      assertTrue(rows.get(0).isNullAt(0), () -> "expected NULL; got: " + rows.get(0));
    } finally {
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }
  }

  /** S4 startingTimestamp: far-past timestamp consumes the whole NULL-INT history. */
  @Test
  public void testAllNullInt_startingTimestamp(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (v INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (NULL), (NULL), (NULL)", tablePath));

    String tsStr = formatTs(0L); // 1970-01-01: before every commit mtime.
    Dataset<Row> df =
        spark
            .readStream()
            .option("startingTimestamp", tsStr)
            .table(str("dsv2.delta.`%s`", tablePath));

    List<Row> rows = processStreamingQuery(df, "edge_all_null_int_starting_ts");
    assertEquals(3, rows.size(), () -> "expected 3 NULL rows; got: " + rows);
    rows.forEach(r -> assertTrue(r.isNullAt(0), () -> "expected NULL; got: " + r));
  }

  /** S6 maxBytesPerTrigger=1b: each single-file NULL commit drains in its own batch. */
  @Test
  public void testAllNullInt_maxBytesPerTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (v INT) USING delta", tablePath));
    for (int i = 0; i < 3; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (NULL)", tablePath));
    }

    Dataset<Row> df =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(str("dsv2.delta.`%s`", tablePath));

    List<Row> rows = processStreamingQuery(df, "edge_all_null_int_maxbytes");
    assertEquals(3, rows.size(), () -> "expected 3 NULL rows; got: " + rows);
  }

  /** S8 Trigger.Once: single batch consumes the whole NULL-INT history. */
  @Test
  public void testAllNullInt_triggerOnce(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (v INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (NULL), (NULL), (NULL)", tablePath));

    Dataset<Row> df = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = runWithTriggerOnce(df, "edge_all_null_int_once");
    assertEquals(3, rows.size(), () -> "expected 3 NULL rows; got: " + rows);
  }

  // Row2: boolean column with nulls. --------

  @Test
  public void testBooleanNulls_startingVersionLatest(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (b BOOLEAN) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (true), (NULL)", tablePath));

    Dataset<Row> df = spark.readStream().option("startingVersion", "latest").table(dsv2Ref);
    String queryName = "edge_bool_nulls_sv_latest";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      query.processAllAvailable();
      assertEquals(0, spark.sql("SELECT * FROM " + queryName).collectAsList().size());

      spark.sql(str("INSERT INTO delta.`%s` VALUES (false), (NULL)", tablePath));
      query.processAllAvailable();
      List<Row> rows = spark.sql("SELECT * FROM " + queryName).collectAsList();
      List<Row> expected =
          Arrays.asList(RowFactory.create(false), RowFactory.create((Object) null));
      assertDataEquals(rows, expected);
    } finally {
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }
  }

  @Test
  public void testBooleanNulls_maxBytesPerTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (b BOOLEAN) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (true)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (false)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (NULL)", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(str("dsv2.delta.`%s`", tablePath));

    List<Row> rows = processStreamingQuery(df, "edge_bool_nulls_maxbytes");
    List<Row> expected =
        Arrays.asList(
            RowFactory.create(true), RowFactory.create(false), RowFactory.create((Object) null));
    assertDataEquals(rows, expected);
  }

  @Test
  public void testBooleanNulls_triggerOnce(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (b BOOLEAN) USING delta", tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (true), (false), (NULL), (true), (NULL)", tablePath));

    Dataset<Row> df = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = runWithTriggerOnce(df, "edge_bool_nulls_once");
    List<Row> expected =
        Arrays.asList(
            RowFactory.create(true),
            RowFactory.create(false),
            RowFactory.create((Object) null),
            RowFactory.create(true),
            RowFactory.create((Object) null));
    assertDataEquals(rows, expected);
  }

  // Row3: complex types (ARRAY / MAP / STRUCT). --------

  private void createComplexTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` ("
                + "id INT, arr ARRAY<INT>, mp MAP<STRING, INT>, st STRUCT<a: INT, b: STRING>) "
                + "USING delta",
            tablePath));
  }

  @Test
  public void testComplexTypes_startingVersionLatest(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    createComplexTable(tablePath);
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES "
                + "(1, array(1,2), map('a',1), named_struct('a',1,'b','x'))",
            tablePath));

    Dataset<Row> df = spark.readStream().option("startingVersion", "latest").table(dsv2Ref);
    String queryName = "edge_complex_sv_latest";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      query.processAllAvailable();
      assertEquals(0, spark.sql("SELECT * FROM " + queryName).collectAsList().size());

      spark.sql(str("INSERT INTO delta.`%s` VALUES (2, NULL, NULL, NULL)", tablePath));
      query.processAllAvailable();
      List<Row> rows = spark.sql("SELECT * FROM " + queryName).collectAsList();
      assertEquals(1, rows.size(), () -> "expected 1 row from post-start commit; got: " + rows);
    } finally {
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }
  }

  @Test
  public void testComplexTypes_maxBytesPerTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createComplexTable(tablePath);
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES "
                + "(1, array(1), map('a',1), named_struct('a',1,'b','x'))",
            tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES "
                + "(2, array(2,3), map('b',2), named_struct('a',2,'b','y'))",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, NULL, NULL, NULL)", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(str("dsv2.delta.`%s`", tablePath));

    List<Row> rows = processStreamingQuery(df, "edge_complex_maxbytes");
    assertEquals(3, rows.size(), () -> "expected 3 rows; got: " + rows);
  }

  @Test
  public void testComplexTypes_triggerOnce(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createComplexTable(tablePath);
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES "
                + "(1, array(1,2), map('a',1), named_struct('a',1,'b','x')), "
                + "(2, NULL, NULL, NULL)",
            tablePath));

    Dataset<Row> df = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = runWithTriggerOnce(df, "edge_complex_once");
    assertEquals(2, rows.size(), () -> "expected 2 rows from Trigger.Once; got: " + rows);
  }

  // Row4: special-char strings. --------

  /** Writes the standard special-char string corpus and returns the row list. */
  private List<Row> writeSpecialCharStrings(String tablePath) {
    StringBuilder veryLong = new StringBuilder();
    for (int i = 0; i < 5_000; i++) veryLong.append("x");
    List<Row> rows =
        Arrays.asList(
            RowFactory.create(1, "a,b,c"),
            RowFactory.create(2, "it's a 'quote'"),
            RowFactory.create(3, "say \"hi\""),
            RowFactory.create(4, ""),
            RowFactory.create(5, "smile=😀"),
            RowFactory.create(6, veryLong.toString()));
    StructType schema =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("s", DataTypes.StringType, true)));
    spark.createDataFrame(rows, schema).write().format("delta").mode("append").save(tablePath);
    return rows;
  }

  @Test
  public void testSpecialCharStrings_startingTimestamp(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, s STRING) USING delta", tablePath));
    List<Row> expected = writeSpecialCharStrings(tablePath);

    String tsStr = formatTs(0L);
    Dataset<Row> df =
        spark
            .readStream()
            .option("startingTimestamp", tsStr)
            .table(str("dsv2.delta.`%s`", tablePath));

    List<Row> rows = processStreamingQuery(df, "edge_special_strings_starting_ts");
    assertDataEquals(rows, expected);
  }

  @Test
  public void testSpecialCharStrings_maxBytesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, s STRING) USING delta", tablePath));
    List<Row> expected = writeSpecialCharStrings(tablePath);

    Dataset<Row> df =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(str("dsv2.delta.`%s`", tablePath));

    List<Row> rows = processStreamingQuery(df, "edge_special_strings_maxbytes");
    assertDataEquals(rows, expected);
  }

  @Test
  public void testSpecialCharStrings_triggerOnce(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, s STRING) USING delta", tablePath));
    List<Row> expected = writeSpecialCharStrings(tablePath);

    Dataset<Row> df = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = runWithTriggerOnce(df, "edge_special_strings_once");
    assertDataEquals(rows, expected);
  }

  // Row5: NULL partition value. --------

  @Test
  public void testNullPartitionValue_startingVersionLatest(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, NULL)", tablePath));

    Dataset<Row> df = spark.readStream().option("startingVersion", "latest").table(dsv2Ref);
    String queryName = "edge_null_part_sv_latest";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      query.processAllAvailable();
      assertEquals(0, spark.sql("SELECT * FROM " + queryName).collectAsList().size());

      spark.sql(str("INSERT INTO delta.`%s` VALUES (3, NULL)", tablePath));
      query.processAllAvailable();
      List<Row> rows = spark.sql("SELECT * FROM " + queryName).collectAsList();
      assertDataEquals(rows, java.util.Collections.singletonList(RowFactory.create(3, null)));
    } finally {
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }
  }

  @Test
  public void testNullPartitionValue_maxBytesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, NULL)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, NULL)", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(str("dsv2.delta.`%s`", tablePath));

    List<Row> rows = processStreamingQuery(df, "edge_null_part_maxbytes");
    List<Row> expected =
        Arrays.asList(
            RowFactory.create(1, "a"), RowFactory.create(2, null), RowFactory.create(3, null));
    assertDataEquals(rows, expected);
  }

  // Row6: special-char partition values. --------

  private List<Row> specialPartitionRows() {
    return Arrays.asList(
        RowFactory.create(1, "with space"),
        RowFactory.create(2, "k=v"),
        RowFactory.create(3, "tag#1"),
        RowFactory.create(4, "a/b"),
        RowFactory.create(5, "100%done"));
  }

  private void writeSpecialPartitionRows(String tablePath, List<Row> rows) {
    StructType schema =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("p", DataTypes.StringType, true)));
    spark
        .createDataFrame(rows, schema)
        .write()
        .format("delta")
        .mode("append")
        .partitionBy("p")
        .save(tablePath);
  }

  @Test
  public void testSpecialCharPartitionValues_startingTimestamp(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));
    List<Row> expected = specialPartitionRows();
    writeSpecialPartitionRows(tablePath, expected);

    String tsStr = formatTs(0L);
    Dataset<Row> df =
        spark
            .readStream()
            .option("startingTimestamp", tsStr)
            .table(str("dsv2.delta.`%s`", tablePath));

    List<Row> rows = processStreamingQuery(df, "edge_special_part_starting_ts");
    assertDataEquals(rows, expected);
  }

  @Test
  public void testSpecialCharPartitionValues_maxBytesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));
    List<Row> expected = specialPartitionRows();
    writeSpecialPartitionRows(tablePath, expected);

    Dataset<Row> df =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(str("dsv2.delta.`%s`", tablePath));

    List<Row> rows = processStreamingQuery(df, "edge_special_part_maxbytes");
    assertDataEquals(rows, expected);
  }

  // Row7: single-row table. --------

  @Test
  public void testSingleRowTable_maxBytesPerTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (42, 'only')", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(str("dsv2.delta.`%s`", tablePath));

    List<Row> rows = processStreamingQuery(df, "edge_single_row_maxbytes");
    assertDataEquals(rows, java.util.Collections.singletonList(RowFactory.create(42, "only")));
  }

  @Test
  public void testSingleRowTable_triggerOnce(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (42, 'only')", tablePath));

    Dataset<Row> df = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = runWithTriggerOnce(df, "edge_single_row_once");
    assertDataEquals(rows, java.util.Collections.singletonList(RowFactory.create(42, "only")));
  }

  // Row8: duplicate first-column values. --------

  @Test
  public void testDuplicateFirstColumnValues_startingTimestamp(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES (1, 'a'), (1, 'b'), (1, 'c'), (2, 'd'), (2, 'e')",
            tablePath));

    String tsStr = formatTs(0L);
    Dataset<Row> df =
        spark
            .readStream()
            .option("startingTimestamp", tsStr)
            .table(str("dsv2.delta.`%s`", tablePath));

    List<Row> rows = processStreamingQuery(df, "edge_dup_first_starting_ts");
    List<Row> expected =
        Arrays.asList(
            RowFactory.create(1, "a"),
            RowFactory.create(1, "b"),
            RowFactory.create(1, "c"),
            RowFactory.create(2, "d"),
            RowFactory.create(2, "e"));
    assertDataEquals(rows, expected);
  }

  @Test
  public void testDuplicateFirstColumnValues_triggerOnce(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES (1, 'a'), (1, 'b'), (1, 'c'), (2, 'd'), (2, 'e')",
            tablePath));

    Dataset<Row> df = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = runWithTriggerOnce(df, "edge_dup_first_once");
    List<Row> expected =
        Arrays.asList(
            RowFactory.create(1, "a"),
            RowFactory.create(1, "b"),
            RowFactory.create(1, "c"),
            RowFactory.create(2, "d"),
            RowFactory.create(2, "e"));
    assertDataEquals(rows, expected);
  }

  // Cluster 2: table-op rows x scenarios S3/S17/S19/S20/S21/S22.
  //
  // S3  = startingVersion=latest (post-DML start admits nothing from history;
  //       subsequent appends after start surface only those rows).
  // S17 = log-pruning related - covered alongside the failOnDataLoss=false leg.
  // S19 = concurrent writer issuing the DML while a stream is running.
  // S20 = corrupt _last_checkpoint just before the streaming read.
  // S21 = log-retention prune of the DML commit JSON (failOnDataLoss=false).
  // S22 = various: replaceWhere variants combined with the above mechanisms.

  // DELETE

  /** S3 startingVersion=latest + DELETE history + skipChangeCommits: empty initial drain. */
  @Test
  public void testDelete_startingVersionLatest_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3), (4), (5)", tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE id < 3", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .option("startingVersion", "latest")
            .table(dsv2Ref);
    String queryName = "del_sv_latest";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      query.processAllAvailable();
      // startingVersion=latest skips the history; the DELETE commit is suppressed by
      // skipChangeCommits if any new append happens later.
      assertEquals(0, spark.sql("SELECT * FROM " + queryName).collectAsList().size());

      spark.sql(str("INSERT INTO delta.`%s` VALUES (6)", tablePath));
      query.processAllAvailable();
      List<Row> rows = spark.sql("SELECT * FROM " + queryName).collectAsList();
      assertEquals(1, rows.size(), () -> "expected 1 new row; got: " + rows);
      assertEquals(6, rows.get(0).getInt(0));
    } finally {
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }
  }

  /**
   * S19 concurrent writer issues DELETE mid-stream; {@code skipChangeCommits=true} must drop the
   * change commit and only the initial snapshot rows surface.
   */
  @Test
  public void testDelete_concurrent_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark
        .range(0, 10)
        .selectExpr("cast(id as int) as id")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);

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
              spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", finalTablePath));
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
    // skipChangeCommits drops the DELETE; the stream emits the 10 initial snapshot rows only.
    assertEquals(10, sink.size(), () -> "expected 10 rows in sink; got: " + sink.size());
  }

  /**
   * S21 log-retention prune across DELETE: checkpoint, then prune the DELETE commit JSON; stream
   * with {@code failOnDataLoss=false} + {@code skipChangeCommits=true} must surface the post-DELETE
   * snapshot rows.
   */
  @Test
  public void testDelete_acrossLogRetentionPrune_failOnDataLossFalse_skipChangeCommits(
      @TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3), (4), (5)", tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE id < 3", tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 1L); // prune the INSERT commit JSON.

    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("skipChangeCommits", "true")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));

    List<Row> rows = processStreamingQuery(df, "del_logprune_fnl");
    // 3 surviving rows (ids 3, 4, 5) from the post-DELETE snapshot.
    assertEquals(3, rows.size(), () -> "expected 3 rows after prune; got: " + rows);
  }

  /**
   * S20 corrupt {@code _last_checkpoint} after a DELETE: streaming should recover (Kernel falls
   * back to log listing) or surface a clean structured error rather than a raw NPE.
   */
  @Test
  public void testDelete_corruptCheckpoint_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    // 12 commits to force a checkpoint at v=10.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (0)", tablePath));
    for (int i = 1; i <= 10; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d)", tablePath, i));
    }
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 0", tablePath));

    checkpoint(tablePath);
    corruptLastCheckpoint(tablePath);

    Dataset<Row> df =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(str("dsv2.delta.`%s`", tablePath));

    Throwable failure = null;
    List<Row> rows = null;
    try {
      rows = processStreamingQuery(df, "del_corrupt_ckpt");
    } catch (Throwable t) {
      failure = t;
    }

    if (failure != null) {
      Throwable cur = failure;
      while (cur != null) {
        if (cur instanceof NullPointerException) {
          fail("Corrupt _last_checkpoint after DELETE produced a raw NPE leak", failure);
        }
        cur = cur.getCause();
      }
    } else {
      // Successful recovery: 10 rows survive (ids 1..10).
      final List<Row> finalRows = rows;
      assertEquals(
          10, finalRows.size(), () -> "expected 10 rows after recovery; got: " + finalRows);
    }
  }

  // UPDATE

  @Test
  public void testUpdate_startingVersionLatest_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));
    spark.sql(str("UPDATE delta.`%s` SET val = 'B' WHERE id = 2", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .option("startingVersion", "latest")
            .table(dsv2Ref);
    String queryName = "upd_sv_latest";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      query.processAllAvailable();
      assertEquals(0, spark.sql("SELECT * FROM " + queryName).collectAsList().size());

      spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));
      query.processAllAvailable();
      List<Row> rows = spark.sql("SELECT * FROM " + queryName).collectAsList();
      assertEquals(1, rows.size(), () -> "expected 1 new row; got: " + rows);
    } finally {
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }
  }

  @Test
  public void testUpdate_concurrent_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);

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
              spark.sql(str("UPDATE delta.`%s` SET val = 'X' WHERE id = 2", finalTablePath));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } catch (Exception ignored) {
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
    // skipChangeCommits drops the UPDATE; the stream emits the 3 initial snapshot rows only.
    assertEquals(3, sink.size(), () -> "expected 3 rows in sink; got: " + sink.size());
  }

  @Test
  public void testUpdate_corruptCheckpoint_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    for (int i = 2; i <= 11; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, 'v%d')", tablePath, i, i));
    }
    spark.sql(str("UPDATE delta.`%s` SET val = 'X' WHERE id = 1", tablePath));

    checkpoint(tablePath);
    corruptLastCheckpoint(tablePath);

    Dataset<Row> df =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(str("dsv2.delta.`%s`", tablePath));

    Throwable failure = null;
    List<Row> rows = null;
    try {
      rows = processStreamingQuery(df, "upd_corrupt_ckpt");
    } catch (Throwable t) {
      failure = t;
    }

    if (failure != null) {
      Throwable cur = failure;
      while (cur != null) {
        if (cur instanceof NullPointerException) {
          fail("Corrupt _last_checkpoint after UPDATE produced a raw NPE leak", failure);
        }
        cur = cur.getCause();
      }
    } else {
      final List<Row> finalRows = rows;
      assertEquals(
          11, finalRows.size(), () -> "expected 11 rows after recovery; got: " + finalRows);
    }
  }

  // MERGE

  @Test
  public void testMerge_startingVersionLatest_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));

    spark.sql("DROP VIEW IF EXISTS merge_sv_latest_src");
    spark
        .sql("SELECT 2 AS id, 'B' AS val UNION ALL SELECT 3 AS id, 'c' AS val")
        .createOrReplaceTempView("merge_sv_latest_src");
    spark.sql(
        str(
            "MERGE INTO delta.`%s` t USING merge_sv_latest_src s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET val = s.val "
                + "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)",
            tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .option("startingVersion", "latest")
            .table(dsv2Ref);
    String queryName = "mrg_sv_latest";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      query.processAllAvailable();
      assertEquals(0, spark.sql("SELECT * FROM " + queryName).collectAsList().size());

      spark.sql(str("INSERT INTO delta.`%s` VALUES (4, 'd')", tablePath));
      query.processAllAvailable();
      List<Row> rows = spark.sql("SELECT * FROM " + queryName).collectAsList();
      assertEquals(1, rows.size(), () -> "expected 1 new row; got: " + rows);
    } finally {
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }
  }

  @Test
  public void testMerge_concurrent_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);

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
              spark.sql("DROP VIEW IF EXISTS merge_concurrent_src");
              spark
                  .sql("SELECT 2 AS id, 'B' AS val UNION ALL SELECT 3 AS id, 'c' AS val")
                  .createOrReplaceTempView("merge_concurrent_src");
              spark.sql(
                  str(
                      "MERGE INTO delta.`%s` t USING merge_concurrent_src s ON t.id = s.id "
                          + "WHEN MATCHED THEN UPDATE SET val = s.val "
                          + "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)",
                      finalTablePath));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } catch (Exception ignored) {
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
    // skipChangeCommits drops the MERGE rewrite; the stream emits only the 2 initial snapshot
    // rows. The MERGE's appended row (id=3) is part of a change commit and is suppressed.
    assertEquals(2, sink.size(), () -> "expected 2 rows in sink; got: " + sink.size());
  }

  /**
   * S21 log-retention prune across MERGE: checkpoint, then prune the INSERT commit JSON; stream
   * with {@code failOnDataLoss=false} must surface the post-MERGE snapshot rows.
   */
  @Test
  public void testMerge_acrossLogRetentionPrune_failOnDataLossFalse(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));

    spark.sql("DROP VIEW IF EXISTS merge_prune_src");
    spark
        .sql("SELECT 2 AS id, 'B' AS val UNION ALL SELECT 3 AS id, 'c' AS val")
        .createOrReplaceTempView("merge_prune_src");
    spark.sql(
        str(
            "MERGE INTO delta.`%s` t USING merge_prune_src s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET val = s.val "
                + "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)",
            tablePath));

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 1L);

    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("skipChangeCommits", "true")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));

    List<Row> rows = processStreamingQuery(df, "mrg_logprune_fnl");
    // 3 rows in the post-MERGE snapshot (ids 1, 2, 3).
    assertEquals(3, rows.size(), () -> "expected 3 rows after prune; got: " + rows);
  }

  // INSERT OVERWRITE (no replaceWhere)

  @Test
  public void testInsertOverwrite_startingVersionLatest_skipChangeCommits(
      @TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));
    spark.sql(str("INSERT OVERWRITE delta.`%s` VALUES (10), (20)", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .option("startingVersion", "latest")
            .table(dsv2Ref);
    String queryName = "io_sv_latest";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      query.processAllAvailable();
      assertEquals(0, spark.sql("SELECT * FROM " + queryName).collectAsList().size());

      spark.sql(str("INSERT INTO delta.`%s` VALUES (30)", tablePath));
      query.processAllAvailable();
      List<Row> rows = spark.sql("SELECT * FROM " + queryName).collectAsList();
      assertEquals(1, rows.size(), () -> "expected 1 new row; got: " + rows);
      assertEquals(30, rows.get(0).getInt(0));
    } finally {
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }
  }

  @Test
  public void testInsertOverwrite_concurrent_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);

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
              spark.sql(str("INSERT OVERWRITE delta.`%s` VALUES (10), (20)", finalTablePath));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } catch (Exception ignored) {
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
    // skipChangeCommits drops the INSERT OVERWRITE; the stream emits the 3 initial rows only.
    assertEquals(3, sink.size(), () -> "expected 3 rows in sink; got: " + sink.size());
  }

  @Test
  public void testInsertOverwrite_corruptCheckpoint_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    for (int i = 0; i < 11; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d)", tablePath, i));
    }
    spark.sql(str("INSERT OVERWRITE delta.`%s` VALUES (100)", tablePath));

    checkpoint(tablePath);
    corruptLastCheckpoint(tablePath);

    Dataset<Row> df =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(str("dsv2.delta.`%s`", tablePath));

    Throwable failure = null;
    List<Row> rows = null;
    try {
      rows = processStreamingQuery(df, "io_corrupt_ckpt");
    } catch (Throwable t) {
      failure = t;
    }

    if (failure != null) {
      Throwable cur = failure;
      while (cur != null) {
        if (cur instanceof NullPointerException) {
          fail("Corrupt _last_checkpoint after INSERT OVERWRITE produced a raw NPE leak", failure);
        }
        cur = cur.getCause();
      }
    } else {
      // Post-OVERWRITE the table has a single row {100}; skipChangeCommits drops the OVERWRITE
      // commit so only that one row surfaces from the reconstructed snapshot.
      final List<Row> finalRows = rows;
      assertEquals(1, finalRows.size(), () -> "expected 1 row after recovery; got: " + finalRows);
    }
  }

  // REPLACE WHERE

  @Test
  public void testReplaceWhere_concurrent_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part INT) USING delta PARTITIONED BY (part)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 0), (2, 0), (10, 1), (11, 1)", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);

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
              spark
                  .createDataFrame(
                      Arrays.asList(RowFactory.create(100, 0), RowFactory.create(101, 0)),
                      new StructType()
                          .add("id", DataTypes.IntegerType)
                          .add("part", DataTypes.IntegerType))
                  .write()
                  .format("delta")
                  .mode("overwrite")
                  .option("replaceWhere", "part = 0")
                  .save(finalTablePath);
            } catch (Exception ignored) {
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
    // skipChangeCommits drops the replaceWhere; the stream emits the 4 initial snapshot rows only.
    assertEquals(4, sink.size(), () -> "expected 4 rows in sink; got: " + sink.size());
  }

  @Test
  public void testReplaceWhere_acrossLogRetentionPrune_failOnDataLossFalse(
      @TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part INT) USING delta PARTITIONED BY (part)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 0), (2, 0), (10, 1), (11, 1)", tablePath));
    spark
        .createDataFrame(
            Arrays.asList(RowFactory.create(100, 0), RowFactory.create(101, 0)),
            new StructType().add("id", DataTypes.IntegerType).add("part", DataTypes.IntegerType))
        .write()
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", "part = 0")
        .save(tablePath);

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 1L);

    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("skipChangeCommits", "true")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));

    List<Row> rows = processStreamingQuery(df, "rw_logprune_fnl");
    // Post-replaceWhere snapshot has 4 rows: (100, 0), (101, 0), (10, 1), (11, 1).
    assertEquals(4, rows.size(), () -> "expected 4 rows after prune; got: " + rows);
  }

  // S22 concurrent DELETE issued from another thread while a stream is running with
  // ignoreDeletes=true. The stream must not error and rows it emits must be internally consistent.
  @Test
  public void testConcurrentDeleteDuringStream(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part INT) USING delta PARTITIONED BY (part)",
            tablePath));
    // Two partition values so DELETE WHERE part = 0 drops whole files (no rewrite), matching the
    // ignoreDeletes contract that whole-file deletes are allowed and only that is allowed.
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES (1, 0), (2, 0), (3, 0), (10, 1), (11, 1), (12, 1)",
            tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().option("ignoreDeletes", "true").table(dsv2Ref);

    final java.util.List<Row> collected =
        java.util.Collections.synchronizedList(new java.util.ArrayList<>());
    VoidFunction2<Dataset<Row>, Long> sink =
        (Dataset<Row> batch, Long batchId) -> collected.addAll(batch.collectAsList());

    final String finalTablePath = tablePath;
    ExecutorService writer = Executors.newSingleThreadExecutor();
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .foreachBatch(sink)
              .outputMode("append")
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .start();

      writer.submit(
          () -> {
            try {
              Thread.sleep(50);
              // Whole-partition delete: ignoreDeletes allows this without erroring the stream.
              spark.sql(str("DELETE FROM delta.`%s` WHERE part = 0", finalTablePath));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } catch (Exception ignored) {
              // Concurrent commit may transiently fail; just continue.
            }
          });

      Thread.sleep(2_000L);
      query.processAllAvailable();
    } finally {
      writer.shutdownNow();
      writer.awaitTermination(5, TimeUnit.SECONDS);
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }

    final StreamingQuery finalQuery = query;
    assertTrue(
        finalQuery.exception().isEmpty(),
        () ->
            "Streaming query failed during concurrent DELETE: "
                + (finalQuery.exception().isDefined()
                    ? finalQuery.exception().get().toString()
                    : ""));

    // Internal consistency: every emitted id is from the original insert set, and ids are unique
    // within the sink (no duplicate emission across batches). The exact subset depends on
    // whether the DELETE lands before or after the initial snapshot is captured, but the surviving
    // partition (part = 1) ids must always be present.
    java.util.Set<Integer> seen = new java.util.HashSet<>();
    java.util.Set<Integer> validIds = new java.util.HashSet<>(Arrays.asList(1, 2, 3, 10, 11, 12));
    for (Row r : collected) {
      int id = r.getInt(0);
      assertTrue(validIds.contains(id), () -> "Unexpected id in stream output: " + id);
      assertTrue(seen.add(id), () -> "Duplicate id in stream output: " + id);
    }
    assertTrue(
        seen.contains(10) && seen.contains(11) && seen.contains(12),
        () -> "Expected surviving part=1 ids 10, 11, 12 in sink; got: " + seen);
  }

  // S22 concurrent MERGE (upsert) issued from another thread while a stream is running with
  // ignoreChanges=true. The stream must not error and all three original ids must surface in the
  // sink (ignoreChanges may re-emit rows from rewritten files, so an id may appear more than once).
  @Test
  public void testConcurrentMergeDuringStream(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().option("ignoreChanges", "true").table(dsv2Ref);

    final java.util.List<Row> collected =
        java.util.Collections.synchronizedList(new java.util.ArrayList<>());
    VoidFunction2<Dataset<Row>, Long> sink =
        (Dataset<Row> batch, Long batchId) -> collected.addAll(batch.collectAsList());

    final String finalTablePath = tablePath;
    ExecutorService writer = Executors.newSingleThreadExecutor();
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .foreachBatch(sink)
              .outputMode("append")
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .start();

      writer.submit(
          () -> {
            try {
              Thread.sleep(50);
              spark.sql("DROP VIEW IF EXISTS concurrent_merge_src");
              spark
                  .sql("SELECT 2 AS id, 'B' AS val UNION ALL SELECT 4 AS id, 'd' AS val")
                  .createOrReplaceTempView("concurrent_merge_src");
              spark.sql(
                  str(
                      "MERGE INTO delta.`%s` t USING concurrent_merge_src s ON t.id = s.id "
                          + "WHEN MATCHED THEN UPDATE SET val = s.val "
                          + "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)",
                      finalTablePath));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } catch (Exception ignored) {
              // Concurrent commit may transiently fail; just continue.
            }
          });

      Thread.sleep(2_000L);
      query.processAllAvailable();
    } finally {
      writer.shutdownNow();
      writer.awaitTermination(5, TimeUnit.SECONDS);
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }

    final StreamingQuery finalQuery = query;
    assertTrue(
        finalQuery.exception().isEmpty(),
        () ->
            "Streaming query failed during concurrent MERGE: "
                + (finalQuery.exception().isDefined()
                    ? finalQuery.exception().get().toString()
                    : ""));

    // All three original ids (1, 2, 3) are accounted for in the sink.
    java.util.Set<Integer> idsSeen = new java.util.HashSet<>();
    for (Row r : collected) {
      idsSeen.add(r.getInt(0));
    }
    assertTrue(
        idsSeen.contains(1) && idsSeen.contains(2) && idsSeen.contains(3),
        () -> "Expected all original ids 1, 2, 3 in sink; got: " + idsSeen);
  }
}
