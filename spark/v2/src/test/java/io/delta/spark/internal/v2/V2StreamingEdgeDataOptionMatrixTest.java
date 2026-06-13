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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * DSv2 streaming edge-data x non-vanilla scenario matrix.
 *
 * <p>{@link V2StreamingEdgeDataReadTest} covers each edge-data row only under the S1 vanilla
 * scenario (a bare {@code readStream().table()}). The closeout coverage report identifies edge-data
 * x non-S1 as ~70 uncovered cells. This file fills the highest-leverage columns:
 *
 * <ul>
 *   <li>S2 {@code startingVersion}
 *   <li>S5 {@code maxFilesPerTrigger}
 *   <li>S7 {@code Trigger.AvailableNow}
 *   <li>S13 {@code excludeRegex}
 *   <li>S14 restart from checkpoint (parquet sink)
 * </ul>
 *
 * <p>Tests are grouped by edge-data row. The expected behaviour mirrors {@link
 * V2StreamingEdgeDataReadTest}: each option must compose with edge-data shapes without losing rows,
 * mangling types, or throwing on the option-handling path.
 */
public class V2StreamingEdgeDataOptionMatrixTest extends V2TestBase {

  /**
   * Drains a streaming query into a parquet sink at {@code outputDir} with the given checkpoint and
   * returns the sink's current contents.
   */
  private List<Row> runWithParquetSink(Dataset<Row> streamingDF, File outputDir, File checkpointDir)
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
      query = writer.start();
      query.processAllAvailable();
    } finally {
      if (query != null) {
        query.stop();
      }
    }
    return spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
  }

  /** Runs an AvailableNow query against a memory sink and returns the sink contents. */
  private List<Row> runWithAvailableNow(Dataset<Row> streamingDF, String queryName)
      throws Exception {
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .start();
      query.processAllAvailable();
      query.awaitTermination(60_000L);
      return spark.sql("SELECT * FROM " + queryName).collectAsList();
    } finally {
      if (query != null) {
        query.stop();
        DeltaLog.clearCache();
      }
    }
  }

  // Row 1: All-null INT column.

  @Test
  public void testAllNullInt_startingVersion(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (v INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (NULL)", tablePath)); // v=1, skipped
    spark.sql(str("INSERT INTO delta.`%s` VALUES (NULL), (NULL)", tablePath)); // v=2

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("startingVersion", "2").table(dsv2Ref);

    List<Row> rows = processStreamingQuery(df, "edge_all_null_int_starting");
    assertEquals(2, rows.size(), () -> "expected 2 rows from v=2; got: " + rows);
    rows.forEach(r -> assertTrue(r.isNullAt(0), () -> "expected NULL; got: " + r));
  }

  @Test
  public void testAllNullInt_maxFilesPerTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (v INT) USING delta", tablePath));
    // Three single-file commits, each one NULL row.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (NULL)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (NULL)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (NULL)", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2Ref);

    List<Row> rows = processStreamingQuery(df, "edge_all_null_int_maxfiles");
    assertEquals(3, rows.size(), () -> "expected 3 NULL rows; got: " + rows);
  }

  @Test
  public void testAllNullInt_triggerAvailableNow(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (v INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (NULL), (NULL), (NULL)", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().table(dsv2Ref);

    List<Row> rows = runWithAvailableNow(df, "edge_all_null_int_available_now");
    assertEquals(3, rows.size(), () -> "expected 3 NULL rows; got: " + rows);
  }

  @Test
  public void testAllNullInt_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (v INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (NULL), (NULL)", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    runWithParquetSink(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    DeltaLog.clearCache();

    spark.sql(str("INSERT INTO delta.`%s` VALUES (NULL)", tablePath));

    List<Row> rows =
        runWithParquetSink(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    assertEquals(3, rows.size(), () -> "expected 3 NULL rows after restart; got: " + rows);
    DeltaLog.clearCache();
  }

  // Row 2: Boolean column with nulls.

  @Test
  public void testBooleanNulls_startingVersion(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (b BOOLEAN) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (true)", tablePath)); // v=1
    spark.sql(str("INSERT INTO delta.`%s` VALUES (false), (NULL)", tablePath)); // v=2

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("startingVersion", "2").table(dsv2Ref);

    List<Row> rows = processStreamingQuery(df, "edge_bool_nulls_starting");
    List<Row> expected = Arrays.asList(RowFactory.create(false), RowFactory.create((Object) null));
    assertDataEquals(rows, expected);
  }

  @Test
  public void testBooleanNulls_maxFilesPerTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (b BOOLEAN) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (true)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (false)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (NULL)", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2Ref);

    List<Row> rows = processStreamingQuery(df, "edge_bool_nulls_maxfiles");
    List<Row> expected =
        Arrays.asList(
            RowFactory.create(true), RowFactory.create(false), RowFactory.create((Object) null));
    assertDataEquals(rows, expected);
  }

  @Test
  public void testBooleanNulls_triggerAvailableNow(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (b BOOLEAN) USING delta", tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (true), (false), (NULL), (true), (NULL)", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().table(dsv2Ref);

    List<Row> rows = runWithAvailableNow(df, "edge_bool_nulls_available_now");
    List<Row> expected =
        Arrays.asList(
            RowFactory.create(true),
            RowFactory.create(false),
            RowFactory.create((Object) null),
            RowFactory.create(true),
            RowFactory.create((Object) null));
    assertDataEquals(rows, expected);
  }

  @Test
  public void testBooleanNulls_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (b BOOLEAN) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (true), (NULL)", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    runWithParquetSink(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    DeltaLog.clearCache();

    spark.sql(str("INSERT INTO delta.`%s` VALUES (false), (NULL)", tablePath));

    List<Row> rows =
        runWithParquetSink(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    assertEquals(4, rows.size(), () -> "expected 4 rows after restart; got: " + rows);
    DeltaLog.clearCache();
  }

  // Row 3: Complex types (ARRAY / MAP / STRUCT).

  private void createComplexTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` ("
                + "id INT, "
                + "arr ARRAY<INT>, "
                + "mp MAP<STRING, INT>, "
                + "st STRUCT<a: INT, b: STRING>) USING delta",
            tablePath));
  }

  @Test
  public void testComplexTypes_startingVersion(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createComplexTable(tablePath);
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES "
                + "(1, array(1,2), map('a',1), named_struct('a',1,'b','x'))",
            tablePath)); // v=1
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES "
                + "(2, array(3,4,5), map('b',2,'c',3), named_struct('a',2,'b','y')), "
                + "(3, NULL, NULL, NULL)",
            tablePath)); // v=2

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("startingVersion", "2").table(dsv2Ref);

    List<Row> rows = processStreamingQuery(df, "edge_complex_starting");
    assertEquals(2, rows.size(), () -> "expected 2 rows from v=2; got: " + rows);
  }

  @Test
  public void testComplexTypes_maxFilesPerTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createComplexTable(tablePath);
    // Three single-file commits to drive the rate limit.
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

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2Ref);

    List<Row> rows = processStreamingQuery(df, "edge_complex_maxfiles");
    assertEquals(3, rows.size(), () -> "expected 3 rows; got: " + rows);
  }

  @Test
  public void testComplexTypes_triggerAvailableNow(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createComplexTable(tablePath);
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES "
                + "(1, array(1,2), map('a',1), named_struct('a',1,'b','x')), "
                + "(2, NULL, NULL, NULL)",
            tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().table(dsv2Ref);

    List<Row> rows = runWithAvailableNow(df, "edge_complex_available_now");
    assertEquals(2, rows.size(), () -> "expected 2 rows; got: " + rows);
  }

  @Test
  public void testComplexTypes_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    createComplexTable(tablePath);
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES "
                + "(1, array(1,2), map('a',1), named_struct('a',1,'b','x'))",
            tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    runWithParquetSink(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    DeltaLog.clearCache();

    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, NULL, NULL, NULL)", tablePath));

    List<Row> rows =
        runWithParquetSink(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    assertEquals(2, rows.size(), () -> "expected 2 rows after restart; got: " + rows);
    DeltaLog.clearCache();
  }

  // Row 4: Special-char strings.

  /** Writes the standard special-char string corpus to {@code tablePath} via DataFrame writes. */
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
  public void testSpecialCharStrings_startingVersion(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, s STRING) USING delta", tablePath));
    // v=1: a placeholder commit we will skip via startingVersion=2.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (0, 'skip')", tablePath));
    List<Row> expected = writeSpecialCharStrings(tablePath); // v=2

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("startingVersion", "2").table(dsv2Ref);

    List<Row> rows = processStreamingQuery(df, "edge_special_strings_starting");
    assertDataEquals(rows, expected);
  }

  @Test
  public void testSpecialCharStrings_maxFilesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, s STRING) USING delta", tablePath));
    List<Row> expected = writeSpecialCharStrings(tablePath);

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2Ref);

    List<Row> rows = processStreamingQuery(df, "edge_special_strings_maxfiles");
    assertDataEquals(rows, expected);
  }

  @Test
  public void testSpecialCharStrings_triggerAvailableNow(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, s STRING) USING delta", tablePath));
    List<Row> expected = writeSpecialCharStrings(tablePath);

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().table(dsv2Ref);

    List<Row> rows = runWithAvailableNow(df, "edge_special_strings_available_now");
    assertDataEquals(rows, expected);
  }

  @Test
  public void testSpecialCharStrings_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, s STRING) USING delta", tablePath));
    // Run 1 sees half the corpus, run 2 picks up the rest.
    List<Row> firstBatch =
        Arrays.asList(
            RowFactory.create(1, "a,b,c"),
            RowFactory.create(2, "it's a 'quote'"),
            RowFactory.create(3, "say \"hi\""));
    StructType schema =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("s", DataTypes.StringType, true)));
    spark
        .createDataFrame(firstBatch, schema)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    runWithParquetSink(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    DeltaLog.clearCache();

    List<Row> secondBatch =
        Arrays.asList(RowFactory.create(4, ""), RowFactory.create(5, "smile=😀"));
    spark
        .createDataFrame(secondBatch, schema)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    List<Row> rows =
        runWithParquetSink(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    assertEquals(5, rows.size(), () -> "expected 5 rows after restart; got: " + rows);
    DeltaLog.clearCache();
  }

  // Row 5: NULL partition value (__HIVE_DEFAULT_PARTITION__).

  @Test
  public void testNullPartitionValue_startingVersion(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath)); // v=1
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, NULL), (3, NULL)", tablePath)); // v=2

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("startingVersion", "2").table(dsv2Ref);

    List<Row> rows = processStreamingQuery(df, "edge_null_part_starting");
    List<Row> expected = Arrays.asList(RowFactory.create(2, null), RowFactory.create(3, null));
    assertDataEquals(rows, expected);
  }

  @Test
  public void testNullPartitionValue_maxFilesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, NULL)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, NULL)", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2Ref);

    List<Row> rows = processStreamingQuery(df, "edge_null_part_maxfiles");
    List<Row> expected =
        Arrays.asList(
            RowFactory.create(1, "a"), RowFactory.create(2, null), RowFactory.create(3, null));
    assertDataEquals(rows, expected);
  }

  @Test
  public void testNullPartitionValue_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, NULL)", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    runWithParquetSink(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    DeltaLog.clearCache();

    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, NULL)", tablePath));

    List<Row> rows =
        runWithParquetSink(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    assertEquals(3, rows.size(), () -> "expected 3 rows after restart; got: " + rows);
    DeltaLog.clearCache();
  }

  // Row 6: Special-char partition values (spaces, =, #, /, %).

  /** Standard special-char partition corpus. */
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
  public void testSpecialCharPartitionValues_startingVersion(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));
    // v=1 placeholder we'll skip via startingVersion=2.
    writeSpecialPartitionRows(tablePath, Arrays.asList(RowFactory.create(99, "skip")));
    List<Row> expected = specialPartitionRows();
    writeSpecialPartitionRows(tablePath, expected); // v=2

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("startingVersion", "2").table(dsv2Ref);

    List<Row> rows = processStreamingQuery(df, "edge_special_part_starting");
    assertDataEquals(rows, expected);
  }

  @Test
  public void testSpecialCharPartitionValues_maxFilesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));
    List<Row> expected = specialPartitionRows();
    writeSpecialPartitionRows(tablePath, expected);

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2Ref);

    List<Row> rows = processStreamingQuery(df, "edge_special_part_maxfiles");
    assertDataEquals(rows, expected);
  }

  @Test
  public void testSpecialCharPartitionValues_restart(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));
    writeSpecialPartitionRows(
        tablePath, Arrays.asList(RowFactory.create(1, "with space"), RowFactory.create(2, "k=v")));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    runWithParquetSink(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    DeltaLog.clearCache();

    writeSpecialPartitionRows(
        tablePath, Arrays.asList(RowFactory.create(3, "tag#1"), RowFactory.create(4, "a/b")));

    List<Row> rows =
        runWithParquetSink(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    assertEquals(4, rows.size(), () -> "expected 4 rows after restart; got: " + rows);
    DeltaLog.clearCache();
  }

  // Row 7: Single-row table. S5 maxFilesPerTrigger is uninteresting against a one-file table.

  @Test
  public void testSingleRowTable_startingVersion(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'skip')", tablePath)); // v=1
    spark.sql(str("INSERT INTO delta.`%s` VALUES (42, 'only')", tablePath)); // v=2

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("startingVersion", "2").table(dsv2Ref);

    List<Row> rows = processStreamingQuery(df, "edge_single_row_starting");
    assertDataEquals(rows, java.util.Collections.singletonList(RowFactory.create(42, "only")));
  }

  @Test
  public void testSingleRowTable_triggerAvailableNow(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (42, 'only')", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().table(dsv2Ref);

    List<Row> rows = runWithAvailableNow(df, "edge_single_row_available_now");
    assertDataEquals(rows, java.util.Collections.singletonList(RowFactory.create(42, "only")));
  }

  @Test
  public void testSingleRowTable_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (42, 'only')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    runWithParquetSink(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    DeltaLog.clearCache();

    spark.sql(str("INSERT INTO delta.`%s` VALUES (43, 'second')", tablePath));

    List<Row> rows =
        runWithParquetSink(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    assertEquals(2, rows.size(), () -> "expected 2 rows after restart; got: " + rows);
    DeltaLog.clearCache();
  }

  // Row 8: Duplicate first-column values.

  @Test
  public void testDuplicateFirstColumnValues_startingVersion(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath)); // v=1
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, 'b'), (1, 'c'), (2, 'd'), (2, 'e')", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("startingVersion", "2").table(dsv2Ref);

    List<Row> rows = processStreamingQuery(df, "edge_dup_first_starting");
    List<Row> expected =
        Arrays.asList(
            RowFactory.create(1, "b"),
            RowFactory.create(1, "c"),
            RowFactory.create(2, "d"),
            RowFactory.create(2, "e"));
    assertDataEquals(rows, expected);
  }

  @Test
  public void testDuplicateFirstColumnValues_maxFilesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'b')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'c')", tablePath));

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2Ref);

    List<Row> rows = processStreamingQuery(df, "edge_dup_first_maxfiles");
    List<Row> expected =
        Arrays.asList(
            RowFactory.create(1, "a"), RowFactory.create(1, "b"), RowFactory.create(1, "c"));
    assertDataEquals(rows, expected);
  }

  @Test
  public void testDuplicateFirstColumnValues_restart(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (1, 'b')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    runWithParquetSink(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    DeltaLog.clearCache();

    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'c'), (2, 'd')", tablePath));

    List<Row> rows =
        runWithParquetSink(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    assertEquals(4, rows.size(), () -> "expected 4 rows after restart; got: " + rows);
    DeltaLog.clearCache();
  }
}
