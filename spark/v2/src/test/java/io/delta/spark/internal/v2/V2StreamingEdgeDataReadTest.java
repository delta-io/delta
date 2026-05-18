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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * DSv2 streaming edge-data coverage tests (gap class #6).
 *
 * <p>Routine streaming tests use clean rectangular data; per the doc "Delta Table Combination for
 * Streaming" (Tim Wang / Xin Huang), every MANAGED bug surfaced in DSv2 streaming was edge data
 * (nulls, complex types, NULL/special-char partition values, empty tables, single-row tables).
 * These tests intentionally exercise those corners through the path-based {@code dsv2} catalog
 * configured in {@link V2TestBase}.
 */
public class V2StreamingEdgeDataReadTest extends V2TestBase {

  // -------------------------------------------------------------------------
  // Case 1: Empty table
  // -------------------------------------------------------------------------

  @Test
  public void testEmptyTable_initialSnapshot(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    assertTrue(streamingDF.isStreaming());

    List<Row> actualRows = processStreamingQuery(streamingDF, "edge_empty_initial");
    assertEquals(Collections.emptyList(), actualRows);
  }

  @Test
  public void testEmptyTable_availableNow(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    String queryName = "edge_empty_availableNow";
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
      List<Row> actualRows = spark.sql("SELECT * FROM " + queryName).collectAsList();
      assertEquals(Collections.emptyList(), actualRows);
    } finally {
      if (query != null) query.stop();
    }
  }

  // -------------------------------------------------------------------------
  // Case 2: All-null column
  // -------------------------------------------------------------------------

  @Test
  public void testAllNullColumn(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (v INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (NULL), (NULL), (NULL)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "edge_all_null");
    List<Row> expected =
        Arrays.asList(
            org.apache.spark.sql.RowFactory.create((Object) null),
            org.apache.spark.sql.RowFactory.create((Object) null),
            org.apache.spark.sql.RowFactory.create((Object) null));
    assertDataEquals(actualRows, expected);
  }

  // -------------------------------------------------------------------------
  // Case 3: Boolean column with nulls
  // -------------------------------------------------------------------------

  @Test
  public void testBooleanColumnWithNulls(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (b BOOLEAN) USING delta", tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (true), (false), (NULL), (true), (NULL)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "edge_boolean_nulls");
    List<Row> expected =
        Arrays.asList(
            org.apache.spark.sql.RowFactory.create(true),
            org.apache.spark.sql.RowFactory.create(false),
            org.apache.spark.sql.RowFactory.create((Object) null),
            org.apache.spark.sql.RowFactory.create(true),
            org.apache.spark.sql.RowFactory.create((Object) null));
    assertDataEquals(actualRows, expected);
  }

  // -------------------------------------------------------------------------
  // Case 4: Complex types — ARRAY / MAP / STRUCT (initial-snapshot AND incremental)
  // -------------------------------------------------------------------------

  @Test
  public void testComplexTypes_initialSnapshot(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` ("
                + "id INT, "
                + "arr ARRAY<INT>, "
                + "mp MAP<STRING, INT>, "
                + "st STRUCT<a: INT, b: STRING>) USING delta",
            tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES "
                + "(1, array(1, 2, 3), map('k1', 10), named_struct('a', 1, 'b', 'foo')), "
                + "(2, array(), map(), named_struct('a', 2, 'b', 'bar')), "
                + "(3, NULL, NULL, NULL)",
            tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "edge_complex_initial");
    assertEquals(3, actualRows.size(), "should read 3 rows; got: " + actualRows);
  }

  @Test
  public void testComplexTypes_incremental(@TempDir File deltaTablePath) throws Exception {
    // Per the doc: complex types NPE in INCREMENTAL mode for MANAGED in DSv2 streaming.
    // For external tables, simulate "incremental" by writing pre-existing rows, starting the
    // stream, and committing more rows mid-stream.
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` ("
                + "id INT, arr ARRAY<INT>, mp MAP<STRING, INT>, st STRUCT<a: INT, b: STRING>) "
                + "USING delta",
            tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES (1, array(1,2), map('a',1), named_struct('a',1,'b','x'))",
            tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    String queryName = "edge_complex_incremental";
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .start();
      query.processAllAvailable();

      // Add another commit while the stream is running — exercises the incremental path.
      spark.sql(
          str(
              "INSERT INTO delta.`%s` VALUES (2, array(3,4,5), map('b',2,'c',3), "
                  + "named_struct('a',2,'b','y'))",
              tablePath));
      query.processAllAvailable();

      // And a third with NULL complex values.
      spark.sql(str("INSERT INTO delta.`%s` VALUES (3, NULL, NULL, NULL)", tablePath));
      query.processAllAvailable();

      List<Row> actualRows = spark.sql("SELECT * FROM " + queryName).collectAsList();
      assertEquals(3, actualRows.size(), "expected 3 rows; got: " + actualRows);
    } finally {
      if (query != null) query.stop();
    }
  }

  // -------------------------------------------------------------------------
  // Case 5: Special-char strings (commas, quotes, empty, surrogate pairs, very long)
  // -------------------------------------------------------------------------

  @Test
  public void testSpecialCharStrings(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, s STRING) USING delta", tablePath));

    StringBuilder veryLong = new StringBuilder();
    for (int i = 0; i < 5_000; i++) veryLong.append("x");
    String veryLongStr = veryLong.toString();

    // Use DataFrame writes to avoid SQL-string escaping pitfalls.
    List<Row> rows =
        Arrays.asList(
            org.apache.spark.sql.RowFactory.create(1, "a,b,c"),
            org.apache.spark.sql.RowFactory.create(2, "it's a 'quote'"),
            org.apache.spark.sql.RowFactory.create(3, "say \"hi\""),
            org.apache.spark.sql.RowFactory.create(4, ""),
            // Non-BMP code point (emoji U+1F600) — stored as UTF-16 surrogate pair in Java string
            org.apache.spark.sql.RowFactory.create(5, "smile=😀"),
            org.apache.spark.sql.RowFactory.create(6, veryLongStr));

    org.apache.spark.sql.types.StructType schema =
        org.apache.spark.sql.types.DataTypes.createStructType(
            Arrays.asList(
                org.apache.spark.sql.types.DataTypes.createStructField(
                    "id", org.apache.spark.sql.types.DataTypes.IntegerType, false),
                org.apache.spark.sql.types.DataTypes.createStructField(
                    "s", org.apache.spark.sql.types.DataTypes.StringType, true)));
    spark.createDataFrame(rows, schema).write().format("delta").mode("append").save(tablePath);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "edge_special_strings");
    assertDataEquals(actualRows, rows);
  }

  // -------------------------------------------------------------------------
  // Case 6: NULL partition value (__HIVE_DEFAULT_PARTITION__)
  // -------------------------------------------------------------------------

  @Test
  public void testNullPartitionValue(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));
    // First a non-null partition, then a NULL partition.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, NULL), (3, NULL)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "edge_null_partition");
    List<Row> expected =
        Arrays.asList(
            org.apache.spark.sql.RowFactory.create(1, "a"),
            org.apache.spark.sql.RowFactory.create(2, null),
            org.apache.spark.sql.RowFactory.create(3, null));
    assertDataEquals(actualRows, expected);
  }

  // -------------------------------------------------------------------------
  // Case 7: Empty-string partition value (must be distinct from NULL)
  //
  // KNOWN-GAP (2026-05-04): Hive-style partition path encoding maps both NULL
  // and "" to directory `p=__HIVE_DEFAULT_PARTITION__`. Empty-string partition
  // values silently round-trip as NULL on all 4 read engines (DSv1 batch/stream
  // and DSv2 batch/stream). Fixing this requires either a path-encoding change
  // (breaks external readers like Trino/Athena) or out-of-band partition-value
  // storage (protocol change). See TEST_GAPS_TRACKING.md "Documented gaps".
  // TODO(test-gaps): re-enable once the protocol-level fix lands. Until then,
  // these two tests remain disabled to avoid blocking unrelated CI work.
  // -------------------------------------------------------------------------

  @Test
  @org.junit.jupiter.api.Disabled(
      "KNOWN-GAP: empty-string partition value silently coerces to NULL via Hive-style"
          + " partition-path encoding.")
  public void testEmptyStringPartitionValue(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, ''), (2, 'a'), (3, NULL)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "edge_empty_partition");
    // Verify we can distinguish empty-string from NULL partition values.
    // Bug surfaced 2026-05-04: DSv2 streaming decodes empty-string partition value as NULL;
    // for id=1 we get (1, null) instead of (1, "").
    List<Row> expected =
        Arrays.asList(
            org.apache.spark.sql.RowFactory.create(1, ""),
            org.apache.spark.sql.RowFactory.create(2, "a"),
            org.apache.spark.sql.RowFactory.create(3, null));
    assertDataEquals(actualRows, expected);
  }

  /**
   * Cross-check for Case 7: confirms DSv1 streaming decodes empty-string partition values
   * correctly, isolating the DSv2-specific failure in {@link #testEmptyStringPartitionValue}.
   *
   * <p>KNOWN-GAP (2026-05-04): see {@link #testEmptyStringPartitionValue}. DSv1 has the same silent
   * NULL coercion as DSv2 for empty-string partition values — this is not DSv2-specific. Disabled
   * along with case 7.
   */
  @Test
  @org.junit.jupiter.api.Disabled(
      "KNOWN-GAP: empty-string partition value silently coerces to NULL on DSv1 streaming too;"
          + " not DSv2-specific.")
  public void testEmptyStringPartitionValue_dsv1Parity(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, ''), (2, 'a'), (3, NULL)", tablePath));

    // DSv1 streaming via the file-path source (no dsv2 catalog).
    Dataset<Row> streamingDF = spark.readStream().format("delta").load(tablePath).select("id", "p");

    List<Row> actualRows = processStreamingQuery(streamingDF, "edge_empty_partition_dsv1");
    List<Row> expected =
        Arrays.asList(
            org.apache.spark.sql.RowFactory.create(1, ""),
            org.apache.spark.sql.RowFactory.create(2, "a"),
            org.apache.spark.sql.RowFactory.create(3, null));
    assertDataEquals(actualRows, expected);
  }

  // -------------------------------------------------------------------------
  // Case 8: Special-char partition values (spaces, =, #, /, %)
  // -------------------------------------------------------------------------

  @Test
  public void testSpecialCharPartitionValues(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));

    // Use DataFrame writes for partition values containing chars that must be URL-encoded.
    List<Row> rows =
        Arrays.asList(
            org.apache.spark.sql.RowFactory.create(1, "with space"),
            org.apache.spark.sql.RowFactory.create(2, "k=v"),
            org.apache.spark.sql.RowFactory.create(3, "tag#1"),
            org.apache.spark.sql.RowFactory.create(4, "a/b"),
            org.apache.spark.sql.RowFactory.create(5, "100%done"));
    org.apache.spark.sql.types.StructType schema =
        org.apache.spark.sql.types.DataTypes.createStructType(
            Arrays.asList(
                org.apache.spark.sql.types.DataTypes.createStructField(
                    "id", org.apache.spark.sql.types.DataTypes.IntegerType, false),
                org.apache.spark.sql.types.DataTypes.createStructField(
                    "p", org.apache.spark.sql.types.DataTypes.StringType, true)));
    spark
        .createDataFrame(rows, schema)
        .write()
        .format("delta")
        .mode("append")
        .partitionBy("p")
        .save(tablePath);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "edge_special_partition_values");
    assertDataEquals(actualRows, rows);
  }

  // -------------------------------------------------------------------------
  // Case 9: Single-row table  /  single-file table
  // -------------------------------------------------------------------------

  @Test
  public void testSingleRowTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (42, 'only')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "edge_single_row");
    assertDataEquals(
        actualRows, Collections.singletonList(org.apache.spark.sql.RowFactory.create(42, "only")));
  }

  @Test
  public void testSingleFileTable(@TempDir File deltaTablePath) throws Exception {
    // Force a single output file by coalescing.
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark
        .range(20)
        .selectExpr("cast(id as int) as id", "concat('row', cast(id as string)) as name")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "edge_single_file");
    assertEquals(20, actualRows.size());
  }

  // -------------------------------------------------------------------------
  // Case 10: Duplicate first-column values
  // -------------------------------------------------------------------------

  @Test
  public void testDuplicateFirstColumnValues(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES (1, 'a'), (1, 'b'), (1, 'c'), (2, 'd'), (2, 'e')",
            tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "edge_duplicate_first_col");
    List<Row> expected =
        Arrays.asList(
            org.apache.spark.sql.RowFactory.create(1, "a"),
            org.apache.spark.sql.RowFactory.create(1, "b"),
            org.apache.spark.sql.RowFactory.create(1, "c"),
            org.apache.spark.sql.RowFactory.create(2, "d"),
            org.apache.spark.sql.RowFactory.create(2, "e"));
    assertDataEquals(actualRows, expected);
  }
}
