/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.stats

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.delta.skipping.ClusteredTableTestUtils
import org.apache.spark.sql.delta.{DeltaColumnMappingEnableIdMode, DeltaLog}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.{array, col, concat, lit, struct}
import org.apache.spark.sql.test.SharedSparkSession

trait PartitionLikeDataSkippingSuiteBase
  extends QueryTest
    with SharedSparkSession
    with BeforeAndAfter
    with DeltaSQLCommandTest
    with ClusteredTableTestUtils {
  import testImplicits._

  // Disable write optimization to ensure close control over the partitioning of ingested files.
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key, "false")

  protected val testTableName = "test_table"

  private def longToTimestampMillis(i: Long): Long = {
    i +                               // Ensure that there are some millis that will be truncated.
      i * 1000L +                     // Add some seconds.
      i * 60 * 1000 +                 // Add some minutes.
      i * 60 * 60 * 1000 +            // Add some hours.
      i * 24L * 60 * 60 * 1000 +      // Add some days.
      i * 30L * 24 * 60 * 60 * 1000 + // Add some months.
      i * 365L * 24 * 60 * 60 * 1000  // Add some years.
  }

  // Helper to validate the expected scan metrics of a query.
  protected def validateExpectedScanMetrics(
      tableName: String,
      query: String,
      expectedNumFiles: Int,
      expectedNumPartitionLikeDataFilters: Int,
      allPredicatesUsed: Boolean,
      minNumFilesToApply: Long): Unit = {
    // Execute the query without partition-like filters.
    val baseResult = sql(query).collect()
    withSQLConf(
      DeltaSQLConf.DELTA_DATASKIPPING_PARTITION_LIKE_FILTERS_ENABLED.key -> "true",
      DeltaSQLConf.DELTA_DATASKIPPING_PARTITION_LIKE_FILTERS_THRESHOLD.key ->
        minNumFilesToApply.toString) {
      // Execute the query with partition-like filters and validate that the result matches.
      val res = sql(query).collect()
      assert(res.sameElements(baseResult))

      val predicates =
        sql(query).queryExecution.optimizedPlan.expressions.flatMap(splitConjunctivePredicates)
      val scanResult = DeltaLog.forTable(spark, TableIdentifier(tableName))
        .update().filesForScan(predicates)
      assert(scanResult.files.length == expectedNumFiles)
      assert(allPredicatesUsed == scanResult.unusedFilters.isEmpty)
      assert(scanResult.partitionLikeDataFilters.size == expectedNumPartitionLikeDataFilters)
    }
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()

    // Create a shared test table to be used by many tests.
    sql(s"CREATE TABLE $testTableName(s STRUCT<a INT, b STRING>, c DATE, d TIMESTAMP, e INT) " +
      s"USING delta CLUSTER BY (s.a, s.b, c, d)")

    // Insert 10 files that each have the same value on all clustering columns.
    val srcDF = (0L until 10L).map{ i =>
      val timestampMillis = longToTimestampMillis(i)
      val timestamp = new Timestamp(timestampMillis)
      (9 - i.toInt, timestamp.toString, new Date(timestampMillis), timestamp, i.toInt)
    }.toDF("a", "b", "c", "d", "e")
      .withColumn("s", struct(col("a"), col("b")))
      .select("s", "c", "d", "e")
    srcDF.repartitionByRange(10, col("s.a"))
      .write
      .format("delta")
      .mode("append")
      .saveAsTable(testTableName)

    // Insert 10 files that each have the same value on all clustering columns, but that also
    // contain partial nulls.
    (0 until 10).foreach { i =>
      srcDF.where(col("a") === i)
        .union(
          spark.range(1)
            .withColumn("a", lit(null).cast("int"))
            .withColumn("b", lit(null).cast("string"))
            .withColumn("s", struct(col("a"), col("b")))
            .select("s")
            .withColumn("c", lit(null).cast("date"))
            .withColumn("d", lit(null).cast("timestamp"))
            .withColumn("e", lit(null).cast("int"))
            .drop("id"))
        .coalesce(1)
        .write.format("delta").mode("append").insertInto(testTableName)
    }

    // Insert 1 file that contains only nulls on each clustering column.
    spark.range(1)
      .withColumn("a", lit(null).cast("int"))
      .withColumn("b", lit(null).cast("string"))
      .withColumn("s", struct(col("a"), col("b")))
      .select("s")
      .withColumn("c", lit(null).cast("date"))
      .withColumn("d", lit(null).cast("timestamp"))
      .withColumn("e", lit(null).cast("int"))
      .drop("id")
      .coalesce(1)
      .write.format("delta").mode("append").insertInto(testTableName)

    // Insert 1 file that does not have perfect clustering.
    srcDF
      .coalesce(1)
      .write.format("delta").mode("append").insertInto(testTableName)

    // Register a deterministic and nondeterministic UDF.
    spark.udf.register("isEven", (x: Int) => x % 2 == 0)
    spark.udf.register("randIsEven", (x: Int) => x % 2 == 0).asNondeterministic()
  }

  override protected def afterAll(): Unit = {
    // Cleanup shared table.
    sql(s"DROP TABLE IF EXISTS $testTableName")
    super.afterAll()
  }

  // Test cases for partition-like data skipping on the shared table.
  // Each test case has the format:
  // (name, predicate, num matching files (out of 10), number of partition-like predicates,
  //  all predicates used for data skipping)
  private val partitionLikeTestCases = Seq(
    // Valid partition-like predicates.
    ("COALESCE", "COALESCE(null, s.a, 1) = 1", 2, 1, true),
    ("COALESCE", "COALESCE(null, s.a, 1) > 3", 6, 1, true),
    ("COALESCE", "COALESCE(null, s.a, 1) != 1", 9, 1, true),
    ("COALESCE", "COALESCE(s.A, 1) != 1", 9, 1, true),
    ("COALESCE", "COALESCE(TO_DATE(S.b), c) = '1976-07-03'", 1, 1, true),
    ("CAST", "CAST(s.A AS STRING) = '1'", 1, 1, true),
    ("CAST", "CAST(s.A AS STRING) < '3'", 3, 1, true),
    ("CAST", "CAST(s.A AS STRING) >= '3'", 7, 1, true),
    ("CAST", "TO_DATE(s.b) = '1976-07-03'", 1, 1, true),
    ("CAST", "CAST(s.a AS TIMESTAMP) = TIMESTAMP_SECONDS(1)", 1, 1, true),
    ("YEAR", "YEAR(TIMESTAMP(s.b)) = 1969", 1, 1, true),
    ("MONTH", "MONTH(TIMESTAMP(s.b)) = 1", 1, 1, true),
    ("DAYOFYEAR", "DAYOFYEAR(TIMESTAMP(s.b)) = 31", 1, 1, true),
    ("DAYOFMONTH", "DAYOFMONTH(TIMESTAMP(s.b)) = 2", 2, 1, true),
    ("MINUTE", "MINUTE(TIMESTAMP(s.b)) = 5", 1, 1, true),
    ("SECOND", "SECOND(TIMESTAMP(s.b)) = 5", 1, 1, true),
    ("LIKE", "s.b LIKE '%7.007'", 1, 1, true),
    ("DATE_FORMAT", "DATE_FORMAT(c, 'yyyy-MM') = '1976-07'", 1, 1, true),
    ("ENDSWITH", "ENDSWITH(s.b, '7.007')", 1, 1, true),
    ("LENGTH", "LENGTH(s.b) = 21", 1, 1, true),
    ("TRIM", "TRIM(CONCAT('      ', s.b, '   ')) = '1971-01-31 17:01:01.001'", 1, 1, true),
    ("LOWER", "LOWER(CONCAT('AAA', s.b)) = 'aaa1971-01-31 17:01:01.001'", 1, 1, true),
    ("CONCAT", "CONCAT(s.b, CAST(s.a AS STRING)) = '1971-01-31 17:01:01.0018'", 1, 1, true),
    ("FROM_UNIXTIME", "FROM_UNIXTIME(s.a) = '1969-12-31 16:00:00'", 1, 1, true),
    ("TO_UNIX_TIMESTAMP", "TO_UNIX_TIMESTAMP(CAST(s.b AS TIMESTAMP)) = 0", 1, 1, true),
    ("DATE_TRUNC", "DATE_TRUNC('MONTH', CAST(s.b AS DATE)) = '1976-07-01'", 1, 1, true),
    ("TRUNC", "TRUNC(CAST(s.b AS DATE), 'MONTH') = '1976-07-01'", 1, 1, true),
    ("DATE_FROM_UNIX_DATE", "DATE_FROM_UNIX_DATE(s.a) = '1970-01-05'", 1, 1, true),
    ("ISNULL", "CAST(s.a AS STRING) IS NULL", 1, 1, true),
    ("ISNOTNULL", "CAST(s.a AS STRING) IS NOT NULL", 10, 1, true),
    // Fully eligible compound predicates.
    ("NOT (valid)", "NOT CONTAINS(s.b, '7.007')", 9, 1, true),
    (
      "AND (both valid)", "(CONTAINS(s.b, '-03') AND CONTAINS(s.b, '04')) OR ENDSWITH(s.b, '008')",
      2,
      1,
      true
    ),
    ("OR (both valid)", "ENDSWITH(s.b, '7.007') OR ENDSWITH(s.b, '8.008')", 2, 1, true),
    // Partially eligible compound predicates.
    (
      "AND (one valid)",
      "(ISEVEN(s.a) AND ENDSWITH(s.b, '7.007')) OR ENDSWITH(s.b, '008')",
      2,
      1,
      true
    ),
    (
      "OR (one valid)",
      "DATE_FROM_UNIX_DATE(e) = '1977-01-05' OR ENDSWITH(s.b, '7.007')",
      11,
      0,
      false
    ),
    // Fully ineligible compound predicates.
    ("NOT (invalid)", "NOT ISEVEN(s.a)", 11, 0, false),
    (
      "AND (invalid)",
      "(LEN(STRING(d)) = 23 AND STRING(e) = '1') OR ENDSWITH(s.b, '008')",
      11,
      0,
      false
    ),
    ("OR (invalid)", "ISEVEN(s.a) OR STRING(e) = '1'", 11, 0, false),
    // Predicates on non-clustering columns.
    ("CAST", "CAST(e AS STRING) = '1'", 10, 0, false),
    ("DATE_FROM_UNIX_DATE", "DATE_FROM_UNIX_DATE(e) = '1977-01-05'", 10, 0, false),
    // Predicates on timestamp column.
    ("CAST", "CAST(d AS STRING) = '1970-01-01 00:00:00.000'", 10, 0, false),
    // Unsupported expressions.
    ("RAND", "RAND(0) * s.a > 0.5", 10, 0, false),
    ("UDF", "ISEVEN(s.a)", 11, 0, false),
    ("UDF", "RANDISEVEN(s.a)", 11, 0, false),
    ("SCALAR_SUBQUERY", s"DATE(s.b) = (SELECT(MAX(c)) FROM $testTableName)", 10, 0, false),
    ("REGEX", "REGEXP_EXTRACT(s.b, '([0-9][0-9][0-9][0-9]).*') = '1970'", 10, 0, false)
  )

  // Test cases for combinations of partition-like data skipping with normal data skipping on the
  // shared table.
  // Each test case has the format:
  // (predicate, num matching files, number of partition-like predicates,
  //  all predicates used for data skipping)
  // For these test cases, the number of matching files will be the sum across 3 groups of files:
  // 1. Files with the same min-max values and no nulls or all nulls (out of 11).
  // 2. Files with the same min-max values, but some nulls (out of 10). Only traditional data
  // skipping can affect these files.
  // 3. Files with different min-max values (out of 1). This file will always be read because it has
  //    the full range of all values across the clustering columns.
  private val combinedDataSkippingTestCases = Seq(
    // All partition-like filters are eligible.
    ("s.a > 5 AND COALESCE(null, s.a, 1) < 7", 1 + 4 + 1, 1, true),
    ("e < 4 AND COALESCE(null, s.a, 1) < 7", 1 + 4 + 1, 1, true),
    ("d < '1975-01-01' AND DATE(s.b) > '1974-01-01'", 1 + 5 + 1, 1, true),
    // Some partition-like filters are eligible.
    ("ISEVEN(s.a) AND ENDSWITH(s.b, '007') AND s.a < 5", 1 + 5 + 1, 1, false),
    ("(ISEVEN(s.a) AND s.a > 5) OR ENDSWITH(s.b, '008')", 5 + 10 + 1, 1, true),
    (
      "((ISEVEN(s.a) AND ENDSWITH(s.b, '007')) OR ENDSWITH(s.b, '008')) AND s.a < 5",
      2 + 5 + 1,
      1,
      true
    ),
    // No partition-like filters are eligible.
    ("ISEVEN(s.a) AND s.a < 5", 5 + 5 + 1, 0, false),
    ("STRING(e) = '1' AND s.a < 5", 5 + 5 + 1, 0, false)
  )

  partitionLikeTestCases.foreach {
    case (name, predicate, expectedNumFiles, expectedNumPredicates, allPredicatesUsed) =>
      test(s"partition-like data skipping for expression $name: $predicate") {
        validateExpectedScanMetrics(
          tableName = testTableName,
          query = s"SELECT * FROM $testTableName WHERE $predicate",
          expectedNumFiles = 11 + expectedNumFiles,
          expectedNumPartitionLikeDataFilters = expectedNumPredicates,
          allPredicatesUsed = allPredicatesUsed,
          minNumFilesToApply = 1L)
      }
  }

  combinedDataSkippingTestCases.foreach {
    case (predicate, expectedNumFiles, expectedNumPredicates, allPredicatesUsed) =>
      test(s"combined data skipping test: $predicate") {
        validateExpectedScanMetrics(
          tableName = testTableName,
          query = s"SELECT * FROM $testTableName WHERE $predicate",
          expectedNumFiles = expectedNumFiles,
          expectedNumPartitionLikeDataFilters = expectedNumPredicates,
          allPredicatesUsed = allPredicatesUsed,
          minNumFilesToApply = 1L)
      }
  }


  test("partition-like data skipping not applied to truncated string column") {
    val tbl = "tbl"
    withClusteredTable(tbl, "a STRING, b BIGINT", "a, b") {
      // Insert 10 files with truncated string values.
      spark.range(10)
        .withColumnRenamed("id", "b")
        .withColumn("a", concat(lit("abcde" * 10), col("b")))
        .select("a", "b") // Reorder columns to ensure the schema matches.
        .repartitionByRange(10, col("a"))
        .write.format("delta").mode("append").insertInto(tbl)

      // Insert 10 files with non-truncated string values.
      spark.range(10)
        .withColumnRenamed("id", "b")
        .withColumn("a", concat(lit("fghij" * 3), col("b")))
        .select("a", "b") // Reorder columns to ensure the schema matches.
        .repartitionByRange(10, col("a"))
        .write.format("delta").mode("append").insertInto(tbl)

      // For a starts-with predicate (existing data skipping), we can skip files normally.
      validateExpectedScanMetrics(
        tbl, s"SELECT * FROM $tbl WHERE STARTSWITH(a, 'fghij')", 10, 0, true, 1L)

      // For an ends-with predicate, we can only skip files that don't have truncated stats.
      validateExpectedScanMetrics(
        tbl, s"SELECT * FROM $tbl WHERE ENDSWITH(a, '9')", 11, 1, true, 1L)
    }
  }

  test("partition-like data skipping not applied to sufficiently small tables") {
    validateExpectedScanMetrics(
      tableName = testTableName,
      query = s"SELECT * FROM $testTableName WHERE COALESCE(null, s.a, 1) = 1",
      expectedNumFiles = 22,
      expectedNumPartitionLikeDataFilters = 0,
      allPredicatesUsed = false,
      minNumFilesToApply = 8000)
  }

  test("partition-like data skipping when predicate returns NULL") {
    // Predicate returns NULL both when the input attributes are NULL and when the input attributes
    // are non-null.
    val predicate = "GET(ARRAY(1, 2, 3), INT(SUBSTR(s.b, 4, 1))) IN (1, 2)"
    validateExpectedScanMetrics(
      tableName = testTableName,
      query = s"SELECT * FROM $testTableName WHERE $predicate",
      expectedNumFiles = 1 + 10 + 1,
      expectedNumPartitionLikeDataFilters = 1,
      allPredicatesUsed = true,
      minNumFilesToApply = 1)
  }

  test("partition-like data skipping expression references non-skipping eligible columns") {
    val tbl = "tbl"
    withClusteredTable(
        table = tbl,
        schema = "a BIGINT, b ARRAY<BIGINT>, c STRUCT<d ARRAY<BIGINT>, e BIGINT>",
        clusterBy = "a") {
      spark.range(10)
        .withColumnRenamed("id", "a")
        .withColumn("b", array(col("a"), lit(0L)))
        .withColumn("c", struct(array(col("a"), lit(0L)), lit(0L)))
        .select("a", "b", "c") // Reorder columns to ensure the schema matches.
        .repartitionByRange(10, col("a"))
        .write.format("delta").mode("append").insertInto(tbl)

      // All files should be read because the filters are on columns that aren't skipping eligible.
      validateExpectedScanMetrics(
        tableName = tbl,
        query = s"SELECT * FROM $tbl WHERE GET(b, 1) = 0",
        expectedNumFiles = 10,
        expectedNumPartitionLikeDataFilters = 0,
        allPredicatesUsed = false,
        minNumFilesToApply = 1)
      validateExpectedScanMetrics(
        tableName = tbl,
        query = s"SELECT * FROM $tbl WHERE GET(c.d, 1) = 0",
        expectedNumFiles = 10,
        expectedNumPartitionLikeDataFilters = 0,
        allPredicatesUsed = false,
        minNumFilesToApply = 1)
    }
  }
}

class PartitionLikeDataSkippingSuite extends PartitionLikeDataSkippingSuiteBase

class PartitionLikeDataSkippingColumnMappingSuite
  extends PartitionLikeDataSkippingSuiteBase with DeltaColumnMappingEnableIdMode {
  override def runAllTests: Boolean = true

  test("partition-like data skipping with special characters in column names") {
    val tbl = "tbl"
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl SHALLOW CLONE $testTableName")
      sql(s"ALTER TABLE $tbl RENAME COLUMN c TO `a.b`")
      sql(s"ALTER TABLE $tbl ADD COLUMN `s.a` STRUCT<b INT, c INT>")
      // Validate clustering columns are resolved with case-insensitive resolution.
      validateExpectedScanMetrics(
        tableName = tbl,
        query = s"SELECT * FROM $tbl WHERE DATE_FORMAT(`A.B`, 'yyyy-MM') = '1976-07'",
        expectedNumFiles = 12,
        expectedNumPartitionLikeDataFilters = 1,
        allPredicatesUsed = true,
        minNumFilesToApply = 1L)

      // Predicate not on a clustering column - should not be eligible for partition-like data
      // skipping.
      validateExpectedScanMetrics(
        tableName = tbl,
        query = s"SELECT * FROM $tbl WHERE COALESCE(null, `s.a`.b, 1) = 1",
        expectedNumFiles = 22,
        allPredicatesUsed = false,
        expectedNumPartitionLikeDataFilters = 0,
        minNumFilesToApply = 1L)
    }
  }
}
