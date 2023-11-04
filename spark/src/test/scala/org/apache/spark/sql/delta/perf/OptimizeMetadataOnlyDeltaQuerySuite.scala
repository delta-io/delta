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

package org.apache.spark.sql.delta.perf

import scala.collection.mutable
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, Dataset, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.delta.{DeltaLog, DeltaTestUtils}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.PrepareDeltaScanBase
import org.apache.spark.sql.delta.stats.StatisticsCollection
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.BeforeAndAfterAll

class OptimizeMetadataOnlyDeltaQuerySuite
  extends QueryTest
    with SharedSparkSession
    with BeforeAndAfterAll
    with DeltaSQLCommandTest {
  val testTableName = "table_basic"
  val noStatsTableName = " table_nostats"
  val mixedStatsTableName = " table_mixstats"

  var dfPart1: DataFrame = null
  var dfPart2: DataFrame = null

  override def beforeAll(): Unit = {
    super.beforeAll()

    dfPart1 = generateRowsDataFrame(spark.range(1L, 6L))
    dfPart2 = generateRowsDataFrame(spark.range(6L, 11L))

    withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
      dfPart1.write.format("delta").mode(SaveMode.Overwrite).saveAsTable(noStatsTableName)
      dfPart1.write.format("delta").mode(SaveMode.Overwrite).saveAsTable(mixedStatsTableName)

      spark.sql(s"DELETE FROM $noStatsTableName WHERE id = 1")
      spark.sql(s"DELETE FROM $mixedStatsTableName WHERE id = 1")

      dfPart2.write.format("delta").mode("append").saveAsTable(noStatsTableName)
    }

    withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "true") {
      dfPart1.write.format("delta").mode(SaveMode.Overwrite).saveAsTable(testTableName)

      spark.sql(s"DELETE FROM $testTableName WHERE id = 1")

      dfPart2.write.format("delta").mode(SaveMode.Append).saveAsTable(testTableName)
      dfPart2.write.format("delta").mode(SaveMode.Append).saveAsTable(mixedStatsTableName)

      // Run updates to generate more Delta Log and trigger a checkpoint
      // and make sure stats works after checkpoints
      for (a <- 1 to 10) {
        spark.sql(s"UPDATE $testTableName SET data='$a' WHERE id = 7")
      }
      spark.sql(s"UPDATE $testTableName SET data=NULL WHERE id = 7")

      // Creates an empty (numRecords == 0) AddFile record
      generateRowsDataFrame(spark.range(11L, 12L))
        .write.format("delta").mode("append").saveAsTable(testTableName)
      spark.sql(s"DELETE FROM $testTableName WHERE id = 11")
    }
  }

  test("count - simple query") {
    val expectedPlan = "LocalRelation [none#0L]"

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*) FROM $testTableName",
      expectedPlan)

    checkResultsAndOptimizedPlan(
      () => spark.read.format("delta").table(testTableName)
        .agg(count(col("*"))),
      expectedPlan)
  }

  test("min-max - simple query") {
    val expectedPlan = "LocalRelation [none#0L, none#1L, none#2, none#3, none#4, none#5, none#6" +
      ", none#7, none#8L, none#9L, none#10, none#11, none#12, none#13, none#14, none#15]"

    checkResultsAndOptimizedPlan(
      s"SELECT MIN(id), MAX(id)" +
        s", MIN(TinyIntColumn), MAX(TinyIntColumn)" +
        s", MIN(SmallIntColumn), MAX(SmallIntColumn)" +
        s", MIN(IntColumn), MAX(IntColumn)" +
        s", MIN(BigIntColumn), MAX(BigIntColumn)" +
        s", MIN(FloatColumn), MAX(FloatColumn)" +
        s", MIN(DoubleColumn), MAX(DoubleColumn)" +
        s", MIN(DateColumn), MAX(DateColumn)" +
        s"FROM $testTableName",
      expectedPlan)

    checkResultsAndOptimizedPlan(() => spark.read.format("delta").table(testTableName)
      .agg(min(col("id")), max(col("id")),
        min(col("TinyIntColumn")), max(col("TinyIntColumn")),
        min(col("SmallIntColumn")), max(col("SmallIntColumn")),
        min(col("IntColumn")), max(col("IntColumn")),
        min(col("BigIntColumn")), max(col("BigIntColumn")),
        min(col("FloatColumn")), max(col("FloatColumn")),
        min(col("DoubleColumn")), max(col("DoubleColumn")),
        min(col("DateColumn")), max(col("DateColumn"))),
      expectedPlan)
  }

  test("min-max - column name non-matching case") {
    checkResultsAndOptimizedPlan(
      s"SELECT MIN(ID), MAX(iD)" +
        s", MIN(tINYINTCOLUMN), MAX(tinyintcolumN)" +
        s", MIN(sMALLINTCOLUMN), MAX(smallintcolumN)" +
        s", MIN(iNTCOLUMN), MAX(intcolumN)" +
        s", MIN(bIGINTCOLUMN), MAX(bigintcolumN)" +
        s", MIN(fLOATCOLUMN), MAX(floatcolumN)" +
        s", MIN(dOUBLECOLUMN), MAX(doublecolumN)" +
        s", MIN(dATECOLUMN), MAX(datecolumN)" +
        s"FROM $testTableName",
      "LocalRelation [none#0L, none#1L, none#2, none#3, none#4, none#5, none#6" +
        ", none#7, none#8L, none#9L, none#10, none#11, none#12, none#13, none#14, none#15]")
  }

  test ("count with column name alias") {
    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*) as MyCount FROM $testTableName",
      "LocalRelation [none#0L]")
  }

  test("count-min-max with column name alias") {
    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*) as MyCount, MIN(id) as MyMinId, MAX(id) as MyMaxId FROM $testTableName",
      "LocalRelation [none#0L, none#1L, none#2L]")
  }

  test("count-min-max - table name with alias") {
    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*), MIN(id), MAX(id) FROM $testTableName MyTable",
      "LocalRelation [none#0L, none#1L, none#2L]")
  }

  test("count-min-max - query using time travel") {
    checkResultsAndOptimizedPlan(s"SELECT COUNT(*), MIN(id), MAX(id) " +
      s"FROM $testTableName VERSION AS OF 0",
      "LocalRelation [none#0L, none#1L, none#2L]")

    checkResultsAndOptimizedPlan(s"SELECT COUNT(*), MIN(id), MAX(id) " +
      s"FROM $testTableName VERSION AS OF 1",
      "LocalRelation [none#0L, none#1L, none#2L]")

    checkResultsAndOptimizedPlan(s"SELECT COUNT(*), MIN(id), MAX(id) " +
      s"FROM $testTableName VERSION AS OF 2",
      "LocalRelation [none#0L, none#1L, none#2L]")
  }

  test("count-min-max - external table") {
    withTempDir { dir =>
      val testTablePath = dir.getAbsolutePath
      dfPart1.write.format("delta").mode("overwrite").save(testTablePath)
      DeltaTable.forPath(spark, testTablePath).delete("id = 1")
      dfPart2.write.format("delta").mode(SaveMode.Append).save(testTablePath)

      checkResultsAndOptimizedPlan(
        s"SELECT COUNT(*), MIN(id), MAX(id) FROM delta.`$testTablePath`",
        "LocalRelation [none#0L, none#1L, none#2L]")
    }
  }

  test("count-min-max - sub-query") {
    checkResultsAndOptimizedPlan(
      s"SELECT (SELECT COUNT(*) FROM $testTableName)",
      "Project [scalar-subquery#0 [] AS #0L]\n:  +- LocalRelation [none#0L]\n+- OneRowRelation")

    checkResultsAndOptimizedPlan(
      s"SELECT (SELECT MIN(id) FROM $testTableName)",
      "Project [scalar-subquery#0 [] AS #0L]\n:  +- LocalRelation [none#0L]\n+- OneRowRelation")

    checkResultsAndOptimizedPlan(
      s"SELECT (SELECT MAX(id) FROM $testTableName)",
      "Project [scalar-subquery#0 [] AS #0L]\n:  +- LocalRelation [none#0L]\n+- OneRowRelation")
  }

  test("count-min-max - sub-query filter") {
    val result = spark.sql(s"SELECT COUNT(*), MIN(id), MAX(id) FROM $testTableName").head
    val totalRows = result.getLong(0)
    val minId = result.getLong(1)
    val maxId = result.getLong(2)

    checkResultsAndOptimizedPlan(
      s"SELECT 'ABC' WHERE" +
        s" (SELECT COUNT(*) FROM $testTableName) = $totalRows",
      "Project [ABC AS #0]\n+- Filter (scalar-subquery#0 [] = " +
        totalRows + ")\n   :  +- LocalRelation [none#0L]\n   +- OneRowRelation")

    checkResultsAndOptimizedPlan(
      s"SELECT 'ABC' WHERE" +
        s" (SELECT MIN(id) FROM $testTableName) = $minId",
      "Project [ABC AS #0]\n+- Filter (scalar-subquery#0 [] = " +
        minId + ")\n   :  +- LocalRelation [none#0L]\n   +- OneRowRelation")

    checkResultsAndOptimizedPlan(
      s"SELECT 'ABC' WHERE" +
        s" (SELECT MAX(id) FROM $testTableName) = $maxId",
      "Project [ABC AS #0]\n+- Filter (scalar-subquery#0 [] = " +
        maxId + ")\n   :  +- LocalRelation [none#0L]\n   +- OneRowRelation")
  }

  test("count-min-max - query with limit") {
    // Limit doesn't affect aggregation results
    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*), MIN(id), MAX(id) FROM $testTableName LIMIT 3",
      "LocalRelation [none#0L, none#1L, none#2L]")
  }

  test("count - empty table") {
    sql(s"CREATE TABLE TestEmpty (c1 int) USING DELTA")

    val query = "SELECT COUNT(*) FROM TestEmpty"

    checkResultsAndOptimizedPlan(query, "LocalRelation [none#0L]")
  }

  test("count-min-max - duplicated functions") {
    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*), COUNT(*), MIN(id), MIN(id), MAX(id), MAX(id) FROM $testTableName",
      "LocalRelation [none#0L, none#1L, none#2L, none#3L, none#4L, none#5L]")
  }

  /** Dates are stored as Int in literals. This test make sure Date columns works
   * and NULL are handled correctly
   */
  test("min-max - date columns") {
    val tableName = "TestDateValues"

    spark.sql(s"CREATE TABLE $tableName (Column1 DATE, Column2 DATE, Column3 DATE) USING DELTA")

    spark.sql(s"INSERT INTO $tableName" +
      s" (Column1, Column2, Column3) VALUES (NULL, current_date(), current_date());")
    spark.sql(s"INSERT INTO $tableName" +
      s" (Column1, Column2, Column3) VALUES (NULL, NULL, current_date());")

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*), MIN(Column1), MAX(Column1), MIN(Column2)" +
        s", MAX(Column2), MIN(Column3), MAX(Column3) FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2, none#3, none#4, none#5, none#6]")
  }

  test("min-max - floating point infinity and NaN values") {
    val tableName = "TestFloatValues"

    spark.sql(s"CREATE TABLE $tableName (FloatColumn Float, DoubleColumn Double) USING DELTA")

    spark.sql(s"INSERT INTO $tableName (FloatColumn, DoubleColumn) VALUES (1, 1);")
    spark.sql(s"INSERT INTO $tableName (FloatColumn, DoubleColumn) VALUES (NULL, NULL);")
    spark.sql(s"INSERT INTO $tableName (FloatColumn, DoubleColumn) VALUES " +
      s"(float('inf'), double('inf'))" +
      s", (float('+inf'), double('+inf'))" +
      s", (float('infinity'), double('infinity'))" +
      s", (float('+infinity'), double('+infinity'))" +
      s", (float('-inf'), double('-inf'))" +
      s", (float('-infinity'), double('-infinity'))")

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*), MIN(FloatColumn), MAX(FloatColumn), MIN(DoubleColumn)" +
        s", MAX(DoubleColumn) FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2, none#3, none#4]")

    // NaN is larger than any other value, including Infinity
    spark.sql(s"INSERT INTO $tableName (FloatColumn, DoubleColumn) VALUES " +
      s"(float('NaN'), double('NaN'));")

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*), MIN(FloatColumn), MAX(FloatColumn), MIN(DoubleColumn)" +
        s", MAX(DoubleColumn) FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2, none#3, none#4]")
  }

  test("min-max - floating point min positive value") {
    val tableName = "TestFloatPrecision"

    spark.sql(s"CREATE TABLE $tableName (FloatColumn Float, DoubleColumn Double) USING DELTA")

    spark.sql(s"INSERT INTO $tableName (FloatColumn, DoubleColumn) VALUES " +
      s"(CAST('1.4E-45' as FLOAT), CAST('4.9E-324' as DOUBLE))" +
      s", (CAST('-1.4E-45' as FLOAT), CAST('-4.9E-324' as DOUBLE))" +
      s", (0, 0);")

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*), MIN(FloatColumn), MAX(FloatColumn), MIN(DoubleColumn)" +
        s", MAX(DoubleColumn) FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2, none#3, none#4]")
  }

  test("min-max - NULL and non-NULL values") {
    val tableName = "TestNullValues"

    spark.sql(s"CREATE TABLE $tableName (Column1 INT, Column2 INT, Column3 INT) USING DELTA")

    spark.sql(s"INSERT INTO $tableName (Column1, Column2, Column3) VALUES (NULL, 1, 1);")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2, Column3) VALUES (NULL, NULL, 1);")

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*), MIN(Column1), MAX(Column1) FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2]")

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*), MIN(Column2), MAX(Column2) FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2]")
  }

  test("min-max - only NULL values") {
    val tableName = "TestOnlyNullValues"

    spark.sql(s"CREATE TABLE $tableName (Column1 INT, Column2 INT, Column3 INT) USING DELTA")

    spark.sql(s"INSERT INTO $tableName (Column1, Column2, Column3) VALUES (NULL, NULL, 1);")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2, Column3) VALUES (NULL, NULL, 2);")

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*), MIN(Column1), MAX(Column1), MIN(Column2), MAX(Column2), " +
        s"MIN(Column3), MAX(Column3) FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2, none#3, none#4, none#5, none#6]")
  }

  test("min-max - all supported data types") {
    val tableName = "TestMinMaxValues"

    spark.sql(s"CREATE TABLE $tableName (" +
      s"TINYINTColumn TINYINT, SMALLINTColumn SMALLINT, INTColumn INT, BIGINTColumn BIGINT, " +
      s"FLOATColumn FLOAT, DOUBLEColumn DOUBLE, DATEColumn DATE) USING DELTA")

    spark.sql(s"INSERT INTO $tableName (TINYINTColumn, SMALLINTColumn, INTColumn, BIGINTColumn," +
      s" FLOATColumn, DOUBLEColumn, DATEColumn)" +
      s" VALUES (-128, -32768, -2147483648, -9223372036854775808," +
      s" -3.4028235E38, -1.7976931348623157E308, CAST('1582-10-15' AS DATE));")
    spark.sql(s"INSERT INTO $tableName (TINYINTColumn, SMALLINTColumn, INTColumn, BIGINTColumn," +
      s" FLOATColumn, DOUBLEColumn, DATEColumn)" +
      s" VALUES (127, 32767, 2147483647, 9223372036854775807," +
      s" 3.4028235E38, 1.7976931348623157E308, CAST('9999-12-31' AS DATE));")

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*)," +
        s"MIN(TINYINTColumn), MAX(TINYINTColumn)" +
        s", MIN(SMALLINTColumn), MAX(SMALLINTColumn)" +
        s", MIN(INTColumn), MAX(INTColumn)" +
        s", MIN(BIGINTColumn), MAX(BIGINTColumn)" +
        s", MIN(FLOATColumn), MAX(FLOATColumn)" +
        s", MIN(DOUBLEColumn), MAX(DOUBLEColumn)" +
        s", MIN(DATEColumn), MAX(DATEColumn)" +
        s" FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2, none#3, none#4, none#5, none#6, none#7L" +
        ", none#8L, none#9, none#10, none#11, none#12, none#13, none#14]")
  }

  test("count-min-max - partitioned table - simple query") {
    val tableName = "TestPartitionedTable"

    spark.sql(s"CREATE TABLE $tableName (Column1 INT, Column2 INT, Column3 INT, Column4 INT)" +
      s" USING DELTA PARTITIONED BY (Column2, Column3)")

    spark.sql(s"INSERT INTO $tableName (Column1, Column2, Column3, Column4) VALUES (1, 2, 3, 4);")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2, Column3, Column4) VALUES (2, 2, 3, 5);")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2, Column3, Column4) VALUES (3, 3, 2, 6);")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2, Column3, Column4) VALUES (4, 3, 2, 7);")

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*)" +
        s", MIN(Column1), MAX(Column1)" +
        s", MIN(Column2), MAX(Column2)" +
        s", MIN(Column3), MAX(Column3)" +
        s", MIN(Column4), MAX(Column4)" +
        s" FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2, none#3, none#4, none#5, none#6, none#7, none#8]")
  }

  /** Partitioned columns should be able to return MIN and MAX data
   * even when there are no column stats */
  test("count-min-max - partitioned table - no stats") {
    val tableName = "TestPartitionedTableNoStats"

    spark.sql(s"CREATE TABLE $tableName (Column1 INT, Column2 INT, Column3 INT, Column4 INT)" +
      s" USING DELTA PARTITIONED BY (Column2, Column3)" +
      s" TBLPROPERTIES('delta.dataSkippingNumIndexedCols' = 0)")

    spark.sql(s"INSERT INTO $tableName (Column1, Column2, Column3, Column4) VALUES (1, 2, 3, 4);")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2, Column3, Column4) VALUES (2, 2, 3, 5);")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2, Column3, Column4) VALUES (3, 3, 2, 6);")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2, Column3, Column4) VALUES (4, 3, 2, 7);")

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*)" +
        s", MIN(Column2), MAX(Column2)" +
        s", MIN(Column3), MAX(Column3)" +
        s" FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2, none#3, none#4]")
  }

  test("min-max - partitioned table - all supported data types") {
    val tableName = "TestAllTypesPartitionedTable"

    spark.sql(s"CREATE TABLE $tableName (" +
      s"TINYINTColumn TINYINT, SMALLINTColumn SMALLINT, INTColumn INT, BIGINTColumn BIGINT, " +
      s"FLOATColumn FLOAT, DOUBLEColumn DOUBLE, DATEColumn DATE, Data INT) USING DELTA" +
      s"  PARTITIONED BY (TINYINTColumn, SMALLINTColumn, INTColumn, BIGINTColumn," +
      s" FLOATColumn, DOUBLEColumn, DATEColumn)")

    spark.sql(s"INSERT INTO $tableName (TINYINTColumn, SMALLINTColumn, INTColumn, BIGINTColumn," +
      s" FLOATColumn, DOUBLEColumn, DATEColumn, Data)" +
      s" VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);")

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*)," +
        s"MIN(TINYINTColumn), MAX(TINYINTColumn)" +
        s", MIN(SMALLINTColumn), MAX(SMALLINTColumn)" +
        s", MIN(INTColumn), MAX(INTColumn)" +
        s", MIN(BIGINTColumn), MAX(BIGINTColumn)" +
        s", MIN(FLOATColumn), MAX(FLOATColumn)" +
        s", MIN(DOUBLEColumn), MAX(DOUBLEColumn)" +
        s", MIN(DATEColumn), MAX(DATEColumn)" +
        s", MIN(Data), MAX(Data)" +
        s" FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2, none#3, none#4, none#5, none#6, none#7L" +
        ", none#8L, none#9, none#10, none#11, none#12, none#13, none#14, none#15, none#16]")

    spark.sql(s"INSERT INTO $tableName (TINYINTColumn, SMALLINTColumn, INTColumn, BIGINTColumn," +
      s" FLOATColumn, DOUBLEColumn, DATEColumn, Data)" +
      s" VALUES (-128, -32768, -2147483648, -9223372036854775808," +
      s" -3.4028235E38, -1.7976931348623157E308, CAST('1582-10-15' AS DATE), 1);")

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*)," +
        s"MIN(TINYINTColumn), MAX(TINYINTColumn)" +
        s", MIN(SMALLINTColumn), MAX(SMALLINTColumn)" +
        s", MIN(INTColumn), MAX(INTColumn)" +
        s", MIN(BIGINTColumn), MAX(BIGINTColumn)" +
        s", MIN(FLOATColumn), MAX(FLOATColumn)" +
        s", MIN(DOUBLEColumn), MAX(DOUBLEColumn)" +
        s", MIN(DATEColumn), MAX(DATEColumn)" +
        s", MIN(Data), MAX(Data)" +
        s" FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2, none#3, none#4, none#5, none#6, none#7L" +
        ", none#8L, none#9, none#10, none#11, none#12, none#13, none#14, none#15, none#16]")
  }

  test("min-max - partitioned table - only NULL values") {
    val tableName = "TestOnlyNullValuesPartitioned"

    spark.sql(s"CREATE TABLE $tableName (Column1 INT, Column2 INT, Column3 INT) " +
      s"USING DELTA PARTITIONED BY (Column2, Column3)")

    spark.sql(s"INSERT INTO $tableName (Column1, Column2, Column3) VALUES (NULL, NULL, 1);")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2, Column3) VALUES (NULL, NULL, NULL);")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2, Column3) VALUES (NULL, NULL, 2);")

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*), MIN(Column1), MAX(Column1), MIN(Column2), MAX(Column2), " +
        s"MIN(Column3), MAX(Column3) FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2, none#3, none#4, none#5, none#6]")
  }

  test("min-max - column name containing punctuation") {
    val tableName = "TestPunctuationColumnName"

    spark.sql(s"CREATE TABLE $tableName (`My.!?Column` INT) USING DELTA")

    spark.sql(s"INSERT INTO $tableName (`My.!?Column`) VALUES (1), (2), (3);")

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*), MIN(`My.!?Column`), MAX(`My.!?Column`) FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2]")
  }

  test("min-max - partitioned table - column name containing punctuation") {
    val tableName = "TestPartitionedPunctuationColumnName"

    spark.sql(s"CREATE TABLE $tableName (`My.!?Column` INT, Data INT)" +
      s" USING DELTA PARTITIONED BY (`My.!?Column`)")

    spark.sql(s"INSERT INTO $tableName (`My.!?Column`, Data) VALUES (1, 1), (2, 1), (3, 1);")

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*), MIN(`My.!?Column`), MAX(`My.!?Column`) FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2]")
  }

  test("min-max - column mapping enabled") {
    val tableName = "TestColumnMapping"

    spark.sql(
      s"""CREATE TABLE $tableName (`My Column` INT) USING DELTA
         | TBLPROPERTIES('delta.columnMapping.mode' = 'name')""".stripMargin)

    spark.sql(s"INSERT INTO $tableName (`My Column`) VALUES (1);")
    spark.sql(s"INSERT INTO $tableName (`My Column`) VALUES (2);")
    spark.sql(s"INSERT INTO $tableName (`My Column`) VALUES (3);")

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*), MIN(`My Column`), MAX(`My Column`) FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2]")
  }

  test("min-max - partitioned table - column mapping enabled") {
    val tableName = "TestColumnMappingPartitioned"

    spark.sql(s"CREATE TABLE $tableName" +
      s" (Column1 INT, Column2 INT, `Column3 .,;{}()\n\t=` INT, Column4 INT)" +
      s" USING DELTA PARTITIONED BY (Column2, `Column3 .,;{}()\n\t=`)" +
      s" TBLPROPERTIES('delta.columnMapping.mode' = 'name')")

    spark.sql(s"INSERT INTO $tableName (Column1, Column2, `Column3 .,;{}()\n\t=`, Column4)" +
      s" VALUES (1, 2, 3, 4);")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2, `Column3 .,;{}()\n\t=`, Column4)" +
      s" VALUES (2, 2, 3, 5);")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2, `Column3 .,;{}()\n\t=`, Column4)" +
      s" VALUES (3, 3, 2, 6);")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2, `Column3 .,;{}()\n\t=`, Column4)" +
      s" VALUES (4, 3, 2, 7);")

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*)" +
        s", MIN(Column1), MAX(Column1)" +
        s", MIN(Column2), MAX(Column2)" +
        s", MIN(`Column3 .,;{}()\n\t=`), MAX(`Column3 .,;{}()\n\t=`)" +
        s", MIN(Column4), MAX(Column4)" +
        s" FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2, none#3, none#4, none#5, none#6, none#7, none#8]")
  }

  test("min-max - recompute column missing stats") {
    val tableName = "TestRecomputeMissingStat"

    spark.sql(s"CREATE TABLE $tableName (Column1 INT, Column2 INT) USING DELTA" +
      s" TBLPROPERTIES('delta.dataSkippingNumIndexedCols' = 0)")

    spark.sql(s"INSERT INTO $tableName (Column1, Column2) VALUES (1, 4);")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2) VALUES (2, 5);")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2) VALUES (3, 6);")

    checkOptimizationIsNotTriggered(s"SELECT COUNT(*), MIN(Column1), MAX(Column1) FROM $tableName")

    spark.sql(s"ALTER TABLE $tableName SET TBLPROPERTIES('delta.dataSkippingNumIndexedCols' = 1);")

    StatisticsCollection.recompute(
      spark,
      DeltaLog.forTable(spark, TableIdentifier(tableName)),
      DeltaTableV2(spark, TableIdentifier(tableName)).catalogTable)

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*), MIN(Column1), MAX(Column1) FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2]")

    checkOptimizationIsNotTriggered(s"SELECT COUNT(*), MIN(Column2), MAX(Column2) FROM $tableName")

    spark.sql(s"ALTER TABLE $tableName SET TBLPROPERTIES('delta.dataSkippingNumIndexedCols' = 2);")

    StatisticsCollection.recompute(
      spark,
      DeltaLog.forTable(spark, TableIdentifier(tableName)),
      DeltaTableV2(spark, TableIdentifier(tableName)).catalogTable)

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*), MIN(Column2), MAX(Column2) FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2]")
  }

  test("min-max - recompute added column") {
    val tableName = "TestRecomputeAddedColumn"

    spark.sql(s"CREATE TABLE $tableName (Column1 INT) USING DELTA")
    spark.sql(s"INSERT INTO $tableName (Column1) VALUES (1);")

    spark.sql(s"ALTER TABLE $tableName ADD COLUMN (Column2 INT)")

    spark.sql(s"INSERT INTO $tableName (Column1, Column2) VALUES (2, 5);")

    checkResultsAndOptimizedPlan(
      s"SELECT COUNT(*), MIN(Column1), MAX(Column1) FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2]")

    checkOptimizationIsNotTriggered(s"SELECT COUNT(*), " +
      s"MIN(Column1), MAX(Column1), MIN(Column2), MAX(Column2) FROM $tableName")

    StatisticsCollection.recompute(
      spark,
      DeltaLog.forTable(spark, TableIdentifier(tableName)),
      DeltaTableV2(spark, TableIdentifier(tableName)).catalogTable)

    checkResultsAndOptimizedPlan(s"SELECT COUNT(*), " +
      s"MIN(Column1), MAX(Column1), MIN(Column2), MAX(Column2) FROM $tableName",
      "LocalRelation [none#0L, none#1, none#2, none#3, none#4]")
  }

  test("Select Count: snapshot isolation") {
    sql(s"CREATE TABLE TestSnapshotIsolation (c1 int) USING DELTA")
    spark.sql("INSERT INTO TestSnapshotIsolation VALUES (1)")

    val scannedVersions = mutable.ArrayBuffer[Long]()
    val query = "SELECT (SELECT COUNT(*) FROM TestSnapshotIsolation), " +
      "(SELECT COUNT(*) FROM TestSnapshotIsolation)"

    checkResultsAndOptimizedPlan(
      query,
      "Project [scalar-subquery#0 [] AS #0L, scalar-subquery#0 [] AS #1L]\n" +
        ":  :- LocalRelation [none#0L]\n" +
        ":  +- LocalRelation [none#0L]\n" +
        "+- OneRowRelation")

    PrepareDeltaScanBase.withCallbackOnGetDeltaScanGenerator(scanGenerator => {
      // Record the scanned version and make changes to the table. We will verify changes in the
      // middle of the query are not visible to the query.
      scannedVersions += scanGenerator.snapshotToScan.version
      // Insert a row after each call to get scanGenerator
      // to test if the count doesn't change in the same query
      spark.sql("INSERT INTO TestSnapshotIsolation VALUES (1)")
    }) {
      val result = spark.sql(query).collect()(0)
      val c1 = result.getLong(0)
      val c2 = result.getLong(1)
      assertResult(c1, "Snapshot isolation should guarantee the results are always the same")(c2)
      assert(
        scannedVersions.toSet.size == 1,
        s"Scanned multiple versions of the same table in one query: ${scannedVersions.toSet}")
    }
  }

  test(".collect() and .show() both use this optimization") {
    var resultRow: Row = null
    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key -> "false") {
      resultRow = spark.sql(s"SELECT COUNT(*), MIN(id), MAX(id) FROM $testTableName").head
    }

    val totalRows = resultRow.getLong(0)
    val minId = resultRow.getLong(1)
    val maxId = resultRow.getLong(2)

    val collectPlans = DeltaTestUtils.withLogicalPlansCaptured(spark, optimizedPlan = true) {
      spark.sql(s"SELECT COUNT(*) FROM $testTableName").collect()
    }
    val collectResultData = collectPlans.collect { case x: LocalRelation => x.data }
    assert(collectResultData.size === 1)
    assert(collectResultData.head.head.getLong(0) === totalRows)

    val showPlans = DeltaTestUtils.withLogicalPlansCaptured(spark, optimizedPlan = true) {
      spark.sql(s"SELECT COUNT(*) FROM $testTableName").show()
    }
    val showResultData = showPlans.collect { case x: LocalRelation => x.data }
    assert(showResultData.size === 1)
    assert(showResultData.head.head.getString(0).toLong === totalRows)

    val showMultAggPlans = DeltaTestUtils.withLogicalPlansCaptured(spark, optimizedPlan = true) {
      spark.sql(s"SELECT COUNT(*), MIN(id), MAX(id) FROM $testTableName").show()
    }

    val showMultipleAggResultData = showMultAggPlans.collect { case x: LocalRelation => x.data }
    assert(showMultipleAggResultData.size === 1)
    val firstRow = showMultipleAggResultData.head.head
    assert(firstRow.getString(0).toLong === totalRows)
    assert(firstRow.getString(1).toLong === minId)
    assert(firstRow.getString(2).toLong === maxId)
  }

  test("min-max .show() - only NULL values") {
    val tableName = "TestOnlyNullValuesShow"

    spark.sql(s"CREATE TABLE $tableName (Column1 INT) USING DELTA")
    spark.sql(s"INSERT INTO $tableName (Column1) VALUES (NULL);")

    val showMultAggPlans = DeltaTestUtils.withLogicalPlansCaptured(spark, optimizedPlan = true) {
      spark.sql(s"SELECT MIN(Column1), MAX(Column1) FROM $tableName").show()
    }

    val showMultipleAggResultData = showMultAggPlans.collect { case x: LocalRelation => x.data }
    assert(showMultipleAggResultData.size === 1)
    val firstRow = showMultipleAggResultData.head.head
    assert(firstRow.getString(0) === "NULL")
    assert(firstRow.getString(1) === "NULL")
  }

  test("min-max .show() - Date Columns") {
    val tableName = "TestDateColumnsShow"

    spark.sql(s"CREATE TABLE $tableName (Column1 DATE, Column2 DATE) USING DELTA")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2) VALUES " +
      s"(CAST('1582-10-15' AS DATE), NULL);")

    val showMultAggPlans = DeltaTestUtils.withLogicalPlansCaptured(spark, optimizedPlan = true) {
      spark.sql(s"SELECT MIN(Column1), MIN(Column2) FROM $tableName").show()
    }

    val showMultipleAggResultData = showMultAggPlans.collect { case x: LocalRelation => x.data }
    assert(showMultipleAggResultData.size === 1)
    val firstRow = showMultipleAggResultData.head.head
    assert(firstRow.getString(0) === "1582-10-15")
    assert(firstRow.getString(1) === "NULL")
  }

  // Tests to validate the optimizer won't use missing or partial stats
  test("count-min-max - not supported - missing stats") {
    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(*) FROM $mixedStatsTableName")

    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(*) FROM $noStatsTableName")

    checkOptimizationIsNotTriggered(
      s"SELECT MIN(id), MAX(id) FROM $mixedStatsTableName")

    checkOptimizationIsNotTriggered(
      s"SELECT MIN(id), MAX(id) FROM $noStatsTableName")
  }

  // Tests to validate the optimizer won't incorrectly change queries it can't correctly handle

  test("min-max - not supported - data types") {
    val tableName = "TestUnsupportedTypes"

    spark.sql(s"CREATE TABLE $tableName " +
      s"(STRINGColumn STRING, DECIMALColumn DECIMAL(38,0)" +
      s", TIMESTAMPColumn TIMESTAMP, BINARYColumn BINARY, " +
      s"BOOLEANColumn BOOLEAN, ARRAYColumn ARRAY<INT>, MAPColumn MAP<INT, INT>, " +
      s"STRUCTColumn STRUCT<Id: INT, Name: STRING>) USING DELTA")

    spark.sql(s"INSERT INTO $tableName" +
      s" (STRINGColumn, DECIMALColumn, TIMESTAMPColumn, BINARYColumn" +
      s", BOOLEANColumn, ARRAYColumn, MAPColumn, STRUCTColumn) VALUES " +
      s"('A', -99999999999999999999999999999999999999, CAST('1900-01-01 00:00:00.0' AS TIMESTAMP)" +
      s", X'1ABF', TRUE, ARRAY(1, 2, 3), MAP(1, 10, 2, 20), STRUCT(1, 'Spark'));")

    val columnNames = List("STRINGColumn", "DECIMALColumn", "TIMESTAMPColumn",
      "BINARYColumn", "BOOLEANColumn", "ARRAYColumn", "STRUCTColumn")

    columnNames.foreach(colName =>
      checkOptimizationIsNotTriggered(s"SELECT MAX($colName) FROM $tableName")
    )
  }

  test("min-max - not supported - sub-query with column alias") {
    val tableName = "TestColumnAliasSubQuery"

    spark.sql(s"CREATE TABLE $tableName (Column1 INT, Column2 INT, Column3 INT) USING DELTA")

    spark.sql(s"INSERT INTO $tableName (Column1, Column2, Column3) VALUES (1, 2, 3);")

    checkOptimizationIsNotTriggered(
      s"SELECT MAX(Column2) FROM (SELECT Column1 AS Column2 FROM $tableName)")

    checkOptimizationIsNotTriggered(
      s"SELECT MAX(Column1), MAX(Column2), MAX(Column3) FROM " +
        s"(SELECT Column1 AS Column2, Column2 AS Column3, Column3 AS Column1 FROM $tableName)")
  }

  test("min-max - not supported - nested columns") {
    val tableName = "TestNestedColumns"

    spark.sql(s"CREATE TABLE $tableName " +
      s"(Column1 STRUCT<Id: INT>, " +
      s"`Column1.Id` INT) USING DELTA")

    spark.sql(s"INSERT INTO $tableName" +
      s" (Column1, `Column1.Id`) VALUES " +
      s"(STRUCT(1), 2);")

    // Nested Column
    checkOptimizationIsNotTriggered(
      s"SELECT MAX(Column1.Id) FROM $tableName")

    checkOptimizationIsNotTriggered(
      s"SELECT MAX(Column1.Id) AS XYZ FROM $tableName")

    // Validate the scenario where all the columns are read
    // since it creates a different query plan
    checkOptimizationIsNotTriggered(
      s"SELECT MAX(Column1.Id), " +
        s"MAX(`Column1.Id`) FROM $tableName")

    // The optimization for columns with dots should still work
    checkResultsAndOptimizedPlan(s"SELECT MAX(`Column1.Id`) FROM $tableName",
      "LocalRelation [none#0]")
  }

  test("count-min-max - not supported - group by") {
    checkOptimizationIsNotTriggered(
      s"SELECT group, COUNT(*) FROM $testTableName GROUP BY group")

    checkOptimizationIsNotTriggered(
      s"SELECT group, MIN(id), MAX(id) FROM $testTableName GROUP BY group")
  }

  test("count-min-max - not supported - plus literal") {
    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(*) + 1 FROM $testTableName")

    checkOptimizationIsNotTriggered(
      s"SELECT MAX(id) + 1 FROM $testTableName")
  }

  test("count - not supported - distinct count") {
    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(DISTINCT data) FROM $testTableName")
  }

  test("count-min-max - not supported - filter") {
    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(*) FROM $testTableName WHERE id > 0")

    checkOptimizationIsNotTriggered(
      s"SELECT MAX(id) FROM $testTableName WHERE id > 0")
  }

  test("count-min-max - not supported - sub-query with filter") {
    checkOptimizationIsNotTriggered(
      s"SELECT (SELECT COUNT(*) FROM $testTableName WHERE id > 0)")

    checkOptimizationIsNotTriggered(
      s"SELECT (SELECT MAX(id) FROM $testTableName WHERE id > 0)")
  }

  test("count - not supported - non-null") {
    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(ALL data) FROM $testTableName")
    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(data) FROM $testTableName")
  }

  test("count-min-max - not supported - join") {
    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(*) FROM $testTableName A, $testTableName B")

    checkOptimizationIsNotTriggered(
      s"SELECT MAX(A.id) FROM $testTableName A, $testTableName B")
  }

  test("count-min-max - not supported - over") {
    checkOptimizationIsNotTriggered(
      s"SELECT COUNT(*) OVER() FROM $testTableName LIMIT 1")

    checkOptimizationIsNotTriggered(
      s"SELECT MAX(id) OVER() FROM $testTableName LIMIT 1")
  }

  private def generateRowsDataFrame(source: Dataset[java.lang.Long]): DataFrame = {
    import testImplicits._

    source.select('id,
      'id.cast("tinyint") as 'TinyIntColumn,
      'id.cast("smallint") as 'SmallIntColumn,
      'id.cast("int") as 'IntColumn,
      'id.cast("bigint") as 'BigIntColumn,
      ('id / 3.3).cast("float") as 'FloatColumn,
      ('id / 3.3).cast("double") as 'DoubleColumn,
      date_add(lit("2022-08-31").cast("date"), col("id").cast("int")) as 'DateColumn,
      ('id % 2).cast("integer") as 'group,
      'id.cast("string") as 'data)
  }

  /** Validate the results of the query is the same with the flag
   * DELTA_OPTIMIZE_METADATA_QUERY_ENABLED enabled and disabled.
   * And the expected Optimized Query Plan with the flag enabled */
  private def checkResultsAndOptimizedPlan(
    query: String,
    expectedOptimizedPlan: String): Unit = {
    checkResultsAndOptimizedPlan(() => spark.sql(query), expectedOptimizedPlan)
  }

  /** Validate the results of the query is the same with the flag
   * DELTA_OPTIMIZE_METADATA_QUERY_ENABLED enabled and disabled.
   * And the expected Optimized Query Plan with the flag enabled. */
  private def checkResultsAndOptimizedPlan(
    generateQueryDf: () => DataFrame,
    expectedOptimizedPlan: String): Unit = {
    var expectedAnswer: scala.Seq[Row] = null
    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key -> "false") {
      expectedAnswer = generateQueryDf().collect()
    }

    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key -> "true") {
      val queryDf = generateQueryDf()
      val optimizedPlan = queryDf.queryExecution.optimizedPlan.canonicalized.toString()

      assert(queryDf.collect().sameElements(expectedAnswer))

      assertResult(expectedOptimizedPlan.trim) {
        optimizedPlan.trim
      }
    }
  }

  /**
   * Verify the query plans and results are the same with/without metadata query optimization.
   * This method can be used to verify cases that we shouldn't trigger optimization
   * or cases that we can potentially improve.
   * @param query
   */
  private def checkOptimizationIsNotTriggered(query: String) {
    var expectedOptimizedPlan: String = null
    var expectedAnswer: scala.Seq[Row] = null

    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key -> "false") {

      val generateQueryDf = spark.sql(query)
      expectedOptimizedPlan = generateQueryDf.queryExecution.optimizedPlan
        .canonicalized.toString()
      expectedAnswer = generateQueryDf.collect()
    }

    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key -> "true") {

      val generateQueryDf = spark.sql(query)
      val optimizationEnabledQueryPlan = generateQueryDf.queryExecution.optimizedPlan
        .canonicalized.toString()

      assert(generateQueryDf.collect().sameElements(expectedAnswer))

      assertResult(expectedOptimizedPlan) {
        optimizationEnabledQueryPlan
      }
    }
  }
}
