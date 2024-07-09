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

import org.apache.spark.sql.delta.{DeletionVectorsTestUtils, DeltaColumnMappingEnableIdMode, DeltaColumnMappingEnableNameMode, DeltaLog, DeltaTestUtils}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.PrepareDeltaScanBase
import org.apache.spark.sql.delta.stats.StatisticsCollection
import org.apache.spark.sql.delta.test.DeltaColumnMappingSelectedTestMixin
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.{DataFrame, Dataset, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

class OptimizeMetadataOnlyDeltaQuerySuite
  extends QueryTest
    with SharedSparkSession
    with BeforeAndAfterAll
    with DeltaSQLCommandTest
    with DeletionVectorsTestUtils {
  val testTableName = "table_basic"
  val noStatsTableName = " table_nostats"
  val mixedStatsTableName = " table_mixstats"

  var dfPart1: DataFrame = null
  var dfPart2: DataFrame = null

  var totalRows: Long = -1
  var minId: Long = -1
  var maxId: Long = -1

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

    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key -> "false") {
      val result = spark.sql(s"SELECT COUNT(*), MIN(id), MAX(id) FROM $testTableName").head
      totalRows = result.getLong(0)
      minId = result.getLong(1)
      maxId = result.getLong(2)
    }
  }

  /** Class to hold test parameters */
  case class ScalaTestParams(name: String, queryScala: () => DataFrame, expectedPlan: String)

  Seq(
    new ScalaTestParams(
      name = "count - simple query",
      queryScala = () => spark.read.format("delta").table(testTableName)
        .agg(count(col("*"))),
      expectedPlan = "LocalRelation [none#0L]"),
    new ScalaTestParams(
      name = "min-max - simple query",
      queryScala = () => spark.read.format("delta").table(testTableName)
      .agg(min(col("id")), max(col("id")),
        min(col("TinyIntColumn")), max(col("TinyIntColumn")),
        min(col("SmallIntColumn")), max(col("SmallIntColumn")),
        min(col("IntColumn")), max(col("IntColumn")),
        min(col("BigIntColumn")), max(col("BigIntColumn")),
        min(col("FloatColumn")), max(col("FloatColumn")),
        min(col("DoubleColumn")), max(col("DoubleColumn")),
        min(col("DateColumn")), max(col("DateColumn"))),
      expectedPlan = "LocalRelation [none#0L, none#1L, none#2, none#3, none#4, none#5, none#6" +
      ", none#7, none#8L, none#9L, none#10, none#11, none#12, none#13, none#14, none#15]"))
    .foreach { testParams =>
      test(s"optimization supported - Scala - ${testParams.name}") {
        checkResultsAndOptimizedPlan(testParams.queryScala, testParams.expectedPlan)
    }
  }

  /** Class to hold test parameters */
  case class SqlTestParams(
    name: String,
    querySql: String,
    expectedPlan: String,
    querySetup: Option[Seq[String]] = None)

  Seq(
    new SqlTestParams(
      name = "count - simple query",
      querySql = s"SELECT COUNT(*) FROM $testTableName",
      expectedPlan = "LocalRelation [none#0L]"),
    new SqlTestParams(
      name = "min-max - simple query",
      querySql = s"SELECT MIN(id), MAX(id)" +
        s", MIN(TinyIntColumn), MAX(TinyIntColumn)" +
        s", MIN(SmallIntColumn), MAX(SmallIntColumn)" +
        s", MIN(IntColumn), MAX(IntColumn)" +
        s", MIN(BigIntColumn), MAX(BigIntColumn)" +
        s", MIN(FloatColumn), MAX(FloatColumn)" +
        s", MIN(DoubleColumn), MAX(DoubleColumn)" +
        s", MIN(DateColumn), MAX(DateColumn)" +
        s"FROM $testTableName",
      expectedPlan = "LocalRelation [none#0L, none#1L, none#2, none#3, none#4, none#5, none#6" +
        ", none#7, none#8L, none#9L, none#10, none#11, none#12, none#13, none#14, none#15]"),
    new SqlTestParams(
      name = "min-max - column name non-matching case",
      querySql = s"SELECT MIN(ID), MAX(iD)" +
        s", MIN(tINYINTCOLUMN), MAX(tinyintcolumN)" +
        s", MIN(sMALLINTCOLUMN), MAX(smallintcolumN)" +
        s", MIN(iNTCOLUMN), MAX(intcolumN)" +
        s", MIN(bIGINTCOLUMN), MAX(bigintcolumN)" +
        s", MIN(fLOATCOLUMN), MAX(floatcolumN)" +
        s", MIN(dOUBLECOLUMN), MAX(doublecolumN)" +
        s", MIN(dATECOLUMN), MAX(datecolumN)" +
        s"FROM $testTableName",
      expectedPlan = "LocalRelation [none#0L, none#1L, none#2, none#3, none#4, none#5, none#6" +
        ", none#7, none#8L, none#9L, none#10, none#11, none#12, none#13, none#14, none#15]"),
    new SqlTestParams(
      name = "count with column name alias",
      querySql = s"SELECT COUNT(*) as MyCount FROM $testTableName",
      expectedPlan = "LocalRelation [none#0L]"),
    new SqlTestParams(
      name = "count-min-max with column name alias",
      querySql = s"SELECT COUNT(*) as MyCount, MIN(id) as MyMinId, MAX(id) as MyMaxId" +
        s" FROM $testTableName",
      expectedPlan = "LocalRelation [none#0L, none#1L, none#2L]"),
    new SqlTestParams(
      name = "count-min-max - table name with alias",
      querySql = s"SELECT COUNT(*), MIN(id), MAX(id) FROM $testTableName MyTable",
      expectedPlan = "LocalRelation [none#0L, none#1L, none#2L]"),
    new SqlTestParams(
      name = "count-min-max - query using time travel - version 0",
      querySql = s"SELECT COUNT(*), MIN(id), MAX(id) " +
      s"FROM $testTableName VERSION AS OF 0",
      expectedPlan = "LocalRelation [none#0L, none#1L, none#2L]"),
    new SqlTestParams(
      name = "count-min-max - query using time travel - version 1",
      querySql = s"SELECT COUNT(*), MIN(id), MAX(id) " +
      s"FROM $testTableName VERSION AS OF 1",
      expectedPlan = "LocalRelation [none#0L, none#1L, none#2L]"),
    new SqlTestParams(
      name = "count-min-max - query using time travel - version 2",
      querySql = s"SELECT COUNT(*), MIN(id), MAX(id) " +
      s"FROM $testTableName VERSION AS OF 2",
      expectedPlan = "LocalRelation [none#0L, none#1L, none#2L]"),
    new SqlTestParams(
      name = "count - sub-query",
      querySql = s"SELECT (SELECT COUNT(*) FROM $testTableName)",
      expectedPlan = "Project [scalar-subquery#0 [] AS #0L]\n" +
        ":  +- LocalRelation [none#0L]\n+- OneRowRelation"),
    new SqlTestParams(
      name = "min - sub-query",
      querySql = s"SELECT (SELECT MIN(id) FROM $testTableName)",
      expectedPlan = "Project [scalar-subquery#0 [] AS #0L]\n" +
        ":  +- LocalRelation [none#0L]\n+- OneRowRelation"),
    new SqlTestParams(
      name = "max - sub-query",
      querySql = s"SELECT (SELECT MAX(id) FROM $testTableName)",
      expectedPlan = "Project [scalar-subquery#0 [] AS #0L]\n" +
        ":  +- LocalRelation [none#0L]\n+- OneRowRelation"),
    new SqlTestParams(
      name = "count - sub-query filter",
      querySql = s"SELECT 'ABC' WHERE" +
        s" (SELECT COUNT(*) FROM $testTableName) = $totalRows",
      expectedPlan = "Project [ABC AS #0]\n+- Filter (scalar-subquery#0 [] = " +
        totalRows + ")\n   :  +- LocalRelation [none#0L]\n   +- OneRowRelation"),
    new SqlTestParams(
      name = "min - sub-query filter",
      querySql = s"SELECT 'ABC' WHERE" +
        s" (SELECT MIN(id) FROM $testTableName) = $minId",
      expectedPlan = "Project [ABC AS #0]\n+- Filter (scalar-subquery#0 [] = " +
        minId + ")\n   :  +- LocalRelation [none#0L]\n   +- OneRowRelation"),
    new SqlTestParams(
      name = "max - sub-query filter",
      querySql = s"SELECT 'ABC' WHERE" +
        s" (SELECT MAX(id) FROM $testTableName) = $maxId",
      expectedPlan = "Project [ABC AS #0]\n+- Filter (scalar-subquery#0 [] = " +
        maxId + ")\n   :  +- LocalRelation [none#0L]\n   +- OneRowRelation"),
    // Limit doesn't affect aggregation results
    new SqlTestParams(
      name = "count-min-max - query with limit",
      querySql = s"SELECT COUNT(*), MIN(id), MAX(id) FROM $testTableName LIMIT 3",
      expectedPlan = "LocalRelation [none#0L, none#1L, none#2L]"),
    new SqlTestParams(
      name = "count-min-max - duplicated functions",
      querySql = s"SELECT COUNT(*), COUNT(*), MIN(id), MIN(id), MAX(id), MAX(id)" +
        s" FROM $testTableName",
      expectedPlan = "LocalRelation [none#0L, none#1L, none#2L, none#3L, none#4L, none#5L]"),
    new SqlTestParams(
      name = "count - empty table",
      querySetup = Some(Seq("CREATE TABLE TestEmpty (c1 int) USING DELTA")),
      querySql = "SELECT COUNT(*) FROM TestEmpty",
      expectedPlan = "LocalRelation [none#0L]"),
    /** Dates are stored as Int in literals. This test make sure Date columns works
     * and NULL are handled correctly
     */
    new SqlTestParams(
      name = "min-max - date columns",
      querySetup = Some(Seq(
        "CREATE TABLE TestDateValues" +
        " (Column1 DATE, Column2 DATE, Column3 DATE) USING DELTA;",
        "INSERT INTO TestDateValues" +
        " (Column1, Column2, Column3) VALUES (NULL, current_date(), current_date());",
        "INSERT INTO TestDateValues" +
        " (Column1, Column2, Column3) VALUES (NULL, NULL, current_date());")),
      querySql = "SELECT COUNT(*), MIN(Column1), MAX(Column1), MIN(Column2)" +
      ", MAX(Column2), MIN(Column3), MAX(Column3) FROM TestDateValues",
      expectedPlan = "LocalRelation [none#0L, none#1, none#2, none#3, none#4, none#5, none#6]"),
    new SqlTestParams(
      name = "min-max - floating point infinity",
      querySetup = Some(Seq(
        "CREATE TABLE TestFloatInfinity (FloatColumn Float, DoubleColumn Double) USING DELTA",
        "INSERT INTO TestFloatInfinity (FloatColumn, DoubleColumn) VALUES (1, 1);",
        "INSERT INTO TestFloatInfinity (FloatColumn, DoubleColumn) VALUES (NULL, NULL);",
        "INSERT INTO TestFloatInfinity (FloatColumn, DoubleColumn) VALUES " +
          "(float('inf'), double('inf'))" +
          ", (float('+inf'), double('+inf'))" +
          ", (float('infinity'), double('infinity'))" +
          ", (float('+infinity'), double('+infinity'))" +
          ", (float('-inf'), double('-inf'))" +
          ", (float('-infinity'), double('-infinity'))"
      )),
      querySql = "SELECT COUNT(*), MIN(FloatColumn), MAX(FloatColumn), MIN(DoubleColumn)" +
        ", MAX(DoubleColumn) FROM TestFloatInfinity",
      expectedPlan = "LocalRelation [none#0L, none#1, none#2, none#3, none#4]"),
    // NaN is larger than any other value, including Infinity
    new SqlTestParams(
      name = "min-max - floating point NaN values",
      querySetup = Some(Seq(
        "CREATE TABLE TestFloatNaN (FloatColumn Float, DoubleColumn Double) USING DELTA",
        "INSERT INTO TestFloatNaN (FloatColumn, DoubleColumn) VALUES (1, 1);",
        "INSERT INTO TestFloatNaN (FloatColumn, DoubleColumn) VALUES (NULL, NULL);",
        "INSERT INTO TestFloatNaN (FloatColumn, DoubleColumn) VALUES " +
          "(float('inf'), double('inf'))" +
          ", (float('+inf'), double('+inf'))" +
          ", (float('infinity'), double('infinity'))" +
          ", (float('+infinity'), double('+infinity'))" +
          ", (float('-inf'), double('-inf'))" +
          ", (float('-infinity'), double('-infinity'))",
        "INSERT INTO TestFloatNaN (FloatColumn, DoubleColumn) VALUES " +
      "(float('NaN'), double('NaN'));"
      )),
      querySql = "SELECT COUNT(*), MIN(FloatColumn), MAX(FloatColumn), MIN(DoubleColumn)" +
        ", MAX(DoubleColumn) FROM TestFloatNaN",
      expectedPlan = "LocalRelation [none#0L, none#1, none#2, none#3, none#4]"),
    new SqlTestParams(
      name = "min-max - floating point min positive value",
      querySetup = Some(Seq(
        "CREATE TABLE TestFloatPrecision (FloatColumn Float, DoubleColumn Double) USING DELTA",
        "INSERT INTO TestFloatPrecision (FloatColumn, DoubleColumn) VALUES " +
      "(CAST('1.4E-45' as FLOAT), CAST('4.9E-324' as DOUBLE))" +
      ", (CAST('-1.4E-45' as FLOAT), CAST('-4.9E-324' as DOUBLE))" +
      ", (0, 0);"
      )),
      querySql = "SELECT COUNT(*), MIN(FloatColumn), MAX(FloatColumn), MIN(DoubleColumn)" +
        ", MAX(DoubleColumn) FROM TestFloatPrecision",
      expectedPlan = "LocalRelation [none#0L, none#1, none#2, none#3, none#4]"),
    new SqlTestParams(
      name = "min-max - NULL and non-NULL values",
      querySetup = Some(Seq(
        "CREATE TABLE TestNullValues (Column1 INT, Column2 INT, Column3 INT) USING DELTA",
        "INSERT INTO TestNullValues (Column1, Column2, Column3) VALUES (NULL, 1, 1);",
        "INSERT INTO TestNullValues (Column1, Column2, Column3) VALUES (NULL, NULL, 1);"
      )),
      querySql = "SELECT COUNT(*), MIN(Column1), MAX(Column1)," +
        "MIN(Column2), MAX(Column2) FROM TestNullValues",
      expectedPlan = "LocalRelation [none#0L, none#1, none#2, none#3, none#4]"),
    new SqlTestParams(
      name = "min-max - only NULL values",
      querySetup = Some(Seq(
        "CREATE TABLE TestOnlyNullValues (Column1 INT, Column2 INT, Column3 INT) USING DELTA",
        "INSERT INTO TestOnlyNullValues (Column1, Column2, Column3) VALUES (NULL, NULL, 1);",
        "INSERT INTO TestOnlyNullValues (Column1, Column2, Column3) VALUES (NULL, NULL, 2);"
      )),
      querySql = "SELECT COUNT(*), MIN(Column1), MAX(Column1), MIN(Column2), MAX(Column2), " +
        "MIN(Column3), MAX(Column3) FROM TestOnlyNullValues",
      expectedPlan = "LocalRelation [none#0L, none#1, none#2, none#3, none#4, none#5, none#6]"),
    new SqlTestParams(
      name = "min-max - all supported data types",
      querySetup = Some(Seq(
        "CREATE TABLE TestMinMaxValues (" +
      "TINYINTColumn TINYINT, SMALLINTColumn SMALLINT, INTColumn INT, BIGINTColumn BIGINT, " +
      "FLOATColumn FLOAT, DOUBLEColumn DOUBLE, DATEColumn DATE) USING DELTA",
        "INSERT INTO TestMinMaxValues (TINYINTColumn, SMALLINTColumn, INTColumn, BIGINTColumn," +
      " FLOATColumn, DOUBLEColumn, DATEColumn)" +
      " VALUES (-128, -32768, -2147483648, -9223372036854775808," +
      " -3.4028235E38, -1.7976931348623157E308, CAST('1582-10-15' AS DATE));",
        "INSERT INTO TestMinMaxValues (TINYINTColumn, SMALLINTColumn, INTColumn, BIGINTColumn," +
      " FLOATColumn, DOUBLEColumn, DATEColumn)" +
      " VALUES (127, 32767, 2147483647, 9223372036854775807," +
      " 3.4028235E38, 1.7976931348623157E308, CAST('9999-12-31' AS DATE));"
      )),
      querySql = "SELECT COUNT(*)," +
        "MIN(TINYINTColumn), MAX(TINYINTColumn)" +
        ", MIN(SMALLINTColumn), MAX(SMALLINTColumn)" +
        ", MIN(INTColumn), MAX(INTColumn)" +
        ", MIN(BIGINTColumn), MAX(BIGINTColumn)" +
        ", MIN(FLOATColumn), MAX(FLOATColumn)" +
        ", MIN(DOUBLEColumn), MAX(DOUBLEColumn)" +
        ", MIN(DATEColumn), MAX(DATEColumn)" +
        " FROM TestMinMaxValues",
      expectedPlan = "LocalRelation [none#0L, none#1, none#2, none#3, none#4, none#5, none#6" +
        ", none#7L, none#8L, none#9, none#10, none#11, none#12, none#13, none#14]"),
    new SqlTestParams(
      name = "count-min-max - partitioned table - simple query",
      querySetup = Some(Seq(
        "CREATE TABLE TestPartitionedTable (Column1 INT, Column2 INT, Column3 INT, Column4 INT)" +
        " USING DELTA PARTITIONED BY (Column2, Column3)",
        "INSERT INTO TestPartitionedTable" +
        " (Column1, Column2, Column3, Column4) VALUES (1, 2, 3, 4);",
        "INSERT INTO TestPartitionedTable" +
        " (Column1, Column2, Column3, Column4) VALUES (2, 2, 3, 5);",
        "INSERT INTO TestPartitionedTable" +
        " (Column1, Column2, Column3, Column4) VALUES (3, 3, 2, 6);",
        "INSERT INTO TestPartitionedTable" +
        " (Column1, Column2, Column3, Column4) VALUES (4, 3, 2, 7);"
      )),
      querySql = "SELECT COUNT(*)" +
        ", MIN(Column1), MAX(Column1)" +
        ", MIN(Column2), MAX(Column2)" +
        ", MIN(Column3), MAX(Column3)" +
        ", MIN(Column4), MAX(Column4)" +
        " FROM TestPartitionedTable",
      expectedPlan = "LocalRelation [none#0L, none#1, none#2, none#3," +
        " none#4, none#5, none#6, none#7, none#8]"),
    /** Partitioned columns should be able to return MIN and MAX data
     * even when there are no column stats */
    new SqlTestParams(
      name = "count-min-max - partitioned table - no stats",
      querySetup = Some(Seq(
        "CREATE TABLE TestPartitionedTableNoStats" +
        " (Column1 INT, Column2 INT, Column3 INT, Column4 INT)" +
        " USING DELTA PARTITIONED BY (Column2, Column3)" +
        " TBLPROPERTIES('delta.dataSkippingNumIndexedCols' = 0)",
        "INSERT INTO TestPartitionedTableNoStats" +
        " (Column1, Column2, Column3, Column4) VALUES (1, 2, 3, 4);",
        "INSERT INTO TestPartitionedTableNoStats" +
        " (Column1, Column2, Column3, Column4) VALUES (2, 2, 3, 5);",
        "INSERT INTO TestPartitionedTableNoStats" +
        " (Column1, Column2, Column3, Column4) VALUES (3, 3, 2, 6);",
        "INSERT INTO TestPartitionedTableNoStats" +
        " (Column1, Column2, Column3, Column4) VALUES (4, 3, 2, 7);"
      )),
      querySql = "SELECT COUNT(*)" +
        ", MIN(Column2), MAX(Column2)" +
        ", MIN(Column3), MAX(Column3)" +
        " FROM TestPartitionedTableNoStats",
      expectedPlan = "LocalRelation [none#0L, none#1, none#2, none#3, none#4]"),
    new SqlTestParams(
      name = "min-max - partitioned table - all supported data types",
      querySetup = Some(Seq(
        "CREATE TABLE TestAllTypesPartitionedTable (" +
        "TINYINTColumn TINYINT, SMALLINTColumn SMALLINT, INTColumn INT, BIGINTColumn BIGINT, " +
        "FLOATColumn FLOAT, DOUBLEColumn DOUBLE, DATEColumn DATE, Data INT) USING DELTA" +
        "  PARTITIONED BY (TINYINTColumn, SMALLINTColumn, INTColumn, BIGINTColumn," +
        " FLOATColumn, DOUBLEColumn, DATEColumn)",
        "INSERT INTO TestAllTypesPartitionedTable" +
        " (TINYINTColumn, SMALLINTColumn, INTColumn, BIGINTColumn," +
        " FLOATColumn, DOUBLEColumn, DATEColumn, Data)" +
        " VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);",
        "INSERT INTO TestAllTypesPartitionedTable" +
        " (TINYINTColumn, SMALLINTColumn, INTColumn, BIGINTColumn," +
        " FLOATColumn, DOUBLEColumn, DATEColumn, Data)" +
        " VALUES (-128, -32768, -2147483648, -9223372036854775808," +
        " -3.4028235E38, -1.7976931348623157E308, CAST('1582-10-15' AS DATE), 1);"
      )),
      querySql = "SELECT COUNT(*)," +
        "MIN(TINYINTColumn), MAX(TINYINTColumn)" +
        ", MIN(SMALLINTColumn), MAX(SMALLINTColumn)" +
        ", MIN(INTColumn), MAX(INTColumn)" +
        ", MIN(BIGINTColumn), MAX(BIGINTColumn)" +
        ", MIN(FLOATColumn), MAX(FLOATColumn)" +
        ", MIN(DOUBLEColumn), MAX(DOUBLEColumn)" +
        ", MIN(DATEColumn), MAX(DATEColumn)" +
        ", MIN(Data), MAX(Data)" +
        " FROM TestAllTypesPartitionedTable",
      expectedPlan = "LocalRelation [none#0L, none#1, none#2, none#3, none#4, none#5, none#6, " +
        "none#7L, none#8L, none#9, none#10, none#11, none#12, none#13, none#14, none#15, none#16]"),
    new SqlTestParams(
      name = "min-max - partitioned table - only NULL values",
      querySetup = Some(Seq(
        "CREATE TABLE TestOnlyNullValuesPartitioned (" +
        "TINYINTColumn TINYINT, SMALLINTColumn SMALLINT, INTColumn INT, BIGINTColumn BIGINT, " +
        "FLOATColumn FLOAT, DOUBLEColumn DOUBLE, DATEColumn DATE, Data INT) USING DELTA" +
        "  PARTITIONED BY (TINYINTColumn, SMALLINTColumn, INTColumn, BIGINTColumn," +
        " FLOATColumn, DOUBLEColumn, DATEColumn)",
        "INSERT INTO TestOnlyNullValuesPartitioned" +
        " (TINYINTColumn, SMALLINTColumn, INTColumn, BIGINTColumn," +
        " FLOATColumn, DOUBLEColumn, DATEColumn, Data)" +
        " VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);"
      )),
      querySql = "SELECT COUNT(*)," +
        "MIN(TINYINTColumn), MAX(TINYINTColumn)" +
        ", MIN(SMALLINTColumn), MAX(SMALLINTColumn)" +
        ", MIN(INTColumn), MAX(INTColumn)" +
        ", MIN(BIGINTColumn), MAX(BIGINTColumn)" +
        ", MIN(FLOATColumn), MAX(FLOATColumn)" +
        ", MIN(DOUBLEColumn), MAX(DOUBLEColumn)" +
        ", MIN(DATEColumn), MAX(DATEColumn)" +
        ", MIN(Data), MAX(Data)" +
        " FROM TestOnlyNullValuesPartitioned",
      expectedPlan = "LocalRelation [none#0L, none#1, none#2, none#3, none#4, none#5, none#6, " +
        "none#7L, none#8L, none#9, none#10, none#11, none#12, none#13, none#14, none#15, none#16]"),
    new SqlTestParams(
      name = "min-max - partitioned table - NULL and NON-NULL values",
      querySetup = Some(Seq(
        "CREATE TABLE TestNullPartitioned (Column1 INT, Column2 INT, Column3 INT)" +
        " USING DELTA PARTITIONED BY (Column2, Column3)",
        "INSERT INTO TestNullPartitioned (Column1, Column2, Column3) VALUES (NULL, NULL, 1);",
        "INSERT INTO TestNullPartitioned (Column1, Column2, Column3) VALUES (NULL, NULL, NULL);",
        "INSERT INTO TestNullPartitioned (Column1, Column2, Column3) VALUES (NULL, NULL, 2);"
      )),
      querySql = "SELECT COUNT(*), MIN(Column1), MAX(Column1), MIN(Column2), MAX(Column2), " +
        "MIN(Column3), MAX(Column3) FROM TestNullPartitioned",
      expectedPlan = "LocalRelation [none#0L, none#1, none#2, none#3, none#4, none#5, none#6]"),
    new SqlTestParams(
      name = "min-max - column name containing punctuation",
      querySetup = Some(Seq(
        "CREATE TABLE TestPunctuationColumnName (`My.!?Column` INT) USING DELTA",
        "INSERT INTO TestPunctuationColumnName (`My.!?Column`) VALUES (1), (2), (3);"
      )),
      querySql = "SELECT COUNT(*), MIN(`My.!?Column`), MAX(`My.!?Column`)" +
        " FROM TestPunctuationColumnName",
      expectedPlan = "LocalRelation [none#0L, none#1, none#2]"),
    new SqlTestParams(
      name = "min-max - partitioned table - column name containing punctuation",
      querySetup = Some(Seq(
        "CREATE TABLE TestPartitionedPunctuationColumnName (`My.!?Column` INT, Data INT)" +
        " USING DELTA PARTITIONED BY (`My.!?Column`)",
        "INSERT INTO TestPartitionedPunctuationColumnName" +
        " (`My.!?Column`, Data) VALUES (1, 1), (2, 1), (3, 1);"
      )),
      querySql = "SELECT COUNT(*), MIN(`My.!?Column`), MAX(`My.!?Column`)" +
        " FROM TestPartitionedPunctuationColumnName",
      expectedPlan = "LocalRelation [none#0L, none#1, none#2]"),
    new SqlTestParams(
      name = "min-max - partitioned table - special characters in column name",
      querySetup = Some(Seq(
        "CREATE TABLE TestColumnMappingPartitioned" +
        " (Column1 INT, Column2 INT, `Column3 .,;{}()\n\t=` INT, Column4 INT)" +
        " USING DELTA PARTITIONED BY (Column2, `Column3 .,;{}()\n\t=`)" +
        " TBLPROPERTIES('delta.columnMapping.mode' = 'name')",
        "INSERT INTO TestColumnMappingPartitioned" +
        " (Column1, Column2, `Column3 .,;{}()\n\t=`, Column4)" +
        " VALUES (1, 2, 3, 4);",
        "INSERT INTO TestColumnMappingPartitioned" +
        " (Column1, Column2, `Column3 .,;{}()\n\t=`, Column4)" +
        " VALUES (2, 2, 3, 5);",
        "INSERT INTO TestColumnMappingPartitioned" +
        " (Column1, Column2, `Column3 .,;{}()\n\t=`, Column4)" +
        " VALUES (3, 3, 2, 6);",
        "INSERT INTO TestColumnMappingPartitioned" +
        " (Column1, Column2, `Column3 .,;{}()\n\t=`, Column4)" +
        " VALUES (4, 3, 2, 7);")),
      querySql = "SELECT COUNT(*)" +
        ", MIN(Column1), MAX(Column1)" +
        ", MIN(Column2), MAX(Column2)" +
        ", MIN(`Column3 .,;{}()\n\t=`), MAX(`Column3 .,;{}()\n\t=`)" +
        ", MIN(Column4), MAX(Column4)" +
        " FROM TestColumnMappingPartitioned",
      expectedPlan = "LocalRelation [none#0L, none#1, none#2, none#3," +
        " none#4, none#5, none#6, none#7, none#8]"))
    .foreach { testParams =>
      test(s"optimization supported - SQL - ${testParams.name}") {
        if (testParams.querySetup.isDefined) {
          testParams.querySetup.get.foreach(spark.sql)
        }
        checkResultsAndOptimizedPlan(testParams.querySql, testParams.expectedPlan)
    }
  }

  test("count-min-max - external table") {
    withTempDir { dir =>
      val testTablePath = dir.getCanonicalPath
      dfPart1.write.format("delta").mode("overwrite").save(testTablePath)
      DeltaTable.forPath(spark, testTablePath).delete("id = 1")
      dfPart2.write.format("delta").mode(SaveMode.Append).save(testTablePath)

      checkResultsAndOptimizedPlan(
        s"SELECT COUNT(*), MIN(id), MAX(id) FROM delta.`$testTablePath`",
        "LocalRelation [none#0L, none#1L, none#2L]")
    }
  }

  test("min-max - partitioned column stats disabled") {
    withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
      val tableName = "TestPartitionedNoStats"

      spark.sql(s"CREATE TABLE $tableName (Column1 INT, Column2 INT)" +
        " USING DELTA PARTITIONED BY (Column2)")

      spark.sql(s"INSERT INTO $tableName (Column1, Column2) VALUES (1, 3);")
      spark.sql(s"INSERT INTO $tableName (Column1, Column2) VALUES (2, 4);")

      // Has no stats, including COUNT
      checkOptimizationIsNotTriggered(
        s"SELECT COUNT(*), MIN(Column2), MAX(Column2) FROM $tableName")

      // Should work for partitioned columns even without stats
      checkResultsAndOptimizedPlan(
        s"SELECT MIN(Column2), MAX(Column2) FROM $tableName",
        "LocalRelation [none#0, none#1]")
    }
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

  test("count - dv-enabled") {
    withTempDir { dir =>
      val tempPath = dir.getCanonicalPath
      spark.range(1, 10, 1, 1).write.format("delta").save(tempPath)

      enableDeletionVectorsInTable(new Path(tempPath), true)
      DeltaTable.forPath(spark, tempPath).delete("id = 1")
      assert(!getFilesWithDeletionVectors(DeltaLog.forTable(spark, new Path(tempPath))).isEmpty)

      checkResultsAndOptimizedPlan(
        s"SELECT COUNT(*) FROM delta.`$tempPath`",
        "LocalRelation [none#0L]")
    }
  }

  test("count - zero rows AddFile") {
    withTempDir { dir =>
      val tempPath = dir.getCanonicalPath
      val df = spark.range(1, 10)
      val expectedResult = df.count()
      df.write.format("delta").save(tempPath)

      // Creates AddFile entries with non-existing files
      // The query should read only the delta log and not the parquet files
      val log = DeltaLog.forTable(spark, tempPath)
      val txn = log.startTransaction()
      txn.commitManually(
        DeltaTestUtils.createTestAddFile(encodedPath = "1.parquet", stats = "{\"numRecords\": 0}"),
        DeltaTestUtils.createTestAddFile(encodedPath = "2.parquet", stats = "{\"numRecords\": 0}"),
        DeltaTestUtils.createTestAddFile(encodedPath = "3.parquet", stats = "{\"numRecords\": 0}"))

      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key -> "true") {
        val queryDf = spark.sql(s"SELECT COUNT(*) FROM delta.`$tempPath`")
        val optimizedPlan = queryDf.queryExecution.optimizedPlan.canonicalized.toString()

        assert(queryDf.head().getLong(0) === expectedResult)

        assertResult("LocalRelation [none#0L]") {
          optimizedPlan.trim
        }
      }
    }
  }

  test("filter on partitioned column") {
    val tableName = "TestPartitionedFilter"

    spark.sql(s"CREATE TABLE $tableName (Column1 INT, Column2 INT)" +
      " USING DELTA PARTITIONED BY (Column2)")

    spark.sql(s"INSERT INTO $tableName (Column1, Column2) VALUES (1, 2);")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2) VALUES (2, 2);")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2) VALUES (3, 3);")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2) VALUES (4, 3);")

    // Filter by partition column
    checkResultsAndOptimizedPlan(
      "SELECT COUNT(*)" +
        ", MIN(Column1), MAX(Column1)" +
        ", MIN(Column2), MAX(Column2)" +
        s" FROM $tableName WHERE Column2 = 2",
      "LocalRelation [none#0L, none#1, none#2, none#3, none#4]")

    // Filter both partition and data columns
    checkOptimizationIsNotTriggered(
      "SELECT COUNT(*)" +
        ", MIN(Column1), MAX(Column1)" +
        ", MIN(Column2), MAX(Column2)" +
        s" FROM $tableName WHERE Column1 = 2 AND Column2 = 2")
  }

  // Tests to validate the optimizer won't incorrectly change queries it can't correctly handle

  Seq((s"SELECT COUNT(*) FROM $mixedStatsTableName", "missing stats"),
    (s"SELECT COUNT(*) FROM $noStatsTableName", "missing stats"),
    (s"SELECT MIN(id), MAX(id) FROM $mixedStatsTableName", "missing stats"),
    (s"SELECT MIN(id), MAX(id) FROM $noStatsTableName", "missing stats"),
    (s"SELECT group, COUNT(*) FROM $testTableName GROUP BY group", "group by"),
    (s"SELECT group, MIN(id), MAX(id) FROM $testTableName GROUP BY group", "group by"),
    (s"SELECT COUNT(*) + 1 FROM $testTableName", "plus literal"),
    (s"SELECT MAX(id) + 1 FROM $testTableName", "plus literal"),
    (s"SELECT COUNT(DISTINCT data) FROM $testTableName", "distinct count"),
    (s"SELECT COUNT(*) FROM $testTableName WHERE id > 0", "filter"),
    (s"SELECT MAX(id) FROM $testTableName WHERE id > 0", "filter"),
    (s"SELECT (SELECT COUNT(*) FROM $testTableName WHERE id > 0)", "sub-query with filter"),
    (s"SELECT (SELECT MAX(id) FROM $testTableName WHERE id > 0)", "sub-query with filter"),
    (s"SELECT COUNT(ALL data) FROM $testTableName", "count non-null"),
    (s"SELECT COUNT(data) FROM $testTableName", "count non-null"),
    (s"SELECT COUNT(*) FROM $testTableName A, $testTableName B", "join"),
    (s"SELECT MAX(A.id) FROM $testTableName A, $testTableName B", "join"),
    (s"SELECT COUNT(*) OVER() FROM $testTableName LIMIT 1", "over"),
    ( s"SELECT MAX(id) OVER() FROM $testTableName LIMIT 1", "over")
    )
    .foreach { case (query, desc) =>
      test(s"optimization not supported - $desc - $query") {
        checkOptimizationIsNotTriggered(query)
    }
  }

  test("optimization not supported - min-max unsupported data types") {
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

  test("optimization not supported - min-max column without stats") {
    val tableName = "TestColumnWithoutStats"

    spark.sql(s"CREATE TABLE $tableName (Column1 INT, Column2 INT) USING DELTA" +
      s" TBLPROPERTIES('delta.dataSkippingNumIndexedCols' = 1)")
    spark.sql(s"INSERT INTO $tableName (Column1, Column2) VALUES (1, 2);")

    checkOptimizationIsNotTriggered(
      s"SELECT MAX(Column2) FROM $tableName")
  }

  // For empty tables the stats won't be found and the query should not be optimized
  test("optimization not supported - min-max empty table") {
    val tableName = "TestMinMaxEmptyTable"

    spark.sql(s"CREATE TABLE $tableName (Column1 INT) USING DELTA")

    checkOptimizationIsNotTriggered(
      s"SELECT MIN(Column1), MAX(Column1) FROM $tableName")
  }

  test("optimization not supported - min-max dv-enabled") {
    withTempDir { dir =>
      val tempPath = dir.getCanonicalPath
      spark.range(1, 10, 1, 1).write.format("delta").save(tempPath)
      val querySql = s"SELECT MIN(id), MAX(id) FROM delta.`$tempPath`"
      checkResultsAndOptimizedPlan(querySql, "LocalRelation [none#0L, none#1L]")

      enableDeletionVectorsInTable(new Path(tempPath), true)
      DeltaTable.forPath(spark, tempPath).delete("id = 1")
      assert(!getFilesWithDeletionVectors(DeltaLog.forTable(spark, new Path(tempPath))).isEmpty)
      checkOptimizationIsNotTriggered(querySql)
    }
  }

  test("optimization not supported - sub-query with column alias") {
    val tableName = "TestColumnAliasSubQuery"

    spark.sql(s"CREATE TABLE $tableName (Column1 INT, Column2 INT, Column3 INT) USING DELTA")

    spark.sql(s"INSERT INTO $tableName (Column1, Column2, Column3) VALUES (1, 2, 3);")

    checkOptimizationIsNotTriggered(
      s"SELECT MAX(Column2) FROM (SELECT Column1 AS Column2 FROM $tableName)")

    checkOptimizationIsNotTriggered(
      s"SELECT MAX(Column1), MAX(Column2), MAX(Column3) FROM " +
        s"(SELECT Column1 AS Column2, Column2 AS Column3, Column3 AS Column1 FROM $tableName)")
  }

  test("optimization not supported - nested columns") {
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

trait OptimizeMetadataOnlyDeltaQueryColumnMappingSuiteBase
  extends DeltaColumnMappingSelectedTestMixin {
  override protected def runAllTests = true
}

class OptimizeMetadataOnlyDeltaQueryIdColumnMappingSuite
  extends OptimizeMetadataOnlyDeltaQuerySuite
  with DeltaColumnMappingEnableIdMode
  with OptimizeMetadataOnlyDeltaQueryColumnMappingSuiteBase

class OptimizeMetadataOnlyDeltaQueryNameColumnMappingSuite
  extends OptimizeMetadataOnlyDeltaQuerySuite
  with DeltaColumnMappingEnableNameMode
  with OptimizeMetadataOnlyDeltaQueryColumnMappingSuiteBase
