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

package org.apache.spark.sql.delta

import java.util.Date

import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

class DeltaCDCSQLSuite extends DeltaCDCSuiteBase with DeltaColumnMappingTestUtils {

  /** Single method to do all kinds of CDC reads */
  def cdcRead(
    tblId: TblId,
    start: Boundary,
    end: Boundary,
    schemaMode: Option[DeltaBatchCDFSchemaMode] = Some(BatchCDFSchemaLegacy),
    // SQL API does not support generic reader options, so it's a noop here
    readerOptions: Map[String, String] = Map.empty): DataFrame = {

    // Set the batch CDF schema mode using SQL conf if we specified it
    if (schemaMode.isDefined) {
      var result: DataFrame = null
      withSQLConf(DeltaSQLConf.DELTA_CDF_DEFAULT_SCHEMA_MODE_FOR_COLUMN_MAPPING_TABLE.key ->
        schemaMode.get.name) {
        result = cdcRead(tblId, start, end, None, readerOptions)
      }
      return result
    }

    val startPrefix: String = start match {
      case startingVersion: StartingVersion =>
        s"""${startingVersion.value}"""

      case startingTimestamp: StartingTimestamp =>
        s"""'${startingTimestamp.value}'"""

      case Unbounded =>
        ""
    }
    val endPrefix: String = end match {
      case endingVersion: EndingVersion =>
        s"""${endingVersion.value}"""

      case endingTimestamp: EndingTimestamp =>
        s"""'${endingTimestamp.value}'"""

      case Unbounded =>
        ""
    }
    val fnName = tblId match {
      case _: TablePath =>
        DeltaTableValueFunctions.CDC_PATH_BASED
      case _: TableName =>
        DeltaTableValueFunctions.CDC_NAME_BASED
      case _ =>
        throw new IllegalArgumentException("No table name or path provided")
    }

    if (endPrefix === "") {
      sql(s"SELECT * FROM $fnName('${tblId.id}', $startPrefix)")
    } else {
      sql(s"SELECT * FROM $fnName('${tblId.id}', $startPrefix, $endPrefix) ")
    }
  }

  override def ctas(
      srcTbl: String,
      dstTbl: String,
      disableCDC: Boolean = false): Unit = {

    val prefix = s"CREATE TABLE ${dstTbl} USING DELTA"
    val suffix = s" AS SELECT * FROM table_changes('${srcTbl}', 0, 1)"

    if (disableCDC) {
      sql(prefix + s" TBLPROPERTIES (${DeltaConfigs.CHANGE_DATA_FEED.key} = false)" + suffix)
    } else {
      sql(prefix + suffix)
    }
  }

  test("select individual column should push down filters") {
    val tblName = "tbl"
    withTable(tblName) {
      createTblWithThreeVersions(tblName = Some(tblName))

      val plans = DeltaTestUtils.withAllPlansCaptured(spark) {
        val res = sql(s"SELECT id, _change_type FROM table_changes('$tblName', 0, 1)")
          .where(col("id") < lit(5))

        assert(res.columns === Seq("id", "_change_type"))
        checkAnswer(
          res,
          spark.range(5)
            .withColumn("_change_type", lit("insert")))
      }
      assert(plans.map(_.executedPlan).toString
        .contains("PushedFilters: [IsNotNull(id), LessThan(id,5)]"))
    }
  }

  test("use cdc query as a subquery") {
    val tblName = "tbl"
    withTable(tblName) {
      createTblWithThreeVersions(tblName = Some(tblName))

      val res = sql(s"""
          SELECT * FROM RANGE(30) WHERE id > (
              SELECT count(*) FROM table_changes('$tblName', 0, 1))
        """)
      checkAnswer(
        res,
        spark.range(21, 30).toDF())
    }
  }

  test("cdc table_changes is not case sensitive") {
    val tblName = "tbl"
    withTempDir { dir =>
      withTable(tblName) {
        createTblWithThreeVersions(tblName = Some(tblName))

        checkAnswer(
          spark.sql(s"SELECT * FROM tabLe_chAnges('$tblName', 0, 1)"),
          spark.sql(s"SELECT * FROM taBle_cHanges('$tblName', 0, 1)")
        )
      }
    }
  }

  test("cdc table_changes_by_path are not case sensitive") {
    withTempDir { dir =>
      createTblWithThreeVersions(path = Some(dir.getAbsolutePath))

      checkAnswer(
        spark.sql(s"SELECT * FROM tabLe_chaNges_By_pAth('${dir.getAbsolutePath}', 0, 1)"),
        spark.sql(s"SELECT * FROM taBle_cHanges_bY_paTh('${dir.getAbsolutePath}', 0, 1)")
      )
    }
  }


  test("parse multi part table name") {
    val tblName = "tbl"
      withTable(tblName) {
        createTblWithThreeVersions(tblName = Some(tblName))

        checkAnswer(
          spark.sql(s"SELECT * FROM table_changes('$tblName', 0, 1)"),
          spark.sql(s"SELECT * FROM table_changes('default.`${tblName}`', 0, 1)")
        )
      }
  }

  test("negative case - invalid number of args") {
    val tbl = "tbl"
    withTable(tbl) {
      spark.range(10).write.format("delta").saveAsTable(tbl)

      val invalidQueries = Seq(
        s"SELECT * FROM table_changes()",
        s"SELECT * FROM table_changes('tbl', 1, 2, 3)",
        s"SELECT * FROM table_changes('tbl')",
        s"SELECT * FROM table_changes_by_path()",
        s"SELECT * FROM table_changes_by_path('tbl', 1, 2, 3)",
        s"SELECT * FROM table_changes_by_path('tbl')"
      )
      invalidQueries.foreach { q =>
        val e = intercept[AnalysisException] {
          sql(q)
        }
        assert(e.getMessage.contains("requires at least 2 arguments and at most 3 arguments"),
          s"failed query: $q ")
      }
    }
  }

  test("negative case - invalid type of args") {
    val tbl = "tbl"
    withTable(tbl) {
      spark.range(10).write.format("delta").saveAsTable(tbl)

      val invalidQueries = Seq(
        s"SELECT * FROM table_changes(1, 1)",
        s"SELECT * FROM table_changes('$tbl', 1.0)",
        s"SELECT * FROM table_changes_by_path(1, 1)",
        s"SELECT * FROM table_changes_by_path('$tbl', 1.0)"
      )

      invalidQueries.foreach { q =>
        val e = intercept[AnalysisException] {
          sql(q)
        }
        assert(e.getMessage.contains("Unsupported expression type"), s"failed query: $q")
      }
    }
  }

  test("resolve expression for timestamp function - now") {
    val tbl = "tbl"
    withTable(tbl) {
      createTblWithThreeVersions(tblName = Some(tbl))

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tbl))

      val currentTime = new Date().getTime
      modifyDeltaTimestamp(deltaLog, 0, currentTime - 100000)
      modifyDeltaTimestamp(deltaLog, 1, currentTime)
      modifyDeltaTimestamp(deltaLog, 2, currentTime + 100000)

      val readDf = sql(s"SELECT * FROM table_changes('$tbl', 0, now())")
      checkCDCAnswer(
        DeltaLog.forTable(spark, TableIdentifier("tbl")),
        readDf,
        spark.range(20)
          .withColumn("_change_type", lit("insert"))
          .withColumn("_commit_version", (col("id") / 10).cast(LongType))
        )

      // more complex expression
      val readDf2 = sql(s"SELECT * FROM table_changes('$tbl', 0, now() + interval 5 seconds)")
      checkCDCAnswer(
        DeltaLog.forTable(spark, TableIdentifier("tbl")),
        readDf2,
        spark.range(20)
          .withColumn("_change_type", lit("insert"))
          .withColumn("_commit_version", (col("id") / 10).cast(LongType))
      )
    }
  }

  test("resolve invalid table name should throw error") {
    var e = intercept[AnalysisException] {
      sql(s"SELECT * FROM table_changes(now(), 1, 1)")
    }
    assert(e.getMessage.contains("Unsupported expression type(TimestampType) for table name." +
      " The supported types are [Literal of type StringType]"))

    e = intercept[AnalysisException] {
      sql(s"SELECT * FROM table_changes('invalidtable', 1, 1)")
    }
    assert(
      e.getMessage.contains("Table or view 'invalidtable' not found in database 'default'") ||
      e.getMessage.contains("Table main.default.invalidtable not found") ||
      e.getMessage.contains("table or view `default`.`invalidtable` cannot be found") ||
      e.getMessage.contains("table or view `main`.`default`.`invalidtable` cannot be found"))

    withTable ("tbl") {
      spark.range(1).write.format("delta").saveAsTable("tbl")
      val e = intercept[AnalysisException] {
        sql(s"SELECT * FROM table_changes(concat('tb', 'l'), 1, 1)")
      }
      assert(e.getMessage.contains("Unsupported expression type(StringType) for table name." +
        " The supported types are [Literal of type StringType]"))
    }
  }


  test("resolution of complex expression should throw an error") {
    val tbl = "tbl"
    withTable(tbl) {
      spark.range(10).write.format("delta").saveAsTable(tbl)
      val e = intercept[AnalysisException] {
        sql(s"SELECT * FROM table_changes('$tbl', 0, id)")
      }
      assert(e.getErrorClass == "MISSING_COLUMN")
      assert(e.getMessage.contains("Column 'id' does not exist"))
    }
  }

  test("protocol version") {
    withTable("tbl") {
      spark.range(10).write.format("delta").saveAsTable("tbl")
      val log = DeltaLog.forTable(spark, TableIdentifier(tableName = "tbl"))
      // We set CDC to be enabled by default, so this should automatically bump the writer protocol
      // to the required version.
      if (columnMappingEnabled) {
        assert(log.snapshot.protocol == Protocol(2, 5))
      } else {
        assert(log.snapshot.protocol == Protocol(1, 4))
      }
    }
  }


  test("table_changes and table_changes_by_path with a non-delta table") {
    withTempDir { dir =>
      withTable("tbl") {
        spark.range(10).write.format("parquet")
          .option("path", dir.getAbsolutePath)
          .saveAsTable("tbl")

        var e = intercept[AnalysisException] {
          spark.sql(s"SELECT * FROM table_changes('tbl', 0, 1)")
        }
        assert(e.getErrorClass == "DELTA_TABLE_NOT_FOUND")
        assert(e.getMessage.contains("Delta table `default`.`tbl` doesn't exist"))

        e = intercept[AnalysisException] {
          spark.sql(s"SELECT * FROM table_changes_by_path('${dir.getAbsolutePath}', 0, 1)")
        }
        assert(e.getErrorClass == "DELTA_TABLE_NOT_FOUND")
        assert(e.getMessage.contains(s"Delta table `${dir.getAbsolutePath}` doesn't exist"))
      }
    }
  }
}
