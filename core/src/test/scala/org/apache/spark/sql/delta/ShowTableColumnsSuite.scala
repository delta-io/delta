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

import org.apache.spark.SparkException
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row, SparkSession}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

import java.io.File

trait ShowTableColumnsSuiteBase extends QueryTest
  with SharedSparkSession with DeltaSQLCommandTest with DeltaTestUtilsForTempViews {

  import testImplicits._

  protected def checkResult(
      result: DataFrame,
      expected: Seq[Seq[Any]],
      columns: Seq[String]): Unit = {
    checkAnswer(
      result.select(columns.head, columns.tail: _*),
      expected.map { x =>
        Row(x: _*)
      }
    )
  }

  private def describeDeltaDetailTest(
      f: File => String,
      schemaName: String,
      tableFormat: String): Unit = {
    val tempDir = Utils.createTempDir()
    Seq(1 -> 1).toDF("column1", "column2")
      .write
      .format(tableFormat)
      .mode("overwrite")
      .save(tempDir.toString)

    val sqlCommand = if (schemaName.nonEmpty) {
      s"SHOW COLUMNS IN ${f(tempDir)} FROM $schemaName"
    } else {
      s"SHOW COLUMNS FROM ${f(tempDir)}"
    }

    checkResult(
      sql(sqlCommand),
      Seq(Seq("column1"), Seq("column2")),
      Seq("columnName"))
  }

  test("delta table: path") {
    describeDeltaDetailTest(f => s"'${f.toString}'", "", "delta")
  }

  // when no schema name provided, default schema name is `default`
  test("delta table: table identifier") {
    describeDeltaDetailTest(f => s"delta.`${f.toString}`", "", "delta")
  }

  test("delta table: table name with separated schema name") {
    describeDeltaDetailTest(f => s"`${f.toString}`", "delta", "delta")
  }

  test("non-delta table: path") {
    describeDeltaDetailTest(f => s"'${f.toString}'", "", "parquet")
  }

  test("non-delta table: table identifier") {
    describeDeltaDetailTest(f => s"`${f.toString}`", "delta", "parquet")
  }

  test("delta table: table identifier with catalog table") {
    withTable("show_columns") {
      sql(
        """
          |CREATE TABLE show_columns(column1 INT, column2 INT)
          |USING delta
          |COMMENT "describe a non delta table"
          """.stripMargin)
      checkResult(
        sql("SHOW COLUMNS IN show_columns"),
        Seq(Seq("column1"), Seq("column2")),
        Seq("columnName"))
    }
  }

  test("delta table: path not found") {
    describeDeltaDetailTest(f => s"'${f.toString}'", "", "delta")
    val fakeFilePath = s"/invalid/path/to/file"
    val e = intercept[AnalysisException] {
      sql(s"SHOW COLUMNS IN `$fakeFilePath`")
    }
    assert(e.getMessage().contains(s"Table identifier or view `$fakeFilePath` not found."))
  }

  test("delta table: table name not found") {
    val fakeTableName = s"test_table"
    val schemaName = s"delta"
    describeDeltaDetailTest(f => s"$schemaName.`${f.toString}`", "", "delta")
    val e = intercept[AnalysisException] {
      sql(s"SHOW COLUMNS IN `$fakeTableName` IN $schemaName")
    }
    assert(e.getMessage()
      .contains(s"Table identifier or view `$schemaName`.`$fakeTableName` not found."))
  }

  test("delta table: duplicated schema name") {
    // If there are 2 schema name in the command, the one attached on table name will be ignored.
    // e.g.: `SHOW COLUMNS delta.test_table IN epsilon` == `SHOW COLUMNS test_table IN epsilon`
    val schemaName = s"epsilon"
    withDatabase(schemaName) {
      sql(s"CREATE DATABASE $schemaName")
      val tableName = "test_table"
      withTable(tableName) {
        sql(
          s"""
             |CREATE TABLE $schemaName.$tableName(column1 INT, column2 INT)
             |USING delta
          """.stripMargin)
        checkResult(
          sql(s"SHOW COLUMNS IN delta.$tableName IN $schemaName"),
          Seq(Seq("column1"), Seq("column2")),
          Seq("columnName"))
      }
    }
  }

  test("delta table: should not use schema name together with path") {
    val e = intercept[AnalysisException] {
      describeDeltaDetailTest(f => s"'${f.toString}'", "delta", "delta")
    }
    assert(e.getMessage().contains(s"extraneous input"))
  }

  test("non-delta table: file format not supported") {
    val fileFormat = List("json", "csv", "orc")
    fileFormat.foreach { x =>
      val e = intercept[SparkException] {
        describeDeltaDetailTest(f => s"'${f.toString}'", "", x)
      }
      assert(e.getMessage.contains(s"is not a Parquet file"))
    }

    // check text format
    val e = intercept[AnalysisException] {
      describeDeltaDetailTest(f => s"'${f.toString}'", "", "text")
    }
    assert(e.getMessage().contains("Text data source does not support"))
  }

  test("non-delta table: table ID not valid") {
    val fakeTableID = s"`delta`.`test_table`"
    describeDeltaDetailTest(f => s"`${f.toString}`", "delta", "parquet")
    val e = intercept[AnalysisException] {
      sql(s"SHOW COLUMNS IN $fakeTableID")
    }
    assert(e.getMessage.contains(s"Table identifier or view $fakeTableID not found."))
  }

  testWithTempView(s"delta table: show columns on temp view") { isSQLTempView =>
    val tableName = "test_table_2"
    withTable(tableName) {
      Seq(1 -> 1).toDF("column1", "column2").write.format("delta").saveAsTable(tableName)
      val viewName = "v"
      createTempViewFromTable(tableName, isSQLTempView)
      checkResult(
        sql(s"SHOW COLUMNS IN $viewName"),
        Seq(Seq("column1"), Seq("column2")),
        Seq("columnName"))
    }
  }
}

class ShowTableColumnsSuite extends ShowTableColumnsSuiteBase
  with DeltaSQLCommandTest
