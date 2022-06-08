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
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
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

  def describeDeltaDetailTest(
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
    print(sqlCommand) // for debugging

    val sqlResult = sql(sqlCommand)
    sqlResult.show() // for debugging
    checkResult(
      sqlResult,
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
    val invalidFilePath = s"/invalid/path/to/file"
    val e = intercept[AnalysisException] {
      sql(s"SHOW COLUMNS IN `$invalidFilePath`")
    }
    assert(e.getMessage().contains(s"Table identifier or view `$invalidFilePath` not found."))
  }

  test("delta table: table name not found") {
    val invalidTableName = s"test_table"
    describeDeltaDetailTest(f => s"delta.`${f.toString}`", "", "delta")
    val e = intercept[AnalysisException] {
      sql(s"SHOW COLUMNS IN `$invalidTableName` IN delta")
    }
    assert(e.getMessage().contains(s"Table identifier or view `delta`.`test_table` not found."))
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
    val e = intercept[SparkException] {
      describeDeltaDetailTest(f => s"'${f.toString}'", "", "json")
    }
    print(e.getMessage)
    assert(e.getMessage.contains(s".json is not a Parquet file."))
  }

  test("non-delta table: table ID not valid") {}

  // TODO: there is no metadata for temp views so we just raise error while querying view's columns?
  testWithTempView(s"delta table: show columns on temp view") { isSQLTempView =>
    withTable("t1") {
      Seq(1, 2, 3).toDF().write.format("delta").saveAsTable("t1")
      val viewName = "v"
      createTempViewFromTable("t1", isSQLTempView)
      val e = intercept[AnalysisException] {
        sql(s"SHOW COLUMNS IN $viewName").show()
      }
      assert(e.getMessage.contains(
        s"`$viewName` is a view. SHOW COLUMNS is only supported for tables."))
    }
  }
}

class ShowTableColumnsSuite extends ShowTableColumnsSuiteBase
  with DeltaSQLCommandTest
