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

import java.io.File

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class ShowTableColumnsSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeltaTestUtilsForTempViews {

  import testImplicits._

  private val outputColumnNames = Seq("col_name")
  private val outputColumnValues = Seq(Seq("column1"), Seq("column2"))

  protected def checkResult(
      result: DataFrame,
      expected: Seq[Seq[Any]],
      columns: Seq[String]): Unit = {
    checkAnswer(
      result.select(columns.head, columns.tail: _*),
      expected.map { x => Row(x: _*)})
    assert(result.columns.toSeq == outputColumnNames)
  }

  private def showDeltaColumnsTest(
      fileToTableNameMapper: File => String,
      schemaName: Option[String] = None): Unit = {
    withDatabase("delta") {
      val tempDir = Utils.createTempDir()
      Seq(1 -> 1)
        .toDF("column1", "column2")
        .write
        .format("delta")
        .mode("overwrite")
        .save(tempDir.toString)

      val finalSchema = if (schemaName.nonEmpty) s"FROM ${schemaName.get}" else ""
      checkResult(sql(s"SHOW COLUMNS IN ${fileToTableNameMapper(tempDir)} $finalSchema"),
        outputColumnValues,
        outputColumnNames)
    }
  }

  test("delta table: table identifier") {
    showDeltaColumnsTest(f => s"delta.`${f.toString}`")
  }

  test("delta table: table name with separated schema name") {
    showDeltaColumnsTest(f => s"`${f.toString}`", schemaName = Some("delta"))
  }

  test("non-delta table: table identifier with catalog table") {
    // Non-Delta table represent by catalog identifier (e.g.: sales.line_ite) is supported in
    // SHOW COLUMNS command.
    withTable("show_columns") {
      sql(s"""
             |CREATE TABLE show_columns(column1 INT, column2 INT)
             |USING parquet
             |COMMENT "describe a non delta table"
      """.stripMargin)
      checkResult(sql("SHOW COLUMNS IN show_columns"), outputColumnValues, outputColumnNames)
    }
  }

  test("delta table: table name not found") {
    val fakeTableName = s"test_table"
    val schemaName = s"delta"
    showDeltaColumnsTest(f => s"$schemaName.`${f.toString}`")
    val e = intercept[AnalysisException] {
      sql(s"SHOW COLUMNS IN `$fakeTableName` IN $schemaName")
    }
    assert(e.getMessage().contains(s"Table or view not found: $schemaName.$fakeTableName") ||
      e.getMessage().contains(s"table or view `$schemaName`.`$fakeTableName` cannot be found"))
  }

  test("delta table: check duplicated schema name") {
    // When `schemaName` and `tableIdentity.database` both exists, we will throw error if they are
    // not the same.
    val schemaName = s"default"
    val tableName = s"test_table"
    val fakeSchemaName = s"epsilon"
    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $tableName(column1 INT, column2 INT)
             |USING delta
        """.stripMargin)

      // when no schema name provided, default schema name is `default`.
      checkResult(
        sql(s"SHOW COLUMNS IN $tableName"),
        outputColumnValues,
        outputColumnNames)
      checkResult(
        sql(s"SHOW COLUMNS IN $schemaName.$tableName"),
        outputColumnValues,
        outputColumnNames)

      var e = intercept[AnalysisException] {
        sql(s"SHOW COLUMNS IN $tableName IN $fakeSchemaName")
      }
      assert(e
        .getMessage()
        .contains(s"Table or view not found: $fakeSchemaName.$tableName") ||
        e.getMessage()
          .contains(s"table or view `$fakeSchemaName`.`$tableName` cannot be found"))

      e = intercept[AnalysisException] {
        sql(s"SHOW COLUMNS IN $fakeSchemaName.$tableName IN $schemaName")
      }
      assert(e
        .getMessage()
        .contains(s"Table or view not found: $fakeSchemaName.$tableName") ||
        e.getMessage()
          .contains(s"table or view `$fakeSchemaName`.`$tableName` cannot be found"))

      e = intercept[AnalysisException] {
        sql(s"SHOW COLUMNS IN $schemaName.$tableName IN $fakeSchemaName")
      }
      assert(e
        .getMessage()
        .contains(s"SHOW COLUMNS with conflicting databases: '$fakeSchemaName' != '$schemaName'"))
    }
  }

  testWithTempView(s"show columns on temp view should fallback to Spark") { isSQLTempView =>
    val tableName = "test_table_2"
    withTable(tableName) {
      Seq(1 -> 1)
        .toDF("column1", "column2")
        .write
        .format("delta")
        .saveAsTable(tableName)
      val viewName = "v"
      createTempViewFromTable(tableName, isSQLTempView)
      checkResult(sql(s"SHOW COLUMNS IN $viewName"), outputColumnValues, outputColumnNames)
    }
  }

  test(s"delta table: show columns on a nested column") {
    withTempDir { tempDir =>
      (70.to(79).seq ++ 75.to(79).seq)
        .toDF("id")
        .withColumn("nested", struct(struct('id + 2 as 'b, 'id + 3 as 'c) as 'sub))
        .write
        .format("delta")
        .save(tempDir.toString)
      checkResult(
        sql(s"SHOW COLUMNS IN delta.`${tempDir.toString}`"),
        Seq(Seq("id"), Seq("nested")),
        outputColumnNames)
    }
  }

  test("delta table: respect the Spark configuration on whether schema name is case sensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      assert(intercept[AnalysisException] {
        showDeltaColumnsTest(f => s"delta.`${f.toString}`", schemaName = Some("DELTA"))
      }.getMessage().contains(s"SHOW COLUMNS with conflicting databases: 'DELTA' != 'delta'"))

      assert(intercept[AnalysisException] {
        showDeltaColumnsTest(f => s"DELTA.`${f.toString}`", schemaName = Some("delta"))
      }.getMessage().contains(s"SHOW COLUMNS with conflicting databases: 'delta' != 'DELTA'"))
    }

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      showDeltaColumnsTest(f => s"delta.`${f.toString}`", schemaName = Some("DELTA"))
      showDeltaColumnsTest(f => s"DELTA.`${f.toString}`", schemaName = Some("delta"))
    }
  }
}
