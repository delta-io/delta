/*
 * Copyright (2020) The Delta Lake Project Authors.
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
import java.io.FileNotFoundException

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

trait DescribeDeltaDetailSuiteBase extends QueryTest
    with SharedSparkSession {

  import testImplicits._

  protected def checkResult(
    result: DataFrame,
    expected: Seq[Any],
    columns: Seq[String]): Unit = {
    checkAnswer(
      result.select(columns.head, columns.tail: _*),
      Seq(Row(expected: _*))
    )
  }

  def describeDeltaDetailTest(f: File => String): Unit = {
    val tempDir = Utils.createTempDir()
    Seq(1 -> 1).toDF("column1", "column2")
      .write
      .format("delta")
      .partitionBy("column1")
      .save(tempDir.toString())
    checkResult(
      sql(s"DESCRIBE DETAIL ${f(tempDir)}"),
      Seq("delta", Array("column1"), 1),
      Seq("format", "partitionColumns", "numFiles"))
  }

  test("delta table: path") {
    describeDeltaDetailTest(f => s"'${f.toString()}'")
  }

  test("delta table: delta table identifier") {
    describeDeltaDetailTest(f => s"delta.`${f.toString()}`")
  }

  test("non-delta table: table name") {
    withTable("describe_detail") {
      sql(
        """
          |CREATE TABLE describe_detail(column1 INT, column2 INT)
          |USING parquet
          |PARTITIONED BY (column1)
          |COMMENT "this is a table comment"
        """.stripMargin)
      sql(
        """
          |INSERT INTO describe_detail VALUES(1, 1)
        """.stripMargin
      )
      checkResult(
        sql("DESCRIBE DETAIL describe_detail"),
        Seq("parquet", Array("column1")),
        Seq("format", "partitionColumns"))
    }
  }

  test("non-delta table: path") {
    val tempDir = Utils.createTempDir().toString
    Seq(1 -> 1).toDF("column1", "column2")
      .write
      .format("parquet")
      .partitionBy("column1")
      .mode("overwrite")
      .save(tempDir)
    checkResult(
      sql(s"DESCRIBE DETAIL '$tempDir'"),
      Seq(tempDir),
      Seq("location"))
  }

  test("non-delta table: path doesn't exist") {
    val tempDir = Utils.createTempDir()
    tempDir.delete()
    val e = intercept[FileNotFoundException] {
      sql(s"DESCRIBE DETAIL '$tempDir'")
    }
    assert(e.getMessage.contains(tempDir.toString))
  }

  test("delta table: table name") {
    withTable("describe_detail") {
      sql(
        """
          |CREATE TABLE describe_detail(column1 INT, column2 INT)
          |USING delta
          |PARTITIONED BY (column1)
          |COMMENT "describe a non delta table"
        """.stripMargin)
      sql(
        """
          |INSERT INTO describe_detail VALUES(1, 1)
        """.stripMargin
      )
      checkResult(
        sql("DESCRIBE DETAIL describe_detail"),
        Seq("delta", Array("column1"), 1),
        Seq("format", "partitionColumns", "numFiles"))
    }
  }

  test("delta table: create table on an existing delta log") {
    val tempDir = Utils.createTempDir().toString
    Seq(1 -> 1).toDF("column1", "column2")
      .write
      .format("delta")
      .partitionBy("column1")
      .mode("overwrite")
      .save(tempDir)
    val tblName1 = "tbl_name1"
    val tblName2 = "tbl_name2"
    withTable(tblName1, tblName2) {
      sql(s"CREATE TABLE $tblName1 USING DELTA LOCATION '$tempDir'")
      sql(s"CREATE TABLE $tblName2 USING DELTA LOCATION '$tempDir'")
      checkResult(
        sql(s"DESCRIBE DETAIL $tblName1"),
        Seq(s"default.$tblName1"),
        Seq("name"))
      checkResult(
        sql(s"DESCRIBE DETAIL $tblName2"),
        Seq(s"default.$tblName2"),
        Seq("name"))
      checkResult(
        sql(s"DESCRIBE DETAIL delta.`$tempDir`"),
        Seq(null),
        Seq("name"))
      checkResult(
        sql(s"DESCRIBE DETAIL '$tempDir'"),
        Seq(null),
        Seq("name"))
    }
  }

  test("SC-37296: describe detail on temp view") {
    val view = "detailTestView"
    withTempView(view) {
      spark.range(10).createOrReplaceTempView(view)
      val e = intercept[AnalysisException] { sql(s"DESCRIBE DETAIL $view") }
      assert(e.getMessage.contains(
        "`detailTestView` is a view. DESCRIBE DETAIL is only supported for tables."))
    }
  }

  test("SC-37296: describe detail on permanent view") {
    val view = "detailTestView"
    withView(view) {
      sql(s"CREATE VIEW $view AS SELECT 1")
      val e = intercept[AnalysisException] { sql(s"DESCRIBE DETAIL $view") }
      assert(e.getMessage.contains(
        "`detailTestView` is a view. DESCRIBE DETAIL is only supported for tables."))
    }
  }

  // TODO: run it with OSS Delta after it's supported
}

class DescribeDeltaDetailSuite
  extends DescribeDeltaDetailSuiteBase with DeltaSQLCommandTest
