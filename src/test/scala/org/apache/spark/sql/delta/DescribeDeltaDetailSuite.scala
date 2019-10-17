/*
 * Copyright 2019 Databricks, Inc.
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

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
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
}

class DescribeDeltaDetailSuite
  extends DescribeDeltaDetailSuiteBase  with org.apache.spark.sql.delta.test.DeltaSQLCommandTest
