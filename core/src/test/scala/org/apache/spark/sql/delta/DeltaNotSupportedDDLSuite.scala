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

import java.util.Locale

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}


class DeltaNotSupportedDDLSuite
  extends DeltaNotSupportedDDLBase
  with SharedSparkSession
  with DeltaSQLCommandTest


abstract class DeltaNotSupportedDDLBase extends QueryTest
    with SQLTestUtils {

  val format = "delta"

  val nonPartitionedTableName = "deltaTbl"

  val partitionedTableName = "partitionedTahoeTbl"

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    try {
      sql(s"""
           |CREATE TABLE $nonPartitionedTableName
           |USING $format
           |AS SELECT 1 as a, 'a' as b
         """.stripMargin)

      sql(s"""
            |CREATE TABLE $partitionedTableName (a INT, b STRING, p1 INT)
            |USING $format
            |PARTITIONED BY (p1)
          """.stripMargin)
      sql(s"INSERT INTO $partitionedTableName SELECT 1, 'A', 2")
    } catch {
      case NonFatal(e) =>
        afterAll()
        throw e
    }
  }

  protected override def afterAll(): Unit = {
    try {
      sql(s"DROP TABLE IF EXISTS $nonPartitionedTableName")
      sql(s"DROP TABLE IF EXISTS $partitionedTableName")
    } finally {
      super.afterAll()
    }
  }


  def assertUnsupported(query: String, messages: String*): Unit = {
    val allErrMessages = "operation not allowed" +: messages
    val e = intercept[AnalysisException] {
      sql(query)
    }
    assert(allErrMessages.exists(err => e.getMessage.toLowerCase(Locale.ROOT).contains(err)))
  }

  private def assertIgnored(query: String): Unit = {
    val outputStream = new java.io.ByteArrayOutputStream()
    Console.withOut(outputStream) {
      sql(query)
    }
    assert(outputStream.toString.contains("The request is ignored"))
  }

  test("bucketing is not supported for delta tables") {
    withTable("tbl") {
      assertUnsupported(
        s"""
          |CREATE TABLE tbl(a INT, b INT)
          |USING $format
          |CLUSTERED BY (a) INTO 5 BUCKETS
        """.stripMargin)
    }
  }

  test("ANALYZE TABLE PARTITION") {
    assertUnsupported(
      s"ANALYZE TABLE $partitionedTableName PARTITION (p1) COMPUTE STATISTICS",
      "not supported for v2 tables")
  }

  test("ALTER TABLE ADD PARTITION") {
    assertUnsupported(
      s"ALTER TABLE $partitionedTableName ADD PARTITION (p1=3)",
      "does not support partition management")
  }

  test("ALTER TABLE DROP PARTITION") {
    assertUnsupported(
      s"ALTER TABLE $partitionedTableName DROP PARTITION (p1=2)",
      "does not support partition management")
  }

  test("ALTER TABLE RECOVER PARTITIONS") {
    assertUnsupported(
      s"ALTER TABLE $partitionedTableName RECOVER PARTITIONS",
      "alter table ... recover partitions is not supported for v2 tables")
    assertUnsupported(
      s"MSCK REPAIR TABLE $partitionedTableName",
      "msck repair table is not supported for v2 tables")
  }

  test("ALTER TABLE SET SERDEPROPERTIES") {
    assertUnsupported(
      s"ALTER TABLE $nonPartitionedTableName SET SERDEPROPERTIES (s1=3)",
      "alter table ... set [serde|serdeproperties] is not supported for v2 tables")
  }


  test("LOAD DATA") {
    assertUnsupported(
      s"""LOAD DATA LOCAL INPATH '/path/to/home' INTO TABLE $nonPartitionedTableName""",
      "not supported for v2 tables")
  }

  test("INSERT OVERWRITE DIRECTORY") {
    assertUnsupported(s"INSERT OVERWRITE DIRECTORY '/path/to/home' USING $format VALUES (1, 'a')")
  }
}
