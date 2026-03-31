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

import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{CreateTableWriter, QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests that the `replaceUsing` and `replaceOn` options are blocked for DFv2
 * create/replace/createOrReplace operations.
 */
class DeltaInsertReplaceOnOrUsingDFv2CreateReplaceBlockingSuite
    extends QueryTest
    with SharedSparkSession {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.REPLACE_USING_OPTION_IN_DATAFRAME_WRITER_ENABLED.key, "true")
    .set(DeltaSQLConf.REPLACE_ON_OPTION_IN_DATAFRAME_WRITER_ENABLED.key, "true")

  import testImplicits._

  private def createTable(tableName: String, tableCols: Seq[String]): Unit = {
    sql(s"CREATE TABLE $tableName ${tableCols.mkString("(", ",", ")")} USING delta")
  }

  private val errorClass = "DELTA_DFV2_CREATE_REPLACE_INCOMPATIBLE_REPLACE_ON_OR_USING"

  private def assertReplaceOnOrUsingBlocked(
      tableName: String,
      optionName: String,
      optionValue: String)(op: CreateTableWriter[Row] => Unit): Unit = {
    checkError(
      exception = intercept[DeltaAnalysisException] {
        op(spark.range(10).toDF("id")
          .writeTo(tableName)
          .using("delta")
          .option(optionName, optionValue))
      },
      condition = errorClass,
      sqlState = "42000",
      parameters = Map.empty[String, String]
    )
  }

  Seq(
    ("replaceOn", "true"),
    ("replaceUsing", "id")
  ).foreach { case (optionName, optionValue) =>
    test(s"create() blocked with $optionName option") {
      withTable("target") {
        assertReplaceOnOrUsingBlocked(
          tableName = "target",
          optionName = optionName,
          optionValue = optionValue)(_.create())
      }
    }

    test(s"replace() blocked with $optionName option") {
      withTable("target") {
        createTable("target", Seq("id BIGINT"))
        assertReplaceOnOrUsingBlocked(
          tableName = "target",
          optionName = optionName,
          optionValue = optionValue)(_.replace())
      }
    }

    test(s"createOrReplace() blocked with $optionName option - table does not exist") {
      withTable("target") {
        assertReplaceOnOrUsingBlocked(
          tableName = "target",
          optionName = optionName,
          optionValue = optionValue)(_.createOrReplace())
      }
    }

    test(s"createOrReplace() blocked with $optionName option - table exists") {
      withTable("target") {
        createTable("target", Seq("id BIGINT"))
        assertReplaceOnOrUsingBlocked(
          tableName = "target",
          optionName = optionName,
          optionValue = optionValue)(_.createOrReplace())
      }
    }
  }

  Seq(
    ("REPLACEON", "true"),
    ("REPLACEUSING", "id")
  ).foreach { case (optionName, optionValue) =>
    test(s"blocked with case-insensitive $optionName option name") {
      withTable("target") {
        assertReplaceOnOrUsingBlocked(
          tableName = "target",
          optionName = optionName,
          optionValue = optionValue)(_.create())
      }
    }
  }
}
