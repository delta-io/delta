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
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests that MERGE INTO a Delta table with an empty schema produces a user-facing
 * [[DeltaAnalysisException]] (DELTA_MERGE_INTO_EMPTY_SCHEMA_TARGET) when schema
 * evolution is off, and succeeds by widening the target when schema evolution is on.
 */
class MergeIntoEmptySchemaTargetSuite
  extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

  private val target = "empty_schema_target"
  private val source = "populated_source"

  private def withEmptyTargetAndSource(body: => Unit): Unit = {
    withTable(target, source) {
      sql(s"CREATE TABLE $target USING delta")
      sql(s"CREATE TABLE $source (id INT, name STRING) USING delta")
      sql(s"INSERT INTO $source VALUES (1, 'a'), (2, 'b')")
      body
    }
  }

  test("empty-schema target without schema evolution errors out") {
    withEmptyTargetAndSource {
      checkError(
        intercept[DeltaAnalysisException] {
          sql(
            s"""MERGE INTO $target USING $source AS s ON false
               |WHEN NOT MATCHED THEN INSERT *""".stripMargin)
        },
        "DELTA_MERGE_INTO_EMPTY_SCHEMA_TARGET",
        sqlState = "428GU",
        parameters = Map.empty[String, String])
    }
  }

  test("empty-schema target with schema evolution adopts the source schema and inserts rows") {
    withEmptyTargetAndSource {
      withSQLConf("spark.databricks.delta.schema.autoMerge.enabled" -> "true") {
        sql(
          s"""MERGE WITH SCHEMA EVOLUTION INTO $target USING $source AS s ON false
             |WHEN NOT MATCHED THEN INSERT *""".stripMargin)
        assert(spark.table(target).schema === spark.table(source).schema)
        checkAnswer(spark.table(target), spark.table(source))
      }
    }
  }

  test("flag gates the check: enabled throws user error, disabled preserves legacy behavior") {
    withEmptyTargetAndSource {
      val mergeStmt =
        s"""MERGE INTO $target USING $source AS s ON false
           |WHEN NOT MATCHED THEN INSERT *""".stripMargin

      // Flag on (default): the new user-facing analysis error is raised.
      withSQLConf(
          DeltaSQLConf.DELTA_MERGE_INTO_EMPTY_SCHEMA_TARGET_CHECK_ENABLED.key -> "true") {
        checkError(
          intercept[DeltaAnalysisException](sql(mergeStmt)),
          "DELTA_MERGE_INTO_EMPTY_SCHEMA_TARGET",
          sqlState = "428GU",
          parameters = Map.empty[String, String])
      }

      // Flag off: the legacy code path runs and fails with an internal AssertionError
      // (wrapped by Spark's runCommand in a SparkException) instead of the friendly error.
      withSQLConf(
          DeltaSQLConf.DELTA_MERGE_INTO_EMPTY_SCHEMA_TARGET_CHECK_ENABLED.key -> "false") {
        val thrown = intercept[Throwable](sql(mergeStmt))
        def hasAssertionErrorCause(t: Throwable): Boolean =
          t != null && (t.isInstanceOf[AssertionError] || hasAssertionErrorCause(t.getCause))
        assert(hasAssertionErrorCause(thrown),
          s"expected legacy AssertionError in cause chain, got: $thrown")
      }
    }
  }
}
