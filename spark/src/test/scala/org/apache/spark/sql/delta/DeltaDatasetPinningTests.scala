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

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Section [4]: Version pinning and refresh in Dataset (show vs collect).
 *
 * Tests that document Dataset's QueryExecution caching behavior:
 * - show() creates a new QueryExecution (sees latest data/schema)
 * - collect() reuses cached QueryExecution (may see stale schema)
 * - count() creates a new QueryExecution (inconsistency with collect())
 *
 * This section is classic-only because Connect always re-analyzes
 * (see DeltaDatasetReanalysisConnectTests for the Connect variant).
 */
trait DeltaDatasetPinningTests {
  self: DeltaTableRefreshTestBase with QueryTest with SharedSparkSession with DeltaSQLCommandTest =>

  // ---------------------------------------------------------------------------
  // Section [4]: Version pinning and refresh in Dataset
  // ---------------------------------------------------------------------------

  test("[4] scenario 1.1: df.show picks up new data after write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      // Fresh SQL always picks up new data
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[4] scenario 1.2: df.collect on same DataFrame after write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      // First collect triggers QueryExecution
      checkAnswer(df, Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      // collect() on the same df reuses QueryExecution,
      // but TahoeFileIndex.getSnapshot() still calls deltaLog.update()
      // during physical execution. For data-only changes, new data is visible.
      checkAnswer(
        df.orderBy("id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[4] scenario 1.2b: count then collect inconsistency on same DataFrame") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      // First collect caches QueryExecution
      checkAnswer(df, Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      // count() creates a new QueryExecution, so it sees the new data
      assert(df.count() == 2)

      // collect() on the same df reuses the cached QueryExecution.
      // This is the documented inconsistency from the design doc:
      // count() returns 2 but collect() returns only 1 row because
      // Dataset remembers and reuses QueryExecution for collect but not for
      // show, count, and other actions.
      checkAnswer(df, Row(1, 100))
    }
  }

  test("[4] scenario 2: df.show and collect after ADD COLUMN keeps original schema") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      if (v2EnableMode == "STRICT") {
        // STRICT mode: V2 catalog caches stale schema after ALTER TABLE ADD COLUMN
        checkError(
          exception = intercept[AnalysisException] {
            writerSql("INSERT INTO t VALUES (2, 200, -1)")
          },
          condition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
          parameters = Map("tableName" -> ".*", "tableColumns" -> ".*",
            "dataColumns" -> ".*", "reason" -> ".*"),
          matchPVals = true)
      } else {
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      // df pins original schema (id, salary) but picks up latest version data
      checkAnswer(
        df.orderBy("id"),
        Seq(Row(1, 100), Row(2, 200)))

      // Fresh SQL picks up new schema
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  test("[4] scenario 3: df after DROP COLUMN (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")

      // Fresh SQL re-analyzes with the new schema and succeeds.
      // The table now only has column "id".
      checkAnswer(sql("SELECT * FROM t"), Row(1))

      // collect() on the same DataFrame reuses the cached QueryExecution,
      // so it still returns old data with the original (id, salary) schema.
      // Dataset remembers and reuses QueryExecution for collect.
      checkAnswer(df, Row(1, 100))
    }
  }

  test("[4] scenario 4.1: df.show after DROP and recreate table (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

      // Fresh SQL re-analyzes and sees the new empty table
      checkAnswer(sql("SELECT * FROM t"), Seq.empty)

      // collect() on the same df references the old table's data files which no longer exist.
      // This results in a runtime error (file not found), not a schema change error.
      intercept[Exception] {
        df.collect()
      }
    }
  }

  test("[4] scenario 5.1: df.show after DROP/ADD column same name same type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")

      // Fresh SQL re-analyzes with the new schema (new column IDs).
      // The old salary data is gone since it's a different physical column.
      checkAnswer(sql("SELECT * FROM t"), Row(1, null))

      // collect() on the same DataFrame reuses the cached QueryExecution.
      // The old QueryExecution still references the original physical column,
      // so it returns old data with the original schema.
      checkAnswer(df, Row(1, 100))
    }
  }

  test("[4] scenario 6.1: df.show after DROP/ADD column same name different type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")

      // Fresh SQL re-analyzes with the new schema (salary is now STRING).
      checkAnswer(sql("SELECT * FROM t"), Row(1, null))

      // collect() on the same DataFrame reuses the cached QueryExecution.
      // The old QueryExecution still references the original physical column,
      // so it returns old data with the original schema.
      checkAnswer(df, Row(1, 100))
    }
  }

  test("[4] scenario 7: df after ALTER COLUMN TYPE INT to BIGINT (type widening)") {
    withTable("t") {
      sql(
        """CREATE TABLE t (id INT, salary INT) USING delta
          |TBLPROPERTIES (
          |  'delta.columnMapping.mode' = 'name',
          |  'delta.enableTypeWidening' = 'true'
          |)""".stripMargin)
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("ALTER TABLE t ALTER COLUMN salary TYPE BIGINT")

      // Fresh SQL re-analyzes with the new schema (salary is now BIGINT).
      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      // collect() on the same DataFrame reuses the cached QueryExecution.
      // Type widening preserves the physical column, so old data is readable.
      checkAnswer(df, Row(1, 100))
    }
  }
}
