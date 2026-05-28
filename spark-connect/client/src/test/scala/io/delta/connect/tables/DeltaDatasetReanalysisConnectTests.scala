/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.tables

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.DeltaQueryTest

trait DeltaDatasetReanalysisConnectTests {
  self: DeltaTableRefreshConnectTestBase with DeltaQueryTest with RemoteSparkSession =>

  // ---------------------------------------------------------------------------
  // Section [4]: Version pinning and refresh in Dataset (Connect)
  // In Connect, there is no distinction between show and collect.
  // Both re-analyze and always see the latest data and schema.
  // ---------------------------------------------------------------------------

  test("[4] connect scenario 1: df picks up new data after write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      // In Connect, the df is re-analyzed on each execution
      checkAnswer(
        df.orderBy("id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[4] connect scenario 1.2: df.collect on same DataFrame after write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      // In Connect, collect() re-analyzes the plan on each execution
      // (unlike classic where it reuses cached QueryExecution).
      // Both show() and collect() see the new data.
      checkAnswer(
        df.orderBy("id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[4] connect scenario 1b: count then collect consistency") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      // In Connect, both count() and collect() re-analyze the plan,
      // so both see the new data. No inconsistency unlike classic mode.
      assert(df.count() == 2)
      checkAnswer(
        df.orderBy("id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[4] connect scenario 2: df picks up ADD COLUMN with new schema") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      // In Connect, df is re-analyzed and picks up the new schema
      checkAnswer(
        df.orderBy("id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }

  test("[4] connect scenario 3: df after DROP COLUMN re-analyzes (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")

      // In Connect, df is re-analyzed with the new schema
      checkAnswer(df, Row(1))
    }
  }

  test("[4] connect scenario 4: df after DROP and recreate table (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

      // In Connect, df re-analyzes to the new empty table
      checkAnswer(df, Seq.empty)
    }
  }

  test("[4] connect scenario 5: df after DROP/ADD column same name same type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")

      // In Connect, df re-analyzes. The old salary data is gone.
      checkAnswer(df, Row(1, null))
    }
  }

  test("[4] connect scenario 6: df after DROP/ADD column same name different type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")
      writerSql("INSERT INTO t VALUES (2, 'BBB')")

      // In Connect, df re-analyzes with new schema (salary is now STRING).
      // The doc shows both rows with the new STRING schema.
      checkAnswer(
        df.orderBy("id"),
        Seq(Row(1, null), Row(2, "BBB")))
    }
  }

  test("[4] connect scenario 7: df after ALTER COLUMN TYPE INT to BIGINT (type widening)") {
    withTable("t") {
      spark.sql(
        """CREATE TABLE t (id INT, salary INT) USING delta
          |TBLPROPERTIES (
          |  'delta.columnMapping.mode' = 'name',
          |  'delta.enableTypeWidening' = 'true'
          |)""".stripMargin)
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("ALTER TABLE t ALTER COLUMN salary TYPE BIGINT")

      // In Connect, df re-analyzes with new schema (salary is now BIGINT).
      // Type widening preserves the physical column, so data is still readable.
      checkAnswer(df, Row(1, 100))
    }
  }
}
