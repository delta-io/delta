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
 * Section [3]: Incrementally constructed queries (joins).
 *
 * Tests that joining two DataFrames analyzed at different times
 * uses consistent table versions. PrepareDeltaScan ensures both
 * scans in a joined plan use the same (latest) snapshot.
 */
trait DeltaJoinRefreshTests {
  self: DeltaTableRefreshTestBase with QueryTest with SharedSparkSession with DeltaSQLCommandTest =>

  // ---------------------------------------------------------------------------
  // Section [3]: Incrementally constructed queries
  // ---------------------------------------------------------------------------

  test("[3] scenario 1: join after write uses consistent version") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("INSERT INTO t VALUES (2, 200)")

      val df2 = spark.table("t")

      // PrepareDeltaScan ensures both scans use the same (latest) snapshot
      val joined = df1.join(df2, df1("id") === df2("id"))
      checkAnswer(
        joined.select(df1("id"), df1("salary"), df2("id"), df2("salary")).orderBy(df1("id")),
        Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
    }
  }

  test("[3] scenario 2: join after ADD COLUMN uses latest data with pinned schema") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

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

      val df2 = spark.table("t")

      // df1 was analyzed with (id, salary), df2 with (id, salary, new_column)
      // Both use the latest version. df1 pins its original schema.
      checkAnswer(
        df1.orderBy("id"),
        Seq(Row(1, 100), Row(2, 200)))

      checkAnswer(
        df2.orderBy("id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))

      // The join should work since df1 projects only (id, salary)
      val joined = df1.join(df2, df1("id") === df2("id"))
      checkAnswer(
        joined.select(
          df1("id"), df1("salary"),
          df2("id"), df2("salary"), df2("new_column")).orderBy(df1("id")),
        Seq(Row(1, 100, 1, 100, null), Row(2, 200, 2, 200, -1)))
      }
    }
  }

  test("[3] scenario 3: join after DROP COLUMN throws (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")

      val df2 = spark.table("t")

      val e = intercept[DeltaAnalysisException] {
        df1.join(df2, df1("id") === df2("id")).collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[3] scenario 4: join after DROP and recreate table (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

      val df2 = spark.table("t")

      val e = intercept[DeltaAnalysisException] {
        df1.join(df2, df1("id") === df2("id")).collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[3] scenario 4: join after DROP and recreate table (no column mapping)") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      val df2 = spark.table("t")

      // Without column mapping, no column ID check. New table is empty.
      val joined = df1.join(df2, df1("id") === df2("id"))
      checkAnswer(joined, Seq.empty)
    }
  }

  test("[3] scenario 5: join after DROP/ADD column same name same type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")

      val df2 = spark.table("t")

      val e = intercept[DeltaAnalysisException] {
        df1.join(df2, df1("id") === df2("id")).collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[3] scenario 6: join after DROP/ADD column same name different type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")

      val df2 = spark.table("t")

      val e = intercept[DeltaAnalysisException] {
        df1.join(df2, df1("id") === df2("id")).collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[3] scenario 7: join after ALTER COLUMN TYPE INT to BIGINT (type widening)") {
    withTable("t") {
      sql(
        """CREATE TABLE t (id INT, salary INT) USING delta
          |TBLPROPERTIES (
          |  'delta.columnMapping.mode' = 'name',
          |  'delta.enableTypeWidening' = 'true'
          |)""".stripMargin)
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t ALTER COLUMN salary TYPE BIGINT")

      val df2 = spark.table("t")

      checkError(
        exception = intercept[DeltaAnalysisException] {
          df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS",
        parameters = Map("schemaDiff" -> "(?s).*", "legacyFlagMessage" -> ""),
        matchPVals = true)
    }
  }
}
