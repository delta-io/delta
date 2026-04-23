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

/**
 * Spark Connect variant of the table refresh and version pinning tests.
 *
 * Key behavioral differences from classic (local) mode:
 *   - In Connect, Dataset is re-analyzed on each execution, so collect() and show() behave
 *     the same: both always see the latest data and schema.
 *   - Temp views created from Dataset capture the plan, and in Connect temp views with stored
 *     plans behave the same as classic for column-mapping schema changes (they throw
 *     DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS).
 *
 * These tests document the "OSS Delta (connect)" column from the
 * "Refreshing and pinning tables in Spark" design doc.
 */
class DeltaTableRefreshAndPinningConnectSuite
  extends DeltaQueryTest with RemoteSparkSession {

  private def createSimpleTable(tableName: String): Unit = {
    spark.sql(s"CREATE TABLE $tableName (id INT, salary INT) USING delta")
  }

  private def createColumnMappingTable(tableName: String): Unit = {
    spark.sql(
      s"""CREATE TABLE $tableName (id INT, salary INT) USING delta
         |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""".stripMargin)
  }

  private def insertInitialData(tableName: String): Unit = {
    spark.sql(s"INSERT INTO $tableName VALUES (1, 100)")
  }

  // ---------------------------------------------------------------------------
  // Section [1]: Temp views with stored plans (Connect behavior)
  // Temp views created from Dataset capture the plan. In Connect, they behave
  // the same as classic for the scenarios tested here.
  // ---------------------------------------------------------------------------

  test("[1.1] connect: temp view picks up session writes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      spark.sql("INSERT INTO t VALUES (2, 200)")

      checkAnswer(
        spark.sql("SELECT * FROM v ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[1] connect scenario 2: temp view with ADD COLUMN preserves original schema") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      spark.sql("ALTER TABLE t ADD COLUMN new_column INT")
      spark.sql("INSERT INTO t VALUES (2, 200, -1)")

      // Same as classic: temp view preserves original schema (id, salary) but picks up new data
      checkAnswer(
        spark.sql("SELECT * FROM v ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[1] connect scenario 3: temp view after DROP COLUMN throws " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("id < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      spark.sql("ALTER TABLE t DROP COLUMN salary")

      // Same as classic: column mapping schema change throws
      val e = intercept[Exception] {
        spark.sql("SELECT * FROM v").collect()
      }
      assert(e.getMessage.contains("DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS") ||
        e.getMessage.contains("schema"))
    }
  }

  test("[1] connect scenario 7: temp view after ALTER COLUMN TYPE INT to BIGINT") {
    withTable("t") {
      spark.sql(
        """CREATE TABLE t (id INT, salary INT) USING delta
          |TBLPROPERTIES (
          |  'delta.columnMapping.mode' = 'name',
          |  'delta.enableTypeWidening' = 'true'
          |)""".stripMargin)
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      spark.sql("ALTER TABLE t ALTER COLUMN salary TYPE BIGINT")

      // Same as classic: type change is detected as incompatible schema change.
      // In Connect, Delta errors are wrapped in SparkException via gRPC.
      val caught = try {
        spark.sql("SELECT * FROM v").collect()
        false
      } catch {
        case e: Exception =>
          assert(e.getMessage.contains("DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS") ||
            e.getMessage.contains("schema"))
          true
      }
      assert(caught, "Expected exception for type widening schema change")
    }
  }

  test("[1] connect scenario 4: temp view after DROP and recreate table") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      spark.sql("DROP TABLE t")
      createSimpleTable("t")

      // Same as classic without column mapping: resolves to new empty table
      checkAnswer(spark.sql("SELECT * FROM v"), Seq.empty)
    }
  }

  test("[1] connect scenario 5: temp view after DROP/ADD column same name same type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("id < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      spark.sql("ALTER TABLE t DROP COLUMN salary")
      spark.sql("ALTER TABLE t ADD COLUMN salary INT")

      // Same as classic: column mapping schema change (column IDs changed) throws
      val e = intercept[Exception] {
        spark.sql("SELECT * FROM v").collect()
      }
      assert(e.getMessage.contains("DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS") ||
        e.getMessage.contains("schema"))
    }
  }

  test("[1] connect scenario 6: temp view after DROP/ADD column same name different type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("id < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      spark.sql("ALTER TABLE t DROP COLUMN salary")
      spark.sql("ALTER TABLE t ADD COLUMN salary STRING")

      // Same as classic: column mapping schema change throws
      val e = intercept[Exception] {
        spark.sql("SELECT * FROM v").collect()
      }
      assert(e.getMessage.contains("DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS") ||
        e.getMessage.contains("schema"))
    }
  }

  // ---------------------------------------------------------------------------
  // Section [2]: Repeated table access with external changes (Connect)
  // ---------------------------------------------------------------------------

  test("[2] connect scenario 1: repeated access picks up new data") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      spark.sql("INSERT INTO t VALUES (2, 200)")

      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[2] connect scenario 2: repeated access reflects schema changes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      spark.sql("ALTER TABLE t ADD COLUMN new_column INT")
      spark.sql("INSERT INTO t VALUES (2, 200, -1)")

      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }

  test("[2] connect scenario 3: repeated access after drop and recreate") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      spark.sql("DROP TABLE t")
      createSimpleTable("t")

      checkAnswer(spark.sql("SELECT * FROM t"), Seq.empty)
    }
  }

  // ---------------------------------------------------------------------------
  // Section [3]: Incrementally constructed queries (Connect)
  // In Connect, Dataset is re-analyzed on each execution.
  // ---------------------------------------------------------------------------

  test("[3] connect scenario 1: both DataFrames use consistent latest version") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      spark.sql("INSERT INTO t VALUES (2, 200)")

      val df2 = spark.table("t")

      // In Connect, both DataFrames are re-analyzed on execution,
      // both use the latest version. Verify each independently since
      // self-joins with duplicate column names hit AMBIGUOUS_COLUMN_OR_FIELD
      // in Connect's Arrow deserialization.
      checkAnswer(df1, Seq(Row(1, 100), Row(2, 200)))
      checkAnswer(df2, Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[3] connect scenario 2: both DataFrames use latest schema after ADD COLUMN") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      spark.sql("ALTER TABLE t ADD COLUMN new_column INT")
      spark.sql("INSERT INTO t VALUES (2, 200, -1)")

      val df2 = spark.table("t")

      // In Connect, both DataFrames are re-analyzed with the latest schema.
      checkAnswer(df1, Seq(Row(1, 100, null), Row(2, 200, -1)))
      checkAnswer(df2, Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }

  test("[3] connect scenario 3: both DataFrames re-analyze after DROP COLUMN " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      spark.sql("ALTER TABLE t DROP COLUMN salary")

      val df2 = spark.table("t")

      // In Connect, both DataFrames are re-analyzed with the new schema (only id).
      checkAnswer(df1, Row(1))
      checkAnswer(df2, Row(1))
    }
  }

  test("[3] connect scenario 4: both DataFrames re-analyze after DROP and recreate " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      spark.sql("DROP TABLE t")
      createColumnMappingTable("t")

      val df2 = spark.table("t")

      // In Connect, both DataFrames re-analyze to the new empty table
      checkAnswer(df1, Seq.empty)
      checkAnswer(df2, Seq.empty)
    }
  }

  test("[3] connect scenario 5: both DataFrames re-analyze after DROP/ADD column " +
      "same name same type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      spark.sql("ALTER TABLE t DROP COLUMN salary")
      spark.sql("ALTER TABLE t ADD COLUMN salary INT")

      val df2 = spark.table("t")

      // In Connect, both DataFrames re-analyze. The old salary data is gone.
      checkAnswer(df1, Row(1, null))
      checkAnswer(df2, Row(1, null))
    }
  }

  test("[3] connect scenario 6: both DataFrames re-analyze after DROP/ADD column " +
      "same name different type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      spark.sql("ALTER TABLE t DROP COLUMN salary")
      spark.sql("ALTER TABLE t ADD COLUMN salary STRING")

      val df2 = spark.table("t")

      // In Connect, both DataFrames re-analyze with new schema.
      checkAnswer(df1, Row(1, null))
      checkAnswer(df2, Row(1, null))
    }
  }

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

      spark.sql("INSERT INTO t VALUES (2, 200)")

      // In Connect, the df is re-analyzed on each execution
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

      spark.sql("ALTER TABLE t ADD COLUMN new_column INT")
      spark.sql("INSERT INTO t VALUES (2, 200, -1)")

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

      spark.sql("ALTER TABLE t DROP COLUMN salary")

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

      spark.sql("DROP TABLE t")
      createColumnMappingTable("t")

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

      spark.sql("ALTER TABLE t DROP COLUMN salary")
      spark.sql("ALTER TABLE t ADD COLUMN salary INT")

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

      spark.sql("ALTER TABLE t DROP COLUMN salary")
      spark.sql("ALTER TABLE t ADD COLUMN salary STRING")

      // In Connect, df re-analyzes with new schema (salary is now STRING)
      checkAnswer(df, Row(1, null))
    }
  }

  // ---------------------------------------------------------------------------
  // Section [5]: CACHE TABLE impact on reads (Connect)
  // ---------------------------------------------------------------------------

  test("[5] connect scenario 1: CACHE TABLE with writes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      spark.sql("INSERT INTO t VALUES (2, 200)")

      // In Connect, cache behavior follows the same pattern as classic.
      // Delta aggressively refreshes, so writes are visible.
      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))

      spark.sql("UNCACHE TABLE t")
    }
  }

  test("[5] connect scenario 2: session write invalidates cache") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      spark.sql("INSERT INTO t VALUES (2, 200)")

      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))

      spark.sql("UNCACHE TABLE t")
    }
  }

  test("[5] connect scenario 3: schema change breaks cache") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      spark.sql("ALTER TABLE t ADD COLUMN new_column INT")
      spark.sql("INSERT INTO t VALUES (2, 200, -1)")

      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))

      spark.sql("UNCACHE TABLE t")
    }
  }

  test("[5] connect scenario 4: drop and recreate table") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      spark.sql("DROP TABLE t")
      createSimpleTable("t")

      checkAnswer(spark.sql("SELECT * FROM t"), Seq.empty)

      spark.sql("UNCACHE TABLE IF EXISTS t")
    }
  }
}
