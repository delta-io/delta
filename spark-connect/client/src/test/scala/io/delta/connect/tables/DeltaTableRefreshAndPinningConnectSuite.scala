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
trait DeltaTableRefreshAndPinningConnectSuiteBase
  extends DeltaQueryTest with RemoteSparkSession {

  /**
   * Override in subclasses to use spark.newSession() for writes. In Connect, newSession()
   * creates a new client session that connects to the same server. The server-side DeltaLog
   * is shared (singleton cache per JVM), so writes from either session update the same
   * DeltaLog.currentSnapshot. This means newSession() is NOT a true external writer.
   * We parameterize with it to verify behavior is identical, documenting that the refresh
   * mechanism is driven by the shared DeltaLog, not by session-level state.
   */
  protected def useExternalSession: Boolean = false

  /** Returns a session for performing writes. */
  protected def writerSession: org.apache.spark.sql.SparkSession = {
    if (useExternalSession) spark.newSession() else spark
  }

  /** Execute SQL using the writer session. */
  protected def writerSql(sqlText: String): Unit = {
    writerSession.sql(sqlText)
  }

  protected def createSimpleTable(tableName: String): Unit = {
    spark.sql(s"CREATE TABLE $tableName (id INT, salary INT) USING delta")
  }

  protected def createColumnMappingTable(tableName: String): Unit = {
    spark.sql(
      s"""CREATE TABLE $tableName (id INT, salary INT) USING delta
         |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""".stripMargin)
  }

  protected def insertInitialData(tableName: String): Unit = {
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

      writerSql("INSERT INTO t VALUES (2, 200)")

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

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

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

      writerSql("ALTER TABLE t DROP COLUMN salary")

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

      writerSql("ALTER TABLE t ALTER COLUMN salary TYPE BIGINT")

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

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      // Same as classic without column mapping: resolves to new empty table
      checkAnswer(spark.sql("SELECT * FROM v"), Seq.empty)
    }
  }

  test("[1] connect scenario 4: temp view after DROP and recreate table " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

      // Column IDs changed, so reading the view should fail.
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
      assert(caught, "Expected exception for column mapping schema change")
    }
  }

  test("[1] connect scenario 5: temp view after DROP/ADD column same name same type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("id < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")

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

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")

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

      writerSql("INSERT INTO t VALUES (2, 200)")

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

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

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

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      checkAnswer(spark.sql("SELECT * FROM t"), Seq.empty)
    }
  }

  // ---------------------------------------------------------------------------
  // Section [3]: Incrementally constructed queries (Connect)
  // In Connect, Dataset is re-analyzed on each execution.
  // ---------------------------------------------------------------------------

  test("[3] connect scenario 1: join after write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("INSERT INTO t VALUES (2, 200)")

      val df2 = spark.table("t")

      // Follow the exact pattern from the design doc:
      // val joined = df1.join(df2, df1("id") === df2("id"))
      // In Connect, both DataFrames re-analyze to the same table, so
      // df1("id") === df2("id") becomes a trivially true self-join predicate.
      // The result has AMBIGUOUS_COLUMN_OR_FIELD on collect because both sides
      // produce identical column names.
      val joined = df1.join(df2, df1("id") === df2("id"))
      val caught = try { joined.collect(); false } catch { case _: Exception => true }
      assert(caught, "Expected AMBIGUOUS_COLUMN_OR_FIELD for Connect self-join")
    }
  }

  test("[3] connect scenario 2: join after ADD COLUMN") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      val df2 = spark.table("t")

      val joined = df1.join(df2, df1("id") === df2("id"))
      val caught = try { joined.collect(); false } catch { case _: Exception => true }
      assert(caught, "Expected AMBIGUOUS_COLUMN_OR_FIELD for Connect self-join")
    }
  }

  test("[3] connect scenario 3: join after DROP COLUMN (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")

      val df2 = spark.table("t")

      // After DROP COLUMN, table only has "id". The join condition
      // df1("id") === df2("id") is a self-join on a single-column table.
      val joined = df1.join(df2, df1("id") === df2("id"))
      val caught = try { joined.collect(); false } catch { case _: Exception => true }
      assert(caught, "Expected AMBIGUOUS_COLUMN_OR_FIELD for Connect self-join")
    }
  }

  test("[3] connect scenario 4: join after DROP and recreate (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

      val df2 = spark.table("t")

      val joined = df1.join(df2, df1("id") === df2("id"))
      val caught = try { joined.collect(); false } catch { case _: Exception => true }
      assert(caught, "Expected AMBIGUOUS_COLUMN_OR_FIELD for Connect self-join")
    }
  }

  test("[3] connect scenario 5: join after DROP/ADD column same name same type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")

      val df2 = spark.table("t")

      val joined = df1.join(df2, df1("id") === df2("id"))
      val caught = try { joined.collect(); false } catch { case _: Exception => true }
      assert(caught, "Expected AMBIGUOUS_COLUMN_OR_FIELD for Connect self-join")
    }
  }

  test("[3] connect scenario 6: join after DROP/ADD column same name different type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")

      val df2 = spark.table("t")

      val joined = df1.join(df2, df1("id") === df2("id"))
      val caught = try { joined.collect(); false } catch { case _: Exception => true }
      assert(caught, "Expected AMBIGUOUS_COLUMN_OR_FIELD for Connect self-join")
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

      writerSql("INSERT INTO t VALUES (2, 200)")

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

      writerSql("INSERT INTO t VALUES (2, 200)")

      // In Connect, cache behavior follows the same pattern as classic.
      // Delta aggressively refreshes, so writes are visible.
      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))

      spark.sql("UNCACHE TABLE t")
    }
  }

  test("[5] connect scenario 2: session write then external write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      // Session write invalidates the cache
      spark.sql("INSERT INTO t VALUES (2, 200)")

      // External writer adds (3, 300)
      writerSql("INSERT INTO t VALUES (3, 300)")

      // Both session and external writes are visible
      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200), Row(3, 300)))

      spark.sql("UNCACHE TABLE t")
    }
  }

  test("[5] connect scenario 3: schema change breaks cache") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

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

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      checkAnswer(spark.sql("SELECT * FROM t"), Seq.empty)

      spark.sql("UNCACHE TABLE IF EXISTS t")
    }
  }
}

/** Same-session writes (default). */
class DeltaTableRefreshAndPinningConnectSuite
  extends DeltaTableRefreshAndPinningConnectSuiteBase

/**
 * Writes go through spark.newSession(). In Connect, this creates a new client session
 * to the same server. The server shares a single DeltaLog instance cache, so writes
 * from either session update the same snapshot. Verifies behavior is identical to
 * same-session writes. See trait scaladoc for details.
 */
class DeltaTableRefreshAndPinningConnectExternalSessionSuite
  extends DeltaTableRefreshAndPinningConnectSuiteBase {
  override protected def useExternalSession: Boolean = true
}
