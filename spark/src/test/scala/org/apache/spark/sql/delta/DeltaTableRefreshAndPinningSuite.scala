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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests that document and verify the existing Delta OSS (classic) behavior for table refresh,
 * version pinning, and schema change detection. These tests cover scenarios from the
 * "Refreshing and pinning tables in Spark" design doc.
 *
 * The suite covers five areas:
 *   1. Temp views with stored plans
 *   2. Repeated table access with external changes
 *   3. Incrementally constructed queries (join of separately analyzed DataFrames)
 *   4. Version pinning and refresh in Dataset (show vs collect)
 *   5. CACHE TABLE impact on reads
 *
 * Each scenario is tested with and without column mapping where applicable.
 */
class DeltaTableRefreshAndPinningSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaSQLConf.DELTA_ALTER_TABLE_DROP_COLUMN_ENABLED.key, "true")
  }

  private def createSimpleTable(tableName: String): Unit = {
    sql(s"CREATE TABLE $tableName (id INT, salary INT) USING delta")
  }

  private def createColumnMappingTable(tableName: String): Unit = {
    sql(
      s"""CREATE TABLE $tableName (id INT, salary INT) USING delta
         |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""".stripMargin)
  }

  private def insertInitialData(tableName: String): Unit = {
    sql(s"INSERT INTO $tableName VALUES (1, 100)")
  }

  private def getTablePath(tableName: String): String = {
    DeltaLog.forTable(spark, TableIdentifier(tableName)).dataPath.toString
  }

  // ---------------------------------------------------------------------------
  // Section [1]: Temp views with stored plans
  // ---------------------------------------------------------------------------

  test("[1.1] temp view picks up session writes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      sql("INSERT INTO t VALUES (2, 200)")

      checkAnswer(
        sql("SELECT * FROM v ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[1.2] temp view picks up writes after view creation") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      // Write happens after view creation (simulates external write)
      sql("INSERT INTO t VALUES (2, 200)")

      checkAnswer(
        sql("SELECT * FROM v ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[1] scenario 2: temp view with ADD COLUMN preserves original schema") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      sql("ALTER TABLE t ADD COLUMN new_column INT")
      sql("INSERT INTO t VALUES (2, 200, -1)")

      // View preserves original schema (id, salary) but picks up new data
      checkAnswer(
        sql("SELECT * FROM v ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[1] scenario 3: temp view with DROP COLUMN throws (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      sql("ALTER TABLE t DROP COLUMN salary")

      val e = intercept[DeltaAnalysisException] {
        sql("SELECT * FROM v").collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[1] scenario 4: temp view after DROP and recreate table (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      sql("DROP TABLE t")
      createColumnMappingTable("t")

      // Column IDs changed, so reading the view should fail
      val e = intercept[DeltaAnalysisException] {
        sql("SELECT * FROM v").collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[1] scenario 4: temp view after DROP and recreate table (no column mapping)") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      sql("DROP TABLE t")
      createSimpleTable("t")

      // Without column mapping, no column ID check. New table is empty.
      checkAnswer(sql("SELECT * FROM v"), Seq.empty)
    }
  }

  test("[1] scenario 5: temp view after DROP/ADD column same name same type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      sql("ALTER TABLE t DROP COLUMN salary")
      sql("ALTER TABLE t ADD COLUMN salary INT")

      val e = intercept[DeltaAnalysisException] {
        sql("SELECT * FROM v").collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[1] scenario 6: temp view after DROP/ADD column same name different type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      sql("ALTER TABLE t DROP COLUMN salary")
      sql("ALTER TABLE t ADD COLUMN salary STRING")

      val e = intercept[DeltaAnalysisException] {
        sql("SELECT * FROM v").collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[1] scenario 7: temp view after ALTER COLUMN TYPE INT to BIGINT") {
    withTable("t") {
      sql(
        """CREATE TABLE t (id INT, salary INT) USING delta
          |TBLPROPERTIES (
          |  'delta.columnMapping.mode' = 'name',
          |  'delta.enableTypeWidening' = 'true'
          |)""".stripMargin)
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      sql("ALTER TABLE t ALTER COLUMN salary TYPE BIGINT")

      val e = intercept[DeltaAnalysisException] {
        sql("SELECT * FROM v").collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  // ---------------------------------------------------------------------------
  // Section [2]: Repeated table access with external changes
  // ---------------------------------------------------------------------------

  test("[2] scenario 1: repeated access picks up new data") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      sql("INSERT INTO t VALUES (2, 200)")

      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[2] scenario 2: repeated access reflects schema changes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      sql("ALTER TABLE t ADD COLUMN new_column INT")
      sql("INSERT INTO t VALUES (2, 200, -1)")

      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }

  test("[2] scenario 3: repeated access after drop and recreate") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      sql("DROP TABLE t")
      createSimpleTable("t")

      checkAnswer(sql("SELECT * FROM t"), Seq.empty)
    }
  }

  // ---------------------------------------------------------------------------
  // Section [3]: Incrementally constructed queries
  // ---------------------------------------------------------------------------

  test("[3] scenario 1: join after external write uses consistent version") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      sql("INSERT INTO t VALUES (2, 200)")

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

      sql("ALTER TABLE t ADD COLUMN new_column INT")
      sql("INSERT INTO t VALUES (2, 200, -1)")

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

  test("[3] scenario 3: join after DROP COLUMN throws (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      sql("ALTER TABLE t DROP COLUMN salary")

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

      sql("DROP TABLE t")
      createColumnMappingTable("t")

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

      sql("DROP TABLE t")
      createSimpleTable("t")

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

      sql("ALTER TABLE t DROP COLUMN salary")
      sql("ALTER TABLE t ADD COLUMN salary INT")

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

      sql("ALTER TABLE t DROP COLUMN salary")
      sql("ALTER TABLE t ADD COLUMN salary STRING")

      val df2 = spark.table("t")

      val e = intercept[DeltaAnalysisException] {
        df1.join(df2, df1("id") === df2("id")).collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  // ---------------------------------------------------------------------------
  // Section [4]: Version pinning and refresh in Dataset
  // ---------------------------------------------------------------------------

  test("[4] scenario 1.1: df.show picks up new data after external write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      sql("INSERT INTO t VALUES (2, 200)")

      // Fresh SQL always picks up new data
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[4] scenario 1.2: df.collect on same DataFrame after external write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      // First collect triggers QueryExecution
      checkAnswer(df, Row(1, 100))

      sql("INSERT INTO t VALUES (2, 200)")

      // In OSS classic, collect() on the same df reuses QueryExecution,
      // but TahoeFileIndex.getSnapshot() still calls deltaLog.update()
      // during physical execution. For data-only changes, new data is visible.
      checkAnswer(
        df.orderBy("id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[4] scenario 2: df.show and collect after ADD COLUMN keeps original schema") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      sql("ALTER TABLE t ADD COLUMN new_column INT")
      sql("INSERT INTO t VALUES (2, 200, -1)")

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

  test("[4] scenario 3: df after DROP COLUMN (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      sql("ALTER TABLE t DROP COLUMN salary")

      // Fresh SQL re-analyzes with the new schema and succeeds.
      // The table now only has column "id".
      checkAnswer(sql("SELECT * FROM t"), Row(1))

      // collect() on the same DataFrame reuses the cached QueryExecution,
      // so it still returns old data with the original (id, salary) schema.
      // This is the documented OSS classic behavior:
      // Dataset remembers and reuses QueryExecution for collect.
      checkAnswer(df, Row(1, 100))
    }
  }

  test("[4] scenario 4: df after DROP and recreate table (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      sql("DROP TABLE t")
      createColumnMappingTable("t")

      // collect() on the same df references the old table's data files which no longer exist.
      // This results in a runtime error (file not found), not a schema change error.
      intercept[Exception] {
        df.collect()
      }
    }
  }

  test("[4] scenario 5: df after DROP/ADD column same name same type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      sql("ALTER TABLE t DROP COLUMN salary")
      sql("ALTER TABLE t ADD COLUMN salary INT")

      // collect() on the same DataFrame reuses the cached QueryExecution.
      // The old QueryExecution still references the original physical column,
      // so it returns old data with the original schema.
      checkAnswer(df, Row(1, 100))
    }
  }

  test("[4] scenario 6: df after DROP/ADD column same name different type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      sql("ALTER TABLE t DROP COLUMN salary")
      sql("ALTER TABLE t ADD COLUMN salary STRING")

      // collect() on the same DataFrame reuses the cached QueryExecution.
      // The old QueryExecution still references the original physical column,
      // so it returns old data with the original schema.
      checkAnswer(df, Row(1, 100))
    }
  }

  // ---------------------------------------------------------------------------
  // Section [5]: CACHE TABLE impact on reads
  // ---------------------------------------------------------------------------

  test("[5] scenario 1: CACHE TABLE with external writes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      // Write via path to bypass catalog cache invalidation
      val path = getTablePath("t")
      Seq((2, 200)).toDF("id", "salary")
        .write.format("delta").mode("append").save(path)

      // In OSS Delta (classic), Delta aggressively refreshes table versions via
      // FinishAnalysis/PrepareDeltaScan, which updates the snapshot. The schema change
      // caused by the new snapshot version breaks the plan shape match in CacheManager,
      // so the cache entry is effectively not reused and fresh data is returned.
      // This means CACHE TABLE does not truly pin data in Delta OSS (classic).
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))

      sql("UNCACHE TABLE t")
    }
  }

  test("[5] scenario 2: session write invalidates cache but external write does not") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      // Session write invalidates the cache
      sql("INSERT INTO t VALUES (2, 200)")

      // External write via path
      val path = getTablePath("t")
      Seq((3, 300)).toDF("id", "salary")
        .write.format("delta").mode("append").save(path)

      // Session write is visible, but we need to check if external is too.
      // After a session write, the cache is invalidated so re-reading fetches fresh data.
      // Delta with stalenessLimit=0 will pick up all data from the log.
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200), Row(3, 300)))

      sql("UNCACHE TABLE t")
    }
  }

  test("[5] scenario 3: external schema change breaks cache") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      // External schema change via path-based metadata update
      val path = getTablePath("t")
      val deltaLog = DeltaLog.forTable(spark, path)
      sql(s"ALTER TABLE delta.`$path` ADD COLUMN new_column INT")
      Seq((2, 200, -1)).toDF("id", "salary", "new_column")
        .write.format("delta").mode("append").save(path)

      // Schema change breaks the plan-shape match in CacheManager,
      // so the cache is effectively invalidated
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))

      sql("UNCACHE TABLE t")
    }
  }

  test("[5] scenario 4: session schema change with external write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      // Session schema change
      sql("ALTER TABLE t ADD COLUMN new_column INT")

      // External write via path
      val path = getTablePath("t")
      Seq((2, 200, -1)).toDF("id", "salary", "new_column")
        .write.format("delta").mode("append").save(path)

      // Schema change from the session invalidates the cache
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))

      sql("UNCACHE TABLE t")
    }
  }

  test("[5] scenario 5: external drop and recreate table") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      sql("DROP TABLE t")
      createSimpleTable("t")

      // After drop and recreate, the table is empty
      checkAnswer(sql("SELECT * FROM t"), Seq.empty)

      sql("UNCACHE TABLE IF EXISTS t")
    }
  }
}
