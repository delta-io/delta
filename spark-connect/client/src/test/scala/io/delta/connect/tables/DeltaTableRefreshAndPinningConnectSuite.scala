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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

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

  /**
   * Simulates a true external write by writing commit files directly to the
   * filesystem using Java NIO, bypassing the server's DeltaLog entirely.
   *
   * This is the Connect equivalent of the classic suite's writeExternalCommit.
   * Since the Connect client shares the same local filesystem as the server,
   * we can write parquet data files and Delta commit JSON files directly.
   * The server's DeltaLog.currentSnapshot is NOT updated.
   *
   * @param tablePath The filesystem path to the Delta table
   * @param data Tuples of (id, salary) to write
   */
  protected def writeExternalCommitViaFilesystem(
      tablePath: String,
      data: Seq[(Int, Int)]): Unit = {
    val deltaLogDir = new File(tablePath, "_delta_log")
    // Find current version by listing commit files
    val commitFiles = deltaLogDir.listFiles().filter(_.getName.endsWith(".json"))
    val currentVersion = commitFiles.map(f =>
      f.getName.stripSuffix(".json").toLong).max

    // Write a parquet file with the data
    val tempDir = Files.createTempDirectory("ext-write").toFile
    try {
      val session = spark
      import session.implicits._
      data.toDF("id", "salary").coalesce(1)
        .write.parquet(s"${tempDir.getAbsolutePath}/out")
      val parquetFile = new File(tempDir, "out").listFiles()
        .filter(_.getName.endsWith(".parquet")).head
      val targetName = s"ext-commit-v${currentVersion + 1}.snappy.parquet"
      Files.copy(
        parquetFile.toPath,
        Paths.get(tablePath).resolve(targetName))

      // Write commit JSON directly to _delta_log
      val addFileJson =
        s"""{"add":{"path":"$targetName","partitionValues":{},"size":${parquetFile.length()},""" +
        s""""modificationTime":${System.currentTimeMillis()},"dataChange":true}}"""
      val commitFile = new File(deltaLogDir,
        f"${currentVersion + 1}%020d.json")
      Files.write(commitFile.toPath, addFileJson.getBytes(StandardCharsets.UTF_8))
    } finally {
      // Clean up temp dir
      tempDir.listFiles().foreach(d =>
        if (d.isDirectory) d.listFiles().foreach(_.delete()))
      tempDir.listFiles().foreach(_.delete())
      tempDir.delete()
    }
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

  test("[3] connect scenario 4: join after DROP and recreate (no column mapping)") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      val df2 = spark.table("t")

      // In Connect, both DataFrames re-analyze. Without column mapping,
      // the new empty table is resolved by both.
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

  // ---------------------------------------------------------------------------
  // Section [5]: CACHE TABLE impact on reads (Connect)
  //
  // Note on external writes: In Connect, all writes go through the same
  // server-side DeltaLog (shared singleton cache). We cannot simulate true
  // external writes (bypassing DeltaLog) from the Connect client. These tests
  // document the same-JVM behavior where all writes update DeltaLog.currentSnapshot,
  // causing Delta's PrepareDeltaScan to discover changes and break the cache.
  //
  // For true external write behavior (cache pinning), see the classic suite's
  // scenarios 6b-6e which use writeExternalCommit via LogStore.
  // ---------------------------------------------------------------------------

  test("[5] connect scenario 1: CACHE TABLE with same-JVM writes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      // Same-JVM write updates DeltaLog.currentSnapshot, so Delta's
      // PrepareDeltaScan discovers the change and breaks the cache.
      // Doc says: (1,100) only for true external writes, but same-JVM
      // writes bypass the cache pinning mechanism.
      writerSql("INSERT INTO t VALUES (2, 200)")

      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))

      spark.sql("UNCACHE TABLE t")
    }
  }

  test("[5] connect scenario 2: session write then same-JVM external write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      // Session write invalidates cache (via SPARK-55631 refreshCache)
      spark.sql("INSERT INTO t VALUES (2, 200)")

      // Same-JVM "external" write also updates DeltaLog.currentSnapshot
      writerSql("INSERT INTO t VALUES (3, 300)")

      // Both writes visible because both go through same-JVM DeltaLog.
      // Doc says: (1,100),(2,200) only for true external writes.
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

      // Schema change via same-JVM SQL goes through AlterTableExec which
      // triggers refreshCache (SPARK-55631), invalidating the cache.
      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      // Doc says: schema changes break table state pinning.
      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))

      spark.sql("UNCACHE TABLE t")
    }
  }

  test("[5] connect scenario 4: session schema change with external write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      // Session schema change invalidates cache (SPARK-55631)
      spark.sql("ALTER TABLE t ADD COLUMN new_column INT")

      // Same-JVM "external" write
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      // Both visible because session ALTER TABLE broke the cache and
      // same-JVM write updated DeltaLog.currentSnapshot.
      // Doc says same for stalenessLimit=0.
      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))

      spark.sql("UNCACHE TABLE t")
    }
  }

  test("[5] connect scenario 5: drop and recreate table") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      // Doc says: empty table after drop and recreate.
      checkAnswer(spark.sql("SELECT * FROM t"), Seq.empty)

      spark.sql("UNCACHE TABLE IF EXISTS t")
    }
  }

  // ---------------------------------------------------------------------------
  // Section [5] continued: True external write simulation via filesystem
  // Uses writeExternalCommitViaFilesystem to write commit files directly to
  // disk, bypassing the server's DeltaLog. This is the Connect equivalent
  // of the classic suite's writeExternalCommit / scenarios 6b-6e.
  // ---------------------------------------------------------------------------

  test("[5] connect scenario 6b: CACHE TABLE pins data against external writes") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")
      spark.sql(s"CACHE TABLE cached_6b AS SELECT * FROM delta.`$path`")

      checkAnswer(spark.sql("SELECT * FROM cached_6b"), Row(1, 100))

      // True external write via filesystem -- server's DeltaLog not updated
      writeExternalCommitViaFilesystem(path, Seq((2, 200)))

      // Doc says: (1,100) only -- CACHE TABLE pins data against external writes.
      // The server's DeltaTableV2.lazy val snapshot is pinned and CacheManager
      // matches the cached plan.
      checkAnswer(spark.sql("SELECT * FROM cached_6b"), Row(1, 100))

      spark.sql("UNCACHE TABLE IF EXISTS cached_6b")

      // After uncaching, fresh query discovers external write (stalenessLimit=0).
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$path` ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[5] connect scenario 6c: session write invalidates, external not visible") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")
      spark.sql(s"CACHE TABLE cached_6c AS SELECT * FROM delta.`$path`")

      checkAnswer(spark.sql("SELECT * FROM cached_6c"), Row(1, 100))

      // Session write invalidates cache
      spark.sql(s"INSERT INTO delta.`$path` VALUES (2, 200)")

      // True external write via filesystem
      writeExternalCommitViaFilesystem(path, Seq((3, 300)))

      // Doc says: (1,100),(2,200) -- session write visible, external not.
      // After session write invalidates the cache, the next query re-analyzes.
      // The server's DeltaLog was updated by the session write but not the
      // external filesystem write.
      checkAnswer(
        spark.sql("SELECT * FROM cached_6c ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))

      spark.sql("UNCACHE TABLE IF EXISTS cached_6c")

      // After uncaching, fresh query discovers all data including external write.
      // The session INSERT updated server's DeltaLog, and UNCACHE triggers
      // a deltaLog.update() that discovers the external commit.
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$path` ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
    }
  }

  test("[5] connect scenario 6d: external schema change with CACHE") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")
      spark.sql(s"CACHE TABLE cached_6d AS SELECT * FROM delta.`$path`")

      checkAnswer(spark.sql("SELECT * FROM cached_6d"), Row(1, 100))

      // True external write via filesystem (data only, no schema change
      // since we can't easily write Metadata actions from the client)
      writeExternalCommitViaFilesystem(path, Seq((2, 200)))

      // External write not visible -- cache pins data
      checkAnswer(spark.sql("SELECT * FROM cached_6d"), Row(1, 100))

      spark.sql("UNCACHE TABLE IF EXISTS cached_6d")

      // After uncaching, fresh query discovers external write (stalenessLimit=0).
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$path` ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[5] connect scenario 6e: session schema change then external write") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")
      spark.sql(s"CACHE TABLE cached_6e AS SELECT * FROM delta.`$path`")

      checkAnswer(spark.sql("SELECT * FROM cached_6e"), Row(1, 100))

      // Session schema change invalidates cache (SPARK-55631)
      spark.sql(s"ALTER TABLE delta.`$path` ADD COLUMN new_column INT")

      // True external write via filesystem
      writeExternalCommitViaFilesystem(path, Seq((2, 200)))

      // Session schema change broke the cache. Next query re-analyzes.
      // The session's ALTER TABLE is visible (via server's DeltaLog),
      // but the external filesystem write may or may not be visible
      // depending on whether the server's DeltaLog lists new commits.
      // With stalenessLimit=0 (default), it discovers everything.
      checkAnswer(
        spark.sql(s"SELECT id, salary FROM delta.`$path` ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))

      spark.sql("UNCACHE TABLE IF EXISTS cached_6e")

      // After uncaching, fresh query discovers all data including external write.
      // The session ALTER TABLE updated server's DeltaLog, and UNCACHE triggers
      // a deltaLog.update() that discovers the external commit.
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$path` ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
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
