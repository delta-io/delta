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

package org.apache.spark.sql.delta.test

import java.io.{File, FileNotFoundException, IOException}
import java.sql.Timestamp

import scala.concurrent.duration._

import org.apache.spark.sql.delta.{DeltaErrors, DeltaFileNotFoundException, DeltaLog, DeltaTimeTravelTestHelpers, InCommitTimestampTestUtils, VersionNotFoundException}
import org.apache.spark.sql.delta.DeltaTestUtils.modifyCommitTimestamp
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames

import org.apache.spark.SparkConf
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.functions

/**
 * DSv2 parity tests for time travel queries exercising the STRICT-mode Kernel-backed connector
 * ([[io.delta.spark.internal.v2.catalog.DeltaV2Table]]).
 *
 * The helper infrastructure is reused from [[DeltaTimeTravelTestHelpers]].
 */
class DeltaHistoryManagerV2Suite extends DeltaTimeTravelTestHelpers {

  import testImplicits._

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(DeltaSQLConf.V2_ENABLE_MODE.key, "STRICT")

  /** Runs table creation / commits on the V1 connector. */
  private def inV1[T](f: => T): T =
    withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "NONE")(f)

  test("SQL VERSION AS OF returns the historical snapshot") {
    val tbl = "delta_tt_v2"
    withTable(tbl) {
      val start = System.currentTimeMillis() - 5.days.toMillis
      inV1 { generateCommits(tbl, start, start + 20.minutes) }

      checkAnswer(sql(s"SELECT * FROM $tbl FOR VERSION AS OF 0"), spark.range(0, 10).toDF())
      checkAnswer(sql(s"SELECT * FROM $tbl VERSION AS OF 1"), spark.range(0, 20).toDF())
    }
  }

  test("SQL VERSION AS OF past the latest version fails with DELTA_VERSION_NOT_FOUND") {
    val tbl = "delta_tt_v2"
    withTable(tbl) {
      val start = System.currentTimeMillis() - 5.days.toMillis
      inV1 { generateCommits(tbl, start, start + 20.minutes) }

      val ex = intercept[VersionNotFoundException] {
        sql(s"SELECT * FROM $tbl FOR VERSION AS OF 2").collect()
      }
      checkError(
        ex,
        "DELTA_VERSION_NOT_FOUND",
        sqlState = "22003",
        parameters = Map("userVersion" -> "2", "earliest" -> "0", "latest" -> "1"))
    }
  }

  test("SQL TIMESTAMP AS OF resolves to the commit active at that time") {
    val tbl = "delta_tt_v2"
    withTable(tbl) {
      val start = System.currentTimeMillis() - 5.days.toMillis
      inV1 { generateCommits(tbl, start, start + 20.minutes, start + 40.minutes) }

      // A timestamp between commits resolves to the commit active at that time.
      checkAnswer(sql(s"SELECT count(*) FROM ${timestampAsOf(tbl, start + 10.minutes)}"), Row(10L))
      checkAnswer(sql(s"SELECT count(*) FROM ${timestampAsOf(tbl, start + 30.minutes)}"), Row(20L))
      // Exact commit timestamps resolve to that commit.
      checkAnswer(sql(s"SELECT count(*) FROM ${timestampAsOf(tbl, start)}"), Row(10L))
      checkAnswer(sql(s"SELECT count(*) FROM ${timestampAsOf(tbl, start + 20.minutes)}"), Row(20L))
    }
  }

  test(
    "SQL TIMESTAMP AS OF after the latest commit fails with DELTA_TIMESTAMP_GREATER_THAN_COMMIT") {
    val tbl = "delta_tt_v2"
    withTable(tbl) {
      val start = 1540415658000L
      inV1 { generateCommits(tbl, start) }

      val ts = Seq(new Timestamp(start + 10.minutes)).toDF("ts")
        .select($"ts".cast("string")).as[String].collect().map(i => s"'$i'")

      val ex = intercept[DeltaErrors.TemporallyUnstableInputException] {
        sql(s"SELECT count(*) FROM ${timestampAsOf(tbl, ts(0))}").collect()
      }
      checkError(
        ex,
        "DELTA_TIMESTAMP_GREATER_THAN_COMMIT",
        sqlState = "42816",
        parameters = Map(
          "providedTimestamp" -> "2018-10-24 14:24:18.0",
          "lastCommitTimestamp" -> "2018-10-24 14:14:18.0",
          "maximumTimestamp" -> "2018-10-24 14:14:18"))
    }
  }

  test("SQL TIMESTAMP AS OF before the earliest recreatable commit fails") {
    val tbl = "delta_tt_v2_ts_recreatable"
    withTable(tbl) {
      val start = System.currentTimeMillis() - 5.days.toMillis
      inV1 {
        // v0, v1, v2. Checkpoint at v2, then delete v0's commit file.
        generateCommits(tbl, start, start + 20.minutes, start + 40.minutes)
        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tbl))
        deltaLog.checkpoint(deltaLog.update())
        new File(FileNames.unsafeDeltaFile(deltaLog.logPath, 0).toUri).delete()
      }

      val ts = Seq(new Timestamp(start)).toDF("ts")
        .select($"ts".cast("string")).as[String].head()
      val ex = intercept[DeltaErrors.TimestampEarlierThanCommitRetentionException] {
        sql(s"SELECT * FROM ${timestampAsOf(tbl, s"'$ts'")}").collect()
      }
      assert(ex.getCondition === "DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION")
      assert(ex.getSqlState === "42816")
    }
  }

  test("SQL TIMESTAMP AS OF reflects the historical schema before a column was added") {
    val tbl = "delta_tt_v2_ts_schema"
    val start = System.currentTimeMillis() - 5.days.toMillis
    withTable(tbl) {
      inV1 {
        sql(s"CREATE TABLE $tbl (id INT) USING delta")      // v0
        sql(s"INSERT INTO $tbl VALUES (1)")                 // v1
        sql(s"ALTER TABLE $tbl ADD COLUMNS (name STRING)")  // v2
        sql(s"INSERT INTO $tbl VALUES (2, 'b')")            // v3
        // Pin deterministic commit timestamps.
        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tbl))
        Seq(0L, 1L, 2L, 3L).foreach { v =>
          modifyCommitTimestamp(deltaLog, v, start + v * 20.minutes)
        }
      }

      val ts = Seq(new Timestamp(start + 30.minutes)).toDF("ts")
        .select($"ts".cast("string")).as[String].head()
      val atV1 = sql(s"SELECT * FROM ${timestampAsOf(tbl, s"'$ts'")}")
      assert(atV1.schema.fieldNames.toSeq === Seq("id"))
      checkAnswer(atV1, Row(1))
      assert(sql(s"SELECT * FROM $tbl").schema.fieldNames.toSeq === Seq("id", "name"))
    }
  }

  test("SQL TIMESTAMP AS OF accepts a timestamp expression") {
    withDatabase("ttdb") {
      sql("CREATE DATABASE ttdb")
      withTable("ttdb.tbl") {
        val ts = inV1 {
          spark.range(10).write.format("delta").saveAsTable("ttdb.tbl")
          sql("DESCRIBE HISTORY ttdb.tbl").select("timestamp").head().getTimestamp(0)
        }
        checkAnswer(
          sql(s"SELECT count(*) FROM ttdb.tbl TIMESTAMP AS OF " +
            s"coalesce(CAST ('$ts' AS TIMESTAMP), current_date())"),
          Row(10L))
      }
    }
  }

  test("SQL VERSION AS OF across multiple versions in one query (relation caching)") {
    val tbl = "delta_tt_v2_cache"
    withTable(tbl) {
      inV1 {
        sql(s"CREATE TABLE $tbl USING delta AS SELECT 1 AS c")
        sql(s"INSERT INTO $tbl SELECT 2 AS c")
      }
      checkAnswer(
        sql(s"SELECT * FROM $tbl VERSION AS OF '0' UNION ALL SELECT * FROM $tbl VERSION AS OF '1'"),
        Row(1) :: Row(1) :: Row(2) :: Nil)
    }
  }

  test("SQL VERSION AS OF reflects historical column defaults") {
    val tbl = "delta_tt_v2_defaults"
    withTable(tbl) {
      inV1 {
        sql(s"CREATE TABLE $tbl(id long) USING delta " +
          "TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'enabled')")
        sql(s"INSERT INTO $tbl SELECT default")
        sql(s"ALTER TABLE $tbl ALTER COLUMN id SET DEFAULT 42")
        sql(s"INSERT INTO $tbl SELECT default")
      }
      checkAnswer(sql(s"SELECT * FROM $tbl FOR VERSION AS OF 0"), Seq.empty[Row])
      checkAnswer(sql(s"SELECT * FROM $tbl FOR VERSION AS OF 1"), Seq(Row(null)))
      checkAnswer(sql(s"SELECT * FROM $tbl"), Seq(Row(null), Row(42L)))
    }
  }

  test("SQL VERSION AS OF below the earliest recreatable commit fails") {
    val tbl = "delta_tt_v2_recreatable"
    withTable(tbl) {
      val start = System.currentTimeMillis() - 5.days.toMillis
      inV1 {
        // v0, v1, v2. Checkpoint at v2, then delete v0's commit file
        generateCommits(tbl, start, start + 20.minutes, start + 40.minutes)
        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tbl))
        deltaLog.checkpoint(deltaLog.update())
        new File(FileNames.unsafeDeltaFile(deltaLog.logPath, 0).toUri).delete()
      }

      val ex = intercept[VersionNotFoundException] {
        sql(s"SELECT * FROM $tbl VERSION AS OF 1").collect()
      }
      checkError(
        ex,
        "DELTA_VERSION_NOT_FOUND",
        sqlState = "22003",
        parameters = Map("userVersion" -> "1", "earliest" -> "2", "latest" -> "2"))
    }
  }

  test("SQL VERSION AS OF a negative version is rejected by the parser") {
    val tbl = "delta_tt_v2_negative"
    withTable(tbl) {
      inV1 { generateCommits(tbl, System.currentTimeMillis() - 5.days.toMillis) }
      intercept[ParseException] {
        sql(s"SELECT * FROM $tbl VERSION AS OF -1").collect()
      }
    }
  }

  test("SQL VERSION AS OF on a non-existent table fails to resolve the table") {
    val ex = intercept[AnalysisException] {
      sql("SELECT * FROM delta_tt_v2_missing VERSION AS OF 0").collect()
    }
    assert(ex.getMessage.contains("delta_tt_v2_missing"))
  }

  test("SQL VERSION AS OF reflects the historical schema before a column was added") {
    val tbl = "delta_tt_v2_schema"
    withTable(tbl) {
      inV1 {
        sql(s"CREATE TABLE $tbl (id INT) USING delta")      // v0
        sql(s"INSERT INTO $tbl VALUES (1)")                 // v1
        sql(s"ALTER TABLE $tbl ADD COLUMNS (name STRING)")  // v2
        sql(s"INSERT INTO $tbl VALUES (2, 'b')")            // v3
      }
      assert(sql(s"SELECT * FROM $tbl VERSION AS OF 1").schema.fieldNames.toSeq === Seq("id"))
      checkAnswer(sql(s"SELECT * FROM $tbl VERSION AS OF 1"), Row(1))
      assert(sql(s"SELECT * FROM $tbl").schema.fieldNames.toSeq === Seq("id", "name"))
      checkAnswer(sql(s"SELECT * FROM $tbl"), Row(1, null) :: Row(2, "b") :: Nil)
    }
  }

  test("SQL VERSION AS OF after VACUUM removed the data files fails") {
    val tbl = "delta_tt_v2_vacuum"
    withTable(tbl) {
      val start = System.currentTimeMillis() - 5.days.toMillis
      inV1 {
        generateCommits(tbl, start, start + 20.minutes)  // v0, v1
        sql(s"OPTIMIZE $tbl")  // the v0/v1 data files become unreferenced
        withSQLConf("spark.databricks.delta.retentionDurationCheck.enabled" -> "false") {
          sql(s"VACUUM $tbl RETAIN 0 HOURS")
        }
      }
      val ex = intercept[Exception] {
        sql(s"SELECT * FROM $tbl VERSION AS OF 0").collect()
      }
      val causes = Iterator.iterate(ex: Throwable)(_.getCause).takeWhile(_ != null).toList
      assert(
        causes.exists { c =>
          c.isInstanceOf[FileNotFoundException] || c.isInstanceOf[IOException]
        },
        "expected a missing-data-file read failure, got:\n" +
          causes.map(c => s"  [${c.getClass.getName}] ${c.getMessage}").mkString("\n"))
    }
  }

  test("SQL catalog multi-version self-join reads two distinct pinned snapshots") {
    val tbl = "delta_tt_v2_cat_multiversion"
    withTable(tbl) {
      inV1 {
        // v0 empty, v1 keys 0-4, v2 keys 0-9.
        sql(s"CREATE TABLE $tbl (key LONG, value LONG) USING delta")
        sql(s"INSERT INTO $tbl SELECT id AS key, id * 10 AS value FROM range(0, 5)")
        sql(s"INSERT INTO $tbl SELECT id AS key, id * 10 AS value FROM range(5, 10)")
      }
      checkAnswer(
        sql(s"SELECT b.key FROM $tbl VERSION AS OF 1 a FULL OUTER JOIN $tbl VERSION AS OF 2 b " +
          "ON a.key = b.key WHERE a.key IS NULL"),
        (5 until 10).map(Row(_)))
    }
  }

  test("SQL catalog versionAsOf on a partitioned table prunes at the pinned version") {
    val tbl = "delta_tt_v2_cat_partition"
    withTable(tbl) {
      inV1 {
        // v0 empty, v1 ids 0-9, v2 ids 0-19.
        sql(s"CREATE TABLE $tbl (id LONG, part LONG) USING delta PARTITIONED BY (part)")
        sql(s"INSERT INTO $tbl SELECT id, id % 2 AS part FROM range(0, 10)")
        sql(s"INSERT INTO $tbl SELECT id, id % 2 AS part FROM range(10, 20)")
      }
      checkAnswer(
        sql(s"SELECT id FROM $tbl VERSION AS OF 1 WHERE part = 0"),
        (0 until 10 by 2).map(Row(_)))
    }
  }

  test("SQL catalog versionAsOf reflects the historical schema before a column was added") {
    val tbl = "delta_tt_v2_cat_schema_scan"
    withTable(tbl) {
      inV1 {
        sql(s"CREATE TABLE $tbl (id INT) USING delta")
        sql(s"INSERT INTO $tbl VALUES (1)")
        sql(s"ALTER TABLE $tbl ADD COLUMNS (name STRING)")
        sql(s"INSERT INTO $tbl VALUES (2, 'b')")
      }
      val atV1 = sql(s"SELECT * FROM $tbl VERSION AS OF 1")
      assert(atV1.schema.fieldNames.toSeq === Seq("id"))
      checkAnswer(atV1, Row(1))
    }
  }

  private def readVersionAsOf(table: String, version: Long): DataFrame =
    spark.read.option("versionAsOf", version).table(table)

  private def readTimestampAsOf(table: String, ts: String): DataFrame =
    spark.read.option("timestampAsOf", ts).table(table)

  /** Formats an epoch-millis value as a Spark-cast timestamp string. */
  private def timestampString(millis: Long): String =
    Seq(new Timestamp(millis)).toDF("ts").select($"ts".cast("string")).as[String].head()

  test("DataFrame versionAsOf returns the historical snapshot") {
    val tbl = "delta_tt_v2_df_ver"
    withTable(tbl) {
      val start = System.currentTimeMillis() - 5.days.toMillis
      inV1 { generateCommits(tbl, start, start + 20.minutes) }

      checkAnswer(readVersionAsOf(tbl, 0), spark.range(0, 10).toDF())
      checkAnswer(readVersionAsOf(tbl, 1), spark.range(0, 20).toDF())
    }
  }

  test("DataFrame versionAsOf past the latest version fails with DELTA_VERSION_NOT_FOUND") {
    val tbl = "delta_tt_v2_df_ver_oob"
    withTable(tbl) {
      val start = System.currentTimeMillis() - 5.days.toMillis
      inV1 { generateCommits(tbl, start, start + 20.minutes) }

      val ex = intercept[VersionNotFoundException] {
        readVersionAsOf(tbl, 2).collect()
      }
      checkError(
        ex,
        "DELTA_VERSION_NOT_FOUND",
        sqlState = "22003",
        parameters = Map("userVersion" -> "2", "earliest" -> "0", "latest" -> "1"))
    }
  }

  test("DataFrame versionAsOf below the earliest recreatable commit fails") {
    val tbl = "delta_tt_v2_df_ver_recreatable"
    withTable(tbl) {
      val start = System.currentTimeMillis() - 5.days.toMillis
      inV1 {
        // v0, v1, v2. Checkpoint at v2, then delete v0's commit file.
        generateCommits(tbl, start, start + 20.minutes, start + 40.minutes)
        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tbl))
        deltaLog.checkpoint(deltaLog.update())
        new File(FileNames.unsafeDeltaFile(deltaLog.logPath, 0).toUri).delete()
      }

      val ex = intercept[VersionNotFoundException] {
        readVersionAsOf(tbl, 1).collect()
      }
      checkError(
        ex,
        "DELTA_VERSION_NOT_FOUND",
        sqlState = "22003",
        parameters = Map("userVersion" -> "1", "earliest" -> "2", "latest" -> "2"))
    }
  }

  test("DataFrame versionAsOf reflects the historical schema before a column was added") {
    val tbl = "delta_tt_v2_df_ver_schema"
    withTable(tbl) {
      inV1 {
        sql(s"CREATE TABLE $tbl (id INT) USING delta")      // v0
        sql(s"INSERT INTO $tbl VALUES (1)")                 // v1
        sql(s"ALTER TABLE $tbl ADD COLUMNS (name STRING)")  // v2
        sql(s"INSERT INTO $tbl VALUES (2, 'b')")            // v3
      }
      val atV1 = readVersionAsOf(tbl, 1)
      assert(atV1.schema.fieldNames.toSeq === Seq("id"))
      checkAnswer(atV1, Row(1))
      assert(spark.read.table(tbl).schema.fieldNames.toSeq === Seq("id", "name"))
      checkAnswer(spark.read.table(tbl), Row(1, null) :: Row(2, "b") :: Nil)
    }
  }

  test("DataFrame timestampAsOf resolves to the commit active at that time") {
    val tbl = "delta_tt_v2_df_ts"
    withTable(tbl) {
      val start = System.currentTimeMillis() - 5.days.toMillis
      inV1 { generateCommits(tbl, start, start + 20.minutes, start + 40.minutes) }

      // A timestamp between commits resolves to the commit active at that time.
      checkAnswer(readTimestampAsOf(tbl, timestampString(start + 10.minutes)).groupBy().count(),
        Row(10L))
      checkAnswer(readTimestampAsOf(tbl, timestampString(start + 30.minutes)).groupBy().count(),
        Row(20L))
      // Exact commit timestamps resolve to that commit.
      checkAnswer(readTimestampAsOf(tbl, timestampString(start)).groupBy().count(), Row(10L))
      checkAnswer(readTimestampAsOf(tbl, timestampString(start + 20.minutes)).groupBy().count(),
        Row(20L))
    }
  }

  test("DataFrame timestampAsOf after the latest commit fails with " +
    "DELTA_TIMESTAMP_GREATER_THAN_COMMIT") {
    val tbl = "delta_tt_v2_df_ts_oob"
    withTable(tbl) {
      val start = 1540415658000L
      inV1 { generateCommits(tbl, start) }

      val ex = intercept[DeltaErrors.TemporallyUnstableInputException] {
        readTimestampAsOf(tbl, timestampString(start + 10.minutes)).collect()
      }
      checkError(
        ex,
        "DELTA_TIMESTAMP_GREATER_THAN_COMMIT",
        sqlState = "42816",
        parameters = Map(
          "providedTimestamp" -> "2018-10-24 14:24:18.0",
          "lastCommitTimestamp" -> "2018-10-24 14:14:18.0",
          "maximumTimestamp" -> "2018-10-24 14:14:18"))
    }
  }

  test("DataFrame timestampAsOf before the earliest recreatable commit fails") {
    val tbl = "delta_tt_v2_df_ts_recreatable"
    withTable(tbl) {
      val start = System.currentTimeMillis() - 5.days.toMillis
      inV1 {
        // v0, v1, v2. Checkpoint at v2, then delete v0's commit file.
        generateCommits(tbl, start, start + 20.minutes, start + 40.minutes)
        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tbl))
        deltaLog.checkpoint(deltaLog.update())
        new File(FileNames.unsafeDeltaFile(deltaLog.logPath, 0).toUri).delete()
      }

      val ex = intercept[DeltaErrors.TimestampEarlierThanCommitRetentionException] {
        readTimestampAsOf(tbl, timestampString(start)).collect()
      }
      assert(ex.getCondition === "DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION")
      assert(ex.getSqlState === "42816")
    }
  }

  test("DataFrame timestampAsOf reflects the historical schema before a column was added") {
    val tbl = "delta_tt_v2_df_ts_schema"
    val start = System.currentTimeMillis() - 5.days.toMillis
    withTable(tbl) {
      inV1 {
        sql(s"CREATE TABLE $tbl (id INT) USING delta")      // v0
        sql(s"INSERT INTO $tbl VALUES (1)")                 // v1
        sql(s"ALTER TABLE $tbl ADD COLUMNS (name STRING)")  // v2
        sql(s"INSERT INTO $tbl VALUES (2, 'b')")            // v3
        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tbl))
        Seq(0L, 1L, 2L, 3L).foreach { v =>
          modifyCommitTimestamp(deltaLog, v, start + v * 20.minutes)
        }
      }
      val atV1 = readTimestampAsOf(tbl, timestampString(start + 30.minutes))
      assert(atV1.schema.fieldNames.toSeq === Seq("id"))
      checkAnswer(atV1, Row(1))
      assert(spark.read.table(tbl).schema.fieldNames.toSeq === Seq("id", "name"))
    }
  }

  test("DataFrame versionAsOf and timestampAsOf together is rejected") {
    val tbl = "delta_tt_v2_df_both"
    withTable(tbl) {
      val start = System.currentTimeMillis() - 5.days.toMillis
      inV1 { generateCommits(tbl, start) }

      val ex = intercept[AnalysisException] {
        spark.read
          .option("versionAsOf", 0)
          .option("timestampAsOf", timestampString(start))
          .table(tbl)
          .collect()
      }
      checkError(ex, "INVALID_TIME_TRAVEL_SPEC", sqlState = Some("42K0E"))
    }
  }

  test("DataFrame timestampAsOf with an invalid timestamp fails") {
    val tbl = "delta_tt_v2_df_ts_invalid"
    withTable(tbl) {
      val start = System.currentTimeMillis() - 5.days.toMillis
      inV1 { generateCommits(tbl, start) }

      val ex = intercept[AnalysisException] {
        readTimestampAsOf(tbl, "i am not a timestamp").collect()
      }
      assert(ex.getMessage.contains("i am not a timestamp"))
    }
  }

  test("a streaming read reflects latest data, not a prior VERSION AS OF pin") {
    val tbl = "delta_tt_v2_stream"
    withTempDir { checkpoint =>
      withTable(tbl) {
        val start = System.currentTimeMillis() - 5.days.toMillis
        inV1 { generateCommits(tbl, start, start + 20.minutes) }
        checkAnswer(sql(s"SELECT count(*) FROM $tbl VERSION AS OF 0"), Row(10L))

        val q = spark.readStream.format("delta").table(tbl).writeStream
          .format("memory")
          .queryName("tt_v2_stream_out")
          .option("checkpointLocation", checkpoint.getCanonicalPath)
          .start()
        try {
          q.processAllAvailable()
          checkAnswer(sql("SELECT count(*) FROM tt_v2_stream_out"), Row(20L))
        } finally {
          q.stop()
        }
      }
    }
  }

  test("SQL path-based VERSION AS OF returns the historical snapshot") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_path"
      val path = dir.getCanonicalPath
      withTable(tbl) {
        val start = System.currentTimeMillis() - 5.days.toMillis
        inV1 { generateCommitsAtPath(tbl, path, start, start + 20.minutes) }

        checkAnswer(
          sql(s"SELECT * FROM delta.`$path` FOR VERSION AS OF 0"), spark.range(0, 10).toDF())
        checkAnswer(
          sql(s"SELECT * FROM delta.`$path` VERSION AS OF 1"), spark.range(0, 20).toDF())
      }
    }
  }

  test("SQL path-based VERSION AS OF past the latest version fails with DELTA_VERSION_NOT_FOUND") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_path_notfound"
      val path = dir.getCanonicalPath
      withTable(tbl) {
        val start = System.currentTimeMillis() - 5.days.toMillis
        inV1 { generateCommitsAtPath(tbl, path, start, start + 20.minutes) }

        val ex = intercept[VersionNotFoundException] {
          sql(s"SELECT * FROM delta.`$path` FOR VERSION AS OF 2").collect()
        }
        checkError(
          ex,
          "DELTA_VERSION_NOT_FOUND",
          sqlState = "22003",
          parameters = Map("userVersion" -> "2", "earliest" -> "0", "latest" -> "1"))
      }
    }
  }

  test("SQL path-based VERSION AS OF across multiple versions in one query (relation caching)") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_path_cache"
      val path = dir.getCanonicalPath
      withTable(tbl) {
        inV1 {
          sql(s"CREATE TABLE $tbl USING delta LOCATION '$path' AS SELECT 1 AS c")
          sql(s"INSERT INTO $tbl SELECT 2 AS c")
        }
        checkAnswer(
          sql(s"SELECT * FROM delta.`$path` VERSION AS OF '0' " +
            s"UNION ALL SELECT * FROM delta.`$path` VERSION AS OF '1'"),
          Row(1) :: Row(1) :: Row(2) :: Nil)
      }
    }
  }

  test("SQL path-based VERSION AS OF reflects historical column defaults") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_path_defaults"
      val path = dir.getCanonicalPath
      withTable(tbl) {
        inV1 {
          sql(s"CREATE TABLE $tbl(id long) USING delta LOCATION '$path' " +
            "TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'enabled')")
          sql(s"INSERT INTO $tbl SELECT default")
          sql(s"ALTER TABLE $tbl ALTER COLUMN id SET DEFAULT 42")
          sql(s"INSERT INTO $tbl SELECT default")
        }
        checkAnswer(sql(s"SELECT * FROM delta.`$path` FOR VERSION AS OF 0"), Seq.empty[Row])
        checkAnswer(sql(s"SELECT * FROM delta.`$path` FOR VERSION AS OF 1"), Seq(Row(null)))
        checkAnswer(sql(s"SELECT * FROM delta.`$path`"), Seq(Row(null), Row(42L)))
      }
    }
  }

  test("SQL path-based VERSION AS OF below the earliest recreatable commit fails") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_path_recreatable"
      val path = dir.getCanonicalPath
      withTable(tbl) {
        val start = System.currentTimeMillis() - 5.days.toMillis
        inV1 {
          // v0, v1, v2. Checkpoint at v2, then delete v0's commit file
          generateCommitsAtPath(tbl, path, start, start + 20.minutes, start + 40.minutes)
          val deltaLog = DeltaLog.forTable(spark, path)
          deltaLog.checkpoint(deltaLog.update())
          new File(FileNames.unsafeDeltaFile(deltaLog.logPath, 0).toUri).delete()
        }

        val ex = intercept[VersionNotFoundException] {
          sql(s"SELECT * FROM delta.`$path` VERSION AS OF 1").collect()
        }
        checkError(
          ex,
          "DELTA_VERSION_NOT_FOUND",
          sqlState = "22003",
          parameters = Map("userVersion" -> "1", "earliest" -> "2", "latest" -> "2"))
      }
    }
  }

  test("SQL path-based VERSION AS OF a negative version is rejected by the parser") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_path_negative"
      val path = dir.getCanonicalPath
      withTable(tbl) {
        inV1 { generateCommitsAtPath(tbl, path, System.currentTimeMillis() - 5.days.toMillis) }
        intercept[ParseException] {
          sql(s"SELECT * FROM delta.`$path` VERSION AS OF -1").collect()
        }
      }
    }
  }

  test("SQL path-based VERSION AS OF on a non-existent path fails with DELTA_PATH_DOES_NOT_EXIST") {
    withTempDir { dir =>
      val path = new File(dir, "does_not_exist").getCanonicalPath
      val ex = intercept[AnalysisException] {
        sql(s"SELECT * FROM delta.`$path` VERSION AS OF 0").collect()
      }
      assert(ex.getCondition === "DELTA_PATH_DOES_NOT_EXIST")
      assert(ex.getMessage.contains(path))
    }
  }

  test("SQL path-based VERSION AS OF reflects the historical schema before a column was added") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_path_schema"
      val path = dir.getCanonicalPath
      withTable(tbl) {
        inV1 {
          sql(s"CREATE TABLE $tbl (id INT) USING delta LOCATION '$path'") // v0
          sql(s"INSERT INTO $tbl VALUES (1)")                             // v1
          sql(s"ALTER TABLE $tbl ADD COLUMNS (name STRING)")              // v2
          sql(s"INSERT INTO $tbl VALUES (2, 'b')")                        // v3
        }
        val atV1 = sql(s"SELECT * FROM delta.`$path` VERSION AS OF 1")
        assert(atV1.schema.fieldNames.toSeq === Seq("id"))
        checkAnswer(atV1, Row(1))
        assert(sql(s"SELECT * FROM delta.`$path`").schema.fieldNames.toSeq === Seq("id", "name"))
        checkAnswer(sql(s"SELECT * FROM delta.`$path`"), Row(1, null) :: Row(2, "b") :: Nil)
      }
    }
  }

  test("SQL path-based VERSION AS OF after VACUUM removed the data files fails") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_path_vacuum"
      val path = dir.getCanonicalPath
      withTable(tbl) {
        val start = System.currentTimeMillis() - 5.days.toMillis
        inV1 {
          generateCommitsAtPath(tbl, path, start, start + 20.minutes)  // v0, v1
          sql(s"OPTIMIZE delta.`$path`")  // the v0/v1 data files become unreferenced
          withSQLConf("spark.databricks.delta.retentionDurationCheck.enabled" -> "false") {
            sql(s"VACUUM delta.`$path` RETAIN 0 HOURS")
          }
        }
        val ex = intercept[Exception] {
          sql(s"SELECT * FROM delta.`$path` VERSION AS OF 0").collect()
        }
        val causes = Iterator.iterate(ex: Throwable)(_.getCause).takeWhile(_ != null).toList
        assert(
          causes.exists { c =>
            c.isInstanceOf[FileNotFoundException] || c.isInstanceOf[IOException]
          },
          "expected a missing-data-file read failure, got:\n" +
            causes.map(c => s"  [${c.getClass.getName}] ${c.getMessage}").mkString("\n"))
      }
    }
  }

  test("SQL path-based TIMESTAMP AS OF resolves to the commit active at that time") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_path_ts"
      val path = dir.getCanonicalPath
      withTable(tbl) {
        val start = System.currentTimeMillis() - 5.days.toMillis
        inV1 { generateCommitsAtPath(tbl, path, start, start + 20.minutes, start + 40.minutes) }

        val q = s"delta.`$path`"
        // A string-literal timestamp is required.
        def countAsOf(millis: Long): DataFrame =
          sql(s"SELECT count(*) FROM ${timestampAsOf(q, s"'${timestampString(millis)}'")}")
        // A timestamp between commits resolves to the commit active at that time.
        checkAnswer(countAsOf(start + 10.minutes), Row(10L))
        checkAnswer(countAsOf(start + 30.minutes), Row(20L))
        // Exact commit timestamps resolve to that commit.
        checkAnswer(countAsOf(start), Row(10L))
        checkAnswer(countAsOf(start + 20.minutes), Row(20L))
      }
    }
  }

  test("SQL path-based TIMESTAMP AS OF after the latest commit fails " +
      "with DELTA_TIMESTAMP_GREATER_THAN_COMMIT") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_path_ts_err"
      val path = dir.getCanonicalPath
      withTable(tbl) {
        val start = 1540415658000L
        inV1 { generateCommitsAtPath(tbl, path, start) }

        val ts = Seq(new Timestamp(start + 10.minutes)).toDF("ts")
          .select($"ts".cast("string")).as[String].collect().map(i => s"'$i'")

        val ex = intercept[DeltaErrors.TemporallyUnstableInputException] {
          sql(s"SELECT count(*) FROM ${timestampAsOf(s"delta.`$path`", ts(0))}").collect()
        }
        checkError(
          ex,
          "DELTA_TIMESTAMP_GREATER_THAN_COMMIT",
          sqlState = "42816",
          parameters = Map(
            "providedTimestamp" -> "2018-10-24 14:24:18.0",
            "lastCommitTimestamp" -> "2018-10-24 14:14:18.0",
            "maximumTimestamp" -> "2018-10-24 14:14:18"))
      }
    }
  }

  test("SQL path-based TIMESTAMP AS OF before the earliest recreatable commit fails") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_path_ts_recreatable"
      val path = dir.getCanonicalPath
      withTable(tbl) {
        val start = System.currentTimeMillis() - 5.days.toMillis
        inV1 {
          // v0, v1, v2. Checkpoint at v2, then delete v0's commit file.
          generateCommitsAtPath(tbl, path, start, start + 20.minutes, start + 40.minutes)
          val deltaLog = DeltaLog.forTable(spark, path)
          deltaLog.checkpoint(deltaLog.update())
          new File(FileNames.unsafeDeltaFile(deltaLog.logPath, 0).toUri).delete()
        }

        val ts = Seq(new Timestamp(start)).toDF("ts")
          .select($"ts".cast("string")).as[String].head()
        val ex = intercept[DeltaErrors.TimestampEarlierThanCommitRetentionException] {
          sql(s"SELECT * FROM ${timestampAsOf(s"delta.`$path`", s"'$ts'")}").collect()
        }
        assert(ex.getCondition === "DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION")
        assert(ex.getSqlState === "42816")
      }
    }
  }

  test("SQL path-based TIMESTAMP AS OF reflects the historical schema before a column was added") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_path_ts_schema"
      val path = dir.getCanonicalPath
      val start = System.currentTimeMillis() - 5.days.toMillis
      withTable(tbl) {
        inV1 {
          sql(s"CREATE TABLE $tbl (id INT) USING delta LOCATION '$path'") // v0
          sql(s"INSERT INTO $tbl VALUES (1)")                             // v1
          sql(s"ALTER TABLE $tbl ADD COLUMNS (name STRING)")              // v2
          sql(s"INSERT INTO $tbl VALUES (2, 'b')")                        // v3
          // Pin deterministic commit timestamps.
          val deltaLog = DeltaLog.forTable(spark, path)
          Seq(0L, 1L, 2L, 3L).foreach { v =>
            modifyCommitTimestamp(deltaLog, v, start + v * 20.minutes)
          }
        }

        val ts = Seq(new Timestamp(start + 30.minutes)).toDF("ts")
          .select($"ts".cast("string")).as[String].head()
        val atV1 = sql(s"SELECT * FROM ${timestampAsOf(s"delta.`$path`", s"'$ts'")}")
        assert(atV1.schema.fieldNames.toSeq === Seq("id"))
        checkAnswer(atV1, Row(1))
        assert(sql(s"SELECT * FROM delta.`$path`").schema.fieldNames.toSeq === Seq("id", "name"))
      }
    }
  }

  test("SQL path-based TIMESTAMP AS OF rejects a non-literal timestamp expression") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_path_ts_expr"
      val path = dir.getCanonicalPath
      withTable(tbl) {
        val ts = inV1 {
          spark.range(10).write.format("delta").option("path", path).saveAsTable(tbl)
          sql(s"DESCRIBE HISTORY delta.`$path`").select("timestamp").head().getTimestamp(0)
        }
        val ex = intercept[AnalysisException] {
          sql(s"SELECT count(*) FROM delta.`$path` TIMESTAMP AS OF " +
            s"coalesce(CAST ('$ts' AS TIMESTAMP), current_date())").collect()
        }
        assert(ex.getCondition === "UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY")
      }
    }
  }

  test("SQL path-based time travel resolves through the native V2 connector (BatchScan)") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_path_planshape"
      val path = dir.getCanonicalPath
      withTable(tbl) {
        val start = System.currentTimeMillis() - 5.days.toMillis
        inV1 { generateCommitsAtPath(tbl, path, start, start + 20.minutes) }

        val plan = sql(s"SELECT * FROM delta.`$path` VERSION AS OF 0").queryExecution.executedPlan
        assert(
          plan.collectFirst { case b: BatchScanExec => b }.isDefined,
          "expected a native DSv2 BatchScan (not a V1 FileSourceScanExec), got:\n" + plan)
        assert(
          plan.collectFirst { case f: FileSourceScanExec => f }.isEmpty,
          "unexpected V1 FileSourceScanExec in the plan:\n" + plan)
      }
    }
  }

  test("SQL path versionAsOf on deletion-vector table ignores DVs added later") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_path_dv"
      val path = dir.getCanonicalPath
      withTable(tbl) {
        inV1 {
          sql(s"CREATE TABLE $tbl (id LONG) USING delta LOCATION '$path' " +
            "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
          sql(s"INSERT INTO $tbl SELECT id FROM range(0, 10)")
          sql(s"DELETE FROM delta.`$path` WHERE id < 5")
        }
        checkAnswer(sql(s"SELECT * FROM delta.`$path` VERSION AS OF 1"), (0 until 10).map(Row(_)))
        checkAnswer(sql(s"SELECT * FROM delta.`$path` VERSION AS OF 2"), (5 until 10).map(Row(_)))
      }
    }
  }

  test("SQL path versionAsOf at an exact checkpoint version reads the pinned version") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_path_checkpoint"
      val path = dir.getCanonicalPath
      withTable(tbl) {
        val start = System.currentTimeMillis() - 5.days.toMillis
        inV1 {
          generateCommitsAtPath(tbl, path, start, start + 20.minutes, start + 40.minutes)
          val deltaLog = DeltaLog.forTable(spark, path)
          deltaLog.checkpoint(deltaLog.update())
        }
        checkAnswer(sql(s"SELECT * FROM delta.`$path` VERSION AS OF 2"), (0 until 30).map(Row(_)))
      }
    }
  }

  test("SQL path @-syntax VERSION returns the historical snapshot") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_at_ver"
      val path = dir.getCanonicalPath
      withTable(tbl) {
        val start = System.currentTimeMillis() - 5.days.toMillis
        inV1 { generateCommitsAtPath(tbl, path, start, start + 20.minutes) }

        checkAnswer(
          sql(s"SELECT * FROM delta.`${identifierWithVersion(path, 0)}`"),
          spark.range(0, 10).toDF())
        checkAnswer(
          sql(s"SELECT * FROM delta.`${identifierWithVersion(path, 1)}`"),
          spark.range(0, 20).toDF())
      }
    }
  }

  test("SQL path @-syntax VERSION past the latest version fails with DELTA_VERSION_NOT_FOUND") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_at_ver_notfound"
      val path = dir.getCanonicalPath
      withTable(tbl) {
        val start = System.currentTimeMillis() - 5.days.toMillis
        inV1 { generateCommitsAtPath(tbl, path, start, start + 20.minutes) }

        val ex = intercept[VersionNotFoundException] {
          sql(s"SELECT * FROM delta.`${identifierWithVersion(path, 2)}`").collect()
        }
        checkError(
          ex,
          "DELTA_VERSION_NOT_FOUND",
          sqlState = "22003",
          parameters = Map("userVersion" -> "2", "earliest" -> "0", "latest" -> "1"))
      }
    }
  }

  test("SQL path @-syntax TIMESTAMP resolves to the commit active at that time") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_at_ts"
      val path = dir.getCanonicalPath
      withTable(tbl) {
        val start = System.currentTimeMillis() - 5.days.toMillis
        inV1 { generateCommitsAtPath(tbl, path, start, start + 20.minutes, start + 40.minutes) }

        // A timestamp between commits resolves to the commit active at that time.
        checkAnswer(
          sql(s"SELECT count(*) FROM delta.`${identifierWithTimestamp(path, start + 10.minutes)}`"),
          Row(10L))
        checkAnswer(
          sql(s"SELECT count(*) FROM delta.`${identifierWithTimestamp(path, start + 30.minutes)}`"),
          Row(20L))
      }
    }
  }

  test("SQL path @-syntax does not time travel a valid path whose name contains @vN") {
    withTempDir { dir =>
      // A real directory literally named `base@v0` which contains the table.
      val path = new File(dir, "base@v0").getCanonicalPath
      val tbl = "delta_tt_v2_at_literal"
      withTable(tbl) {
        inV1 { generateCommitsAtPath(tbl, path, System.currentTimeMillis() - 5.days.toMillis) }
        inV1 { spark.range(10, 20).write.format("delta").mode("append").save(path) }

        checkAnswer(
          sql(s"SELECT * FROM delta.`$path`"), spark.range(0, 20).toDF())
      }
    }
  }

  test("SQL path @-syntax resolves through the native V2 connector (BatchScan)") {
    withTempDir { dir =>
      val tbl = "delta_tt_v2_at_planshape"
      val path = dir.getCanonicalPath
      withTable(tbl) {
        val start = System.currentTimeMillis() - 5.days.toMillis
        inV1 { generateCommitsAtPath(tbl, path, start, start + 20.minutes) }

        val plan = sql(s"SELECT * FROM delta.`${identifierWithVersion(path, 0)}`")
          .queryExecution.executedPlan
        assert(
          plan.collectFirst { case b: BatchScanExec => b }.isDefined,
          "expected a native DSv2 BatchScan (not a V1 FileSourceScanExec), got:\n" + plan)
        assert(
          plan.collectFirst { case f: FileSourceScanExec => f }.isEmpty,
          "unexpected V1 FileSourceScanExec in the plan:\n" + plan)
      }
    }
  }

}
