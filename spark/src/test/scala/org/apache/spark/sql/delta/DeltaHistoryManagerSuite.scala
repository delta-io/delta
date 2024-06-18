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

import java.io.{File, FileNotFoundException}
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import scala.concurrent.duration._
import scala.language.implicitConversions

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.DeltaHistoryManagerSuiteShims._
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.DeltaTestUtils.filterUsageRecords
import org.apache.spark.sql.delta.actions.{Action, CommitInfo}
import org.apache.spark.sql.delta.coordinatedcommits.CoordinatedCommitsBaseSuite
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.StatsUtils
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{DeltaCommitFileProvider, FileNames, JsonUtils}
import org.apache.hadoop.fs.Path
import org.scalatest.GivenWhenThen

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.{ManualClock, Utils}

/** A set of tests which we can open source after Spark 3.0 is released. */
trait DeltaTimeTravelTests extends QueryTest
    with SharedSparkSession
    with GivenWhenThen
    with DeltaSQLCommandTest
    with StatsUtils
    with CoordinatedCommitsBaseSuite {
  protected implicit def durationToLong(duration: FiniteDuration): Long = {
    duration.toMillis
  }

  protected implicit def longToTimestamp(ts: Long): Timestamp = new Timestamp(ts)

  protected val timeFormatter = new SimpleDateFormat("yyyyMMddHHmmssSSS")

  protected def modifyCommitTimestamp(deltaLog: DeltaLog, version: Long, ts: Long): Unit = {
    val filePath = DeltaCommitFileProvider(deltaLog.update()).deltaFile(version)
    val crc = new File(FileNames.checksumFile(deltaLog.logPath, version).toUri)
    if (isICTEnabledForNewTables) {
      InCommitTimestampTestUtils.overwriteICTInDeltaFile(deltaLog, filePath, Some(ts))
      if (crc.exists()) {
        InCommitTimestampTestUtils.overwriteICTInCrc(deltaLog, version, Some(ts))
      }
    } else {
      val file = new File(filePath.toUri)
      file.setLastModified(ts)
      if (crc.exists()) {
        crc.setLastModified(ts)
      }
    }
  }

  protected def versionAsOf(table: String, version: Long): String = {
    s"$table version as of $version"
  }

  protected def timestampAsOf(table: String, expr: String): String = {
    s"$table timestamp as of $expr"
  }

  protected def verifyLogging(
      tableVersion: Long,
      queriedVersion: Long,
      accessType: String,
      apiUsed: String)(f: => Unit): Unit = {
    // TODO: would be great to verify our logging metrics
  }

  protected def getTableLocation(table: String): String = {
    spark.sessionState.catalog.getTableMetadata(TableIdentifier(table)).location.toString
  }

  /** Generate commits with the given timestamp in millis. */
  protected def generateCommitsCheap(
      deltaLog: DeltaLog, commits: Long*): Unit = {
    var startVersion = deltaLog.snapshot.version + 1
    commits.foreach { ts =>
      val action =
        createTestAddFile(encodedPath = startVersion.toString, modificationTime = startVersion)
      deltaLog.startTransaction().commitManually(action)
      modifyCommitTimestamp(deltaLog, startVersion, ts)
      startVersion += 1
    }
  }

  protected def generateCommitsAtPath(table: String, path: String, commits: Long*): Unit = {
    generateCommitsBase(table, Some(path), commits: _*)
  }

  /** Generate commits with the given timestamp in millis. */
  protected def generateCommits(table: String, commits: Long*): Unit = {
    generateCommitsBase(table, None, commits: _*)
  }

  private def generateCommitsBase(table: String, path: Option[String], commits: Long*): Unit = {
    var commitList = commits.toSeq
    if (commitList.isEmpty) return
    if (!spark.sessionState.catalog.tableExists(TableIdentifier(table))) {
      if (path.isDefined) {
        spark.range(0, 10).write.format("delta")
          .mode("append")
          .option("path", path.get)
          .saveAsTable(table)
      } else {
        spark.range(0, 10).write.format("delta")
          .mode("append")
          .saveAsTable(table)
      }
      val deltaLog = DeltaLog.forTable(spark, new TableIdentifier(table))
      modifyCommitTimestamp(deltaLog, 0, commitList.head)
      commitList = commits.slice(1, commits.length) // we already wrote the first commit here
      var startVersion = deltaLog.snapshot.version + 1
      commitList.foreach { ts =>
        val rangeStart = startVersion * 10
        val rangeEnd = rangeStart + 10
        spark.range(rangeStart, rangeEnd).write.format("delta").mode("append").saveAsTable(table)
        modifyCommitTimestamp(deltaLog, startVersion, ts)
        startVersion += 1
      }
    }
  }

  /** Alternate for `withTables` as we leave some tables in an unusable state for clean up */
  protected def withTable(tableName: String, dir: String)(f: => Unit): Unit = {
    try f finally {
      try {
        Utils.deleteRecursively(new File(dir.toString))
      } catch {
        case _: Throwable =>
          Nil // do nothing, this can fail if the table was deleted by the test.
      } finally {
        try {
          sql(s"DROP TABLE IF EXISTS $tableName")
        } catch {
          case _: Throwable =>
            // There is one test that fails the drop table as well
            // we ignore this exception as that test uses a path based location.
            Nil
        }
      }
    }
  }

  protected implicit def longToTimestampExpr(value: Long): String = {
    s"cast($value / 1000 as timestamp)"
  }

  import testImplicits._

  test("time travel with partition changes and data skipping - should instantiate old schema") {
    withTempDir { dir =>
      val tblLoc = dir.getCanonicalPath
      val v0 = spark.range(10).withColumn("part5", 'id % 5)

      v0.write.format("delta").partitionBy("part5").mode("append").save(tblLoc)
      val deltaLog = DeltaLog.forTable(spark, tblLoc)

      val schemaString = spark.range(10, 20).withColumn("part2", 'id % 2).schema.json
        deltaLog.startTransaction().commit(
          Seq(deltaLog.snapshot.metadata.copy(
            schemaString = schemaString,
            partitionColumns = Seq("part2"))),
          DeltaOperations.ManualUpdate
        )
      checkAnswer(
        spark.read.option("versionAsOf", 0).format("delta").load(tblLoc).where("part5 = 1"),
        v0.where("part5 = 1"))
    }
  }

  test("can't provide both version and timestamp in DataFrameReader") {
    val e = intercept[IllegalArgumentException] {
      spark.read.option("versionaSof", 1)
          .option("timestampAsOF", "fake").format("delta").load("/some/fake")
    }
    assert(e.getMessage.contains("either provide 'timestampAsOf' or 'versionAsOf'"))
  }


  test("don't time travel a valid non-delta path with @ syntax") {
    val format = "json"
    withTempDir { dir =>
      val path = new File(dir, "base@v0").getCanonicalPath
      spark.range(10).write.format(format).mode("append").save(path)
      spark.range(10).write.format(format).mode("append").save(path)

      checkAnswer(
        spark.read.format(format).load(path),
        spark.range(10).union(spark.range(10)).toDF()
      )

      checkAnswer(
        spark.table(s"$format.`$path`"),
        spark.range(10).union(spark.range(10)).toDF()
      )

      intercept[AnalysisException] {
        spark.read.format(format).load(path + "@v0").count()
      }

      intercept[AnalysisException] {
        spark.table(s"$format.`$path@v0`").count()
      }
    }
  }

  ///////////////////////////
  // Time Travel SQL Tests //
  ///////////////////////////

  test("AS OF support does not impact non-delta tables") {
    withTable("t1") {
      spark.range(10).write.format("parquet").mode("append").saveAsTable("t1")
      spark.range(10, 20).write.format("parquet").mode("append").saveAsTable("t1")

      // We should still use the default, non-delta code paths for a non-delta table.
      // For parquet, that means to fail with QueryCompilationErrors::tableNotSupportTimeTravelError
      val e = intercept[Exception] {
        spark.sql("SELECT * FROM t1 VERSION AS OF 0")
      }.getMessage
      assert(e.contains("does not support time travel") ||
        e.contains("The feature is not supported: Time travel on the relation"))
    }
  }

  // scalastyle:off line.size.limit
  test("as of timestamp in between commits should use commit before timestamp") {
    // scalastyle:off line.size.limit
    val tblName = "delta_table"
    withTable(tblName) {
      val start = 1540415658000L
      generateCommits(tblName, start, start + 20.minutes, start + 40.minutes)

      verifyLogging(2L, 0L, "timestamp", "sql") {
        checkAnswer(
          sql(s"select count(*) from ${timestampAsOf(tblName, start + 10.minutes)}"),
          Row(10L)
        )
      }


      verifyLogging(2L, 0L, "timestamp", "sql") {
        checkAnswer(
          sql("select count(*) from " +
            s"${timestampAsOf(s"delta.`${getTableLocation(tblName)}`", start + 10.minutes)}"),
          Row(10L)
        )
      }


      checkAnswer(
        sql(s"select count(*) from ${timestampAsOf(tblName, start + 30.minutes)}"),
        Row(20L)
      )
    }
  }

  test("as of timestamp on exact timestamp") {
    val tblName = "delta_table"
    withTable(tblName) {
      val start = 1540415658000L
      generateCommits(tblName, start, start + 20.minutes)

      // Simulate getting the timestamp directly from Spark SQL
      val ts = Seq(new Timestamp(start), new Timestamp(start + 20.minutes)).toDF("ts")
        .select($"ts".cast("string")).as[String].collect()
        .map(i => s"'$i'")

      checkAnswer(
        sql(s"select count(*) from ${timestampAsOf(tblName, ts(0))}"),
        Row(10L)
      )
      checkAnswer(
        sql(s"select count(*) from ${timestampAsOf(tblName, start)}"),
        Row(10L)
      )


      checkAnswer(
        sql(s"select count(*) from ${timestampAsOf(tblName, start + 20.minutes)}"),
        Row(20L)
      )

      checkAnswer(
        sql(s"select count(*) from ${timestampAsOf(tblName, ts(1))}"),
        Row(20L)
      )
    }
  }

  test("as of with versions") {
    val tblName = s"delta_table"
    withTempDir { dir =>
      withTable(tblName, dir.toString) {
        val start = 1540415658000L
        generateCommitsAtPath(tblName, dir.toString, start, start + 20.minutes, start + 40.minutes)
        verifyLogging(2L, 0L, "version", "sql") {
          checkAnswer(
            sql(s"select count(*) from ${versionAsOf(tblName, 0)}"),
            Row(10L)
          )
        }

        verifyLogging(2L, 0L, "version", "dfReader") {
          checkAnswer(
            spark.read.format("delta").option("versionAsOf", "0")
              .load(getTableLocation(tblName)).groupBy().count(),
            Row(10)
          )
        }
        checkAnswer(
          sql(s"select count(*) from ${versionAsOf(tblName, 1)}"),
          Row(20L)
        )
        checkAnswer(
          spark.read.format("delta").option("versionAsOf", 1)
            .load(getTableLocation(tblName)).groupBy().count(),
          Row(20)
        )
        checkAnswer(
          sql(s"select count(*) from ${versionAsOf(tblName, 2)}"),
          Row(30L)
        )
        val e1 = intercept[AnalysisException] {
          sql(s"select count(*) from ${versionAsOf(tblName, 3)}").collect()
        }
        assert(e1.getMessage.contains("[0, 2]"))

        val deltaLog = DeltaLog.forTable(spark, getTableLocation(tblName))
        new File(FileNames.unsafeDeltaFile(deltaLog.logPath, 0).toUri).delete()
        // Delta Lake will create a DeltaTableV2 explicitly with time travel options in the catalog.
        // These options will be verified by DeltaHistoryManager, which will throw an
        // AnalysisException.
        val e2 = intercept[AnalysisException] {
          sql(s"select count(*) from ${versionAsOf(tblName, 0)}").collect()
        }
        if (coordinatedCommitsBackfillBatchSize.exists(_ > 2)) {
          assert(e2.getMessage.contains("No commits found at"))
        } else {
          assert(e2.getMessage.contains("No recreatable commits found at"))
        }
      }
    }
  }

  test("as of exact timestamp after last commit should fail") {
    val tblName = "delta_table"
    withTable(tblName) {
      val start = 1540415658000L
      generateCommits(tblName, start)

      // Simulate getting the timestamp directly from Spark SQL
      val ts = Seq(new Timestamp(start + 10.minutes)).toDF("ts")
        .select($"ts".cast("string")).as[String].collect()
        .map(i => s"'$i'")

      val e1 = intercept[AnalysisException] {
        sql(s"select count(*) from ${timestampAsOf(tblName, ts(0))}").collect()
      }
      assert(e1.getMessage.contains("VERSION AS OF 0"))
      assert(e1.getMessage.contains("TIMESTAMP AS OF '2018-10-24 14:14:18'"))

      val e2 = intercept[AnalysisException] {
        sql(s"select count(*) from ${timestampAsOf(tblName, start + 10.minutes)}").collect()
      }
      assert(e2.getMessage.contains("VERSION AS OF 0"))
      assert(e2.getMessage.contains("TIMESTAMP AS OF '2018-10-24 14:14:18'"))

      checkAnswer(
        sql(s"select count(*) from ${timestampAsOf(tblName, "'2018-10-24 14:14:18'")}"),
        Row(10)
      )

      verifyLogging(0L, 0L, "timestamp", "dfReader") {
        checkAnswer(
          spark.read.format("delta").option("timestampAsOf", "2018-10-24 14:14:18")
            .load(getTableLocation(tblName)).groupBy().count(),
          Row(10)
        )
      }
    }
  }

  test("time travelling with adjusted timestamps") {
    if (isICTEnabledForNewTables) {
      // ICT Timestamps are always monotonically increasing. Therefore,
      // this test is not needed when ICT is enabled.
      cancel("This test is not compatible with InCommitTimestamps.")
    }
    val tblName = "delta_table"
    withTable(tblName) {
      val start = 1540415658000L
      generateCommits(tblName, start, start - 5.seconds, start + 3.minutes)

      checkAnswer(
        sql(s"select count(*) from ${timestampAsOf(tblName, start)}"),
        Row(10L)
      )

      checkAnswer(
        sql(s"select count(*) from ${timestampAsOf(tblName, start + 1.milli)}"),
        Row(20L)
      )

      checkAnswer(
        sql(s"select count(*) from ${timestampAsOf(tblName, start + 119.seconds)}"),
        Row(20L)
      )

      val e = intercept[AnalysisException] {
        sql(s"select count(*) from ${timestampAsOf(tblName, start - 3.seconds)}").collect()
      }
      assert(e.getMessage.contains("before the earliest version"))
    }
  }

  test("Time travel with schema changes") {
    val tblName = "delta_table"
    withTable(tblName) {
      spark.range(10).write.format("delta").mode("append").saveAsTable(tblName)
      sql(s"ALTER TABLE $tblName ADD COLUMNS (part bigint)")
      spark.range(10, 20).withColumn("part", 'id)
        .write.format("delta").mode("append").saveAsTable(tblName)

      val tableLoc = getTableLocation(tblName)
      checkAnswer(
        sql(s"select * from ${versionAsOf(tblName, 0)}"),
        spark.range(10).toDF())

      checkAnswer(
        sql(s"select * from ${versionAsOf(s"delta.`$tableLoc`", 0)}"),
        spark.range(10).toDF())

      checkAnswer(
        spark.read.option("versionAsOf", 0).format("delta").load(tableLoc),
        spark.range(10).toDF())

    }
  }

  test("data skipping still works with time travel") {
    val tblName = "delta_table"
    withTable(tblName) {
      val start = 1540415658000L
      generateCommits(tblName, start, start + 20.minutes)

      def testScan(df: DataFrame): Unit = {
        val scan = getStats(df)
        assert(scan.scanned.bytesCompressed.get < scan.total.bytesCompressed.get)
      }

      testScan(sql(s"select * from ${versionAsOf(tblName, 0)} where id = 2"))

      testScan(spark.read.format("delta").option("versionAsOf", 0).load(getTableLocation(tblName))
        .where("id = 2"))

    }
  }

  test("fail to time travel a different relation than Delta") {
    withTempDir { output =>
      val dir = output.getCanonicalPath
      spark.range(10).write.mode("append").parquet(dir)
      spark.range(10).write.mode("append").parquet(dir)
      def assertFormatFailure(f: => Unit): Unit = {
        val e = intercept[AnalysisException] {
          f
        }
        assert(
          e.getMessage.contains("path-based tables") ||
            e.message.contains("[UNSUPPORTED_FEATURE.TIME_TRAVEL] The feature is not supported"),
          s"Returned instead:\n$e")
      }

      assertFormatFailure {
        sql(s"select * from ${versionAsOf(s"parquet.`$dir`", 0)}").collect()
      }

      assertFormatFailure {
        sql(s"select * from ${versionAsOf(s"parquet.`$dir`", 0)}").collect()
      }


      checkAnswer(
        spark.read.option("versionAsOf", 0).parquet(dir), // do not time travel other relations
        spark.range(10).union(spark.range(10)).toDF()
      )

      checkAnswer(
        // do not time travel other relations
        spark.read.option("timestampAsOf", "2018-10-12 01:01:01").parquet(dir),
        spark.range(10).union(spark.range(10)).toDF()
      )

      val tblName = "parq_table"
      withTable(tblName) {
        sql(s"create table $tblName using parquet as select * from parquet.`$dir`")
        val e = intercept[Exception] {
          sql(s"select * from ${versionAsOf(tblName, 0)}").collect()
        }
        val catalogName = CatalogManager.SESSION_CATALOG_NAME
        val catalogPrefix = catalogName + "."
        assert(e.getMessage.contains(
          s"Table ${catalogPrefix}default.parq_table does not support time travel") ||
          e.getMessage.contains(s"Time travel on the relation: `$catalogName`.`default`.`parq_table`"))
      }

      val viewName = "parq_view"
      assertFormatFailure {
        sql(s"create temp view $viewName as select * from ${versionAsOf(s"parquet.`$dir`", 0)}")
      }
    }
  }
}

abstract class DeltaHistoryManagerBase extends DeltaTimeTravelTests
  {
  test("cannot time travel target tables of insert/delete/update/merge") {
    val tblName = "delta_table"
    withTable(tblName) {
      val start = 1540415658000L
      generateCommits(tblName, start, start + 20.minutes)

      // These all actually fail parsing
      intercept[ParseException] {
        sql(s"insert into ${versionAsOf(tblName, 0)} values (11, 12, 13)")
      }

      intercept[ParseException] {
        sql(s"update ${versionAsOf(tblName, 0)} set id = id - 1 where id < 10")
      }

      intercept[ParseException] {
        sql(s"delete from ${versionAsOf(tblName, 0)} id < 10")
      }

      intercept[ParseException] {
        sql(s"""merge into ${versionAsOf(tblName, 0)} old
               |using $tblName new
               |on old.id = new.id
               |when not matched then insert *
           """.stripMargin)
      }
    }
  }

  test("vacuumed version") {
    quietly {
      val tblName = "delta_table"
      withTable(tblName) {
        val start = 1540415658000L
        generateCommits(tblName, start, start + 20.minutes)
        sql(s"optimize $tblName")

        withSQLConf(
          // Disable query rewrite or else the parquet files are not scanned.
          DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key -> "false",
          DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false") {
          sql(s"vacuum $tblName retain 0 hours")
          intercept[SparkException] {
            sql(s"select * from ${versionAsOf(tblName, 0)}").collect()
          }
          intercept[SparkException] {
            sql(s"select count(*) from ${versionAsOf(tblName, 1)}").collect()
          }
        }
      }
    }
  }


  test("as of with table API") {
    val tblName = "delta_table"
    withTable(tblName) {
      val start = 1540415658000L
      generateCommits(tblName, start, start + 20.minutes, start + 40.minutes)

      assert(spark.read.format("delta").option("versionAsOf", "0").table(tblName).count() == 10)
      assert(spark.read.format("delta").option("versionAsOf", 1).table(tblName).count() == 20)
      assert(spark.read.format("delta").option("versionAsOf", 2).table(tblName).count() == 30)
      val e1 = intercept[AnalysisException] {
        spark.read.format("delta").option("versionAsOf", 3).table(tblName).collect()
      }
      assert(e1.getMessage.contains("[0, 2]"))

      val e2 = intercept[MULTIPLE_TIME_TRAVEL_FORMATS_ERROR_TYPE] {
        spark.read.format("delta")
          .option("versionAsOf", 3)
          .option("timestampAsOf", "2020-10-22 23:20:11")
          .table(tblName).collect()
      }

      assert(e2.getMessage.contains(MULTIPLE_TIME_TRAVEL_FORMATS_ERROR_MSG))

    }
  }

  test("getHistory returns the correct set of commits") {
    val tblName = "delta_table"
    withTable(tblName) {
      val start = 1540415658000L
      generateCommits(tblName, start, start + 20.minutes, start + 40.minutes, start + 60.minutes)
      val deltaLog = DeltaLog.forTable(spark, getTableLocation(tblName))

      def testGetHistory(
          start: Long,
          endOpt: Option[Long],
          versions: Seq[Long],
          expectedLogUpdates: Int): Unit = {
        val usageRecords = Log4jUsageLogger.track {
          val history = deltaLog.history.getHistory(start, endOpt)
          assert(history.map(_.getVersion) == versions)
        }
        assert(filterUsageRecords(usageRecords, "deltaLog.update").size === expectedLogUpdates)
      }

      testGetHistory(start = 0, endOpt = Some(2), versions = Seq(2, 1, 0), expectedLogUpdates = 0)
      testGetHistory(start = 1, endOpt = Some(1), versions = Seq(1), expectedLogUpdates = 0)
      testGetHistory(start = 2, endOpt = None, versions = Seq(3, 2), expectedLogUpdates = 1)
      testGetHistory(start = 1, endOpt = Some(5), versions = Seq(3, 2, 1), expectedLogUpdates = 1)
      testGetHistory(start = 4, endOpt = None, versions = Seq.empty, expectedLogUpdates = 1)
      testGetHistory(start = 2, endOpt = Some(1), versions = Seq.empty, expectedLogUpdates = 0)
    }
  }
}

/** Uses V2 resolution code paths */
class DeltaHistoryManagerSuite extends DeltaHistoryManagerBase {
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST.key, "parquet,json")
  }
}

class DeltaHistoryManagerWithCoordinatedCommitsBatch1Suite extends DeltaHistoryManagerSuite {
  override def coordinatedCommitsBackfillBatchSize: Option[Int] = Some(1)
}

class DeltaHistoryManagerWithCoordinatedCommitsBatch2Suite extends DeltaHistoryManagerSuite {
  override def coordinatedCommitsBackfillBatchSize: Option[Int] = Some(2)
}

class DeltaHistoryManagerWithCoordinatedCommitsBatch100Suite extends DeltaHistoryManagerSuite {
  override def coordinatedCommitsBackfillBatchSize: Option[Int] = Some(100)
}
