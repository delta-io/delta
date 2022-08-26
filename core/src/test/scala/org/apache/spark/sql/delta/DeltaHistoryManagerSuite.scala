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
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import scala.concurrent.duration._
import scala.language.implicitConversions

import org.apache.spark.sql.delta.DeltaTestUtils.OptimisticTxnTestHelper
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames
import org.scalatest.GivenWhenThen

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

/** A set of tests which we can open source after Spark 3.0 is released. */
trait DeltaTimeTravelTests extends QueryTest
    with SharedSparkSession    with GivenWhenThen
    with DeltaSQLCommandTest {
  protected implicit def durationToLong(duration: FiniteDuration): Long = {
    duration.toMillis
  }

  protected implicit def longToTimestamp(ts: Long): Timestamp = new Timestamp(ts)

  protected val timeFormatter = new SimpleDateFormat("yyyyMMddHHmmssSSS")

  protected def modifyCommitTimestamp(deltaLog: DeltaLog, version: Long, ts: Long): Unit = {
    val file = new File(FileNames.deltaFile(deltaLog.logPath, version).toUri)
    file.setLastModified(ts)
    val crc = new File(FileNames.checksumFile(deltaLog.logPath, version).toUri)
    if (crc.exists()) {
      crc.setLastModified(ts)
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
      val action = AddFile(startVersion.toString, Map.empty, 10L, startVersion, dataChange = true)
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
      val file = new File(FileNames.deltaFile(deltaLog.logPath, 0).toUri)
      file.setLastModified(commitList.head)
      commitList = commits.slice(1, commits.length) // we already wrote the first commit here
      var startVersion = deltaLog.snapshot.version + 1
      commitList.foreach { ts =>
        val rangeStart = startVersion * 10
        val rangeEnd = rangeStart + 10
        spark.range(rangeStart, rangeEnd).write.format("delta").mode("append").saveAsTable(table)
        val file = new File(FileNames.deltaFile(deltaLog.logPath, startVersion).toUri)
        file.setLastModified(ts)
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
      val e = intercept[UnsupportedOperationException] {
        spark.sql("SELECT * FROM t1 VERSION AS OF 0")
      }.getMessage
      assert(e.contains("does not support time travel"))
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
        new File(FileNames.deltaFile(deltaLog.logPath, 0).toUri).delete()
        // Delta Lake will create a DeltaTableV2 explicitly with time travel options in the catalog.
        // These options will be verified by DeltaHistoryManager, which will throw an
        // AnalysisException.
        val e2 = intercept[AnalysisException] {
          sql(s"select count(*) from ${versionAsOf(tblName, 0)}").collect()
        }
        assert(e2.getMessage.contains("No reproducible commits found at"))
      }
    }
  }
}

abstract class DeltaHistoryManagerBase extends DeltaTimeTravelTests
  {

}

/** Uses V2 resolution code paths */
class DeltaHistoryManagerSuite extends DeltaHistoryManagerBase {
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST.key, "parquet,json")
  }
}
