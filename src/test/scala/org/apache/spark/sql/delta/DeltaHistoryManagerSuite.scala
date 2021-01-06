/*
 * Copyright (2020) The Delta Lake Project Authors.
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
import java.time.{Instant, LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale}

import scala.concurrent.duration._
import scala.language.implicitConversions

import com.databricks.spark.util.MetricDefinitions
import org.apache.spark.sql.delta.DeltaTestUtils.OptimisticTxnTestHelper
import org.apache.spark.sql.delta.actions.{AddFile, Metadata, Protocol}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.scalatest.{GivenWhenThen, Tag}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.{Clock, ManualClock, Utils}

/** A set of tests which we can open source after Spark 3.0 is released. */
trait DeltaTimeTravelTests extends QueryTest with SharedSparkSession with GivenWhenThen
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

  /** Generate commits with the given timestamp in millis. */
  protected def generateCommits(table: String, commits: Long*): Unit = {
    val tablePath = spark.sessionState.catalog.defaultTablePath(TableIdentifier(table))
    val deltaLog = DeltaLog.forTable(spark, new Path(tablePath))
    var startVersion = deltaLog.snapshot.version + 1
    commits.foreach { ts =>
      val rangeStart = startVersion * 10
      val rangeEnd = rangeStart + 10
      spark.range(rangeStart, rangeEnd).write.format("delta").mode("append").saveAsTable(table)
      val file = new File(FileNames.deltaFile(deltaLog.logPath, startVersion).toUri)
      file.setLastModified(ts)
      startVersion += 1
    }
  }

  /** Alternate for `withTables` as we leave some tables in an unusable state for clean up */
  override protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    val tablePaths = tableNames.map(t =>
      spark.sessionState.catalog.defaultTablePath(TableIdentifier(t)))
    try f finally {
      tablePaths.foreach(p => Utils.deleteRecursively(new File(p)))
      tableNames.foreach(t => sql(s"DROP TABLE IF EXISTS $t"))
    }
  }

  protected implicit def longToTimestampExpr(value: Long): String = {
    s"cast($value / 1000 as timestamp)"
  }

  private def forTable(spark: SparkSession, dataPath: File): DeltaLog = {
    DeltaLog.forTable(spark, dataPath)
  }

  private def forTable(
      spark: SparkSession, dataPath: File, clock: Clock): DeltaLog = {
    DeltaLog.forTable(spark, dataPath)
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

  test("don't time travel a temp view with name @ syntax") {
    val dangerousName = "`delta_table@v0`"
    withTempView(dangerousName) {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        spark.range(10).write.format("delta").mode("append").save(path)
        spark.range(10).write.format("delta").mode("append").save(path)

        sql(s"create temp view $dangerousName as select * from delta.`$path`")

        checkAnswer(
          spark.table(dangerousName),
          spark.range(10).union(spark.range(10)).toDF()
        )
      }
    }
  }

  test("don't time travel a valid delta path with @ syntax") {
    withTempDir { dir =>
      val path = new File(dir, "base@v0").getCanonicalPath
      spark.range(10).write.format("delta").mode("append").save(path)
      spark.range(10).write.format("delta").mode("append").save(path)

      checkAnswer(
        spark.read.format("delta").load(path),
        spark.range(10).union(spark.range(10)).toDF()
      )

      checkAnswer(
        spark.read.parquet(path),
        spark.range(10).union(spark.range(10)).toDF()
      )

      checkAnswer(
        spark.table(s"delta.`$path`"),
        spark.range(10).union(spark.range(10)).toDF()
      )
    }
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
}

abstract class DeltaHistoryManagerBase extends DeltaTimeTravelTests {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  import testImplicits._

  test("time travelling a stream is not supported") {
    val tblName = "delta_table"
    val streamName = "delta_table_stream"
    withTable(tblName) {
      withTempView(streamName) {
        val start = 1540415658000L
        generateCommits(tblName, start, start + 20.minutes)
        spark.readStream.table(tblName).createTempView(streamName)

        val e2 = intercept[AnalysisException] {
          val sq = spark.readStream.option("versionAsOf", "0").table(tblName)
            .writeStream.format("memory").queryName("fake").start()
        }
        assert(e2.getMessage.toLowerCase(Locale.ROOT).contains("streams"))
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

      val e3 = intercept[IllegalArgumentException] {
        spark.read.format("delta")
          .option("versionAsOf", 3)
          .option("timestampAsOf", "2020-10-22 23:20:11")
          .table(tblName).collect()
      }
      assert(e3.getMessage.contains("either provide 'timestampAsOf' or 'versionAsOf'"))
    }
  }
}

/** Uses V2 resolution code paths */
class DeltaHistoryManagerSuite extends DeltaHistoryManagerBase {
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST.key, "parquet,json")
  }
}

