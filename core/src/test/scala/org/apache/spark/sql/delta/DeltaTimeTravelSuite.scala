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

import java.io.File
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}

import scala.concurrent.duration._
import scala.language.implicitConversions

import org.apache.spark.sql.delta.DeltaHistoryManager.BufferingLogDeletionIterator
import org.apache.spark.sql.delta.DeltaTestUtils.OptimisticTxnTestHelper
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.util.FileNames
import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.{functions, AnalysisException, QueryTest, Row}
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.util.ManualClock

class DeltaTimeTravelSuite extends QueryTest
  with SharedSparkSession  with SQLTestUtils {

  import testImplicits._

  private val timeFormatter = new SimpleDateFormat("yyyyMMddHHmmssSSS")

  private implicit def durationToLong(duration: FiniteDuration): Long = {
    duration.toMillis
  }

  private implicit def longToTimestamp(ts: Long): Timestamp = new Timestamp(ts)

  private def modifyCommitTimestamp(deltaLog: DeltaLog, version: Long, ts: Long): Unit = {
    val file = new File(FileNames.deltaFile(deltaLog.logPath, version).toUri)
    file.setLastModified(ts)
    val crc = new File(FileNames.checksumFile(deltaLog.logPath, version).toUri)
    if (crc.exists()) {
      crc.setLastModified(ts)
    }
  }

  private def modifyCheckpointTimestamp(deltaLog: DeltaLog, version: Long, ts: Long): Unit = {
    val file = new File(FileNames.checkpointFileSingular(deltaLog.logPath, version).toUri)
    file.setLastModified(ts)
  }

  /** Generate commits with the given timestamp in millis. */
  private def generateCommitsCheap(deltaLog: DeltaLog, commits: Long*): Unit = {
    var startVersion = deltaLog.snapshot.version + 1
    commits.foreach { ts =>
      val action = AddFile(startVersion.toString, Map.empty, 10L, startVersion, dataChange = true)
      deltaLog.startTransaction().commitManually(action)
      modifyCommitTimestamp(deltaLog, startVersion, ts)
      startVersion += 1
    }
  }

  /** Generate commits with the given timestamp in millis. */
  private def generateCommits(location: String, commits: Long*): Unit = {
    val deltaLog = DeltaLog.forTable(spark, location)
    var startVersion = deltaLog.snapshot.version + 1
    commits.foreach { ts =>
      val rangeStart = startVersion * 10
      val rangeEnd = rangeStart + 10
      spark.range(rangeStart, rangeEnd).write.format("delta").mode("append").save(location)
      val file = new File(FileNames.deltaFile(deltaLog.logPath, startVersion).toUri)
      file.setLastModified(ts)
      startVersion += 1
    }
  }

  private def identifierWithTimestamp(identifier: String, ts: Long): String = {
    s"$identifier@${timeFormatter.format(new Date(ts))}"
  }

  private def identifierWithVersion(identifier: String, v: Long): String = {
    s"$identifier@v$v"
  }

  private implicit def longToTimestampExpr(value: Long): String = {
    s"cast($value / 1000 as timestamp)"
  }

  private def getSparkFormattedTimestamps(values: Long*): Seq[String] = {
    // Simulates getting timestamps directly from Spark SQL
    values.map(new Timestamp(_)).toDF("ts")
      .select($"ts".cast("string")).as[String].collect()
      .map(i => s"$i")
  }

  private def historyTest(testName: String)(f: DeltaLog => Unit): Unit = {
    testQuietly(testName) {
      withTempDir { dir => f(DeltaLog.forTable(spark, dir)) }
    }
  }

  historyTest("getCommits should monotonize timestamps") { deltaLog =>
    val start = 1540415658000L
    // Make the commits out of order
    generateCommitsCheap(deltaLog,
      start,
      start - 5.seconds, // adjusts to start + 1 ms
      start + 1.milli,   // adjusts to start + 2 ms
      start + 2.millis,  // adjusts to start + 3 ms
      start - 2.seconds, // adjusts to start + 4 ms
      start + 10.seconds)

    val commits = DeltaHistoryManager.getCommits(deltaLog.store, deltaLog.logPath, 0, None)
    assert(commits.map(_.timestamp) === Seq(start,
      start + 1.millis, start + 2.millis, start + 3.millis, start + 4.millis, start + 10.seconds))
  }

  historyTest("describe history timestamps are adjusted according to file timestamp") { deltaLog =>
    // this is in '2018-10-24', so earlier than today. The recorded timestamps in commitInfo will
    // be much after this
    val start = 1540415658000L
    // Make the commits out of order
    generateCommitsCheap(deltaLog, start,
      start - 5.seconds, // adjusts to start + 1 ms
      start + 1.milli   // adjusts to start + 2 ms
    )

    val history = new DeltaHistoryManager(deltaLog)
    val commits = history.getHistory(None)
    assert(commits.map(_.timestamp.getTime) === Seq(start + 2.millis, start + 1.milli, start))
  }

  historyTest("should filter only delta files when computing earliest version") { deltaLog =>
    val start = 1540415658000L
    generateCommitsCheap(deltaLog, start, start + 10.seconds, start + 20.seconds)

    val history = new DeltaHistoryManager(deltaLog)
    assert(history.getActiveCommitAtTime(start + 15.seconds, false).version === 1)

    val commits2 = history.getHistory(Some(10))
    assert(commits2.last.version === Some(0))

    assert(new File(FileNames.deltaFile(deltaLog.logPath, 0L).toUri).delete())
    val e = intercept[AnalysisException] {
      history.getActiveCommitAtTime(start + 15.seconds, false).version
    }
    assert(e.getMessage.contains("reproducible"))
  }

  historyTest("resolving commits should return commit before timestamp") { deltaLog =>
    val start = 1540415658000L
    // Make a commit every 20 minutes
    val commits = Seq.tabulate(10)(i => start + (i * 20).minutes)
    generateCommitsCheap(deltaLog, commits: _*)
    // When maxKeys is 2, we will use the parallel search algorithm, when it is 1000, we will
    // use the linear search method
    Seq(1, 2, 1000).foreach { maxKeys =>
      val history = new DeltaHistoryManager(deltaLog, maxKeys)

      (0 until 10).foreach { i =>
        assert(history.getActiveCommitAtTime(start + (i * 20 + 10).minutes, true).version === i)
      }

      val e = intercept[AnalysisException] {
        // This is 20 minutes after the last commit
        history.getActiveCommitAtTime(start + 200.minutes, false)
      }
      assert(e.getMessage.contains("after the latest commit timestamp"))
      assert(history.getActiveCommitAtTime(start + 180.minutes, true).version === 9)

      val e2 = intercept[AnalysisException] {
        history.getActiveCommitAtTime(start - 10.minutes, true)
      }
      assert(e2.getMessage.contains("before the earliest version"))
    }
  }

  testQuietly("log cleanup should handle adjusted commit timestamps") {
    withTempDir { dir =>
      val start = 1540415658000L
      val date = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
      date.setTimeInMillis(start)
      // We unfortunately need to set a weird timestamp due to day cutoffs
      val time = DateUtils.truncate(date, Calendar.DAY_OF_MONTH).getTimeInMillis - 10.seconds
      val clock = new ManualClock(time)
      val deltaLog = DeltaLog.forTable(spark, new Path(dir.toURI), clock)
      generateCommitsCheap(deltaLog, time)

      // we need this checkpoint so that we can delete starting with the first version
      deltaLog.checkpoint()
      modifyCheckpointTimestamp(deltaLog, deltaLog.snapshot.version, time)
      generateCommitsCheap(deltaLog, Seq(5, 10, 7, 8, 14).map(time + _.seconds): _*)
      // We need this checkpoint so that we can delete up to the last version
      deltaLog.checkpoint()
      modifyCheckpointTimestamp(deltaLog, deltaLog.snapshot.version, time + 14.seconds)

      assert(deltaLog.history.getHistory(0, None).map(_.timestamp.getTime).reverse ===
        Seq(time, time + 5.seconds,
          time + 10.seconds, time + 10.seconds + 1, time + 10.seconds + 2, time + 14.seconds))

      // We shouldn't be able to delete anything in the version range 2-4
      clock.setTime(time + 10.seconds + deltaLog.deltaRetentionMillis)
      // Should delete commits at time and time + 5
      deltaLog.cleanUpExpiredLogs()
      assert(deltaLog.history.getHistory(0, None).map(_.timestamp.getTime).reverse ===
        Seq(time + 10.seconds, time + 10.seconds + 1, time + 10.seconds + 2, time + 14.seconds))

      clock.setTime(time + 10.seconds + 1 + deltaLog.deltaRetentionMillis)
      deltaLog.cleanUpExpiredLogs()
      assert(deltaLog.history.getHistory(0, None).map(_.timestamp.getTime).reverse ===
        Seq(time + 10.seconds, time + 10.seconds + 1, time + 10.seconds + 2, time + 14.seconds))

      clock.setTime(time + 10.seconds + 2 + deltaLog.deltaRetentionMillis)
      deltaLog.cleanUpExpiredLogs()
      assert(deltaLog.history.getHistory(0, None).map(_.timestamp.getTime).reverse ===
        Seq(time + 10.seconds, time + 10.seconds + 1, time + 10.seconds + 2, time + 14.seconds))

      clock.setTime(time + 1.day + deltaLog.deltaRetentionMillis)
      deltaLog.cleanUpExpiredLogs()
      assert(deltaLog.history.getHistory(0, None).map(_.timestamp.getTime).reverse ===
        Seq(time + 10.seconds, time + 10.seconds + 1, time + 10.seconds + 2, time + 14.seconds))
    }
  }

  /**
   * Creates FileStatus objects, where the name is the version of a commit, and the modification
   * timestamps come from the input.
   */
  private def createFileStatuses(modTimes: Long*): Iterator[FileStatus] = {
    modTimes.zipWithIndex.map { case (time, version) =>
      new FileStatus(10L, false, 1, 10L, time, new Path(version.toString))
    }.iterator
  }

  /**
   * Creates a log deletion iterator with a retention `maxTimestamp` and `maxVersion` (both
   * inclusive). The input iterator takes the original file timestamps, and the deleted output will
   * return the adjusted timestamps of files that would actually be consumed by the iterator.
   */
  private def testBufferingLogDeletionIterator(
      maxTimestamp: Long,
      maxVersion: Long)(inputTimestamps: Seq[Long], deleted: Seq[Long]): Unit = {
    val i = new BufferingLogDeletionIterator(
      createFileStatuses(inputTimestamps: _*), maxTimestamp, maxVersion, _.getName.toLong)
    deleted.foreach { ts =>
      assert(i.hasNext, s"Was supposed to delete $ts, but iterator returned hasNext: false")
      assert(i.next().getModificationTime === ts, "Returned files out of order!")
    }
    assert(!i.hasNext, "Iterator should be consumed")
  }

  test("BufferingLogDeletionIterator: iterator behavior") {
    val i1 = new BufferingLogDeletionIterator(Iterator.empty, 100, 100, _ => 1)
    intercept[NoSuchElementException](i1.next())
    assert(!i1.hasNext)

    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 100)(
      inputTimestamps = Seq(10),
      deleted = Seq(10)
    )

    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 100)(
      inputTimestamps = Seq(10, 15, 25),
      deleted = Seq(10, 15, 25)
    )
  }

  test("BufferingLogDeletionIterator: " +
    "early exit while handling adjusted timestamps due to timestamp") {
    // only should return 5 because 5 < 7
    testBufferingLogDeletionIterator(maxTimestamp = 7, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8, 12),
      deleted = Seq(5)
    )

    // Should only return 5, because 10 is used to adjust the following 8 to 11
    testBufferingLogDeletionIterator(maxTimestamp = 10, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8, 12),
      deleted = Seq(5)
    )

    // When it is 11, we can delete both 10 and 8
    testBufferingLogDeletionIterator(maxTimestamp = 11, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8, 12),
      deleted = Seq(5, 10, 11)
    )

    // When it is 12, we can return all
    testBufferingLogDeletionIterator(maxTimestamp = 12, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8, 12),
      deleted = Seq(5, 10, 11, 12)
    )

    // Should only return 5, because 10 is used to adjust the following 8 to 11
    testBufferingLogDeletionIterator(maxTimestamp = 10, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8),
      deleted = Seq(5)
    )

    // When it is 11, we can delete both 10 and 8
    testBufferingLogDeletionIterator(maxTimestamp = 11, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8),
      deleted = Seq(5, 10, 11)
    )
  }

  test("BufferingLogDeletionIterator: " +
    "early exit while handling adjusted timestamps due to version") {
    // only should return 5 because we can delete only up to version 0
    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 0)(
      inputTimestamps = Seq(5, 10, 8, 12),
      deleted = Seq(5)
    )

    // Should only return 5, because 10 is used to adjust the following 8 to 11
    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 1)(
      inputTimestamps = Seq(5, 10, 8, 12),
      deleted = Seq(5)
    )

    // When we can delete up to version 2, we can return up to version 2
    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 2)(
      inputTimestamps = Seq(5, 10, 8, 12),
      deleted = Seq(5, 10, 11)
    )

    // When it is version 3, we can return all
    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 3)(
      inputTimestamps = Seq(5, 10, 8, 12),
      deleted = Seq(5, 10, 11, 12)
    )

    // Should only return 5, because 10 is used to adjust the following 8 to 11
    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 1)(
      inputTimestamps = Seq(5, 10, 8),
      deleted = Seq(5)
    )

    // When we can delete up to version 2, we can return up to version 2
    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 2)(
      inputTimestamps = Seq(5, 10, 8),
      deleted = Seq(5, 10, 11)
    )
  }

  test("BufferingLogDeletionIterator: multiple adjusted timestamps") {
    Seq(9, 10, 11).foreach { retentionTimestamp =>
      // Files should be buffered but not deleted, because of the file 11, which has adjusted ts 12
      testBufferingLogDeletionIterator(maxTimestamp = retentionTimestamp, maxVersion = 100)(
        inputTimestamps = Seq(5, 10, 8, 11, 14),
        deleted = Seq(5)
      )
    }

    // Safe to delete everything before (including) file: 11 which has adjusted timestamp 12
    testBufferingLogDeletionIterator(maxTimestamp = 12, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8, 11, 14),
      deleted = Seq(5, 10, 11, 12)
    )

    Seq(0, 1, 2).foreach { retentionVersion =>
      testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = retentionVersion)(
        inputTimestamps = Seq(5, 10, 8, 11, 14),
        deleted = Seq(5)
      )
    }

    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 3)(
      inputTimestamps = Seq(5, 10, 8, 11, 14),
      deleted = Seq(5, 10, 11, 12)
    )

    // Test when the last element is adjusted with both timestamp and version
    Seq(9, 10, 11).foreach { retentionTimestamp =>
      testBufferingLogDeletionIterator(maxTimestamp = retentionTimestamp, maxVersion = 100)(
        inputTimestamps = Seq(5, 10, 8, 9),
        deleted = Seq(5)
      )
    }

    testBufferingLogDeletionIterator(maxTimestamp = 12, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8, 9),
      deleted = Seq(5, 10, 11, 12)
    )

    Seq(0, 1, 2).foreach { retentionVersion =>
      testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = retentionVersion)(
        inputTimestamps = Seq(5, 10, 8, 9),
        deleted = Seq(5)
      )
    }

    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 3)(
      inputTimestamps = Seq(5, 10, 8, 9),
      deleted = Seq(5, 10, 11, 12)
    )

    Seq(9, 10, 11).foreach { retentionTimestamp =>
      testBufferingLogDeletionIterator(maxTimestamp = retentionTimestamp, maxVersion = 100)(
        inputTimestamps = Seq(10, 8, 9),
        deleted = Nil
      )
    }

    // Test the first element causing cascading adjustments
    testBufferingLogDeletionIterator(maxTimestamp = 12, maxVersion = 100)(
      inputTimestamps = Seq(10, 8, 9),
      deleted = Seq(10, 11, 12)
    )

    Seq(0, 1).foreach { retentionVersion =>
      testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = retentionVersion)(
        inputTimestamps = Seq(10, 8, 9),
        deleted = Nil
      )
    }

    testBufferingLogDeletionIterator(maxTimestamp = 100, maxVersion = 2)(
      inputTimestamps = Seq(10, 8, 9),
      deleted = Seq(10, 11, 12)
    )

    // Test multiple batches of time adjustments
    testBufferingLogDeletionIterator(maxTimestamp = 12, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8, 9, 12, 15, 14, 14), // 5, 10, 11, 12, 13, 15, 16, 17
      deleted = Seq(5)
    )

    Seq(13, 14, 15, 16).foreach { retentionTimestamp =>
      testBufferingLogDeletionIterator(maxTimestamp = retentionTimestamp, maxVersion = 100)(
        inputTimestamps = Seq(5, 10, 8, 9, 12, 15, 14, 14), // 5, 10, 11, 12, 13, 15, 16, 17
        deleted = Seq(5, 10, 11, 12, 13)
      )
    }

    testBufferingLogDeletionIterator(maxTimestamp = 17, maxVersion = 100)(
      inputTimestamps = Seq(5, 10, 8, 9, 12, 15, 14, 14), // 5, 10, 11, 12, 13, 15, 16, 17
      deleted = Seq(5, 10, 11, 12, 13, 15, 16, 17)
    )
  }

  test("as of timestamp in between commits should use commit before timestamp") {
    withTempDir { dir =>
      val tblLoc = dir.getCanonicalPath
      val start = 1540415658000L
      generateCommits(tblLoc, start, start + 20.minutes, start + 40.minutes)

      val tablePathUri = identifierWithTimestamp(tblLoc, start + 10.minutes)

      val df1 = spark.read.format("delta").load(tablePathUri)
      checkAnswer(df1.groupBy().count(), Row(10L))

      // 2 minutes after start
      val df2 = spark.read.format("delta").option("timestampAsOf", "2018-10-24 14:16:18")
        .load(tblLoc)

      checkAnswer(df2.groupBy().count(), Row(10L))
    }
  }

  test("as of timestamp on exact timestamp") {
    withTempDir { dir =>
      val tblLoc = dir.getCanonicalPath
      val start = 1540415658000L
      generateCommits(tblLoc, start, start + 20.minutes)

      // Simulate getting the timestamp directly from Spark SQL
      val ts = getSparkFormattedTimestamps(start, start + 20.minutes)

      checkAnswer(
        spark.read.format("delta").option("timestampAsOf", ts.head).load(tblLoc).groupBy().count(),
        Row(10L)
      )

      checkAnswer(
        spark.read.format("delta").option("timestampAsOf", ts(1)).load(tblLoc).groupBy().count(),
        Row(20L)
      )

      checkAnswer(
        spark.read.format("delta").load(identifierWithTimestamp(tblLoc, start)).groupBy().count(),
        Row(10L)
      )

      checkAnswer(
        spark.read.format("delta").load(identifierWithTimestamp(tblLoc, start + 20.minutes))
          .groupBy().count(),
        Row(20L)
      )
    }
  }

  test("as of timestamp on invalid timestamp") {
    withTempDir { dir =>
      val tblLoc = dir.getCanonicalPath
      val start = 1540415658000L
      generateCommits(tblLoc, start, start + 20.minutes)

      val ex = intercept[AnalysisException] {
        spark.read.format("delta").option("timestampAsOf", "i am not a timestamp")
          .load(tblLoc).groupBy().count()
      }

      assert(ex.getMessage.contains(
        "The provided timestamp ('i am not a timestamp') cannot be converted to a valid timestamp"))
    }
  }

  test("as of exact timestamp after last commit should fail") {
    withTempDir { dir =>
      val tblLoc = dir.getCanonicalPath
      val start = 1540415658000L
      generateCommits(tblLoc, start)

      // Simulate getting the timestamp directly from Spark SQL
      val ts = getSparkFormattedTimestamps(start + 10.minutes)

      val e1 = intercept[AnalysisException] {
        spark.read.format("delta").option("timestampAsOf", ts.head).load(tblLoc).collect()
      }
      assert(e1.getMessage.contains("VERSION AS OF 0"))
      assert(e1.getMessage.contains("TIMESTAMP AS OF '2018-10-24 14:14:18'"))

      val e2 = intercept[AnalysisException] {
        spark.read.format("delta").load(identifierWithTimestamp(tblLoc, start + 10.minutes))
          .collect()
      }
      assert(e2.getMessage.contains("VERSION AS OF 0"))
      assert(e2.getMessage.contains("TIMESTAMP AS OF '2018-10-24 14:14:18'"))

      checkAnswer(
        spark.read.format("delta").option("timestampAsOf", "2018-10-24 14:14:18")
          .load(tblLoc).groupBy().count(),
        Row(10)
      )
    }
  }

  test("as of with versions") {
    withTempDir { dir =>
      val tblLoc = dir.getCanonicalPath
      val start = 1540415658000L
      generateCommits(tblLoc, start, start + 20.minutes, start + 40.minutes)

      val df = spark.read.format("delta").load(identifierWithVersion(tblLoc, 0))
      checkAnswer(df.groupBy().count(), Row(10L))

      checkAnswer(
        spark.read.format("delta").option("versionAsOf", "0").load(tblLoc).groupBy().count(),
        Row(10)
      )

      checkAnswer(
        spark.read.format("delta").option("versionAsOf", 1).load(tblLoc).groupBy().count(),
        Row(20)
      )

      val e1 = intercept[AnalysisException] {
        spark.read.format("delta").option("versionAsOf", 3).load(tblLoc).collect()
      }
      assert(e1.getMessage.contains("[0, 2]"))

      val deltaLog = DeltaLog.forTable(spark, tblLoc)
      new File(FileNames.deltaFile(deltaLog.logPath, 0).toUri).delete()
      val e2 = intercept[AnalysisException] {
        spark.read.format("delta").option("versionAsOf", 0).load(tblLoc).collect()
      }
      assert(e2.getMessage.contains("reproducible"))
    }
  }

  test("time travelling with adjusted timestamps") {
    withTempDir { dir =>
      val tblLoc = dir.getCanonicalPath
      val start = 1540415658000L
      generateCommits(tblLoc, start, start - 5.seconds, start + 3.minutes)

      val ts = getSparkFormattedTimestamps(
        start, start + 1.milli, start + 119.seconds, start - 3.seconds)

      checkAnswer(
        spark.read.option("timestampAsOf", ts.head).format("delta").load(tblLoc).groupBy().count(),
        Row(10L)
      )

      checkAnswer(
        spark.read.option("timestampAsOf", ts(1)).format("delta").load(tblLoc).groupBy().count(),
        Row(20L)
      )

      checkAnswer(
        spark.read.option("timestampAsOf", ts(2)).format("delta").load(tblLoc).groupBy().count(),
        Row(20L)
      )

      val e = intercept[AnalysisException] {
        spark.read.option("timestampAsOf", ts(3)).format("delta").load(tblLoc).collect()
      }
      assert(e.getMessage.contains("before the earliest version"))
    }
  }

  test("can't provide both version and timestamp in DataFrameReader") {
    val e = intercept[IllegalArgumentException] {
      spark.read.option("versionaSof", 1)
        .option("timestampAsOF", "fake").format("delta").load("/some/fake")
    }
    assert(e.getMessage.contains("either provide 'timestampAsOf' or 'versionAsOf'"))
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
        spark.read.format("delta").load(path + "@v0"),
        spark.range(10).toDF()
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

  test("scans on different versions of same table are executed correctly") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(5).selectExpr("id as key", "id * 10 as value").write.format("delta").save(path)

      spark.range(5, 10).selectExpr("id as key", "id * 10 as value")
        .write.format("delta").mode("append").save(path)

      val df = spark.read.format("delta").option("versionAsOf", "0").load(path).as("a").join(
        spark.read.format("delta").option("versionAsOf", "1").load(path).as("b"),
        functions.expr("a.key == b.key"),
        "fullOuter"
      ).where("a.key IS NULL")  // keys 5 to 9 should be null
      assert(df.count() == 5)
    }
  }

  test("time travel with schema changes - should instantiate old schema") {
    withTempDir { dir =>
      val tblLoc = dir.getCanonicalPath
      spark.range(10).write.format("delta").mode("append").save(tblLoc)
      spark.range(10, 20).withColumn("part", 'id)
        .write.format("delta").mode("append").option("mergeSchema", true).save(tblLoc)

      checkAnswer(
        spark.read.option("versionAsOf", 0).format("delta").load(tblLoc),
        spark.range(10).toDF())

      checkAnswer(
        spark.read.format("delta").load(identifierWithVersion(tblLoc, 0)),
        spark.range(10).toDF())
    }
  }

  test("time travel with partition changes - should instantiate old schema") {
    withTempDir { dir =>
      val tblLoc = dir.getCanonicalPath
      val v0 = spark.range(10).withColumn("part5", 'id % 5)

      v0.write.format("delta").partitionBy("part5").mode("append").save(tblLoc)
      spark.range(10, 20).withColumn("part2", 'id % 2)
        .write
        .format("delta")
        .partitionBy("part2")
        .mode("overwrite")
        .option("overwriteSchema", true)
        .save(tblLoc)

      checkAnswer(
        spark.read.option("versionAsOf", 0).format("delta").load(tblLoc),
        v0)

      checkAnswer(
        spark.read.format("delta").load(identifierWithVersion(tblLoc, 0)),
        v0)
    }
  }
}
