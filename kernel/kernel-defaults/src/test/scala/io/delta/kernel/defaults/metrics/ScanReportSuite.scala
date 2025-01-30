/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.metrics

import java.util.Collections

import io.delta.kernel._
import io.delta.kernel.data.FilteredColumnarBatch
import io.delta.kernel.engine._
import io.delta.kernel.expressions.{Column, Literal, Predicate}
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.metrics.Timer
import io.delta.kernel.internal.util.{FileNames, Utils}
import io.delta.kernel.metrics.{ScanReport, SnapshotReport}
import io.delta.kernel.types.{IntegerType, LongType, StructType}
import io.delta.kernel.utils.CloseableIterator

import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.StatisticsCollection

class ScanReportSuite extends AnyFunSuite with MetricsReportTestUtils {

  /**
   * Creates a [[Scan]] using `getScan` and then requests and consumes the scan files. Uses a custom
   * engine to collect emitted metrics reports (exactly 1 [[ScanReport]] and 1 [[SnapshotReport]] is
   * expected). Also times and returns the duration it takes to consume the scan files.
   *
   * @param getScan function to generate a [[Scan]] given an engine
   * @param expectException whether we expect consuming the scan files to throw an exception, which
   *                        if so, is caught and returned with the other results
   * @return (ScanReport, durationToConsumeScanFiles, SnapshotReport, ExceptionIfThrown)
   */
  def getScanAndSnapshotReport(
    getScan: Engine => Scan,
    expectException: Boolean,
    consumeScanFiles: CloseableIterator[FilteredColumnarBatch] => Unit
  ): (ScanReport, Long, SnapshotReport, Option[Exception]) = {
    val timer = new Timer()

    val (metricsReports, exception) = collectMetricsReports(
      engine => {
        val scan = getScan(engine)
        // Time the actual operation
        timer.timeCallable(() => consumeScanFiles(scan.getScanFiles(engine)))
      },
      expectException
    )

    val scanReports = metricsReports.filter(_.isInstanceOf[ScanReport])
    assert(scanReports.length == 1, "Expected exactly 1 ScanReport")
    val snapshotReports = metricsReports.filter(_.isInstanceOf[SnapshotReport])
    assert(snapshotReports.length == 1, "Expected exactly 1 SnapshotReport")
    (scanReports.head.asInstanceOf[ScanReport], timer.totalDurationNs(),
      snapshotReports.head.asInstanceOf[SnapshotReport], exception)
  }

  /**
   * Given a table path, constructs the latest snapshot, and uses it to generate a Scan with the
   * provided filter and readSchema (if provided). Consumes the scan files from the scan and
   * collects the emitted [[ScanReport]] and checks that the report is as expected.
   *
   * @param path table path to query
   * @param expectException whether we expect consuming the scan files to throw an exception
   * @param expectedNumAddFiles expected number of add files seen
   * @param expectedNumAddFilesFromDeltaFiles expected number of add files seen from delta files
   * @param expectedNumActiveAddFiles expected number of active add files
   * @param expectedNumDuplicateAddFiles expected number of duplicate add files seen
   * @param expectedNumRemoveFilesSeenFromDeltaFiles expected number of remove files seen
   * @param expectedPartitionPredicate expected partition predicate
   * @param expectedDataSkippingFilter expected data skipping filter
   * @param filter filter to build the scan with
   * @param readSchema read schema to build the scan with
   * @param consumeScanFiles function to consume scan file iterator
   */
  // scalastyle:off
  def checkScanReport(
    path: String,
    expectException: Boolean,
    expectedNumAddFiles: Long,
    expectedNumAddFilesFromDeltaFiles: Long,
    expectedNumActiveAddFiles: Long,
    expectedNumDuplicateAddFiles: Long = 0,
    expectedNumRemoveFilesSeenFromDeltaFiles: Long = 0,
    expectedPartitionPredicate: Option[Predicate] = None,
    expectedDataSkippingFilter: Option[Predicate] = None,
    expectedIsFullyConsumed: Boolean = true,
    filter: Option[Predicate] = None,
    readSchema: Option[StructType] = None,
    // toSeq triggers log replay, consumes the actions and closes the iterator
    consumeScanFiles: CloseableIterator[FilteredColumnarBatch] => Unit = iter => iter.toSeq
  ): Unit = {
    // scalastyle:on
    // We need to save the snapshotSchema to check against the generated scan report
    // In order to use the utils to collect the reports, we need to generate the snapshot in a anon
    // fx, thus we save the snapshotSchema as a side-effect
    var snapshotSchema: StructType = null

    val (scanReport, durationNs, snapshotReport, exceptionOpt) = getScanAndSnapshotReport(
      engine => {
        val snapshot = Table.forPath(engine, path).getLatestSnapshot(engine)
        snapshotSchema = snapshot.getSchema(engine)
        var scanBuilder = snapshot.getScanBuilder(engine)
        if (filter.nonEmpty) {
          scanBuilder = scanBuilder.withFilter(engine, filter.get)
        }
        if (readSchema.nonEmpty) {
          scanBuilder = scanBuilder.withReadSchema(engine, readSchema.get)
        }
        scanBuilder.build()
      },
      expectException,
      consumeScanFiles
    )

    // Verify contents
    assert(scanReport.getTablePath == defaultEngine.getFileSystemClient.resolvePath(path))
    assert(scanReport.getOperationType == "Scan")
    exceptionOpt match {
      case Some(e) =>
        assert(scanReport.getException().isPresent)
        assert(scanReport.getException().get().getClass == e.getClass)
        assert(scanReport.getException().get().getMessage == e.getMessage)
      case None => assert(!scanReport.getException().isPresent)
    }
    assert(scanReport.getReportUUID != null)

    assert(snapshotReport.getVersion.isPresent,
      "Version should be present for success SnapshotReport")
    assert(scanReport.getTableVersion() == snapshotReport.getVersion.get())
    assert(scanReport.getTableSchema() == snapshotSchema)
    assert(scanReport.getSnapshotReportUUID == snapshotReport.getReportUUID)
    assert(scanReport.getFilter.toScala == filter)
    assert(scanReport.getReadSchema == readSchema.getOrElse(snapshotSchema))
    assert(scanReport.getPartitionPredicate.toScala == expectedPartitionPredicate)
    assert(scanReport.getIsFullyConsumed == expectedIsFullyConsumed)

    (scanReport.getDataSkippingFilter.toScala, expectedDataSkippingFilter) match {
      case (Some(found), Some(expected)) =>
        assert(found.getName == expected.getName && found.getChildren == expected.getChildren)
      case (found, expected) => assert(found == expected)
    }

    // Since we cannot know the actual duration of the scan we sanity check that they are > 0 and
    // less than the total operation duration
    assert(scanReport.getScanMetrics.getTotalPlanningDurationNs > 0)
    assert(scanReport.getScanMetrics.getTotalPlanningDurationNs < durationNs)

    assert(scanReport.getScanMetrics.getNumAddFilesSeen == expectedNumAddFiles)
    assert(scanReport.getScanMetrics.getNumAddFilesSeenFromDeltaFiles ==
      expectedNumAddFilesFromDeltaFiles)
    assert(scanReport.getScanMetrics.getNumActiveAddFiles == expectedNumActiveAddFiles)
    assert(scanReport.getScanMetrics.getNumDuplicateAddFiles == expectedNumDuplicateAddFiles)
    assert(scanReport.getScanMetrics.getNumRemoveFilesSeenFromDeltaFiles ==
      expectedNumRemoveFilesSeenFromDeltaFiles)
  }

  test("ScanReport: basic case with no extra parameters") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      // Set up delta table with 1 add file
      spark.range(10).write.format("delta").mode("append").save(path)

      checkScanReport(
        path,
        expectException = false,
        expectedNumAddFiles = 1,
        expectedNumAddFilesFromDeltaFiles = 1,
        expectedNumActiveAddFiles = 1
      )
    }
  }

  test("ScanReport: basic case with read schema") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      // Set up delta table with 1 add file
      spark.range(10).withColumn("c2", col("id") % 2)
        .write.format("delta").mode("append").save(path)

      checkScanReport(
        path,
        expectException = false,
        expectedNumAddFiles = 1,
        expectedNumAddFilesFromDeltaFiles = 1,
        expectedNumActiveAddFiles = 1,
        readSchema = Some(new StructType().add("id", LongType.LONG))
      )
    }
  }

  test("ScanReport: different filter scenarios") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      // Set up partitioned table
      spark.range(10).withColumn("part", col("id") % 2)
        .write.format("delta").partitionBy("part").save(path)

      val partFilter = new Predicate("=", new Column("part"), Literal.ofLong(0))
      val dataFilter = new Predicate("<=", new Column("id"), Literal.ofLong(0))
      val expectedSkippingFilter = new Predicate(
        "<=", new Column(Array("minValues", "id")), Literal.ofLong(0))

      // The below metrics are incremented during log replay before any filtering happens and thus
      // should be the same for all of the following test cases
      val expectedNumAddFiles = 2
      val expectedNumAddFilesFromDeltaFiles = 2
      val expectedNumActiveAddFiles = 2

      // No filter - 2 add files one for each partition
      checkScanReport(
        path,
        expectException = false,
        expectedNumAddFiles = expectedNumAddFiles,
        expectedNumAddFilesFromDeltaFiles = expectedNumAddFilesFromDeltaFiles,
        expectedNumActiveAddFiles = expectedNumActiveAddFiles
      )

      // With partition filter
      checkScanReport(
        path,
        expectException = false,
        expectedNumAddFiles = expectedNumAddFiles,
        expectedNumAddFilesFromDeltaFiles = expectedNumAddFilesFromDeltaFiles,
        expectedNumActiveAddFiles = expectedNumActiveAddFiles,
        filter = Some(partFilter),
        expectedPartitionPredicate = Some(partFilter)
      )

      // With data filter
      checkScanReport(
        path,
        expectException = false,
        expectedNumAddFiles = expectedNumAddFiles,
        expectedNumAddFilesFromDeltaFiles = expectedNumAddFilesFromDeltaFiles,
        expectedNumActiveAddFiles = expectedNumActiveAddFiles,
        filter = Some(dataFilter),
        expectedDataSkippingFilter = Some(expectedSkippingFilter)
      )

      // With data and partition filter
      checkScanReport(
        path,
        expectException = false,
        expectedNumAddFiles = expectedNumAddFiles,
        expectedNumAddFilesFromDeltaFiles = expectedNumAddFilesFromDeltaFiles,
        expectedNumActiveAddFiles = expectedNumActiveAddFiles,
        filter = Some(new Predicate("AND", partFilter, dataFilter)),
        expectedDataSkippingFilter = Some(expectedSkippingFilter),
        expectedPartitionPredicate = Some(partFilter)
      )
    }
  }

  test("ScanReport: close scan file iterator early") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      // Set up delta table with 2 add files
      spark.range(10).write.format("delta").mode("append").save(path)
      spark.range(10).write.format("delta").mode("append").save(path)

      checkScanReport(
        path,
        expectException = false,
        expectedNumAddFiles = 1,
        expectedNumAddFilesFromDeltaFiles = 1,
        expectedNumActiveAddFiles = 1,
        expectedIsFullyConsumed = false,
        consumeScanFiles = iter => iter.close() // Close iterator before consuming any scan files
      )
    }
  }

  //////////////////
  // Error cases ///
  //////////////////

  test("ScanReport error case - unrecognized partition filter") {
    // Thrown during partition pruning when the expression handler cannot evaluate the filter
    // Because partition pruning happens within a `map` on the iterator, this is caught and reported
    // within `map`
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      // Set up partitioned table
      spark.range(10).withColumn("part", col("id") % 2)
        .write.format("delta").partitionBy("part").save(path)

      val partFilter = new Predicate("foo", new Column("part"), Literal.ofLong(0))

      checkScanReport(
        path,
        expectException = true,
        expectedNumAddFiles = 0,
        expectedNumAddFilesFromDeltaFiles = 0,
        expectedNumActiveAddFiles = 0,
        expectedIsFullyConsumed = false,
        filter = Some(partFilter),
        expectedPartitionPredicate = Some(partFilter)
      )
    }
  }

  test("ScanReport error case - error reading the log files") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      // We set up a table with a giberish json file at version 0 and a valid json file at version 1
      // that contains the P&M
      // This is so the snapshot loading will happen successful, and we will only fail when trying
      // to load up the scan files
      // This exception is thrown from within the `hasNext` method on the iterator since that is
      // when we load the actions from the log files. This exception is caught and reported within
      // `hasNext`
      spark.range(10).write.format("delta").save(path)
      // Update protocol and metadata (so that version 1 has both P&M present)
      spark.sql(
        s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
      // Overwrite json file with giberish (this will have a schema mismatch issue for `add`)
      val giberishRow = new GenericRow(
        new StructType().add("add", IntegerType.INTEGER),
        Collections.singletonMap(0, Integer.valueOf(0))
      )
      defaultEngine.getJsonHandler.writeJsonFileAtomically(
        FileNames.deltaFile(new Path(tempDir.toString, "_delta_log"), 0),
        Utils.singletonCloseableIterator(giberishRow),
        true
      )

      checkScanReport(
        path,
        expectException = true,
        expectedNumAddFiles = 0,
        expectedNumAddFilesFromDeltaFiles = 0,
        expectedNumActiveAddFiles = 0,
        expectedIsFullyConsumed = false
      )
    }
  }

  ///////////////////////////////
  // Log replay metrics tests ///
  ///////////////////////////////

  test("active add files log replay metrics: only delta files") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      for (_ <- 0 to 9) {
        appendCommit(path)
      }

      checkScanReport(
        path,
        expectException = false,
        expectedNumAddFiles = 20, // each commit creates 2 files
        expectedNumAddFilesFromDeltaFiles = 20,
        expectedNumActiveAddFiles = 20
      )
    }
  }

  Seq(true, false).foreach { multipartCheckpoint =>
    val checkpointStr = if (multipartCheckpoint) "multipart " else ""
    test(s"active add files log replay metrics: ${checkpointStr}checkpoint + delta files") {
      withTempDir { tempDir =>
        val path = tempDir.getCanonicalPath
        for (_ <- 0 to 3) {
          appendCommit(path)
        }
        checkpoint(path, actionsPerFile = if (multipartCheckpoint) 2 else 1000000)
        for (_ <- 4 to 9) {
          appendCommit(path)
        }

        checkScanReport(
          path,
          expectException = false,
          expectedNumAddFiles = 20, // each commit creates 2 files
          expectedNumAddFilesFromDeltaFiles = 12, // checkpoint is created at version 3
          expectedNumActiveAddFiles = 20
        )
      }
    }
  }

  Seq(true, false).foreach { multipartCheckpoint =>
    val checkpointStr = if (multipartCheckpoint) "multipart " else ""
    test(s"active add files log replay metrics: ${checkpointStr}checkpoint + " +
      s"delta files + tombstones") {
      withTempDir { tempDir =>
        val path = tempDir.getCanonicalPath
        for (_ <- 0 to 3) {
          appendCommit(path)
        } // has 8 add files
        deleteCommit(path) // version 4 - deletes 4 files and adds 1 file
        checkpoint(path, actionsPerFile = if (multipartCheckpoint) 2 else 1000000) // version 4
        appendCommit(path) // version 5 - adds 2 files
        deleteCommit(path) // version 6 - deletes 1 file and adds 1 file
        appendCommit(path) // version 7 - adds 2 files
        appendCommit(path) // version 8 - adds 2 files
        deleteCommit(path) // version 9 - deletes 2 files and adds 1 file

        checkScanReport(
          path,
          expectException = false,
          expectedNumAddFiles = 5 /* checkpoint */ + 8, /* delta */
          expectedNumAddFilesFromDeltaFiles = 8,
          expectedNumActiveAddFiles = 10,
          expectedNumRemoveFilesSeenFromDeltaFiles = 3
        )
      }
    }
  }

  Seq(true, false).foreach { multipartCheckpoint =>
    val checkpointStr = if (multipartCheckpoint) "multipart " else ""
    test(s"active add files log replay metrics: ${checkpointStr}checkpoint + delta files +" +
      s" tombstones + duplicate adds") {
      withTempDir { tempDir =>
        val path = tempDir.getCanonicalPath
        for (_ <- 0 to 1) {
          appendCommit(path)
        } // activeAdds = 4
        deleteCommit(path) // ver 2 - deletes 2 files and adds 1 file, activeAdds = 3
        checkpoint(path, actionsPerFile = if (multipartCheckpoint) 2 else 1000000) // version 2
        appendCommit(path) // ver 3 - adds 2 files, activeAdds = 5
        recomputeStats(path) // ver 4 - adds the same 5 add files again, activeAdds = 5, dupes = 5
        deleteCommit(path) // ver 5 - removes 1 file and adds 1 file, activeAdds = 5, dupes = 5
        appendCommit(path) // ver 6 - adds 2 files, activeAdds = 7, dupes = 4
        recomputeStats(path) // ver 7 - adds the same 7 add files again, activeAdds = 7, dupes = 12
        deleteCommit(path) // ver 8 - removes 1 file and adds 1 files, activeAdds = 7, dupes = 12

        checkScanReport(
          path,
          expectException = false,
          expectedNumAddFiles = 3 /* checkpoint */ + 18, /* delta */
          expectedNumAddFilesFromDeltaFiles = 18,
          expectedNumActiveAddFiles = 7,
          expectedNumDuplicateAddFiles = 12,
          expectedNumRemoveFilesSeenFromDeltaFiles = 2
        )
      }
    }
  }

  /////////////////////////////////////////////
  // Helpers for testing log replay metrics ///
  /////////////////////////////////////////////

  def appendCommit(path: String): Unit =
    spark.range(10).repartition(2).write.format("delta").mode("append").save(path)

  def deleteCommit(path: String): Unit = {
    spark.sql("DELETE FROM delta.`%s` WHERE id = 5".format(path))
  }

  def recomputeStats(path: String): Unit = {
    val deltaLog = DeltaLog.forTable(spark, new org.apache.hadoop.fs.Path(path))
    StatisticsCollection.recompute(spark, deltaLog, catalogTable = None)
  }

  def checkpoint(path: String, actionsPerFile: Int): Unit = {
    withSQLConf(DeltaSQLConf.DELTA_CHECKPOINT_PART_SIZE.key -> actionsPerFile.toString) {
      DeltaLog.forTable(spark, path).checkpoint()
    }
  }
}
