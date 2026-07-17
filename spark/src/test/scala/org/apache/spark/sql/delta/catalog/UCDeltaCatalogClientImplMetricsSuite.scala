/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.catalog

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, RemoveFile}
import org.apache.spark.sql.delta.stats.FileSizeHistogramUtils
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Unit tests for `UCDeltaCatalogClientImpl.buildCommitReport`, which converts per-commit
 * Delta state (actions, version, snapshot histogram) into the `UCDeltaModels.CommitReport`
 * shipped to UC's Delta metrics endpoint.
 *
 * Same-package as the function under test so the package-private helper is reachable
 * without widening its visibility.
 */
class UCDeltaCatalogClientImplMetricsSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  test("buildCommitReport: file metrics from synthetic AddFile/RemoveFile actions") {
    val add1 = AddFile("f1.parquet", Map.empty, 1024L,
      System.currentTimeMillis(), dataChange = true)
    val add2 = AddFile("f2.parquet", Map.empty, 2048L,
      System.currentTimeMillis(), dataChange = true)
    val add3 = AddFile("f3.parquet", Map.empty, 4096L,
      System.currentTimeMillis(), dataChange = true)
    val removed = RemoveFile("old.parquet",
      Some(System.currentTimeMillis()),
      dataChange = true, size = Some(512L))

    val snapshotHist = FileSizeHistogramUtils.emptyHistogram
    snapshotHist.insert(1024L)
    snapshotHist.insert(2048L)
    snapshotHist.insert(4096L)

    val actions: Seq[Action] = Seq(add1, add2, add3, removed)
    val report = UCDeltaCatalogClientImpl.buildCommitReport(
      actions, committedVersion = 7L, snapshotHistogram = Some(snapshotHist))

    assert(report.getNumFilesAdded == 3L, "3 AddFiles")
    assert(report.getNumFilesRemoved == 1L, "1 RemoveFile")
    assert(report.getNumBytesAdded == 1024L + 2048L + 4096L, "sum of add sizes")
    assert(report.getNumBytesRemoved == 512L, "sum of remove sizes")

    assert(report.getFileSizeHistogram.isPresent)
    val hist = report.getFileSizeHistogram.get
    assert(hist.getCommitVersion == 7L, "commit_version must be set")
    assert(hist.getSortedBinBoundaries.get(0) == 0L, "bins must start at 0")
    assert(hist.getFileCounts.asScala.map(_.longValue).sum == 3L,
      "histogram covers all files")
    assert(hist.getTotalBytes.asScala.map(_.longValue).sum == 1024L + 2048L + 4096L,
      "histogram totalBytes")
  }

  test("buildCommitReport: row metrics prefer operationMetrics over file stats") {
    val addFileWithStats = AddFile("f.parquet", Map.empty, 1000L,
      System.currentTimeMillis(), dataChange = true,
      stats = """{"numRecords": 999}""")
    val commitInfoWithMetrics = commitInfo(
      operation = "WRITE",
      operationMetrics = Some(Map("numOutputRows" -> "100")))

    val r1 = UCDeltaCatalogClientImpl.buildCommitReport(
      Seq(commitInfoWithMetrics, addFileWithStats), 0L, None)
    assert(r1.getNumRowsInserted.get == 100L,
      "operationMetrics wins over file stats")

    val commitInfoNoMetrics = commitInfo(
      operation = "WRITE", operationMetrics = None, version = Some(1L))

    val r2 = UCDeltaCatalogClientImpl.buildCommitReport(
      Seq(commitInfoNoMetrics, addFileWithStats), 1L, None)
    assert(r2.getNumRowsInserted.get == 999L,
      "fallback to numLogicalRecords from file stats")

    val r3 = UCDeltaCatalogClientImpl.buildCommitReport(
      Seq(addFileWithStats), 2L, None)
    assert(r3.getNumRowsInserted.get == 999L,
      "fallback works with no CommitInfo in actions")
  }

  test("buildCommitReport: row metrics for MERGE operation") {
    val ci = commitInfo(
      operation = "MERGE",
      operationMetrics = Some(Map(
        "numTargetRowsInserted" -> "10",
        "numTargetRowsDeleted" -> "5",
        "numTargetRowsUpdated" -> "3")))
    val r = UCDeltaCatalogClientImpl.buildCommitReport(Seq(ci), 0L, None)
    assert(r.getNumRowsInserted.get == 10L)
    assert(r.getNumRowsRemoved.get == 5L)
    assert(r.getNumRowsUpdated.get == 3L)
  }

  test("buildCommitReport: row metrics for DELETE operation") {
    val ci = commitInfo(
      operation = "DELETE",
      operationMetrics = Some(Map("numDeletedRows" -> "42")))
    val r = UCDeltaCatalogClientImpl.buildCommitReport(Seq(ci), 0L, None)
    assert(r.getNumRowsRemoved.get == 42L)
  }

  test("buildCommitReport: row metrics for UPDATE operation") {
    val ci = commitInfo(
      operation = "UPDATE",
      operationMetrics = Some(Map("numUpdatedRows" -> "17")))
    val r = UCDeltaCatalogClientImpl.buildCommitReport(Seq(ci), 0L, None)
    assert(r.getNumRowsUpdated.get == 17L)
  }

  test("buildCommitReport: row metrics absent when neither operationMetrics nor file stats " +
      "can supply them") {
    // AddFile/RemoveFile without numRecords stats, and no CommitInfo => all three row
    // counts must be absent (the falsy branch of every extractRows* helper).
    val add = AddFile("f.parquet", Map.empty, 1024L,
      System.currentTimeMillis(), dataChange = true)
    val remove = RemoveFile("r.parquet", Some(System.currentTimeMillis()),
      dataChange = true, size = Some(512L))
    val r = UCDeltaCatalogClientImpl.buildCommitReport(Seq(add, remove), 0L, None)
    assert(!r.getNumRowsInserted.isPresent, "no opMetrics + no AddFile stats => empty")
    assert(!r.getNumRowsRemoved.isPresent, "no opMetrics + no RemoveFile stats => empty")
    assert(!r.getNumRowsUpdated.isPresent, "no opMetrics => empty (no file-stats fallback)")
    assert(r.getNumFilesAdded == 1L)
    assert(r.getNumFilesRemoved == 1L)
  }

  test("buildCommitReport: uses snapshot histogram when provided") {
    val snapshotHist = FileSizeHistogramUtils.emptyHistogram
    snapshotHist.insert(1024L)
    snapshotHist.insert(2048L)
    snapshotHist.insert(4096L)

    // AddFile with a DIFFERENT size than what's in the snapshot histogram; histogram should
    // come from the snapshot, not from the per-commit AddFiles.
    val add = AddFile("f.parquet", Map.empty, 8192L,
      System.currentTimeMillis(), dataChange = true)

    val r = UCDeltaCatalogClientImpl.buildCommitReport(
      Seq(add), committedVersion = 5L, snapshotHistogram = Some(snapshotHist))
    val hist = r.getFileSizeHistogram.get

    assert(hist.getFileCounts.asScala.map(_.longValue).sum == 3L,
      "should use snapshot histogram file counts, not per-commit AddFiles")
    assert(hist.getTotalBytes.asScala.map(_.longValue).sum == 1024L + 2048L + 4096L,
      "should use snapshot histogram total bytes")
    assert(hist.getCommitVersion == 5L)
  }

  test("buildCommitReport: omits histogram when snapshot is None") {
    val add = AddFile("f.parquet", Map.empty, 1024L,
      System.currentTimeMillis(), dataChange = true)
    val r = UCDeltaCatalogClientImpl.buildCommitReport(
      Seq(add), committedVersion = 3L, snapshotHistogram = None)

    assert(!r.getFileSizeHistogram.isPresent,
      "histogram should be absent when snapshot is unavailable")
  }

  // -- Helpers --

  private def commitInfo(
      operation: String,
      operationMetrics: Option[Map[String, String]],
      version: Option[Long] = Some(0L)): CommitInfo = {
    CommitInfo(
      version = version,
      inCommitTimestamp = None,
      timestamp = new java.sql.Timestamp(System.currentTimeMillis()),
      userId = None, userName = None,
      operation = operation,
      operationParameters = Map.empty,
      job = None, notebook = None, clusterId = None,
      readVersion = None, isolationLevel = None,
      isBlindAppend = Some(false),
      operationMetrics = operationMetrics,
      userMetadata = None, tags = None, engineInfo = None, txnId = None,
      lastManifestCommit = None)
  }
}
