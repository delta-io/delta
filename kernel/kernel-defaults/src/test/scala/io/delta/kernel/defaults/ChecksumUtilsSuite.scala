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
package io.delta.kernel.defaults
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.util.{Collections, Optional}

import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._

import io.delta.kernel.Snapshot.ChecksumWriteMode
import io.delta.kernel.Table
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.internal.json.JsonUtils
import io.delta.kernel.defaults.utils.WriteUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.{Column, Literal}
import io.delta.kernel.internal.{SnapshotImpl, TableImpl}
import io.delta.kernel.internal.actions.{AddFile => KernelAddFile, CommitInfo => KernelCommitInfo, DeletionVectorDescriptor, DomainMetadata => KernelDomainMetadata, Format, Metadata => KernelMetadata, Protocol => KernelProtocol, SetTransaction => KernelSetTransaction}
import io.delta.kernel.internal.actions.SingleAction._
import io.delta.kernel.internal.checksum.{ChecksumReader, ChecksumUtils, CRCInfo}
import io.delta.kernel.internal.stats.FileSizeHistogram
import io.delta.kernel.internal.util.{ManualClock, VectorUtils}
import io.delta.kernel.types.{StringType, StructType}
import io.delta.kernel.utils.CloseableIterable.emptyIterable
import io.delta.kernel.utils.FileStatus

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{CommitInfo, SetTransaction}

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite for io.delta.kernel.internal.checksum.ChecksumUtils
 */
class ChecksumUtilsSuite extends AnyFunSuite with WriteUtils with LogReplayBaseSuite {

  private def initialTestTable(tablePath: String, engine: Engine): Unit = {
    createEmptyTable(engine, tablePath, testSchema, clock = new ManualClock(0))
    appendData(
      engine,
      tablePath,
      isNewTable = false,
      data = Seq(Map.empty[String, Literal] -> dataBatches1))
  }

  test("Create checksum for different version") {
    withTempDirAndEngine { (tablePath, engine) =>
      initialTestTable(tablePath, engine)

      val snapshot0 = Table.forPath(
        engine,
        tablePath).getSnapshotAsOfVersion(engine, 0).asInstanceOf[SnapshotImpl]
      ChecksumUtils.computeStateAndWriteChecksum(engine, snapshot0.getLogSegment)
      verifyChecksumForSnapshot(snapshot0)

      val snapshot1 = Table.forPath(
        engine,
        tablePath).getSnapshotAsOfVersion(engine, 1).asInstanceOf[SnapshotImpl]
      ChecksumUtils.computeStateAndWriteChecksum(engine, snapshot1.getLogSegment)
      verifyChecksumForSnapshot(snapshot1)
    }
  }

  test("computeChecksum returns the CRCInfo without writing a checksum file") {
    withTempDirAndEngine { (tablePath, engine) =>
      initialTestTable(tablePath, engine)

      val snapshot1 = Table.forPath(
        engine,
        tablePath).getSnapshotAsOfVersion(engine, 1).asInstanceOf[SnapshotImpl]

      // Same computation as computeStateAndWriteChecksum
      val crcInfo = ChecksumUtils.computeChecksum(engine, snapshot1.getLogSegment)
      assert(crcInfo.getVersion === 1)

      // No checksum file was written: a freshly loaded log segment still sees no checksum.
      val reloaded = Table.forPath(
        engine,
        tablePath).getSnapshotAsOfVersion(engine, 1).asInstanceOf[SnapshotImpl]
      assert(!reloaded.getLogSegment.getLastSeenChecksum.isPresent)

      // The writing counterpart still persists an equivalent, valid checksum.
      ChecksumUtils.computeStateAndWriteChecksum(engine, snapshot1.getLogSegment)
      verifyChecksumForSnapshot(snapshot1)
    }
  }

  test("withInCommitTimestamp stamps a caller-supplied ICT onto a computed CRCInfo") {
    withTempDirAndEngine { (tablePath, engine) =>
      initialTestTable(tablePath, engine)

      val snapshot1 = Table.forPath(
        engine,
        tablePath).getSnapshotAsOfVersion(engine, 1).asInstanceOf[SnapshotImpl]

      // computeChecksum derives from file actions, so ICT is absent (not derivable from replay).
      val crcInfo = ChecksumUtils.computeChecksum(engine, snapshot1.getLogSegment)
      assert(!crcInfo.getInCommitTimestamp.isPresent)

      // A caller holding the commit's CommitInfo stamps the ICT on; all other fields are preserved.
      val ict = java.lang.Long.valueOf(1749830855993L)
      val withIct = crcInfo.withInCommitTimestamp(Optional.of(ict))
      assert(withIct.getInCommitTimestamp === Optional.of(ict))
      assert(withIct.getVersion === crcInfo.getVersion)
      assert(withIct.getNumFiles === crcInfo.getNumFiles)
      assert(withIct.getTableSizeBytes === crcInfo.getTableSizeBytes)
      assert(withIct.getMetadata === crcInfo.getMetadata)
      assert(withIct.getProtocol === crcInfo.getProtocol)
      assert(withIct.getDomainMetadata === crcInfo.getDomainMetadata)
    }
  }

  /** Commits an idempotent (txnAppId/txnVersion) append via the kernel write path. */
  private def commitTxn(
      tablePath: String,
      engine: Engine,
      appId: String,
      txnVersion: Long): Unit = {
    val txn = getUpdateTxn(engine, tablePath, txnId = Some((appId, txnVersion)))
    commitAppendData(engine, txn, Seq(Map.empty[String, Literal] -> dataBatches1))
  }

  test("computeChecksum captures setTransactions from the log (full replay path)") {
    withTempDirAndEngine { (tablePath, engine) =>
      // No .crc is written by the kernel write path, so computeChecksum takes the full-replay path.
      createEmptyTable(engine, tablePath, testSchema, clock = new ManualClock(0))
      commitTxn(tablePath, engine, "app-1", 1)
      commitTxn(tablePath, engine, "app-1", 2) // supersedes app-1@1
      commitTxn(tablePath, engine, "app-2", 5)

      val crcInfo = ChecksumUtils.computeChecksum(engine, logSegmentAtLatest(tablePath, engine))

      // The newest version wins for a repeated appId (app-1 -> 2, not 1).
      assert(crcInfo.getSetTransactions.isPresent)
      val byAppId =
        crcInfo.getSetTransactions.get().asScala.map(t => t.getAppId -> t.getVersion).toMap
      assert(byAppId === Map("app-1" -> 2L, "app-2" -> 5L))
    }
  }

  test("computeChecksum (incremental) keeps the newest SetTransaction version per appId") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, testSchema, clock = new ManualClock(0))
      commitTxn(tablePath, engine, "app-1", 1)
      commitTxn(tablePath, engine, "app-2", 7)

      // Seed a base .crc that carries {app-1@1, app-2@7} (full replay, since no prior .crc exists).
      val seedSegment = logSegmentAtLatest(tablePath, engine)
      ChecksumUtils.computeStateAndWriteChecksum(engine, seedSegment)
      val seedCrc = ChecksumUtils.computeChecksum(engine, seedSegment)
      assert(seedCrc.getSetTransactions.isPresent)
      assert(seedCrc.getSetTransactions.get().asScala.map(t => t.getAppId -> t.getVersion).toMap
        === Map("app-1" -> 1L, "app-2" -> 7L))

      // A new commit supersedes app-1 and leaves app-2 untouched. The base .crc makes
      // computeChecksum build incrementally, folding the new commit's txn over the base txns.
      commitTxn(tablePath, engine, "app-1", 2)
      val crcInfo = ChecksumUtils.computeChecksum(engine, logSegmentAtLatest(tablePath, engine))

      // Newest version wins for the superseded appId; the untouched appId is carried from the base.
      assert(crcInfo.getSetTransactions.isPresent)
      val byAppId =
        crcInfo.getSetTransactions.get().asScala.map(t => t.getAppId -> t.getVersion).toMap
      assert(byAppId === Map("app-1" -> 2L, "app-2" -> 7L))
    }
  }

  test("computeChecksum carries setTransactions forward across an incremental commit") {
    withTempDirAndEngine { (tablePath, engine) =>
      initialTestTable(tablePath, engine)

      val snapshot1 = Table.forPath(
        engine,
        tablePath).getSnapshotAsOfVersion(engine, 1).asInstanceOf[SnapshotImpl]
      ChecksumUtils.computeStateAndWriteChecksum(engine, snapshot1.getLogSegment)
      val txnsV1 = ChecksumUtils.computeChecksum(engine, snapshot1.getLogSegment).getSetTransactions

      appendData(
        engine,
        tablePath,
        isNewTable = false,
        data = Seq(Map.empty[String, Literal] -> dataBatches1))
      val snapshot2 = Table.forPath(
        engine,
        tablePath).getSnapshotAsOfVersion(engine, 2).asInstanceOf[SnapshotImpl]
      val crcV2 = ChecksumUtils.computeChecksum(engine, snapshot2.getLogSegment)

      // setTransactions is maintained incrementally and remains present + unique per appId.
      assert(crcV2.getVersion === 2)
      assert(crcV2.getSetTransactions.isPresent)
      assert(txnsV1.isPresent)
      val v2AppIds = crcV2.getSetTransactions.get().asScala.map(_.getAppId).toSet
      assert(txnsV1.get().asScala.map(_.getAppId).toSet.subsetOf(v2AppIds))
    }
  }

  private def logSegmentAtLatest(
      tablePath: String,
      engine: Engine): io.delta.kernel.internal.snapshot.LogSegment = {
    val version = Table.forPath(engine, tablePath).getLatestSnapshot(engine).getVersion()
    Table.forPath(engine, tablePath)
      .getSnapshotAsOfVersion(engine, version).asInstanceOf[SnapshotImpl].getLogSegment
  }

  // Writes one idempotent Spark append (producing a SetTransaction with a real-clock lastUpdated)
  // to a table configured with delta.setTransactionRetentionDuration. Remove crc files so kernel
  // recomputes from the log.
  private def retentionTable(
      tablePath: String,
      engine: Engine,
      retention: String): (io.delta.kernel.internal.snapshot.LogSegment, String) = {
    val appId = "retention-app"
    spark.sql(
      s"CREATE TABLE delta.`$tablePath` (id LONG) USING delta " +
        s"TBLPROPERTIES ('delta.setTransactionRetentionDuration' = '$retention')")
    spark.range(end = 5).write.format("delta")
      .option("txnAppId", appId)
      .option("txnVersion", 1)
      .mode("append").save(tablePath)
    val version = Table.forPath(engine, tablePath).getLatestSnapshot(engine).getVersion()
    deleteChecksumFileForTableUsingHadoopFs(tablePath.stripPrefix("file:"), (0 to version.toInt))
    (logSegmentAtLatest(tablePath, engine), appId)
  }

  test("computeChecksum (full replay) retains setTransactions newer than the retention cutoff") {
    withTempDirAndEngine { (tablePath, engine) =>
      val (logSegment, appId) = retentionTable(tablePath, engine, retention = "interval 30 days")

      // Write is well within a 30-day retention window.
      val clock = new ManualClock(System.currentTimeMillis() + 1000L)
      val crcInfo = ChecksumUtils.computeChecksum(engine, logSegment, clock)

      assert(crcInfo.getSetTransactions.isPresent)
      assert(crcInfo.getSetTransactions.get().asScala.map(_.getAppId).contains(appId))
    }
  }

  test("computeChecksum (full replay) drops setTransactions older than the retention cutoff") {
    withTempDirAndEngine { (tablePath, engine) =>
      val (logSegment, appId) = retentionTable(tablePath, engine, retention = "interval 1 hours")

      // Test is a year in the future, so a 1-hour retention window has long since expired.
      val oneYearMillis = 365L * 24 * 60 * 60 * 1000
      val clock = new ManualClock(System.currentTimeMillis() + oneYearMillis)
      val crcInfo = ChecksumUtils.computeChecksum(engine, logSegment, clock)

      // Field is present but the expired transaction is filtered.
      assert(crcInfo.getSetTransactions.isPresent)
      assert(!crcInfo.getSetTransactions.get().asScala.map(_.getAppId).contains(appId))
    }
  }

  test("computeChecksum (full replay) applies retention before the .crc threshold " +
    "so expired appIds do not evict the live set") {
    withTempDirAndEngine { (tablePath, engine) =>
      val threshold = ChecksumUtils.DEFAULT_SET_TRANSACTIONS_IN_CRC_THRESHOLD.toInt
      val base = 1000000000000L
      val expiredUpdated = base - (2L * 60 * 60 * 1000) // 2h before base (outside a 1h window)

      spark.sql(
        s"CREATE TABLE delta.`$tablePath` (id LONG) USING delta " +
          "TBLPROPERTIES ('delta.setTransactionRetentionDuration' = 'interval 1 hours')")

      // Add live up to exactly the threshold and expired ones after that.
      val liveTxns = (0 until threshold).map(i =>
        SetTransaction(s"live-$i", version = 1L, lastUpdated = Some(base)))
      val expiredTxns = (0 until 5).map(i =>
        SetTransaction(s"expired-$i", version = 1L, lastUpdated = Some(expiredUpdated)))
      DeltaLog.forTable(spark, tablePath).startTransaction()
        .commitManuallyWithValidation((liveTxns ++ expiredTxns): _*)

      val version = Table.forPath(engine, tablePath).getLatestSnapshot(engine).getVersion()
      deleteChecksumFileForTableUsingHadoopFs(tablePath.stripPrefix("file:"), (0 to version.toInt))

      // "now" == base, so cutoff is base - 1h: live txns (base) survive, expired (base - 2h) drop.
      val crcInfo =
        ChecksumUtils.computeChecksum(
          engine,
          logSegmentAtLatest(tablePath, engine),
          new ManualClock(base))

      // The field is present with exactly the `threshold` live appIds.
      assert(crcInfo.getSetTransactions.isPresent)
      val appIds = crcInfo.getSetTransactions.get().asScala.map(_.getAppId).toSet
      assert(appIds.size === threshold)
      assert(appIds.forall(_.startsWith("live-")))
    }
  }

  test("computeChecksum (incremental) omits setTransactions when retention is configured") {
    withTempDirAndEngine { (tablePath, engine) =>
      val (logSegment0, appId) = retentionTable(tablePath, engine, retention = "interval 30 days")
      ChecksumUtils.computeStateAndWriteChecksum(engine, logSegment0)

      val seedCrc = ChecksumUtils.computeChecksum(engine, logSegment0)
      assert(seedCrc.getSetTransactions.isPresent)
      assert(seedCrc.getSetTransactions.get().asScala.map(_.getAppId).contains(appId))

      spark.range(5, 10).write.format("delta")
        .option("txnAppId", "retention-app-2")
        .option("txnVersion", 1)
        .mode("append").save(tablePath)
      val version = Table.forPath(engine, tablePath).getLatestSnapshot(engine).getVersion()
      deleteChecksumFileForTableUsingHadoopFs(tablePath.stripPrefix("file:"), Seq(version.toInt))

      val crcInfo = ChecksumUtils.computeChecksum(engine, logSegmentAtLatest(tablePath, engine))

      // Incremental path omits the field entirely.
      assert(!crcInfo.getSetTransactions.isPresent)
    }
  }

  test("computeChecksum populates deletion-vector metrics after a DELETE on a DV table") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create a deletion-vectors-enabled table via Spark and delete some rows, producing DVs.
      spark.sql(
        s"CREATE TABLE delta.`$tablePath` (id LONG) USING DELTA " +
          "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
      spark.range(0, 10).write.format("delta").mode("append").save(tablePath)
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id < 3")

      val snapshot = Table.forPath(engine, tablePath)
        .getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      val crcInfo = ChecksumUtils.computeChecksum(engine, snapshot.getLogSegment)

      // DV metrics are present (DV-capable table) and reflect the delete.
      assert(crcInfo.getNumDeletionVectors.isPresent)
      assert(crcInfo.getNumDeletedRecords.isPresent)
      assert(crcInfo.getNumDeletionVectors.get() >= 1L)
      assert(crcInfo.getNumDeletedRecords.get() === 3L)
    }
  }

  test("computeChecksum leaves deletion-vector metrics absent for a non-DV table") {
    withTempDirAndEngine { (tablePath, engine) =>
      initialTestTable(tablePath, engine)
      val snapshot = Table.forPath(engine, tablePath)
        .getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      val crcInfo = ChecksumUtils.computeChecksum(engine, snapshot.getLogSegment)
      assert(!crcInfo.getNumDeletionVectors.isPresent)
      assert(!crcInfo.getNumDeletedRecords.isPresent)
    }
  }

  test("computeChecksum falls back to full replay when the base CRC omits DV metrics on a " +
    "DV-enabled table") {
    withTempDirAndEngine { (tablePath, engine) =>
      // DV-enabled table with two deletes at two versions (5 total deleted records).
      spark.sql(
        s"CREATE TABLE delta.`$tablePath` (id LONG) USING DELTA " +
          "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
      spark.range(0, 10).write.format("delta").mode("append").save(tablePath)
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id < 3")
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id >= 8")

      deleteChecksumFileForTableUsingHadoopFs(tablePath, 0 to 3)
      val snapshot2 = Table.forPath(engine, tablePath)
        .getSnapshotAsOfVersion(engine, 2).asInstanceOf[SnapshotImpl]
      ChecksumUtils.computeStateAndWriteChecksum(engine, snapshot2.getLogSegment)

      // Simulate a base CRC that never captured DV metrics.
      rewriteChecksumFileToExcludeDvMetrics(engine, tablePath, 2)

      // computeChecksum at v3 finds the stripped v2 CRC as its base. Fall back to full replay.
      val snapshot3 = Table.forPath(engine, tablePath)
        .getSnapshotAsOfVersion(engine, 3).asInstanceOf[SnapshotImpl]
      val crcInfo = ChecksumUtils.computeChecksum(engine, snapshot3.getLogSegment)

      assert(crcInfo.getVersion === 3)
      assert(crcInfo.getNumDeletedRecords.isPresent)
      assert(crcInfo.getNumDeletionVectors.isPresent)
      assert(crcInfo.getNumDeletedRecords.get() === 5L)
      assert(crcInfo.getNumDeletionVectors.get() >= 1L)
    }
  }

  test("computeChecksum populates allFiles for a small table (full replay path)") {
    withTempDirAndEngine { (tablePath, engine) =>
      initialTestTable(tablePath, engine)

      val snapshot1 = Table.forPath(
        engine,
        tablePath).getSnapshotAsOfVersion(engine, 1).asInstanceOf[SnapshotImpl]
      val crcInfo = ChecksumUtils.computeChecksum(engine, snapshot1.getLogSegment)

      assert(crcInfo.getAllFiles.isPresent)
      assert(crcInfo.getAllFiles.get().size() === crcInfo.getNumFiles)
      assert(crcInfo.getNumFiles > 0)
    }
  }

  test("computeChecksum omits allFiles once the file count exceeds the threshold") {
    withTempDirAndEngine { (tablePath, engine) =>
      initialTestTable(tablePath, engine)

      // Append until the table has more than DEFAULT_ALL_FILES_IN_CRC_THRESHOLD (20) files.
      var version = 1L
      var crossedThreshold = false
      var iterations = 0L
      while (!crossedThreshold && iterations < CRCInfo.DEFAULT_ALL_FILES_IN_CRC_THRESHOLD + 1) {
        val snapshot = Table.forPath(
          engine,
          tablePath).getSnapshotAsOfVersion(engine, version).asInstanceOf[SnapshotImpl]
        val crc = ChecksumUtils.computeChecksum(engine, snapshot.getLogSegment)
        if (crc.getNumFiles > CRCInfo.DEFAULT_ALL_FILES_IN_CRC_THRESHOLD) {
          assert(
            !crc.getAllFiles.isPresent,
            s"expected allFiles absent at ${crc.getNumFiles} files (> threshold)")
          crossedThreshold = true
        } else {
          assert(
            crc.getAllFiles.isPresent,
            s"expected allFiles present at ${crc.getNumFiles} files (<= threshold)")
          assert(crc.getAllFiles.get().size() === crc.getNumFiles)
          appendData(
            engine,
            tablePath,
            isNewTable = false,
            data = Seq(Map.empty[String, Literal] -> dataBatches1))
          version += 1
        }
        iterations += 1
      }
      assert(crossedThreshold, "test did not grow the table beyond the allFiles threshold")
    }
  }

  test("computeChecksum omits allFiles when collectAllFiles=false") {
    withTempDirAndEngine { (tablePath, engine) =>
      initialTestTable(tablePath, engine)

      val snapshot1 = Table.forPath(
        engine,
        tablePath).getSnapshotAsOfVersion(engine, 1).asInstanceOf[SnapshotImpl]

      // Default (true) collects allFiles.
      val withFiles = ChecksumUtils.computeChecksum(engine, snapshot1.getLogSegment)
      val collectAllFiles = false
      val withoutFiles =
        ChecksumUtils.computeChecksum(engine, snapshot1.getLogSegment, collectAllFiles)

      assert(withFiles.getAllFiles.isPresent)
      assert(!withoutFiles.getAllFiles.isPresent)

      assert(withoutFiles.getNumFiles === withFiles.getNumFiles)
      assert(withoutFiles.getTableSizeBytes === withFiles.getTableSizeBytes)
      assert(withoutFiles.getMetadata === withFiles.getMetadata)
      assert(withoutFiles.getProtocol === withFiles.getProtocol)
    }
  }

  test("computeChecksum maintains allFiles across an incremental commit") {
    withTempDirAndEngine { (tablePath, engine) =>
      initialTestTable(tablePath, engine)

      // Persist v1's checksum (with allFiles) so the next computation can build incrementally.
      val snapshot1 = Table.forPath(
        engine,
        tablePath).getSnapshotAsOfVersion(engine, 1).asInstanceOf[SnapshotImpl]
      ChecksumUtils.computeStateAndWriteChecksum(engine, snapshot1.getLogSegment)
      val numFilesV1 = ChecksumUtils.computeChecksum(engine, snapshot1.getLogSegment).getNumFiles

      appendData(
        engine,
        tablePath,
        isNewTable = false,
        data = Seq(Map.empty[String, Literal] -> dataBatches1))
      val snapshot2 = Table.forPath(
        engine,
        tablePath).getSnapshotAsOfVersion(engine, 2).asInstanceOf[SnapshotImpl]
      val crcV2 = ChecksumUtils.computeChecksum(engine, snapshot2.getLogSegment)

      // allFiles is carried forward and stays consistent with the file count.
      assert(crcV2.getVersion === 2)
      assert(crcV2.getAllFiles.isPresent)
      assert(crcV2.getAllFiles.get().size() === crcV2.getNumFiles)
      assert(crcV2.getNumFiles > numFilesV1)
    }
  }

  test("computeChecksum (incremental) resolves a file added and removed within the folded range") {
    withTempDirAndEngine { (tablePath, engine) =>
      spark.sql(
        s"CREATE TABLE delta.`$tablePath` (id LONG) USING DELTA " +
          "TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')")
      spark.range(0, 5).write.format("delta").mode("append").save(tablePath) // v1 -> file A

      deleteChecksumFileForTableUsingHadoopFs(tablePath.stripPrefix("file:"), 0 to 3)
      val snapshot1 = Table.forPath(engine, tablePath)
        .getSnapshotAsOfVersion(engine, 1).asInstanceOf[SnapshotImpl]
      ChecksumUtils.computeStateAndWriteChecksum(engine, snapshot1.getLogSegment)
      val baseCrc = ChecksumUtils.computeChecksum(engine, snapshot1.getLogSegment)
      assert(baseCrc.getAllFiles.isPresent)
      val numFilesV1 = baseCrc.getNumFiles

      spark.range(100, 105).write.format("delta").mode("append").save(tablePath) // v2 -> file B
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id >= 100") // v3 -> removes file B
      deleteChecksumFileForTableUsingHadoopFs(tablePath.stripPrefix("file:"), 2 to 3)

      val snapshot3 = Table.forPath(engine, tablePath)
        .getSnapshotAsOfVersion(engine, 3).asInstanceOf[SnapshotImpl]
      val crcV3 = ChecksumUtils.computeChecksum(engine, snapshot3.getLogSegment)

      assert(crcV3.getVersion === 3)
      assert(crcV3.getNumFiles === numFilesV1)
      assert(crcV3.getAllFiles.isPresent)
      assert(crcV3.getAllFiles.get().size() === crcV3.getNumFiles)
    }
  }

  test("computeChecksum returns the existing CRCInfo as-is for an already-checksummed " +
    "version, without recomputation") {
    withTempDirAndEngine { (tablePath, engine) =>
      initialTestTable(tablePath, engine)

      val snapshot1 = Table.forPath(
        engine,
        tablePath).getSnapshotAsOfVersion(engine, 1).asInstanceOf[SnapshotImpl]

      // Persist a checksum for v1, then reload so the log segment sees it as lastSeenChecksum.
      ChecksumUtils.computeStateAndWriteChecksum(engine, snapshot1.getLogSegment)
      val reloaded = Table.forPath(
        engine,
        tablePath).getSnapshotAsOfVersion(engine, 1).asInstanceOf[SnapshotImpl]
      assert(reloaded.getLogSegment.getLastSeenChecksum.isPresent)

      // A checksum already exists for exactly this version => returned as-is (no full replay).
      val crcInfo = ChecksumUtils.computeChecksum(engine, reloaded.getLogSegment)
      assert(crcInfo.getVersion === 1)
    }
  }

  test("Create checksum is idempotent") {
    withTempDirAndEngine { (tablePath, engine) =>
      initialTestTable(tablePath, engine)

      val snapshot = Table.forPath(
        engine,
        tablePath).getSnapshotAsOfVersion(engine, 0).asInstanceOf[SnapshotImpl]

      // First call should create the checksum file
      ChecksumUtils.computeStateAndWriteChecksum(engine, snapshot.getLogSegment)
      verifyChecksumForSnapshot(snapshot)

      // Second call should be a no-op (no exception thrown)
      ChecksumUtils.computeStateAndWriteChecksum(engine, snapshot.getLogSegment)
      verifyChecksumForSnapshot(snapshot)
    }
  }

  private def writeCommitWithOperation(
      tablePath: String,
      version: Long,
      operation: String,
      actions: Row*): Unit = {
    val commitInfo = new KernelCommitInfo(
      Optional.empty(),
      version,
      Optional.empty(),
      Optional.ofNullable(operation),
      Collections.emptyMap(),
      Optional.empty(),
      Optional.empty(),
      Collections.emptyMap())
    val json = (createCommitInfoSingleAction(commitInfo.toRow) +: actions)
      .map(JsonUtils.rowToJson).mkString("", "\n", "\n")
    val logPath = Files.createDirectories(Paths.get(tablePath, "_delta_log"))
    Files.write(logPath.resolve(f"$version%020d.json"), json.getBytes(UTF_8))
  }

  private def operationTestMetadata(description: Optional[String]): KernelMetadata =
    new KernelMetadata(
      "crc-operation-test",
      Optional.empty(),
      description,
      new Format(),
      testSchema.toJson,
      testSchema,
      VectorUtils.buildArrayValue(Collections.emptyList(), StringType.STRING),
      Optional.of(0L),
      VectorUtils.stringStringMapValue(Map("delta.enableDeletionVectors" -> "true").asJava))

  private val operationTestMetadata0 = operationTestMetadata(Optional.empty())
  private val operationTestMetadata1 = operationTestMetadata(Optional.of("commit 1"))
  private val operationTestMetadata2 = operationTestMetadata(Optional.of("commit 2"))
  private val operationTestProtocol0 = new KernelProtocol(
    3,
    7,
    Set("deletionVectors").asJava,
    Set("deletionVectors", "domainMetadata").asJava)
  private val operationTestProtocol2 = new KernelProtocol(
    3,
    7,
    Set("deletionVectors").asJava,
    Set("deletionVectors", "domainMetadata", "appendOnly").asJava)
  private val operationTestFiles = (1 to 10).map { i =>
    new KernelAddFile(KernelAddFile.createAddFileRowWithStatsJson(
      s"file-$i.parquet",
      VectorUtils.stringStringMapValue(Collections.emptyMap[String, String]()),
      i * 100L,
      0L,
      true,
      Optional.of(new DeletionVectorDescriptor(
        "p",
        s"file:/deletion_vector_$i.bin",
        Optional.empty(),
        32,
        i)),
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.of("""{"numRecords":100,"tightBounds":false}""")))
  }
  private val operationTestActions0: Seq[Row] = Seq(
    createMetadataSingleAction(operationTestMetadata0.toRow),
    createProtocolSingleAction(operationTestProtocol0.toRow),
    createDomainMetadataSingleAction(new KernelDomainMetadata("keep", "0", false).toRow),
    createDomainMetadataSingleAction(new KernelDomainMetadata("update", "0", false).toRow),
    createDomainMetadataSingleAction(new KernelDomainMetadata("drop", "0", false).toRow),
    createTxnSingleAction(new KernelSetTransaction("shared", 0L, Optional.of(0L)).toRow),
    createTxnSingleAction(new KernelSetTransaction("unchanged", 0L, Optional.of(0L)).toRow)) ++
    operationTestFiles.take(6).map(file => createAddFileSingleAction(file.toRow))
  private val operationTestActions1: Seq[Row] = Seq(
    createAddFileSingleAction(operationTestFiles(6).toRow),
    createAddFileSingleAction(operationTestFiles(7).toRow),
    createRemoveFileSingleAction(operationTestFiles(0).toRemoveFileRow(true, Optional.of(1L))),
    createRemoveFileSingleAction(operationTestFiles(1).toRemoveFileRow(true, Optional.of(1L))),
    createDomainMetadataSingleAction(new KernelDomainMetadata("update", "1", false).toRow),
    createDomainMetadataSingleAction(new KernelDomainMetadata("drop", "1", false).toRow),
    createDomainMetadataSingleAction(new KernelDomainMetadata("new", "1", false).toRow),
    createTxnSingleAction(new KernelSetTransaction("shared", 1L, Optional.of(1L)).toRow),
    createMetadataSingleAction(operationTestMetadata1.toRow))
  private val operationTestActions2: Seq[Row] = Seq(
    createAddFileSingleAction(operationTestFiles(8).toRow),
    createAddFileSingleAction(operationTestFiles(9).toRow),
    createRemoveFileSingleAction(operationTestFiles(2).toRemoveFileRow(true, Optional.of(2L))),
    createRemoveFileSingleAction(operationTestFiles(3).toRemoveFileRow(true, Optional.of(2L))),
    createDomainMetadataSingleAction(new KernelDomainMetadata("update", "2", false).toRow),
    createDomainMetadataSingleAction(new KernelDomainMetadata("drop", "2", true).toRow),
    createTxnSingleAction(new KernelSetTransaction("shared", 2L, Optional.of(2L)).toRow),
    createMetadataSingleAction(operationTestMetadata2.toRow),
    createProtocolSingleAction(operationTestProtocol2.toRow))

  private def operationTestCrcFixture(
      tableSizeBytes: Long,
      numFiles: Long,
      numDeletedRecords: Long,
      numDeletionVectors: Long,
      allFiles: Option[Seq[KernelAddFile]],
      version: Long = 2L): CRCInfo = {
    // Every fixture file fits in the first bin; use the explicit expected totals for that bin.
    val histogram = FileSizeHistogram.createDefaultHistogram().captureFileSizeHistogramResult()
    histogram.getFileCounts()(0) = numFiles
    histogram.getTotalBytes()(0) = tableSizeBytes
    new CRCInfo(
      version,
      operationTestMetadata2,
      new KernelProtocol(
        3,
        7,
        Set("deletionVectors").asJava,
        Set("deletionVectors", "domainMetadata", "appendOnly").asJava),
      tableSizeBytes,
      numFiles,
      Optional.empty(), // txnId
      Optional.of(Set(
        new KernelDomainMetadata("keep", "0", false),
        new KernelDomainMetadata("update", "2", false),
        new KernelDomainMetadata("new", "1", false)).asJava),
      Optional.of(FileSizeHistogram.fromFileSizeHistogramResult(histogram)),
      Optional.empty(), // inCommitTimestamp
      Optional.of(Seq(
        new KernelSetTransaction("shared", 2L, Optional.of(2L)),
        new KernelSetTransaction("unchanged", 0L, Optional.of(0L))).asJava),
      Optional.of(numDeletedRecords),
      Optional.of(numDeletionVectors),
      Optional.ofNullable(allFiles.map(_.asJava).orNull))
  }

  private val includeAllCrc = operationTestCrcFixture(
    4500L,
    6L,
    45L,
    6L,
    Some(Seq(4, 5, 8, 9, 6, 7).map(operationTestFiles)))
  private val ignoreCommit1AddsCrc = operationTestCrcFixture(3000L, 4L, 30L, 4L, None)
  private val ignoreCommit1RemovesCrc = operationTestCrcFixture(4800L, 8L, 48L, 8L, None)
  private val ignoreCommit2AddsCrc = operationTestCrcFixture(2600L, 4L, 26L, 4L, None)
  private val ignoreCommit2RemovesCrc = operationTestCrcFixture(5200L, 8L, 52L, 8L, None)

  test("FULL mode using incremental computation with respect to operation types") {
    val normalOperations = Seq(
      "WRITE",
      "MERGE",
      "UPDATE",
      "DELETE",
      "TRUNCATE",
      "OPTIMIZE",
      "CREATE TABLE",
      "REPLACE TABLE",
      "CREATE TABLE AS SELECT",
      "REPLACE TABLE AS SELECT",
      "CREATE OR REPLACE TABLE AS SELECT",
      "ADD COLUMNS",
      "DROP COLUMNS",
      "RENAME COLUMN",
      "CHANGE COLUMN",
      "CHANGE COLUMNS",
      "REPLACE COLUMNS",
      "UPDATE SCHEMA",
      "REMOVE COLUMN MAPPING",
      "SET TBLPROPERTIES",
      "ADD CONSTRAINT",
      "DROP CONSTRAINT",
      "UPGRADE PROTOCOL",
      "RECOMPRESSION",
      "REORG",
      "VACUUM START",
      "VACUUM END",
      "REPAIR METADATA",
      "CLUSTER BY",
      "Empty Commit",
      "DOMAIN METADATA CLEANUP")
    val ignoreAddOperations = Seq(
      "COMPUTE STATS",
      "ROW TRACKING BACKFILL",
      "ROW TRACKING UNBACKFILL",
      "DROP FEATURE",
      "LIQUID BACKFILL",
      "LIQUID TAGGING",
      "REPLACE PARTITIONED BY WITH CLUSTER BY")
    val ignoreRemoveOperations = Seq("Deletion Vector Tombstones")
    val cases = normalOperations.map(op => (op, op, includeAllCrc)) ++
      ignoreAddOperations.flatMap { op =>
        Seq((op, "WRITE", ignoreCommit1AddsCrc), ("WRITE", op, ignoreCommit2AddsCrc))
      } ++ ignoreRemoveOperations.flatMap { op =>
        Seq((op, "WRITE", ignoreCommit1RemovesCrc), ("WRITE", op, ignoreCommit2RemovesCrc))
      }

    cases.foreach { case (operation1, operation2, expectedCrc) =>
      withClue(s"commit 1: $operation1, commit 2: $operation2: ") {
        withTempDirAndMetricsEngine { (path, engine) =>
          writeCommitWithOperation(path, 0, "CREATE TABLE", operationTestActions0: _*)
          Table.forPath(engine, path).checksum(engine, 0)
          writeCommitWithOperation(path, 1, operation1, operationTestActions1: _*)
          writeCommitWithOperation(path, 2, operation2, operationTestActions2: _*)
          val snapshot = Table.forPath(engine, path).getSnapshotAsOfVersion(engine, 2)
          assert(snapshot.getStatistics.getChecksumWriteMode.get() === ChecksumWriteMode.FULL)
          engine.resetMetrics()
          snapshot.writeChecksum(engine, ChecksumWriteMode.FULL)
          assertMetrics(engine, Seq(2, 1), Nil, expChecksumReadSet = Seq(0))
          val writtenCrc = ChecksumReader.tryReadChecksumFile(
            engine,
            FileStatus.of(Paths.get(path, "_delta_log", f"${2L}%020d.crc").toString)).get()
          assert(writtenCrc === expectedCrc)
        }
      }
    }
  }

  test("Ignore-add operation list only applies to CRC aggregate stats, not allFiles") {
    val files = Seq(8, 9, 6, 7, 4, 5).map(operationTestFiles)
    val analyzedFiles = files.take(3).map { file =>
      new KernelAddFile(KernelAddFile.createAddFileRowWithStatsJson(
        file.getPath,
        file.getPartitionValues,
        file.getSize,
        file.getModificationTime,
        false,
        file.getDeletionVector,
        file.getTags,
        file.getBaseRowId,
        file.getDefaultRowCommitVersion,
        Optional.of("""{"numRecords":100,"tightBounds":false,"minValues":{"id":0},""" +
          """"maxValues":{"id":99},"nullCount":{"id":0}}""")))
    }
    val expectedCrc = operationTestCrcFixture(
      4500L,
      6L,
      45L,
      6L,
      Some(analyzedFiles ++ files.drop(3)),
      version = 3L)

    withTempDirAndMetricsEngine { (path, engine) =>
      writeCommitWithOperation(path, 0, "CREATE TABLE", operationTestActions0: _*)
      writeCommitWithOperation(path, 1, "WRITE", operationTestActions1: _*)
      writeCommitWithOperation(path, 2, "WRITE", operationTestActions2: _*)
      Table.forPath(engine, path).checksum(engine, 2)

      // ANALYZE ... COMPUTE DELTA STATISTICS re-adds the same (path, DV) with new statistics.
      writeCommitWithOperation(
        path,
        3,
        "COMPUTE STATS",
        analyzedFiles.map(file => createAddFileSingleAction(file.toRow)): _*)
      val snapshot = Table.forPath(engine, path).getSnapshotAsOfVersion(engine, 3)
      assert(snapshot.getStatistics.getChecksumWriteMode.get() === ChecksumWriteMode.FULL)
      engine.resetMetrics()
      snapshot.writeChecksum(engine, ChecksumWriteMode.FULL)
      assertMetrics(engine, Seq(3), Nil, expChecksumReadSet = Seq(2))
      val writtenCrc = ChecksumReader.tryReadChecksumFile(
        engine,
        FileStatus.of(Paths.get(path, "_delta_log", f"${3L}%020d.crc").toString)).get()
      assert(writtenCrc === expectedCrc)
      // CRCInfo equality compares only numRecords within file statistics.
      assert(writtenCrc.getAllFiles.get().asScala.map(_.getStatsJson) ===
        expectedCrc.getAllFiles.get().asScala.map(_.getStatsJson))
    }
  }

  test("FULL CRC falls back to full log replay for unsupported operations") {
    val expectedCrc = operationTestCrcFixture(
      4500L,
      6L,
      45L,
      6L,
      Some(Seq(8, 9, 6, 7, 4, 5).map(operationTestFiles)))
    Seq(
      "Manual Update",
      "COPY INTO",
      "UNSET TBLPROPERTIES",
      "CREATE OR REPLACE TABLE",
      "UNKNOWN OPERATION",
      "",
      null)
      .foreach { operation =>
        withClue(s"unsupported operation ${Option(operation)}: ") {
          withTempDirAndMetricsEngine { (path, engine) =>
            // GIVEN: a readable base CRC with corrupted aggregate counters.
            writeCommitWithOperation(path, 0, "CREATE TABLE", operationTestActions0: _*)
            Table.forPath(engine, path).checksum(engine, 0)
            val crcPath = Paths.get(path, "_delta_log", f"${0L}%020d.crc")
            val corruptedCrc = OBJ_MAPPER.readTree(Files.readAllBytes(crcPath))
              .asInstanceOf[ObjectNode]
            corruptedCrc.put("tableSizeBytes", 999999L)
            corruptedCrc.put("numDeletedRecordsOpt", 999L)
            corruptedCrc.put("numDeletionVectorsOpt", 999L)
            engine.getFileSystemClient.delete(crcPath.toString)
            Files.write(crcPath, OBJ_MAPPER.writeValueAsBytes(corruptedCrc))
            assert(ChecksumReader.tryReadChecksumFile(engine, FileStatus.of(crcPath.toString))
              .get().getTableSizeBytes === 999999L)

            // WHEN: an unsupported operation appears after the base CRC.
            writeCommitWithOperation(path, 1, operation, operationTestActions1: _*)
            writeCommitWithOperation(path, 2, "WRITE", operationTestActions2: _*)
            val snapshot = Table.forPath(engine, path).getSnapshotAsOfVersion(engine, 2)
            snapshot.writeChecksum(engine, ChecksumWriteMode.FULL)

            // THEN: full log replay recovers the correct CRC without using the corrupted counters.
            val writtenCrc = ChecksumReader.tryReadChecksumFile(
              engine,
              FileStatus.of(Paths.get(path, "_delta_log", f"${2L}%020d.crc").toString)).get()
            assert(writtenCrc === expectedCrc)
          }
        }
      }
  }

  test("test checksum -- no checksum, with checkpoint") {
    withTableWithCrc { (table, _, engine) =>
      // Need to use HadoopFs to delete file to avoid fs throwing checksum mismatch on read.
      deleteChecksumFileForTableUsingHadoopFs(table.getPath(engine).stripPrefix("file:"), (0 to 11))
      engine.resetMetrics()
      table.checksum(engine, 11)
      assertMetrics(
        engine,
        Seq(11),
        Seq(10),
        Seq(1),
        expChecksumReadSet = Nil)
      verifyChecksumForSnapshot(table.getSnapshotAsOfVersion(engine, 11))
    }
  }

  test("test checksum -- stale checksum without file size histogram" +
    ", no checkpoint => fall back to full state construction") {
    withTableWithCrc { (table, _, engine) =>
      deleteChecksumFileForTableUsingHadoopFs(table.getPath(engine), (5 to 8))
      engine.resetMetrics()
      table.checksum(engine, 8)
      assertMetrics(
        engine,
        Seq(8, 7, 6, 5, 4, 3, 2, 1, 0),
        Nil,
        Nil,
        expChecksumReadSet = Seq(4))
      verifyChecksumForSnapshot(table.getSnapshotAsOfVersion(engine, 8))
    }
  }

  test("test checksum -- stale checksum, no checkpoint => incrementally load from checksum") {
    withTableWithCrc { (table, _, engine) =>
      deleteChecksumFileForTableUsingHadoopFs(table.getPath(engine).stripPrefix("file:"), (5 to 8))
      // Spark generated CRC from Spark doesn't include file size histogram, regenerate it.
      table.checksum(engine, 5)
      engine.resetMetrics()
      table.checksum(engine, 8)
      assertMetrics(
        engine,
        Seq(8, 7, 6),
        Nil,
        Nil,
        expChecksumReadSet = Seq(5))
      verifyChecksumForSnapshot(table.getSnapshotAsOfVersion(engine, 8))
    }
  }

  test("test checksum -- stale checksum, checkpoint after checksum " +
    "=> checkpoint with log replay") {
    withTableWithCrc { (table, _, engine) =>
      deleteChecksumFileForTableUsingHadoopFs(table.getPath(engine), (5 to 11))
      engine.resetMetrics()
      table.checksum(engine, 11)
      assertMetrics(
        engine,
        Seq(11),
        Seq(10),
        Seq(1),
        expChecksumReadSet = Nil)
      verifyChecksumForSnapshot(table.getSnapshotAsOfVersion(engine, 11))
    }
  }

  test("test checksum -- not allowlisted operation => fallback with full state construction") {
    withTableWithCrc { (table, path, engine) =>
      val deltaLog = DeltaLog.forTable(spark, new Path(path))
      deltaLog
        .startTransaction()
        .commitManuallyWithValidation(
          CommitInfo(
            time = 12345,
            operation = "MANUAL UPDATE",
            inCommitTimestamp = Some(12345),
            operationParameters = Map.empty,
            commandContext = Map.empty,
            readVersion = Some(11),
            isolationLevel = None,
            isBlindAppend = None,
            operationMetrics = None,
            userMetadata = None,
            tags = None,
            txnId = None),
          deltaLog.getSnapshotAt(11).allFiles.head().copy(dataChange = false).wrap.unwrap)
      deleteChecksumFileForTableUsingHadoopFs(
        table.getPath(engine).stripPrefix("file:"),
        Seq(11, 12))
      table.checksum(engine, 11)
      engine.resetMetrics()
      table.checksum(engine, 12)
      assertMetrics(
        engine,
        Seq(12, 11),
        Seq(10),
        Seq(1),
        // Tries to incrementally load CRC but fall back with unable to handle
        // Add file without data change(compute stats).
        expChecksumReadSet = Seq(11))
      verifyChecksumForSnapshot(table.getSnapshotAsOfVersion(engine, 12))
    }
  }

  test("test checksum -- removeFile without Stats => fallback with full state construction") {
    withTableWithCrc { (table, path, engine) =>
      val deltaLog = DeltaLog.forTable(spark, new Path(path))
      deltaLog
        .startTransaction()
        .commitManuallyWithValidation(
          CommitInfo(
            time = 12345,
            operation = "REPLACE TABLE",
            inCommitTimestamp = Some(12345),
            operationParameters = Map.empty,
            commandContext = Map.empty,
            readVersion = Some(11),
            isolationLevel = None,
            isBlindAppend = None,
            operationMetrics = None,
            userMetadata = None,
            tags = None,
            txnId = None),
          deltaLog.getSnapshotAt(11).allFiles.head().remove.copy(size = None).wrap.unwrap)
      // Spark generated CRC from Spark doesn't include file size histogram
      deleteChecksumFileForTableUsingHadoopFs(
        table.getPath(engine).stripPrefix("file:"),
        Seq(11, 12))
      table.checksum(engine, 11)
      engine.resetMetrics()
      table.checksum(engine, 12)
      assertMetrics(
        engine,
        Seq(12, 11),
        Seq(10),
        Seq(1),
        // Tries to incrementally load CRC but fall back with unable to handle
        // Remove file without stats.
        expChecksumReadSet = Seq(11))
      verifyChecksumForSnapshot(table.getSnapshotAsOfVersion(engine, 12))
    }
  }

  test("test checksum -- Optimize => incrementally build from CRC") {
    withTableWithCrc { (table, path, engine) =>
      spark.sql(s"OPTIMIZE delta.`$path`")
      // Spark generated CRC from Spark doesn't include file size histogram
      deleteChecksumFileForTableUsingHadoopFs(
        table.getPath(engine).stripPrefix("file:"),
        Seq(11, 12))
      table.checksum(engine, 11)
      engine.resetMetrics()
      table.checksum(engine, 12)
      assertMetrics(
        engine,
        Seq(12),
        Nil,
        Nil,
        expChecksumReadSet = Seq(11))
      verifyChecksumForSnapshot(table.getSnapshotAsOfVersion(engine, 12))
    }
  }

  test("test checksum -- replace table updating p&m, domain metadata is picked up") {
    withTableWithCrc { (table, path, engine) =>
      // Spark generated CRC from Spark doesn't include file size histogram
      deleteChecksumFileForTableUsingHadoopFs(
        table.getPath(engine).stripPrefix("file:"),
        Seq(11))
      table.checksum(engine, 11)
      getReplaceTxn(
        engine,
        path,
        new StructType().add(
          "a",
          StringType.STRING),
        clusteringColsOpt = Some(Seq(new Column("a"))),
        withDomainMetadataSupported = true).commit(engine, emptyIterable[Row])
      engine.resetMetrics()
      table.checksum(engine, 12)
      assertMetrics(
        engine,
        Seq(12),
        Nil,
        Nil,
        expChecksumReadSet = Seq(11))
      verifyChecksumForSnapshot(table.getSnapshotAsOfVersion(engine, 12))
    }
  }

  test("test checksum -- commit info not in the first action => " +
    "fallback with full state construction") {
    withTableWithCrc { (table, path, engine) =>
      val deltaLog = DeltaLog.forTable(spark, new Path(path))
      deltaLog
        .startTransaction()
        .commitManuallyWithValidation(
          deltaLog.getSnapshotAt(11).allFiles.head().remove.wrap.unwrap,
          CommitInfo(
            time = 12345,
            operation = "REPLACE TABLE",
            inCommitTimestamp = Some(12345),
            operationParameters = Map.empty,
            commandContext = Map.empty,
            readVersion = Some(11),
            isolationLevel = None,
            isBlindAppend = None,
            operationMetrics = None,
            userMetadata = None,
            tags = None,
            txnId = None).wrap.unwrap)
      // Spark generated CRC from Spark doesn't include file size histogram
      deleteChecksumFileForTableUsingHadoopFs(
        table.getPath(engine).stripPrefix("file:"),
        Seq(11, 12))
      table.checksum(engine, 11)
      engine.resetMetrics()
      table.checksum(engine, 12)
      assertMetrics(
        engine,
        Seq(12, 11),
        Seq(10),
        Seq(1),
        expChecksumReadSet = Seq(11))
      verifyChecksumForSnapshot(table.getSnapshotAsOfVersion(engine, 12))
    }
  }

  test("test checksum -- commit info missing => fallback with full state construction") {
    withTableWithCrc { (table, path, engine) =>
      val deltaLog = DeltaLog.forTable(spark, new Path(path))
      deltaLog
        .startTransaction()
        .commitManuallyWithValidation(
          deltaLog.getSnapshotAt(11).allFiles.head().remove.wrap.unwrap)
      // Spark generated CRC from Spark doesn't include file size histogram
      deleteChecksumFileForTableUsingHadoopFs(
        table.getPath(engine).stripPrefix("file:"),
        Seq(11, 12))
      table.checksum(engine, 11)
      engine.resetMetrics()
      table.checksum(engine, 12)
      assertMetrics(
        engine,
        Seq(12, 11),
        Seq(10),
        Seq(1),
        expChecksumReadSet = Seq(11))
      verifyChecksumForSnapshot(table.getSnapshotAsOfVersion(engine, 12))
    }
  }

  test("test checksum -- crc missing domain metadata => fallback with full state construction") {
    withTableWithCrc { (table, path, engine) =>
      // Spark generated CRC from Spark doesn't include file size histogram
      deleteChecksumFileForTableUsingHadoopFs(
        table.getPath(engine).stripPrefix("file:"),
        Seq(11))
      rewriteChecksumFileToExcludeDomainMetadata(engine, path, 10)
      engine.resetMetrics()
      table.checksum(engine, 11)
      assertMetrics(
        engine,
        Seq(11),
        Seq(10),
        Seq(1),
        expChecksumReadSet = Seq(10))
      verifyChecksumForSnapshot(table.getSnapshotAsOfVersion(engine, 11))
    }
  }
}
