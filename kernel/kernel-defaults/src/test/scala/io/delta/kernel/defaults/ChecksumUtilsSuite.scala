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
import java.util.Optional

import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._

import io.delta.kernel.Table
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.WriteUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.{Column, Literal}
import io.delta.kernel.internal.{SnapshotImpl, TableImpl}
import io.delta.kernel.internal.checksum.ChecksumUtils
import io.delta.kernel.internal.util.ManualClock
import io.delta.kernel.types.{StringType, StructType}
import io.delta.kernel.utils.CloseableIterable.emptyIterable

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{CommitInfo, SetTransaction}

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
