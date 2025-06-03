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
import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters.seqAsJavaListConverter

import io.delta.kernel.Table
import io.delta.kernel.data.Row
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.{Column, Literal}
import io.delta.kernel.internal.{SnapshotImpl, TableImpl}
import io.delta.kernel.internal.checksum.ChecksumUtils
import io.delta.kernel.internal.util.ManualClock
import io.delta.kernel.types.{StringType, StructType}
import io.delta.kernel.utils.CloseableIterable.emptyIterable

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.delta.test.DeltaTestImplicits.OptimisticTxnTestHelper

import org.apache.hadoop.fs.Path

/**
 * Test suite for io.delta.kernel.internal.checksum.ChecksumUtils
 */
class ChecksumUtilsSuite extends DeltaTableWriteSuiteBase with LogReplayBaseSuite {

  private def initialTestTable(tablePath: String, engine: Engine): Unit = {
    createEmptyTable(engine, tablePath, testSchema, clock = new ManualClock(0))
    appendData(
      engine,
      tablePath,
      isNewTable = false,
      testSchema,
      partCols = Seq.empty,
      Seq(Map.empty[String, Literal] -> dataBatches1))
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
        .commitManually(
          List(
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
            deltaLog.getSnapshotAt(11).allFiles.head().copy(dataChange = false).wrap.unwrap): _*)
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
        .commitManually(
          List(
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
            deltaLog.getSnapshotAt(11).allFiles.head().remove.copy(size = None).wrap.unwrap): _*)
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

  test("test checksum -- metadata updated and picked up in the crc") {
    withTableWithCrc { (table, path, engine) =>
      spark.sql(s"ALTER TABLE delta.`$path` ADD COLUMNS (b String, c Int)")
      deleteChecksumFileForTableUsingHadoopFs(table.getPath(engine), (5 to 12))
      engine.resetMetrics()
      table.checksum(engine, 12)
      assertMetrics(
        engine,
        Seq(12, 11),
        Seq(10),
        Seq(1),
        expChecksumReadSet = Nil)
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
      Table.forPath(engine, path).asInstanceOf[TableImpl]
        .createReplaceTableTransactionBuilder(engine, "test")
        .withDomainMetadataSupported()
        .withClusteringColumns(engine, Seq(new Column("a")).asJava)
        .withSchema(
          engine,
          new StructType().add(
            "a",
            StringType.STRING)).build(engine)
        .commit(engine, emptyIterable[Row])
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

  test("test checksum -- commit info missing => fallback") {
    withTableWithCrc { (table, path, engine) =>
      val deltaLog = DeltaLog.forTable(spark, new Path(path))
      deltaLog
        .startTransaction()
        .commitManually(
          List(
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
              txnId = None).wrap.unwrap): _*)
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

  test("test checksum -- commit info not in the first action in an version => fallback") {
    withTableWithCrc { (table, path, engine) =>
      val deltaLog = DeltaLog.forTable(spark, new Path(path))
      deltaLog
        .startTransaction()
        .commitManually(
          List(
            deltaLog.getSnapshotAt(11).allFiles.head().remove.wrap.unwrap): _*)
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

  test("test checksum -- checksum missing domain metadata => fallback") {
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
