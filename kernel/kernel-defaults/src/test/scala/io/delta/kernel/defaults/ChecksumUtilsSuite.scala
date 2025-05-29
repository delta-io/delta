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

import io.delta.kernel.Table
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.checksum.ChecksumUtils
import io.delta.kernel.internal.util.ManualClock

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
      // Need to use HadoopFs to delete file to avoid fs throwing checksum mismatch on
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

  test("test checksum -- stale checksum, no checkpoint") {
    withTableWithCrc { (table, _, engine) =>
      deleteChecksumFileForTable(table.getPath(engine).stripPrefix("file:"), (5 to 8))
      engine.resetMetrics()
      table.checksum(engine, 8)
      assertMetrics(
        engine,
        Seq(8, 7, 6, 5, 4, 3, 2, 1, 0),
        Nil,
        Nil,
        expChecksumReadSet = Nil)
      verifyChecksumForSnapshot(table.getSnapshotAsOfVersion(engine, 8))
    }
  }

  test("test checksum -- stale checksum, checkpoint after checksum" +
    " => use checkpoint for full replay") {
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
}
