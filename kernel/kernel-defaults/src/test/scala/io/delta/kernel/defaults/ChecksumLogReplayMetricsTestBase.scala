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

import io.delta.kernel.Table
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames

import org.apache.spark.sql.delta.sources.DeltaSQLConf

/**
 * Base trait for testing log replay optimizations when reading tables with checksum files.
 * This trait contains common test setup and test cases but allows specific metadata types
 * to customize how they load and verify data.
 *
 * Test subclasses implement specialized behavior for loading different items:
 * - PandMCheckSumLogReplayMetricsSuite - tests Protocol and Metadata loading
 * - DomainMetadataCheckSumReplayMetricsSuite - tests Domain Metadata loading
 */
trait ChecksumLogReplayMetricsTestBase extends LogReplayBaseSuite {

  /////////////////////////
  // Test Helper Methods //
  /////////////////////////

  // Abstract method to be implemented by concrete test classes
  protected def loadSnapshotFieldsCheckMetrics(
      table: Table,
      engine: MetricsEngine,
      expJsonVersionsRead: Seq[Long],
      expParquetVersionsRead: Seq[Long],
      expParquetReadSetSizes: Seq[Long],
      expChecksumReadSet: Seq[Long],
      readVersion: Long = -1): Unit

  // Method to adjust list of versions of checkpoint file read.
  // For example, if crc is missing and P&M is loaded from checkpoint.
  // Domain metadata will load from checkpoint as well.
  protected def getExpectedCheckpointReadSize(size: Seq[Long]): Seq[Long] = size

  ///////////
  // Tests //
  ///////////

  Seq(-1L, 0L, 3L, 4L).foreach { readVersion => // -1 means latest version
    test(
      s"checksum found at the read version: ${if (readVersion == -1) "latest" else readVersion}") {
      withTableWithCrc { (table, _, engine) =>
        loadSnapshotFieldsCheckMetrics(
          table,
          engine,
          // shouldn't need to read commit or checkpoint files as P&M/DM are found through checksum
          expJsonVersionsRead = Nil,
          expParquetVersionsRead = Nil,
          expParquetReadSetSizes = Nil,
          expChecksumReadSet = Seq(if (readVersion == -1) 11 else readVersion),
          readVersion)
      }
    }
  }

  test(
    "checksum not found at read version and checkpoint exists at read version => use checkpoint") {
    withTableWithCrc { (table, tablePath, engine) =>
      val checkpointVersion = 10
      deleteChecksumFileForTable(tablePath, Seq(checkpointVersion))

      loadSnapshotFieldsCheckMetrics(
        table,
        engine,
        // 10.crc missing, 10.checkpoint.parquet exists.
        // Attempt to read 10.crc fails and read 10.checkpoint.parquet succeeds.
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Seq(10),
        expParquetReadSetSizes = getExpectedCheckpointReadSize(Seq(1)),
        expChecksumReadSet = Nil,
        readVersion = 10)
    }
  }

  test(
    "checksum missing read version & the previous version, " +
      "checkpoint exists the read version and the previous version => use checkpoint") {
    withTableWithCrc { (table, tablePath, engine) =>
      val checkpointVersion = 10
      deleteChecksumFileForTable(tablePath, Seq(checkpointVersion, checkpointVersion + 1))

      // 11.crc, 10.crc missing, 10.checkpoint.parquet exists.
      // Attempt to read 11.crc fails and read 10.checkpoint.parquet and 11.json succeeds.
      loadSnapshotFieldsCheckMetrics(
        table,
        engine,
        expJsonVersionsRead = Seq(11),
        expParquetVersionsRead = Seq(10),
        expParquetReadSetSizes = getExpectedCheckpointReadSize(Seq(1)),
        expChecksumReadSet = Nil,
        readVersion = 11)
    }
  }

  test("crc found at read version and checkpoint at read version => use checksum") {
    withTableWithCrc { (table, _, engine) =>
      loadSnapshotFieldsCheckMetrics(
        table,
        engine,
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        expChecksumReadSet = Seq(10),
        readVersion = 10)
    }
  }

  test("checksum not found at the read version, but found at a previous version") {
    withTableWithCrc { (table, tablePath, engine) =>
      deleteChecksumFileForTable(tablePath, Seq(10, 11, 5, 6))

      loadSnapshotFieldsCheckMetrics(
        table,
        engine,
        expJsonVersionsRead = Seq(11),
        expParquetVersionsRead = Seq(10),
        expParquetReadSetSizes = getExpectedCheckpointReadSize(Seq(1)),
        expChecksumReadSet = Nil)

      loadSnapshotFieldsCheckMetrics(
        table,
        engine,
        // We find the checksum from crc at version 4, but still read commit files 5 and 6
        // to find the P&M which could have been updated in version 5 and 6.
        expJsonVersionsRead = Seq(6, 5),
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        expChecksumReadSet = Seq(4),
        readVersion = 6)

      // now try to load version 3 and it should get P&M from checksum files only
      loadSnapshotFieldsCheckMetrics(
        table,
        engine,
        // We find the checksum from crc at version 3, so shouldn't read anything else
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        expChecksumReadSet = Seq(3),
        readVersion = 3)
    }
  }

  test(
    "checksum missing read version, " +
      "both checksum and checkpoint exist the read version the previous version => use checksum") {
    withTableWithCrc { (table, tablePath, engine) =>
      val checkpointVersion = 10
      val readVersion = checkpointVersion + 1
      deleteChecksumFileForTable(tablePath, Seq(checkpointVersion + 1))

      // 11.crc, missing, 10.crc and 10.checkpoint.parquet exist.
      // read 10.crc and 11.json.
      loadSnapshotFieldsCheckMetrics(
        table,
        engine,
        expJsonVersionsRead = Seq(readVersion),
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        expChecksumReadSet = Seq(checkpointVersion),
        readVersion = readVersion)
    }
  }
}
