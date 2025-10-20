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

import io.delta.kernel.defaults.utils.{AbstractTestUtils, TestUtilsWithLegacyKernelAPIs, TestUtilsWithTableManagerAPIs}

/** Suite to test engine metrics when loading Protocol and Metadata through checksum files. */
class PandMCheckSumLogReplayMetricsSuite extends ChecksumLogReplayMetricsTestBase
    with TestUtilsWithTableManagerAPIs

/** Suite to test engine metrics when loading Protocol and Metadata through checksum files. */
class LegacyPandMCheckSumLogReplayMetricsSuite extends ChecksumLogReplayMetricsTestBase
    with TestUtilsWithLegacyKernelAPIs

/**
 * Base trait for testing log replay optimizations when reading tables with checksum files.
 * This trait contains common test setup and test cases but allows specific metadata types
 * to customize how they load and verify data.
 *
 * Test subclasses implement specialized behavior for loading different items:
 * - PandMCheckSumLogReplayMetricsSuite - tests Protocol and Metadata loading
 * - DomainMetadataCheckSumReplayMetricsSuite - tests Domain Metadata loading
 */
trait ChecksumLogReplayMetricsTestBase extends LogReplayBaseSuite { self: AbstractTestUtils =>

  /////////////////////////
  // Test Helper Methods //
  /////////////////////////

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
      withTableWithCrc { (tablePath, engine) =>
        loadPandMCheckMetrics(
          tablePath,
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
    withTableWithCrc { (tablePath, engine) =>
      val checkpointVersion = 10
      deleteChecksumFileForTable(tablePath, Seq(checkpointVersion))

      loadPandMCheckMetrics(
        tablePath,
        engine,
        // 10.crc missing, 10.checkpoint.parquet exists.
        // Attempt to read 10.crc fails and read 10.checkpoint.parquet succeeds.
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Seq(10),
        expParquetReadSetSizes = getExpectedCheckpointReadSize(Seq(1)),
        expChecksumReadSet = Nil,
        version = 10)
    }
  }

  test(
    "checksum not found at read version but before and after version => use previous version") {
    withTableWithCrc { (tablePath, engine) =>
      deleteChecksumFileForTable(tablePath, Seq(8))
      loadPandMCheckMetrics(
        tablePath,
        engine,
        expJsonVersionsRead = Seq(8),
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        expChecksumReadSet = Seq(7),
        version = 8)
    }
  }

  test(
    "checksum missing read version & the previous version, " +
      "checkpoint exists the read version and the previous version => use checkpoint") {
    withTableWithCrc { (tablePath, engine) =>
      val checkpointVersion = 10
      deleteChecksumFileForTable(tablePath, Seq(checkpointVersion, checkpointVersion + 1))

      // 11.crc, 10.crc missing, 10.checkpoint.parquet exists.
      // Attempt to read 11.crc fails and read 10.checkpoint.parquet and 11.json succeeds.
      loadPandMCheckMetrics(
        tablePath,
        engine,
        expJsonVersionsRead = Seq(11),
        expParquetVersionsRead = Seq(10),
        expParquetReadSetSizes = getExpectedCheckpointReadSize(Seq(1)),
        expChecksumReadSet = Nil,
        version = 11)
    }
  }

  test("crc found at read version and checkpoint at read version => use checksum") {
    withTableWithCrc { (tablePath, engine) =>
      loadPandMCheckMetrics(
        tablePath,
        engine,
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        expChecksumReadSet = Seq(10),
        version = 10)
    }
  }

  test("checksum not found at the read version, but found at a previous version") {
    withTableWithCrc { (tablePath, engine) =>
      deleteChecksumFileForTable(tablePath, Seq(10, 11, 5, 6))

      loadPandMCheckMetrics(
        tablePath,
        engine,
        expJsonVersionsRead = Seq(11),
        expParquetVersionsRead = Seq(10),
        expParquetReadSetSizes = getExpectedCheckpointReadSize(Seq(1)),
        expChecksumReadSet = Nil)

      loadPandMCheckMetrics(
        tablePath,
        engine,
        // We find the checksum from crc at version 4, but still read commit files 5 and 6
        // to find the P&M which could have been updated in version 5 and 6.
        expJsonVersionsRead = Seq(6, 5),
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        expChecksumReadSet = Seq(4),
        version = 6)

      // now try to load version 3 and it should get P&M from checksum files only
      loadPandMCheckMetrics(
        tablePath,
        engine,
        // We find the checksum from crc at version 3, so shouldn't read anything else
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        expChecksumReadSet = Seq(3),
        version = 3)
    }
  }

  test(
    "checksum missing read version, " +
      "both checksum and checkpoint exist the read version the previous version => use checksum") {
    withTableWithCrc { (tablePath, engine) =>
      val checkpointVersion = 10
      val readVersion = checkpointVersion + 1
      deleteChecksumFileForTable(tablePath, Seq(checkpointVersion + 1))

      // 11.crc, missing, 10.crc and 10.checkpoint.parquet exist.
      // read 10.crc and 11.json.
      loadPandMCheckMetrics(
        tablePath,
        engine,
        expJsonVersionsRead = Seq(readVersion),
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        expChecksumReadSet = Seq(checkpointVersion),
        version = readVersion)
    }
  }
}
