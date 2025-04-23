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

/**
 * Suite to test the engine metrics when loading Protocol and Metadata through checksum files.
 */
class PandMCheckSumLogReplayMetricsSuite extends ChecksumLogReplayMetricsTestBase {

  /////////////////////////
  // Test Helper Methods //
  /////////////////////////

  override protected def loadSnapshotFieldsCheckMetrics(
      table: Table,
      engine: MetricsEngine,
      expJsonVersionsRead: Seq[Long],
      expParquetVersionsRead: Seq[Long],
      expParquetReadSetSizes: Seq[Long],
      expChecksumReadSet: Seq[Long],
      readVersion: Long = -1): Unit = {

    loadPandMCheckMetrics(
      table,
      engine,
      expJsonVersionsRead,
      expParquetVersionsRead,
      expParquetReadSetSizes,
      expChecksumReadSet,
      readVersion)
  }

  //////////
  // Test //
  //////////

  test("snapshot hint found for read version and crc found at read version => use hint") {
    withTableWithCrc { (table, _, engine) =>
      table.getLatestSnapshot(engine)

      loadSnapshotFieldsCheckMetrics(
        table,
        engine,
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        expChecksumReadSet = Nil)
    }
  }

  test("checksum not found at the read version, but uses snapshot hint lower bound") {
    withTableWithCrc { (table, tablePath, engine) =>
      // Delete checksum files for versions 3 to 6
      deleteChecksumFileForTable(tablePath, (3 to 6).toSeq)

      loadSnapshotFieldsCheckMetrics(
        table,
        engine,
        // There are no checksum files for versions 4. Latest is at version 2.
        // We need to read the commit files 3 and 4 to get the P&M in addition the P&M from
        // checksum file at version 2
        expJsonVersionsRead = Seq(4, 3),
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        // First attempted to read checksum for version 4 which doesn't exists,
        // then we do a listing of last 100 crc files and read the latest
        // one which is version 2 (as version 3-6 are deleted)
        expChecksumReadSet = Seq(4, 2),
        readVersion = 4)
      // read version 4 which sets the snapshot P&M hint to 4

      // now try to load version 6 and we expect no checksums are read
      loadSnapshotFieldsCheckMetrics(
        table,
        engine,
        // We have snapshot P&M hint at version 4, and no checksum after 2
        expJsonVersionsRead = Seq(6, 5),
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        // First we attempt to read at version 6, then we do a listing of last 100 crc files
        // bound by the snapshot hint which is at version 4 and we don't try to read checksums
        // beyond version 4
        expChecksumReadSet = Seq(6),
        readVersion = 6)
    }
  }

  // TODO: Move to ChecksumLogReplayMetricsTestBase
  // after incremental CRC loading for domain metadata implemented
  test("checksum not found at the read version, but found at a previous version") {
    withTableWithCrc { (table, tablePath, engine) =>
      deleteChecksumFileForTable(tablePath, Seq(10, 11, 5, 6))

      loadSnapshotFieldsCheckMetrics(
        table,
        engine,
        // 10.checkpoint found, so use it and combined with 11.crc
        expJsonVersionsRead = Seq(11),
        expParquetVersionsRead = Seq(10),
        expParquetReadSetSizes = getExpectedCheckpointReadVersions(Seq(1)),
        expChecksumReadSet = Seq(11))

      loadSnapshotFieldsCheckMetrics(
        table,
        engine,
        // We find the checksum from crc at version 4, but still read commit files 5 and 6
        // to find the P&M which could have been updated in version 5 and 6.
        expJsonVersionsRead = Seq(6, 5),
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        // First attempted to read checksum for version 6, then we do a listing of
        // last 100 crc files and read the latest one which is version 4 (as version 5 is deleted)
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

  // TODO: Move to ChecksumLogReplayMetricsTestBase
  // after incremental CRC loading for domain metadata implemented
  test(
    "checksum missing read version, " +
      "both checksum and checkpoint exist the read version the previous version => use checksum") {
    withTableWithCrc { (table, tablePath, engine) =>
      val checkpointVersion = 10
      deleteChecksumFileForTable(tablePath, Seq(checkpointVersion + 1))

      // 11.crc, missing, 10.crc and 10.checkpoint.parquet exist.
      // Attempt to read 11.crc fails and read 10.checkpoint.parquet and 11.json succeeds.
      loadSnapshotFieldsCheckMetrics(
        table,
        engine,
        expJsonVersionsRead = Seq(11),
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        expChecksumReadSet = Seq(10),
        readVersion = 11)
    }
  }
}
