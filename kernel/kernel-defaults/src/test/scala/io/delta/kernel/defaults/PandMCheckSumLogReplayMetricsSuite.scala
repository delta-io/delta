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
        expChecksumReadSet = Seq(2),
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
        expChecksumReadSet = Nil,
        readVersion = 6)
    }
  }
}
