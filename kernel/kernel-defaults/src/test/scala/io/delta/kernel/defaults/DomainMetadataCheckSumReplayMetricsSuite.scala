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

/**
 * Suite to test the engine metrics when loading Domain Metadata through checksum files.
 */
class DomainMetadataCheckSumReplayMetricsSuite extends ChecksumLogReplayMetricsTestBase {

  override protected def loadSnapshotFieldsCheckMetrics(
      table: Table,
      engine: MetricsEngine,
      expJsonVersionsRead: Seq[Long],
      expParquetVersionsRead: Seq[Long],
      expParquetReadSetSizes: Seq[Long],
      expChecksumReadSet: Seq[Long],
      readVersion: Long = -1): Unit = {

    engine.resetMetrics()

    readVersion match {
      case -1 => table.getLatestSnapshot(engine).getDomainMetadata("foo")
      case ver => table.getSnapshotAsOfVersion(engine, ver).getDomainMetadata("foo")
    }

    assertMetrics(
      engine,
      expJsonVersionsRead,
      expParquetVersionsRead,
      expParquetReadSetSizes,
      expChecksumReadSet = expChecksumReadSet)
  }

  // Domain metadata requires reading checkpoint files twice:
  // 1. First read happens during loading Protocol & Metadata in snapshot construction.
  // 2. Second read happens specifically for domain metadata loading.
  override protected def getExpectedCheckpointReadSize(sizes: Seq[Long]): Seq[Long] = {
    // we read each checkpoint file twice: once for P&M and once for domain metadata
    sizes.flatMap(size => Seq(size, size))
  }

  // TODO: Remove to ChecksumLogReplayMetricsTestBase
  // after incremental CRC loading for domain metadata implemented
  test("checksum missing at read version and checkpoint version (readVersion - 1) " +
    " => use checkpoint") {
    withTableWithCrc { (table, tablePath, engine) =>
      val checkpointVersion = 10
      val readVersion = checkpointVersion + 1
      deleteChecksumFileForTable(tablePath, Seq(readVersion))

      loadSnapshotFieldsCheckMetrics(
        table,
        engine,
        expJsonVersionsRead = Seq(readVersion),
        expParquetVersionsRead = Seq(checkpointVersion),
        expParquetReadSetSizes = Seq(1),
        expChecksumReadSet = Seq(checkpointVersion),
        readVersion = readVersion)
    }
  }

  test("checksum doesn't contain domain metadata => read from logs") {
    withTableWithCrc { (table, tablePath, engine) =>
      val readVersion = 5L
      rewriteChecksumFileToExcludeDomainMetadata(engine, tablePath, readVersion)

      loadSnapshotFieldsCheckMetrics(
        table,
        engine,
        expJsonVersionsRead = Seq.range(readVersion, -1, -1),
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        expChecksumReadSet = Seq(readVersion),
        readVersion = readVersion)
    }
  }
}
