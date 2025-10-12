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
import io.delta.kernel.defaults.utils.{AbstractTestUtils, TestUtilsWithLegacyKernelAPIs, TestUtilsWithTableManagerAPIs}

class LegacyDomainMetadataCheckSumReplayMetricsSuite
    extends AbstractDomainMetadataCheckSumReplayMetricsSuite
    with TestUtilsWithLegacyKernelAPIs {

  // SnapshotHint tests only apply for the legacy APIs since in the new APIs there is no persistent
  // Table instance
  test("read domain metadata fro checksum even if snapshot hint exists") {
    withTableWithCrc { (tablePath, engine) =>
      val readVersion = 11
      val table = Table.forPath(engine, tablePath)
      // Get snapshot to produce a snapshot hit at version 11.
      table.getLatestSnapshot(engine)

      engine.resetMetrics()
      table.getLatestSnapshot(engine).getDomainMetadata("foo")

      assertMetrics(
        engine,
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Seq(),
        expChecksumReadSet = Seq(readVersion))
    }
  }

}

class DomainMetadataCheckSumReplayMetricsSuite
    extends AbstractDomainMetadataCheckSumReplayMetricsSuite
    with TestUtilsWithTableManagerAPIs

/**
 * Suite to test the engine metrics when loading Domain Metadata through checksum files.
 */
trait AbstractDomainMetadataCheckSumReplayMetricsSuite extends ChecksumLogReplayMetricsTestBase {
  self: AbstractTestUtils =>

  override protected def loadPandMCheckMetrics(
      tablePath: String,
      engine: MetricsEngine,
      expJsonVersionsRead: Seq[Long],
      expParquetVersionsRead: Seq[Long],
      expParquetReadSetSizes: Seq[Long],
      expChecksumReadSet: Seq[Long],
      readVersion: Long = -1): Unit = {

    engine.resetMetrics()

    readVersion match {
      case -1 =>
        getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath).getDomainMetadata("foo")
      case ver => getTableManagerAdapter.getSnapshotAtVersion(
          engine,
          tablePath,
          ver).getDomainMetadata("foo")
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

  test("checksum doesn't contain domain metadata => read from logs") {
    withTableWithCrc { (tablePath, engine) =>
      val readVersion = 5L
      rewriteChecksumFileToExcludeDomainMetadata(engine, tablePath, readVersion)

      loadPandMCheckMetrics(
        tablePath,
        engine,
        expJsonVersionsRead = Seq.range(readVersion, -1, -1),
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        expChecksumReadSet = Seq(readVersion),
        readVersion = readVersion)
    }
  }
}
