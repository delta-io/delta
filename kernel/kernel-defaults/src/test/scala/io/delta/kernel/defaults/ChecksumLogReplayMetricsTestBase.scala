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

  /**
   * Creates a temporary directory with a test engine and builds a table with CRC files.
   * Returns the created table and engine for testing.
   *
   * @param f code to run with the table and engine
   */
  protected def withTableWithCrc(f: (Table, String, MetricsEngine) => Any): Unit = {
    withTempDirAndMetricsEngine { (path, engine) =>
      // Produce a test table with 0 to 11 .json, 0 to 11.crc, 10.checkpoint.parquet
      withSQLConf(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "true") {
        spark.sql(
          s"CREATE TABLE delta.`$path` USING DELTA AS " +
            s"SELECT 0L as id")
        for (_ <- 0 to 10) { appendCommit(path) }
        assert(checkpointFileExistsForTable(path, 10))
      }
      val table = Table.forPath(engine, path)
      f(table, path, engine)
    }
  }

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
  protected def getExpectedCheckpointReadVersions(versions: Seq[Long]): Seq[Long] = versions

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
          // shouldn't need to read commit or checkpoint files as P&M are found through checksum
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
        expParquetReadSetSizes = getExpectedCheckpointReadVersions(Seq(1)),
        expChecksumReadSet = Seq(10),
        readVersion = 10)
    }
  }

  test(
    "checksum missing read version and the previous version, " +
      "checkpoint exists the read version the previous version => use checkpoint") {
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
        expParquetReadSetSizes = getExpectedCheckpointReadVersions(Seq(1)),
        expChecksumReadSet = Seq(11),
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
}
