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
import java.io.File
import java.nio.file.Files

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
   * Creates a test table with checksum files.
   * Produces a table with versions 0 to 11 including .json files, .crc files,
   * and a checkpoint at version 10.
   *
   * @param path Path where the table should be created
   */
  def buildTableWithCrc(path: String): Unit = {
    withSQLConf(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "true") {
      spark.sql(
        s"CREATE TABLE delta.`$path` USING DELTA AS " +
          s"SELECT 0L as id")
      for (_ <- 0 to 10) { appendCommit(path) }
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
      version: Long = -1): Unit

  // Method to adjust checkpoint file read set sizes for different implementations
  protected def getExpectedCheckpointReadSetSizes(sizes: Seq[Long]): Seq[Long] = sizes

  ///////////
  // Tests //
  ///////////

  Seq(-1L, 0L, 3L, 4L).foreach { version => // -1 means latest version
    test(s"checksum found at the read version: ${if (version == -1) "latest" else version}") {
      withTempDirAndMetricsEngine { (path, engine) =>
        // Produce a test table with 0 to 11 .json, 0 to 11.crc, 10.checkpoint.parquet
        buildTableWithCrc(path)
        val table = Table.forPath(engine, path)

        loadSnapshotFieldsCheckMetrics(
          table,
          engine,
          // shouldn't need to read commit or checkpoint files as P&M are found through checksum
          expJsonVersionsRead = Nil,
          expParquetVersionsRead = Nil,
          expParquetReadSetSizes = Nil,
          expChecksumReadSet = Seq(if (version == -1) 11 else version),
          version)
      }
    }
  }

  test(
    "checksum not found at read version and checkpoint exists at read version => use checkpoint") {
    withTempDirAndMetricsEngine { (path, engine) =>
      // Produce a test table with 0 to 11 .json, 0 to 11.crc, 10.checkpoint.parquet
      buildTableWithCrc(path)
      val checkpointVersion = 10;
      val logPath = f"$path/_delta_log"
      assert(
        Files.exists(
          new File(
            FileNames
              .checkpointFileSingular(new Path(logPath), checkpointVersion)
              .toString).toPath))
      assert(
        Files.deleteIfExists(
          new File(FileNames.checksumFile(new Path(logPath), checkpointVersion).toString).toPath))

      val table = Table.forPath(engine, path)

      loadSnapshotFieldsCheckMetrics(
        table,
        engine,
        // 10.crc missing, 10.checkpoint.parquet exists.
        // Attempt to read 10.crc fails and read 10.checkpoint.parquet succeeds.
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Seq(10),
        expParquetReadSetSizes = getExpectedCheckpointReadSetSizes(Seq(1)),
        expChecksumReadSet = Seq(10),
        version = 10)
    }
  }

  test(
    "checksum missing read version and the previous version, " +
      "checkpoint exists the read version the previous version => use checkpoint") {
    withTempDirAndMetricsEngine { (path, engine) =>
      // Produce a test table with 0 to 11 .json, 0 to 11.crc, 10.checkpoint.parquet
      buildTableWithCrc(path)
      val checkpointVersion = 10;
      val logPath = f"$path/_delta_log"
      assert(
        Files
          .exists(
            new File(
              FileNames.checkpointFileSingular(
                new Path(logPath),
                checkpointVersion).toString).toPath))
      assert(
        Files.deleteIfExists(
          new File(FileNames.checksumFile(new Path(logPath), checkpointVersion).toString).toPath))
      assert(
        Files.deleteIfExists(
          new File(
            FileNames.checksumFile(new Path(logPath), checkpointVersion + 1).toString).toPath))

      val table = Table.forPath(engine, path)

      // 11.crc, 10.crc missing, 10.checkpoint.parquet exists.
      // Attempt to read 11.crc fails and read 10.checkpoint.parquet and 11.json succeeds.
      loadSnapshotFieldsCheckMetrics(
        table,
        engine,
        expJsonVersionsRead = Seq(11),
        expParquetVersionsRead = Seq(10),
        expParquetReadSetSizes = getExpectedCheckpointReadSetSizes(Seq(1)),
        expChecksumReadSet = Seq(11),
        version = 11)
    }
  }

  test("crc found at read version and checkpoint at read version => use checksum") {
    withTempDirAndMetricsEngine { (path, engine) =>
      // Produce a test table with 0 to 11 .json, 0 to 11.crc, 10.checkpoint.parquet
      buildTableWithCrc(path)
      val checkpointVersion = 10;
      val logPath = new Path(s"$path/_delta_log")
      assert(
        Files.exists(
          new File(
            FileNames
              .checkpointFileSingular(logPath, checkpointVersion)
              .toString).toPath))
      val table = Table.forPath(engine, path)

      loadSnapshotFieldsCheckMetrics(
        table,
        engine,
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        expChecksumReadSet = Seq(10),
        version = checkpointVersion)
    }
  }
}
