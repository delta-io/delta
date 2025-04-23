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
 * Suite to test the engine metrics when loading Protocol and Metadata through checksum files.
 *
 * Tests verify the behavior of log replay when reading tables with checksum files available,
 * focusing on optimizations where checksums provide metadata without reading log files.
 */
class ChecksumLogReplayMetricsSuite extends LogReplayBaseSuite {

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

  /////////////////////////
  // Test Cases          //
  /////////////////////////

  Seq(-1L, 0L, 3L, 4L).foreach { version => // -1 means latest version
    test(s"checksum found at the read version: ${if (version == -1) "latest" else version}") {
      withTempDirAndMetricsEngine { (path, engine) =>
        // Produce a test table with 0 to 11 .json, 0 to 11.crc, 10.checkpoint.parquet
        buildTableWithCrc(path)
        val table = Table.forPath(engine, path)

        loadPandMCheckMetrics(
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

  test("checksum not found at the read version, but found at a previous version") {
    withTempDirAndMetricsEngine { (path, engine) =>
      // Produce a test table with 0 to 11 .json, 0 to 11.crc, 10.checkpoint.parquet
      buildTableWithCrc(path)
      Seq(10L, 11L, 5L, 6L).foreach { version =>
        assert(
          Files.deleteIfExists(
            new File(
              FileNames.checksumFile(new Path(f"$path/_delta_log"), version).toString).toPath))
      }

      loadPandMCheckMetrics(
        Table.forPath(engine, path),
        engine,
        // 10.checkpoint found, so use it and combined with 11.crc
        expJsonVersionsRead = Seq(11),
        expParquetVersionsRead = Seq(10),
        expParquetReadSetSizes = Seq(1),
        expChecksumReadSet = Seq(11))

      loadPandMCheckMetrics(
        Table
          .forPath(engine, path),
        engine,
        // We find the checksum from crc at version 4, but still read commit files 5 and 6
        // to find the P&M which could have been updated in version 5 and 6.
        expJsonVersionsRead = Seq(6, 5),
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        // First attempted to read checksum for version 6, then we do a listing of
        // last 100 crc files and read the latest one which is version 4 (as version 5 is deleted)
        expChecksumReadSet = Seq(6, 4),
        version = 6)

      // now try to load version 3 and it should get P&M from checksum files only
      loadPandMCheckMetrics(
        Table.forPath(engine, path),
        engine,
        // We find the checksum from crc at version 3, so shouldn't read anything else
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        expChecksumReadSet = Seq(3),
        version = 3)
    }
  }

  test("checksum not found at the read version, but uses snapshot hint lower bound") {
    withTempDirAndMetricsEngine { (path, engine) =>
      // Produce a test table with 0 to 11 .json, 0 to 11.crc, 10.checkpoint.parquet
      buildTableWithCrc(path)
      (3 to 6).foreach { version =>
        assert(
          Files.deleteIfExists(
            new File(FileNames.checksumFile(
              new Path(f"$path/_delta_log"),
              version).toString).toPath))
      }

      val table = Table.forPath(engine, path)

      loadPandMCheckMetrics(
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
        version = 4)
      // read version 4 which sets the snapshot P&M hint to 4

      // now try to load version 6 and we expect no checksums are read
      loadPandMCheckMetrics(
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
        version = 6)
    }
  }

  test("snapshot hint found for read version and crc found at read version => use hint") {
    withTempDirAndMetricsEngine { (path, engine) =>
      // Produce a test table with 0 to 11 .json, 0 to 11.crc, 10.checkpoint.parquet
      buildTableWithCrc(path)

      val table = Table.forPath(engine, path)
      table.getLatestSnapshot(engine)

      loadPandMCheckMetrics(
        table,
        engine,
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        expChecksumReadSet = Nil)
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

      loadPandMCheckMetrics(
        table,
        engine,
        // 10.crc missing, 10.checkpoint.parquet exists.
        // Attempt to read 10.crc fails and read 10.checkpoint.parquet succeeds.
        expJsonVersionsRead = Nil,
        expParquetVersionsRead = Seq(10),
        expParquetReadSetSizes = Seq(1),
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
      loadPandMCheckMetrics(
        table,
        engine,
        expJsonVersionsRead = Seq(11),
        expParquetVersionsRead = Seq(10),
        expParquetReadSetSizes = Seq(1),
        expChecksumReadSet = Seq(11),
        version = 11)
    }
  }

  test(
    "checksum missing read version, " +
      "both checksum and checkpoint exist the read version the previous version => use checksum") {
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
      assert(
        Files.deleteIfExists(
          new File(
            FileNames.checksumFile(logPath, checkpointVersion + 1).toString).toPath))

      val table = Table.forPath(engine, path)

      // 11.crc, missing, 10.crc and 10.checkpoint.parquet exist.
      // Attempt to read 11.crc fails and read 10.checkpoint.parquet and 11.json succeeds.
      loadPandMCheckMetrics(
        table,
        engine,
        expJsonVersionsRead = Seq(11),
        expParquetVersionsRead = Nil,
        expParquetReadSetSizes = Nil,
        expChecksumReadSet = Seq(11, 10),
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

      loadPandMCheckMetrics(
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
