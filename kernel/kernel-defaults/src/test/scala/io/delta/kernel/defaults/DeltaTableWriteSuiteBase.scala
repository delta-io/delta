/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.util.FileNames.checkpointFileSingular
import io.delta.kernel.{Table, TransactionCommitResult}
import io.delta.kernel.internal.actions.{Metadata, SingleAction}
import io.delta.kernel.internal.fs.{Path => DeltaPath}
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator
import io.delta.kernel.internal.{SnapshotImpl, TableConfig}
import io.delta.kernel.utils.FileStatus
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.VersionNotFoundException
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.Optional
import scala.collection.JavaConverters._

/**
 * Common utility methods for write test suites.
 */
trait DeltaTableWriteSuiteBase extends AnyFunSuite with TestUtils {
  def withTempDirAndEngine(f: (String, Engine) => Unit): Unit = {
    val engine = DefaultEngine.create(new Configuration() {
      {
        // Set the batch sizes to small so that we get to test the multiple batch/file scenarios.
        set("delta.kernel.default.parquet.reader.batch-size", "20");
        set("delta.kernel.default.json.reader.batch-size", "20");
        set("delta.kernel.default.parquet.writer.targetMaxFileSize", "20");
      }
    })
    withTempDir { dir => f(dir.getAbsolutePath, engine) }
  }

  def verifyLastCheckpointMetadata(tablePath: String, checkpointAt: Long, expSize: Long): Unit = {
    val filePath = f"$tablePath/_delta_log/_last_checkpoint"

    val source = scala.io.Source.fromFile(filePath)
    val result = try source.getLines().mkString(",") finally source.close()

    assert(result === s"""{"version":$checkpointAt,"size":$expSize}""")
  }

  /** Helper method to remove the delta files before the given version, to make sure the read is
   * using a checkpoint as base for state reconstruction.
   */
  def deleteDeltaFilesBefore(tablePath: String, beforeVersion: Long): Unit = {
    Seq.range(0, beforeVersion).foreach { version =>
      val filePath = new Path(f"$tablePath/_delta_log/$version%020d.json")
      new Path(tablePath).getFileSystem(new Configuration()).delete(filePath, false /* recursive */)
    }

    // try to query a version < beforeVersion
    val ex = intercept[VersionNotFoundException] {
      spark.read.format("delta").option("versionAsOf", beforeVersion - 1).load(tablePath)
    }
    assert(ex.getMessage().contains(
      s"Cannot time travel Delta table to version ${beforeVersion - 1}"))
  }

  def setCheckpointInterval(tablePath: String, interval: Int): Unit = {
    spark.sql(s"ALTER TABLE delta.`$tablePath` " +
      s"SET TBLPROPERTIES ('delta.checkpointInterval' = '$interval')")
  }

  def dataFileCount(tablePath: String): Int = {
    Files.walk(Paths.get(tablePath)).iterator().asScala
      .count(path => path.toString.endsWith(".parquet") && !path.toString.contains("_delta_log"))
  }

  def checkpointFilePath(tablePath: String, checkpointVersion: Long): String = {
    f"$tablePath/_delta_log/$checkpointVersion%020d.checkpoint.parquet"
  }

  def assertCheckpointExists(tablePath: String, atVersion: Long): Unit = {
    val cpPath = checkpointFilePath(tablePath, checkpointVersion = atVersion)
    assert(new File(cpPath).exists())
  }

  def copyTable(goldenTableName: String, targetLocation: String): Unit = {
    val source = new File(goldenTablePath(goldenTableName))
    val target = new File(targetLocation)
    FileUtils.copyDirectory(source, target)
  }

  def checkpointIfReady(
    engine: Engine,
    tablePath: String,
    result: TransactionCommitResult,
    expSize: Long): Unit = {
    if (result.isReadyForCheckpoint) {
      Table.forPath(engine, tablePath).checkpoint(engine, result.getVersion)
      verifyLastCheckpointMetadata(tablePath, checkpointAt = result.getVersion, expSize)
    }
  }

  def getMetadataActionFromCommit(
    engine: Engine, table: Table, version: Long): Optional[Metadata] = {
    val logPath = new DeltaPath(table.getPath(engine), "_delta_log")
    val file = FileStatus.of(FileNames.deltaFile(logPath, version), 0, 0)
    val columnarBatches =
      engine.getJsonHandler.readJsonFiles(
        singletonCloseableIterator(file),
        SingleAction.FULL_SCHEMA,
        Optional.empty())
    if (!columnarBatches.hasNext) {
      return Optional.empty()
    }
    val metadataVector = columnarBatches.next().getColumnVector(3)
    for (i <- 0 until metadataVector.getSize) {
      if (!metadataVector.isNullAt(i)) {
        return Optional.ofNullable(Metadata.fromColumnVector(metadataVector, i, engine))
      }
    }
    Optional.empty()
  }

  def assertMetadataProp(
    snapshot: SnapshotImpl, key: TableConfig[_ <: Any], expectedValue: Any): Unit = {
    assert(key.fromMetadata(snapshot.getMetadata) == expectedValue)
  }
}
