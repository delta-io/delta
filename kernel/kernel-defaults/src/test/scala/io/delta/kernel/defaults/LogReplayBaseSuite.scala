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
import java.util.Optional

import scala.collection.convert.ImplicitConversions._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.Table
import io.delta.kernel.data.ColumnarBatch
import io.delta.kernel.defaults.engine.{DefaultEngine, DefaultJsonHandler, DefaultParquetHandler}
import io.delta.kernel.defaults.engine.fileio.FileIO
import io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.{Engine, ExpressionHandler, FileSystemClient}
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.internal.checkpoints.Checkpointer
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

/**
 * Base trait containing shared code for log replay metric testing.
 *
 * This trait provides common infrastructure for testing how Delta log files are read
 * during table operations, with utilities for metrics collection and verification.
 */
trait LogReplayBaseSuite extends AnyFunSuite with TestUtils {

  protected def withTempDirAndMetricsEngine(f: (String, MetricsEngine) => Unit): Unit = {
    val hadoopFileIO = new HadoopFileIO(new Configuration() {
      {
        // Set the batch sizes to small so that we get to test the multiple batch scenarios.
        set("delta.kernel.default.parquet.reader.batch-size", "2");
        set("delta.kernel.default.json.reader.batch-size", "2");
      }
    })

    val engine = new MetricsEngine(hadoopFileIO)

    withTempDir { dir =>
      f(dir.getAbsolutePath, engine)
    }
  }

  protected def loadPandMCheckMetrics(
      table: Table,
      engine: MetricsEngine,
      expJsonVersionsRead: Seq[Long],
      expParquetVersionsRead: Seq[Long],
      expParquetReadSetSizes: Seq[Long] = null,
      expChecksumReadSet: Seq[Long] = null,
      version: Long = -1): Unit = {
    engine.resetMetrics()

    version match {
      case -1 => table.getLatestSnapshot(engine).getSchema()
      case ver => table.getSnapshotAsOfVersion(engine, ver).getSchema()
    }

    assertMetrics(
      engine,
      expJsonVersionsRead,
      expParquetVersionsRead,
      expParquetReadSetSizes,
      expChecksumReadSet = expChecksumReadSet)
  }

  protected def assertMetrics(
      engine: MetricsEngine,
      expJsonVersionsRead: Seq[Long],
      expParquetVersionsRead: Seq[Long],
      expParquetReadSetSizes: Seq[Long] = null,
      expLastCheckpointReadCalls: Option[Int] = None,
      expChecksumReadSet: Seq[Long] = null): Unit = {
    val actualJsonVersionsRead = engine.getJsonHandler.getVersionsRead
    val actualParquetVersionsRead = engine.getParquetHandler.getVersionsRead

    assert(
      actualJsonVersionsRead === expJsonVersionsRead,
      s"Expected to read json versions " +
        s"$expJsonVersionsRead but read $actualJsonVersionsRead")
    assert(
      actualParquetVersionsRead === expParquetVersionsRead,
      s"Expected to read parquet " +
        s"versions $expParquetVersionsRead but read $actualParquetVersionsRead")

    if (expParquetReadSetSizes != null) {
      val actualParquetReadSetSizes = engine.getParquetHandler.checkpointReadRequestSizes
      assert(
        actualParquetReadSetSizes === expParquetReadSetSizes,
        s"Expected parquet read set sizes " +
          s"$expParquetReadSetSizes but read $actualParquetReadSetSizes")
    }

    expLastCheckpointReadCalls.foreach { expCalls =>
      val actualCalls = engine.getJsonHandler.getLastCheckpointMetadataReadCalls
      assert(
        actualCalls === expCalls,
        s"Expected to read last checkpoint metadata $expCalls times but read $actualCalls times")
    }

    if (expChecksumReadSet != null) {
      val actualChecksumReadSet = engine.getJsonHandler.checksumsRead
      assert(
        actualChecksumReadSet === expChecksumReadSet,
        s"Expected checksum read set " +
          s"$expChecksumReadSet but read $actualChecksumReadSet")
    }
  }

  protected def appendCommit(path: String): Unit =
    spark.range(10).write.format("delta").mode("append").save(path)
}

////////////////////
// Helper Classes //
////////////////////

/** An engine that records the Delta commit (.json) and checkpoint (.parquet) files read */
class MetricsEngine(fileIO: FileIO) extends Engine {
  private val impl = DefaultEngine.create(fileIO)
  private val jsonHandler = new MetricsJsonHandler(fileIO)
  private val parquetHandler = new MetricsParquetHandler(fileIO)

  def resetMetrics(): Unit = {
    jsonHandler.resetMetrics()
    parquetHandler.resetMetrics()
  }

  override def getExpressionHandler: ExpressionHandler = impl.getExpressionHandler

  override def getJsonHandler: MetricsJsonHandler = jsonHandler

  override def getFileSystemClient: FileSystemClient = impl.getFileSystemClient

  override def getParquetHandler: MetricsParquetHandler = parquetHandler
}

/**
 * Helper trait which wraps an underlying json/parquet read and collects the versions (e.g. 10.json,
 * 10.checkpoint.parquet) read
 */
trait FileReadMetrics { self: Object =>
  // number of times read is requested on `_last_checkpoint`
  private var lastCheckpointMetadataReadCalls = 0

  val checksumsRead = new ArrayBuffer[Long]() // versions of checksum files read

  private val versionsRead = ArrayBuffer[Long]()

  private val compactionVersionsRead = ArrayBuffer[(Long, Long)]()

  // Number of checkpoint files requested read in each readParquetFiles call
  val checkpointReadRequestSizes = new ArrayBuffer[Long]()

  private def updateVersionsRead(fileStatus: FileStatus): Unit = {
    val path = new Path(fileStatus.getPath)
    if (FileNames.isCommitFile(path.getName) || FileNames.isCheckpointFile(path.getName)) {
      val version = FileNames.getFileVersion(path)

      // We may split json/parquet reads, so don't record the same file multiple times
      if (!versionsRead.contains(version)) {
        versionsRead += version
      }
    } else if (Checkpointer.LAST_CHECKPOINT_FILE_NAME.equals(path.getName)) {
      lastCheckpointMetadataReadCalls += 1
    } else if (FileNames.isChecksumFile(path.getName)) {
      checksumsRead += FileNames.getFileVersion(path)
    } else if (FileNames.isLogCompactionFile(path.getName)) {
      val versions = FileNames.logCompactionVersions(path)
      compactionVersionsRead += ((versions._1, versions._2))
    }
  }

  def getVersionsRead: Seq[Long] = versionsRead.toSeq

  def getCompactionsRead: Seq[(Long, Long)] = compactionVersionsRead.toSeq

  def getLastCheckpointMetadataReadCalls: Int = lastCheckpointMetadataReadCalls

  def resetMetrics(): Unit = {
    lastCheckpointMetadataReadCalls = 0
    versionsRead.clear()
    compactionVersionsRead.clear()
    checkpointReadRequestSizes.clear()
    checksumsRead.clear()
  }

  def collectReadFiles(fileIter: CloseableIterator[FileStatus]): CloseableIterator[FileStatus] = {
    fileIter.map(file => {
      updateVersionsRead(file)
      file
    })
  }
}

/** A JsonHandler that collects metrics on the Delta commit (.json) files read */
class MetricsJsonHandler(fileIO: FileIO)
    extends DefaultJsonHandler(fileIO)
    with FileReadMetrics {

  override def readJsonFiles(
      fileIter: CloseableIterator[FileStatus],
      physicalSchema: StructType,
      predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
    super.readJsonFiles(collectReadFiles(fileIter), physicalSchema, predicate)
  }
}

/** A ParquetHandler that collects metrics on the Delta checkpoint (.parquet) files read */
class MetricsParquetHandler(fileIO: FileIO)
    extends DefaultParquetHandler(fileIO)
    with FileReadMetrics {

  override def readParquetFiles(
      fileIter: CloseableIterator[FileStatus],
      physicalSchema: StructType,
      predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
    val fileReadSet = fileIter.toSeq
    checkpointReadRequestSizes += fileReadSet.size
    super.readParquetFiles(
      collectReadFiles(toCloseableIterator(fileReadSet.iterator)),
      physicalSchema,
      predicate)
  }
}
