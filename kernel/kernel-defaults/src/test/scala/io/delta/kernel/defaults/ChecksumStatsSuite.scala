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

import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel.{Table, Transaction, TransactionCommitResult}
import io.delta.kernel.data.Row
import io.delta.kernel.engine.Engine
import io.delta.kernel.hook.PostCommitHook.PostCommitHookType
import io.delta.kernel.internal.{InternalScanFileUtils, SnapshotImpl, TableConfig, TableImpl}
import io.delta.kernel.internal.actions.{AddFile, GenerateIcebergCompatActionUtils, RemoveFile}
import io.delta.kernel.internal.checksum.ChecksumReader
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.stats.FileSizeHistogram
import io.delta.kernel.internal.util.FileNames.checksumFile
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.utils.{CloseableIterable, DataFileStatus, FileStatus}
import io.delta.kernel.utils.CloseableIterable.inMemoryIterable

/**
 * Functional e2e test suite for verifying file stats collection in CRC are correct.
 */
trait ChecksumStatsSuiteBase extends DeltaTableWriteSuiteBase {

  test("Check stats in checksum are correct") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Currently only table with IcebergWriterCompatV1 could easily
      // support both add/remove files.
      val tableProperties = Map(
        TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.getKey -> "true")
      createEmptyTable(engine, tablePath, testSchema, tableProperties = tableProperties)
      val expectedFileSizeHistogram = FileSizeHistogram.createDefaultHistogram()

      val dataFiles = Map("file1.parquet" -> 100L, "file2.parquet" -> 100802L)
      addFiles(engine, tablePath, dataFiles, expectedFileSizeHistogram)
      checkCrcCorrect(
        engine,
        tablePath,
        version = 1,
        expectedFileCount = 2,
        expectedTableSize = 100902,
        expectedFileSizeHistogram = expectedFileSizeHistogram)

      removeFiles(
        engine,
        tablePath,
        Map("file1.parquet" -> 100),
        expectedFileSizeHistogram)
      checkCrcCorrect(
        engine,
        tablePath,
        version = 2,
        expectedFileCount = 1,
        expectedTableSize = 100902 - 100,
        expectedFileSizeHistogram = expectedFileSizeHistogram)
    }
  }

  /**
   * Verifies that the CRC information at the given version matches expectations.
   *
   * @param engine The Delta Kernel engine
   * @param tablePath Path to the Delta table
   * @param version The table version to check
   * @param expectedFileCount Expected number of files in the table
   * @param expectedTableSize Expected total size of all files in bytes
   * @param expectedFileSizeHistogram Expected file size histogram
   */
  def checkCrcCorrect(
      engine: Engine,
      tablePath: String,
      version: Long,
      expectedFileCount: Long,
      expectedTableSize: Long,
      expectedFileSizeHistogram: FileSizeHistogram): Unit = {
    def verifyCrcExistsAndCorrect(): Unit = {
      val crcInfo = ChecksumReader.getCRCInfo(
        engine,
        FileStatus.of(checksumFile(
          new Path(tablePath + "/_delta_log"),
          version).toString))
        .orElseThrow(() => new AssertionError("CRC information should be present"))
      assert(crcInfo.getNumFiles === expectedFileCount)
      assert(crcInfo.getTableSizeBytes === expectedTableSize)
      assert(crcInfo.getFileSizeHistogram === Optional.of(expectedFileSizeHistogram))
    }
    verifyCrcExistsAndCorrect()
    // Delete existing CRC to regenerate a new one from state construction.
    engine.getFileSystemClient.delete(buildCrcPath(tablePath, version).toString)
    Table.forPath(engine, tablePath).checksum(engine, version)
    verifyCrcExistsAndCorrect()
  }

  /**
   * Adds files to the table and updates the expected histogram.
   *
   * @param engine The Delta Kernel engine
   * @param tablePath Path to the Delta table
   * @param filesToAdd Map of file paths to their sizes
   * @param histogram The histogram to update with new file sizes
   */
  protected def addFiles(
      engine: Engine,
      tablePath: String,
      filesToAdd: Map[String, Long],
      histogram: FileSizeHistogram): Unit = {

    val txn = createTxn(engine, tablePath, maxRetries = 0)

    val actionsToCommit = filesToAdd.map { case (path, size) =>
      histogram.insert(size)
      GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
        txn.getTransactionState(engine),
        generateDataFileStatus(tablePath, path, fileSize = size),
        Collections.emptyMap(),
        true /* dataChange */ )
    }.toSeq

    commitTransaction(
      txn,
      engine,
      inMemoryIterable(toCloseableIterator(actionsToCommit.asJava.iterator())))
  }

  /**
   * Removes files from the table and updates the expected histogram.
   *
   * @param engine The Delta Kernel engine
   * @param tablePath Path to the Delta table
   * @param filesToRemove Map of file paths to their sizes
   * @param histogram The histogram to update by removing file sizes
   */
  protected def removeFiles(
      engine: Engine,
      tablePath: String,
      filesToRemove: Map[String, Long],
      histogram: FileSizeHistogram): Unit = {

    val txn = createTxn(engine, tablePath, maxRetries = 0)

    val actionsToCommit = filesToRemove.map { case (path, size) =>
      histogram.remove(size)
      GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1RemoveAction(
        txn.getTransactionState(engine),
        generateDataFileStatus(tablePath, path, fileSize = size),
        Collections.emptyMap(),
        true /* dataChange */ )
    }.toSeq

    commitTransaction(
      txn,
      engine,
      inMemoryIterable(toCloseableIterator(actionsToCommit.asJava.iterator())))
  }

  override def commitTransaction(
      txn: Transaction,
      engine: Engine,
      dataActions: CloseableIterable[Row]): TransactionCommitResult = {
    val result = txn.commit(engine, dataActions)

    // Verify that we don't have both checksum hook types
    val simpleHooks = result.getPostCommitHooks.stream()
      .filter(hook => hook.getType == PostCommitHookType.CHECKSUM_SIMPLE)
      .count()
    val fullHooks = result.getPostCommitHooks.stream()
      .filter(hook => hook.getType == PostCommitHookType.CHECKSUM_FULL)
      .count()
    assert(
      simpleHooks == 0 || fullHooks == 0,
      "Both CHECKSUM_SIMPLE and CHECKSUM_FULL hooks should not be present")

    val checksumHook = result.getPostCommitHooks.stream().filter(hook =>
      hook.getType == getPostCommitHookType).findFirst()
    if (getPostCommitHookType == PostCommitHookType.CHECKSUM_SIMPLE) {
      assert(checksumHook.isPresent, "CHECKSUM_SIMPLE hook should be present")
      // When result.getVersion is 0, there will only no CHECKSUM_FULL.
    } else if (result.getVersion > 0) {
      assert(checksumHook.isPresent, "CHECKSUM_FULL hook should be present for version > 0")
    }
    checksumHook.ifPresent(_.threadSafeInvoke(engine))
    result
  }

  protected def getPostCommitHookType: PostCommitHookType
}

class ChecksumSimpleStatsSuite extends ChecksumStatsSuiteBase {
  override def getPostCommitHookType: PostCommitHookType = PostCommitHookType.CHECKSUM_SIMPLE
}

class ChecksumFullStatsSuite extends ChecksumStatsSuiteBase {
  override def getPostCommitHookType: PostCommitHookType = PostCommitHookType.CHECKSUM_FULL

  // Delete the checksum, so that the subsequent commit will generate CHECKSUM_FULL hook.
  override def addFiles(
      engine: Engine,
      tablePath: String,
      filesToAdd: Map[String, Long],
      histogram: FileSizeHistogram): Unit = {
    val previousVersion = Table.forPath(engine, tablePath).asInstanceOf[TableImpl]
      .getLatestSnapshot(engine).getVersion
    deleteChecksumFileForTable(tablePath.stripPrefix("file:"), Seq(previousVersion.toInt))
    super.addFiles(engine, tablePath, filesToAdd, histogram)
  }

  override def removeFiles(
      engine: Engine,
      tablePath: String,
      filesToRemove: Map[String, Long],
      histogram: FileSizeHistogram): Unit = {
    val previousVersion =
      Table.forPath(engine, tablePath).asInstanceOf[TableImpl].getLatestSnapshot(engine).getVersion
    deleteChecksumFileForTable(tablePath.stripPrefix("file:"), Seq(previousVersion.toInt))
    super.removeFiles(engine, tablePath, filesToRemove, histogram)
  }
}
