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

import java.nio.file.{FileAlreadyExistsException, Files}
import java.util.{Collections, Optional}

import scala.jdk.CollectionConverters._

import io.delta.kernel.{Snapshot, Table, Transaction, TransactionCommitResult}
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.engine.{DefaultEngine, DefaultJsonHandler}
import io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO
import io.delta.kernel.defaults.utils.WriteUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.hook.PostCommitHook.PostCommitHookType
import io.delta.kernel.internal.{TableConfig, TableImpl}
import io.delta.kernel.internal.actions.{AddFile, DeletionVectorDescriptor, GenerateIcebergCompatActionUtils, SingleAction}
import io.delta.kernel.internal.checksum.ChecksumReader
import io.delta.kernel.internal.data.TransactionStateRow
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.stats.FileSizeHistogram
import io.delta.kernel.internal.util.FileNames.checksumFile
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.statistics.DataFileStatistics
import io.delta.kernel.utils.{CloseableIterable, CloseableIterator, FileStatus}
import io.delta.kernel.utils.CloseableIterable.inMemoryIterable

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

/**
 * Functional e2e test suite for verifying file stats collection in CRC are correct.
 */
trait ChecksumStatsSuiteBase extends AnyFunSuite with WriteUtils {

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
      val crcInfo = ChecksumReader.tryReadChecksumFile(
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

  /** Verifies the stored CRC, including DV metrics, without regenerating it. */
  def checkCrcCorrect(
      engine: Engine,
      tablePath: String,
      version: Long,
      expectedFileCount: Long,
      expectedTableSize: Long,
      expectedFileSizeHistogram: Option[FileSizeHistogram],
      expectedNumDeletionVectors: Option[Long] = None,
      expectedNumDeletedRecords: Option[Long] = None): Unit = {
    val crcInfo = ChecksumReader.tryReadChecksumFile(
      engine,
      FileStatus.of(checksumFile(new Path(tablePath + "/_delta_log"), version).toString))
      .orElseThrow(() => new AssertionError("CRC information should be present"))
    assert(crcInfo.getNumFiles === expectedFileCount)
    assert(crcInfo.getTableSizeBytes === expectedTableSize)
    assert(crcInfo.getFileSizeHistogram.toScala === expectedFileSizeHistogram)
    assert(crcInfo.getNumDeletionVectors.toScala.map(_.longValue()) === expectedNumDeletionVectors)
    assert(crcInfo.getNumDeletedRecords.toScala.map(_.longValue()) === expectedNumDeletedRecords)
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

    val txn = getUpdateTxn(engine, tablePath, maxRetries = 0)

    val actionsToCommit = filesToAdd.map { case (path, size) =>
      histogram.insert(size)
      GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
        txn.getTransactionState(engine),
        generateDataFileStatus(tablePath, path, fileSize = size),
        Collections.emptyMap(),
        true, /* dataChange */
        Optional.empty())
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

    val txn = getUpdateTxn(engine, tablePath, maxRetries = 0)

    val actionsToCommit = filesToRemove.map { case (path, size) =>
      histogram.remove(size)
      GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1RemoveAction(
        txn.getTransactionState(engine),
        generateDataFileStatus(tablePath, path, fileSize = size),
        Collections.emptyMap(),
        true, /* dataChange */
        Optional.of(TransactionStateRow.getPhysicalSchema(txn.getTransactionState(engine))))
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

class ChecksumSimpleStatsSuite extends ChecksumStatsSuiteBase with LogReplayBaseSuite {
  override def getPostCommitHookType: PostCommitHookType = PostCommitHookType.CHECKSUM_SIMPLE

  private val dvProperties = Map("delta.enableDeletionVectors" -> "true")

  private def addFile(path: String, cardinality: Option[Long] = None): AddFile = {
    val dv = cardinality.map { count =>
      new DeletionVectorDescriptor(
        "p",
        s"/deletion_vector_${path}_$count.bin",
        Optional.empty(),
        32,
        count)
    }
    new AddFile(AddFile.createAddFileRow(
      testSchema,
      path,
      VectorUtils.stringStringMapValue(Collections.emptyMap[String, String]()),
      100L,
      0L,
      true,
      Optional.ofNullable(dv.orNull),
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.of(new DataFileStatistics(
        100L,
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Optional.of(false)))))
  }

  private def addFileToRow(file: AddFile): Row = SingleAction.createAddFileSingleAction(file.toRow)

  private def addFileToRemoveFileRow(file: AddFile): Row =
    SingleAction.createRemoveFileSingleAction(file.toRemoveFileRow(true, Optional.empty()))

  private def actions(rows: Row*): CloseableIterable[Row] =
    inMemoryIterable(toCloseableIterator(rows.toSeq.asJava.iterator()))

  private def commitSimple(
      txn: Transaction,
      engine: Engine,
      rows: Row*): TransactionCommitResult =
    commitTransaction(txn, engine, actions(rows: _*))

  private def histogramForSize100Files(numFiles: Int): FileSizeHistogram = {
    val result = FileSizeHistogram.createDefaultHistogram()
    (0 until numFiles).foreach(_ => result.insert(100L))
    result
  }

  for (supportsDv <- Seq(false, true)) {
    test(s"SIMPLE CRC initializes empty-table DV metrics only with DV support=$supportsDv") {
      withTempDirAndEngine { (path, engine) =>
        commitSimple(
          getCreateTxn(
            engine,
            path,
            testSchema,
            tableProperties = if (supportsDv) dvProperties else Map.empty[String, String]),
          engine)
        checkCrcCorrect(
          engine,
          path,
          0,
          0,
          0,
          Some(histogramForSize100Files(0)),
          expectedNumDeletionVectors = if (supportsDv) Some(0L) else None,
          expectedNumDeletedRecords = if (supportsDv) Some(0L) else None)
      }
    }
  }

  test("SIMPLE CRC omits DV metrics when creating a table with DV supported but not enabled") {
    withTempDirAndEngine { (path, engine) =>
      commitSimple(
        getCreateTxn(
          engine,
          path,
          testSchema,
          tableProperties = Map(
            "delta.feature.deletionVectors" -> "supported",
            "delta.enableDeletionVectors" -> "false")),
        engine)
      assert(getProtocol(engine, path).getReaderFeatures.contains("deletionVectors"))
      checkCrcCorrect(engine, path, 0, 0, 0, Some(histogramForSize100Files(0)))
    }
  }

  test("SIMPLE CRC preserves DV metrics when DV supported but not enabled") {
    withTempDirAndEngine { (path, engine) =>
      val firstDv = addFile("first.parquet", Some(3L))
      commitSimple(
        getCreateTxn(engine, path, testSchema, tableProperties = dvProperties),
        engine,
        addFileToRow(firstDv))
      commitSimple(
        getUpdateTxn(
          engine,
          path,
          tableProperties = Map("delta.enableDeletionVectors" -> "false")),
        engine,
        addFileToRow(addFile("plain.parquet")))
      checkCrcCorrect(
        engine,
        path,
        1,
        2,
        200,
        Some(histogramForSize100Files(2)),
        Some(1L),
        Some(3L))

      commitSimple(getUpdateTxn(engine, path), engine, addFileToRemoveFileRow(firstDv))
      checkCrcCorrect(
        engine,
        path,
        2,
        1,
        100,
        Some(histogramForSize100Files(1)),
        Some(0L),
        Some(0L))
    }
  }

  test("SIMPLE CRC maintains DV metrics across appends, DV updates, and removals") {
    withTempDirAndEngine { (path, engine) =>
      val firstDv = addFile("first.parquet", Some(3L))
      val secondDv = addFile("second.parquet", Some(5L))
      val noDV = addFile("plain.parquet")
      commitSimple(
        getCreateTxn(engine, path, testSchema, tableProperties = dvProperties),
        engine,
        addFileToRow(firstDv))
      checkCrcCorrect(
        engine,
        path,
        0,
        1,
        100,
        Some(histogramForSize100Files(1)),
        Some(1L),
        Some(3L))

      commitSimple(getUpdateTxn(engine, path), engine, addFileToRow(secondDv))
      checkCrcCorrect(
        engine,
        path,
        1,
        2,
        200,
        Some(histogramForSize100Files(2)),
        Some(2L),
        Some(8L))
      commitSimple(getUpdateTxn(engine, path), engine, addFileToRow(noDV))
      checkCrcCorrect(
        engine,
        path,
        2,
        3,
        300,
        Some(histogramForSize100Files(3)),
        Some(2L),
        Some(8L))

      val updatedFirstDv = addFile("first.parquet", Some(7L))
      commitSimple(
        getUpdateTxn(engine, path),
        engine,
        addFileToRemoveFileRow(firstDv),
        addFileToRow(updatedFirstDv))
      checkCrcCorrect(
        engine,
        path,
        3,
        3,
        300,
        Some(histogramForSize100Files(3)),
        Some(2L),
        Some(12L))
      commitSimple(
        getUpdateTxn(engine, path),
        engine,
        addFileToRemoveFileRow(secondDv),
        addFileToRemoveFileRow(noDV))
      checkCrcCorrect(
        engine,
        path,
        4,
        1,
        100,
        Some(histogramForSize100Files(1)),
        Some(1L),
        Some(7L))
      commitSimple(getUpdateTxn(engine, path), engine, addFileToRemoveFileRow(updatedFirstDv))
      checkCrcCorrect(
        engine,
        path,
        5,
        0,
        0,
        Some(histogramForSize100Files(0)),
        Some(0L),
        Some(0L))
    }
  }

  test("Missing previous CRC selects FULL and reconstructs DV metrics") {
    withTempDirAndEngine { (path, engine) =>
      commitSimple(
        getCreateTxn(engine, path, testSchema, tableProperties = dvProperties),
        engine,
        addFileToRow(addFile("old.parquet", Some(9L))))
      deleteChecksumFileForTable(path, Seq(0))
      val result = getUpdateTxn(engine, path).commit(
        engine,
        actions(addFileToRow(addFile("new.parquet", Some(2L)))))
      val checksumHooks = result.getPostCommitHooks.asScala.filter(hook =>
        hook.getType == PostCommitHookType.CHECKSUM_SIMPLE ||
          hook.getType == PostCommitHookType.CHECKSUM_FULL)
      assert(checksumHooks.map(_.getType) === Seq(PostCommitHookType.CHECKSUM_FULL))
      checksumHooks.head.threadSafeInvoke(engine)
      checkCrcCorrect(
        engine,
        path,
        1,
        2,
        200,
        Some(histogramForSize100Files(2)),
        Some(2L),
        Some(11L))
    }
  }

  test("SIMPLE CRC keeps missing DV metrics absent when enabling DV support") {
    withTempDirAndEngine { (path, engine) =>
      commitSimple(
        getCreateTxn(engine, path, testSchema),
        engine,
        addFileToRow(addFile("plain.parquet")))
      commitSimple(
        getUpdateTxn(engine, path, tableProperties = dvProperties),
        engine,
        addFileToRow(addFile("dv.parquet", Some(4L))))
      checkCrcCorrect(engine, path, 1, 2, 200, Some(histogramForSize100Files(2)))
    }
  }

  test("SIMPLE CRC leaves missing DV metrics unknown on a DV table") {
    withTempDirAndEngine { (path, engine) =>
      commitSimple(
        getCreateTxn(engine, path, testSchema, tableProperties = dvProperties),
        engine,
        addFileToRow(addFile("old.parquet", Some(3L))))
      rewriteChecksumFileToExcludeDvMetrics(engine, path, 0)
      commitSimple(
        getUpdateTxn(engine, path),
        engine,
        addFileToRow(addFile("new.parquet", Some(5L))))
      checkCrcCorrect(engine, path, 1, 2, 200, Some(histogramForSize100Files(2)))
    }
  }

  test("SIMPLE CRC correctly preserves stats for retries") {
    withTempDirAndEngine { (path, engine) =>
      commitSimple(
        getCreateTxn(engine, path, testSchema, tableProperties = dvProperties),
        engine,
        addFileToRow(addFile("initial.parquet", Some(3L))))

      val fileIO = new HadoopFileIO(new Configuration())
      var attempts = 0
      val retryEngine = new DefaultEngine(fileIO) {
        private val handler = new DefaultJsonHandler(fileIO) {
          override def writeJsonFileAtomically(
              filePath: String,
              data: CloseableIterator[Row],
              overwrite: Boolean): Unit = {
            if (filePath.endsWith(".json")) {
              attempts += 1
              if (attempts == 1) {
                // A failed attempt can consume actions before discovering the conflict.
                while (data.hasNext) data.next()
                throw new FileAlreadyExistsException(filePath)
              }
            }
            super.writeJsonFileAtomically(filePath, data, overwrite)
          }
        }
        override def getJsonHandler: DefaultJsonHandler = handler
      }
      val losingTxn = getUpdateTxn(retryEngine, path)
      commitSimple(
        getUpdateTxn(engine, path),
        engine,
        addFileToRow(addFile("winner.parquet", Some(5L))))
      val result =
        commitSimple(losingTxn, retryEngine, addFileToRow(addFile("loser.parquet", Some(7L))))
      assert(result.getVersion === 2)
      assert(attempts === 2)
      checkCrcCorrect(engine, path, 2, 3, 300, None, Some(3L), Some(15L))
    }
  }

  test("FULL CRC writing mode can incrementally replay from a CRC with expected fields") {
    withTempDirAndMetricsEngine { (path, engine) =>
      commitSimple(
        getCreateTxn(engine, path, testSchema, tableProperties = dvProperties),
        engine,
        addFileToRow(addFile("first.parquet", Some(3L))))
      commitSimple(getUpdateTxn(engine, path), engine, addFileToRow(addFile("plain.parquet")))
      getUpdateTxn(engine, path).commit(
        engine,
        actions(addFileToRow(addFile("second.parquet", Some(5L)))))

      // Corrupt the base CRC: incremental replay must produce DV metrics of 11 / 35.
      // Full log replay would reconstruct the actual totals of 2 / 8.
      val crcPath = buildCrcPath(path, 1)
      val mapper = new ObjectMapper()
      val baseCrc = mapper.readTree(Files.readAllBytes(crcPath)).asInstanceOf[ObjectNode]
      baseCrc.put("numDeletionVectorsOpt", 10L)
      baseCrc.put("numDeletedRecordsOpt", 30L)
      val output = new HadoopFileIO(new Configuration())
        .newOutputFile(crcPath.toString).create(false)
      try output.write(mapper.writeValueAsBytes(baseCrc))
      finally output.close()

      val snapshot = Table.forPath(engine, path).getLatestSnapshot(engine)
      engine.resetMetrics()
      snapshot.writeChecksum(engine, Snapshot.ChecksumWriteMode.FULL)
      assertMetrics(engine, Seq(2L), Nil, expChecksumReadSet = Seq(1L))
      checkCrcCorrect(
        engine,
        path,
        2,
        3,
        300,
        Some(histogramForSize100Files(3)),
        Some(11L),
        Some(35L))
    }
  }
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
