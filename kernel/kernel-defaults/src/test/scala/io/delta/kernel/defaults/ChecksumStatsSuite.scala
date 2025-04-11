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

import scala.jdk.CollectionConverters.{asJavaIteratorConverter, mapAsJavaMapConverter, seqAsJavaListConverter, setAsJavaSetConverter}

import io.delta.kernel.{Table, Transaction, TransactionCommitResult}
import io.delta.kernel.data.Row
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.{InternalScanFileUtils, SnapshotImpl, TableConfig, TableImpl}
import io.delta.kernel.internal.actions.{AddFile, GenerateIcebergCompatActionUtils, RemoveFile}
import io.delta.kernel.internal.checksum.ChecksumReader
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.stats.FileSizeHistogram
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.utils.{CloseableIterable, DataFileStatus}
import io.delta.kernel.utils.CloseableIterable.inMemoryIterable

/**
 * Functional e2e test suite for verifying file stats collection in CRC are correct.
 */
class ChecksumStatsSuite extends DeltaTableWriteSuiteBase {

  // Currently only table with IcebergWriterCompatV1 could easily
  // support both add/remove files.
  private val tblPropertiesWithIcebergWriterCompatV1 = Map(
    TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.getKey -> "true")

  test("Check stats in checksum are correct") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        testSchema,
        tableProperties = tblPropertiesWithIcebergWriterCompatV1)
      val expectedFileSizeHistogram = FileSizeHistogram.createDefaultHistogram()

      {
        val txn = createTxn(engine, tablePath, maxRetries = 0)
        val actionsToCommit = Seq(
          GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
            txn.getTransactionState(engine),
            generateDataFileStatus(tablePath, "file1.parquet", fileSize = 100),
            Collections.emptyMap(),
            true /* dataChange */ ),
          GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
            txn.getTransactionState(engine),
            generateDataFileStatus(tablePath, "file2.parquet", fileSize = 100802),
            Collections.emptyMap(),
            true /* dataChange */ ))
        expectedFileSizeHistogram.insert(100)
        expectedFileSizeHistogram.insert(100802)
        commitTransaction(
          txn,
          engine,
          inMemoryIterable(toCloseableIterator(actionsToCommit.asJava.iterator())))
        checkCrcCorrect(
          engine,
          tablePath,
          version = 1,
          expectedFileCount = 2,
          expectedTableSize = 100902,
          expectedFileSizeHistogram = expectedFileSizeHistogram)
      }

      {
        val txn = createTxn(engine, tablePath, maxRetries = 0)
        val actionsToCommit = Seq(
          GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1RemoveAction(
            txn.getTransactionState(engine),
            generateDataFileStatus(tablePath, "file1.parquet", fileSize = 100),
            Collections.emptyMap(),
            true /* dataChange */ ))
        expectedFileSizeHistogram.remove(100)
        commitTransaction(
          txn,
          engine,
          inMemoryIterable(toCloseableIterator(actionsToCommit.asJava.iterator())))
        checkCrcCorrect(
          engine,
          tablePath,
          version = 2,
          expectedFileCount = 2,
          expectedTableSize = 100902,
          expectedFileSizeHistogram = expectedFileSizeHistogram)
      }
    }
  }

  def checkCrcCorrect(
      engine: Engine,
      tablePath: String,
      version: Long,
      expectedFileCount: Long,
      expectedTableSize: Long,
      expectedFileSizeHistogram: FileSizeHistogram): Unit = {
    // Verify checksum exists and content are correct.
    val crcInfo = ChecksumReader.getCRCInfo(
      engine,
      new Path(tablePath + "/_delta_log"),
      version,
      version)
      .orElseThrow(() => new AssertionError("CRC information should be present"))
    assert(crcInfo.getNumFiles === expectedFileCount)
    assert(crcInfo.getTableSizeBytes === expectedTableSize)
    assert(crcInfo.getFileSizeHistogram === Optional.of(expectedFileSizeHistogram))
  }

  def deleteOneFile(engine: Engine, tablePath: String): Unit = {
    val txn = createTxn(engine, tablePath, maxRetries = 0)
    val firstFile =
      InternalScanFileUtils.getAddFileStatus(Table.forPath(
        engine,
        tablePath).getLatestSnapshot(engine).asInstanceOf[
        SnapshotImpl].getScanBuilder.build().getScanFiles(engine).next().getRows.next())
    val actionsToCommit = Seq(
      GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1RemoveAction(
        txn.getTransactionState(engine),
        new DataFileStatus(
          firstFile.getPath,
          firstFile.getSize,
          firstFile.getModificationTime,
          Optional.empty()),
        Collections.emptyMap(),
        true /* dataChange */ ))
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
    result.getPostCommitHooks
      .stream()
      .forEach(hook => hook.threadSafeInvoke(engine))
    result
  }
}
