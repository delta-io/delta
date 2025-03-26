package io.delta.kernel.defaults

import java.util.Optional

import scala.jdk.CollectionConverters.{asJavaIteratorConverter, mapAsJavaMapConverter}

import io.delta.kernel.{Table, Transaction}
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.checksum.ChecksumReader
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.stats.FileSizeHistogram
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.utils.CloseableIterable.inMemoryIterable
import io.delta.kernel.utils.DataFileStatus

/**
 * Functional e2e test suite for verifying checksum correctness.
 */
class ChecksumSuite extends DeltaTableWriteSuiteBase {

  test("Check checksum fields are correct") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create transaction for a new table with test schema
      val txn = createTxn(engine, tablePath, isNewTable = true, testSchema, Seq.empty)
      val txnState = txn.getTransactionState(engine)

      // Create test data files
      val dataFiles = Seq(
        new DataFileStatus("/path/to/file1", 1, 100, Optional.empty()),
        new DataFileStatus("/path/to/file2", 1025, 100, Optional.empty()))
      val expectedHistogram = FileSizeHistogram.createDefaultHistogram()
      expectedHistogram.insert(1)
      expectedHistogram.insert(1025)

      // Generate append actions
      val addedFiles = toCloseableIterator(dataFiles.iterator.asJava)
      val writeContext = Transaction.getWriteContext(
        defaultEngine,
        txnState,
        Map.empty[String, Literal].asJava)
      val actions = inMemoryIterable(
        Transaction.generateAppendActions(defaultEngine, txnState, addedFiles, writeContext))

      // Commit transaction and collect P&M
      val commitResult = commitTransaction(txn, engine, actions)
      val latestSnapshot =
        Table.forPath(engine, tablePath).getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      val expectedMetadata = latestSnapshot.getMetadata
      val expectedProtocol = latestSnapshot.getProtocol

      // Invoke post-commit hooks including the CRC_SIMPLE
      commitResult.getPostCommitHooks.forEach(hook => hook.threadSafeInvoke(engine))

      // Verify checksum exists and extract it
      val crcInfo =
        Option(ChecksumReader.getCRCInfo(engine, new Path(tablePath + "/_delta_log"), 0L, 0L))
          .filter(_.isPresent)
          .map(_.get()).getOrElse {
            fail("Checksum information should be present")
          }

      // Verify file count and total size
      assert(crcInfo.getNumFiles === 2)
      assert(crcInfo.getTableSizeBytes === 1026)

      // Verify file size histogram
      assert(crcInfo.getFileSizeHistogram === Optional.of(expectedHistogram))

      // Verify metadata and protocol match table snapshot
      assert(crcInfo.getMetadata === expectedMetadata)
      assert(crcInfo.getProtocol === expectedProtocol)
    }
  }
}
