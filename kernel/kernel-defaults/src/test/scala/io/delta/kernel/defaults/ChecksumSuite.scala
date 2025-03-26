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

import scala.collection.immutable.Seq
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
      val addFiles = Seq(
        new DataFileStatus("/path/to/file1", 1, 100, Optional.empty()),
        new DataFileStatus("/path/to/file2", 1025, 100, Optional.empty()))
      val expectedFileSizeHistogram = FileSizeHistogram.createDefaultHistogram()
      expectedFileSizeHistogram.insert(1)
      expectedFileSizeHistogram.insert(1025)

      // Generate append actions
      val writeContext = Transaction.getWriteContext(
        defaultEngine,
        txnState,
        Map.empty[String, Literal].asJava /* partitionValues */ )
      val actions = inMemoryIterable(
        Transaction.generateAppendActions(
          defaultEngine,
          txnState,
          toCloseableIterator(addFiles.iterator.asJava),
          writeContext))

      // Commit transaction and collect P&M
      val commitResult = commitTransaction(txn, engine, actions)
      val latestSnapshot =
        Table.forPath(engine, tablePath).getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      val expectedMetadata = latestSnapshot.getMetadata
      val expectedProtocol = latestSnapshot.getProtocol

      // Invoke post-commit hooks including the CRC_SIMPLE
      commitResult.getPostCommitHooks.forEach(hook => hook.threadSafeInvoke(engine))

      // Verify checksum exists and content are correct.
      val crcInfo =
        Option(ChecksumReader.getCRCInfo(engine, new Path(tablePath + "/_delta_log"), 0L, 0L))
          .filter(_.isPresent)
          .map(_.get()).getOrElse {
            fail("CRC information should be present")
          }
      assert(crcInfo.getNumFiles === 2)
      assert(crcInfo.getTableSizeBytes === 1026)
      assert(crcInfo.getFileSizeHistogram === Optional.of(expectedFileSizeHistogram))
      assert(crcInfo.getMetadata === expectedMetadata)
      assert(crcInfo.getProtocol === expectedProtocol)
    }
  }
}
