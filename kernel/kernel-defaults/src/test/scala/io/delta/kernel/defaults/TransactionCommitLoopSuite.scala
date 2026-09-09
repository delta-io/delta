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

import java.nio.file.FileAlreadyExistsException

import scala.collection.immutable.Seq

import io.delta.kernel.Table
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.engine.{DefaultEngine, DefaultJsonHandler}
import io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO
import io.delta.kernel.defaults.utils.{AbstractWriteUtils, WriteUtilsWithV1Builders, WriteUtilsWithV2Builders}
import io.delta.kernel.engine.JsonHandler
import io.delta.kernel.exceptions.{CommitStateUnknownException, MaxCommitRetryLimitReachedException}
import io.delta.kernel.expressions.Literal
import io.delta.kernel.utils.{CloseableIterator, FileStatus}
import io.delta.kernel.utils.CloseableIterable.emptyIterable

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class TransactionCommitLoopTransactionBuilderV1Suite extends AbstractTransactionCommitLoopSuite
    with WriteUtilsWithV1Builders {}

class TransactionCommitLoopTransactionBuilderV2Suite extends AbstractTransactionCommitLoopSuite
    with WriteUtilsWithV2Builders {}

trait AbstractTransactionCommitLoopSuite extends AnyFunSuite { self: AbstractWriteUtils =>

  private val fileIO = new HadoopFileIO(new Configuration())

  test("Txn attempts to commit *next* version on CFE(isRetryable=true, isConflict=true)") {
    withTempDirAndEngine { (tablePath, engine) =>
      val initialTxn = getCreateTxn(engine, tablePath, testSchema)
      commitTransaction(initialTxn, engine, emptyIterable()) // 000.json

      val kernelTxn = getUpdateTxn(engine, tablePath, maxRetries = 5)

      // Create 001.json. This will make the engine throw a FileAlreadyExistsException when trying
      // to write 001.json. The default committer will turn this into a
      // CFE(isRetryable=true, isConflict=true).
      appendData(engine, tablePath, data = Seq(Map.empty[String, Literal] -> dataBatches1))

      val result = commitTransaction(kernelTxn, engine, emptyIterable())

      assert(result.getVersion == 2)
      assert(result.getTransactionReport.getTransactionMetrics.getNumCommitAttempts == 2)
    }
  }

  test("Txn attempts to commit *same* version on CFE(isRetryable=true, isConflict=false)") {
    withTempDirAndEngine { (tablePath, engine) =>
      val initialTxn = getCreateTxn(engine, tablePath, testSchema)
      commitTransaction(initialTxn, engine, emptyIterable()) // 000.json

      var attemptCount = 0 // Will be incremented when actual writeJson attempt occurs
      val attemptNumberToSucceedAt = 5
      val attemptedFilePaths = scala.collection.mutable.Set[String]()

      class CustomJsonHandler extends DefaultJsonHandler(fileIO) {
        override def writeJsonFileAtomically(
            filePath: String,
            data: CloseableIterator[Row],
            overwrite: Boolean): Unit = {
          attemptCount += 1
          attemptedFilePaths += filePath
          if (attemptCount < attemptNumberToSucceedAt) {
            // The default committer will turn this into a CFE(isRetryable=true, isConflict=false)
            throw new java.io.IOException("Transient network error")
          }
          super.writeJsonFileAtomically(filePath, data, overwrite)
        }
      }

      class CustomEngine extends DefaultEngine(fileIO) {
        val jsonHandler = new CustomJsonHandler()
        override def getJsonHandler: JsonHandler = jsonHandler
      }

      val transientErrorEngine = new CustomEngine()
      val txn = getUpdateTxn(transientErrorEngine, tablePath)
      val result = commitTransaction(txn, transientErrorEngine, emptyIterable())

      assert(result.getVersion == 1)
      assert(attemptCount == attemptNumberToSucceedAt)
      assert(attemptedFilePaths.size == 1) // we should only be attempting to write 001.json
      assert(result.getTransactionReport.getTransactionMetrics.getNumCommitAttempts ==
        attemptNumberToSucceedAt)
    }
  }

  test("Txn throws MaxCommitRetryLimitReachedException on too many retries") {
    withTempDirAndEngine { (tablePath, engine) =>
      val initialTxn = getCreateTxn(engine, tablePath, testSchema)
      commitTransaction(initialTxn, engine, emptyIterable()) // 000.json

      class CustomJsonHandler extends DefaultJsonHandler(fileIO) {
        override def writeJsonFileAtomically(
            filePath: String,
            data: CloseableIterator[Row],
            overwrite: Boolean): Unit = {
          // The default committer will turn this into a CFE(isRetryable=true, isConflict=false)
          throw new java.io.IOException("Transient network error")
        }
      }

      class AlwaysFailingEngine extends DefaultEngine(fileIO) {
        val jsonHandler = new CustomJsonHandler()
        override def getJsonHandler: JsonHandler = jsonHandler
      }

      val alwaysFailingEngine = new AlwaysFailingEngine()
      val txn = getUpdateTxn(alwaysFailingEngine, tablePath, maxRetries = 10)

      val exMsg = intercept[MaxCommitRetryLimitReachedException] {
        commitTransaction(txn, alwaysFailingEngine, emptyIterable())
      }.getMessage

      assert(exMsg.contains("Commit attempt for version 1 failed with a retryable exception but " +
        "will not be retried because the maximum number of retries (10) has been reached."))
    }
  }

  test("Txn throws CommitStateUnknownException if it sees CFE(true,false) then CFE(true,true)") {
    withTempDirAndEngine { (tablePath, engine) =>
      val initialTxn = getCreateTxn(engine, tablePath, testSchema)
      commitTransaction(initialTxn, engine, emptyIterable()) // 000.json

      // This tests the case of:
      // - first commit attempt: We succeed at writing 001.json, BUT a transient network error
      //   occurs, so Kernel txn sees a failure.
      // - second commit attempt: We try again to write 001.json, but we see that it already exists!
      //   For now, we just throw, but in the future we could try detecting if that 001.json was
      //   written by us on the previous attempt, or written by another writer.

      class CustomJsonHandler extends DefaultJsonHandler(fileIO) {
        var attemptCount = 0 // Will be incremented when actual writeJson attempt occurs

        override def writeJsonFileAtomically(
            filePath: String,
            data: CloseableIterator[Row],
            overwrite: Boolean): Unit = {
          attemptCount += 1

          if (attemptCount == 1) {
            // The default committer will turn this into a CFE(isRetryable=true, isConflict=false)
            throw new java.io.IOException("Transient network error")
          } else {
            // The default committer will turn this into a CFE(isRetryable=true, isConflict=true)
            throw new FileAlreadyExistsException("001.json already exists")
          }
        }
      }

      class CustomEngine extends DefaultEngine(fileIO) {
        private val jsonHandler = new CustomJsonHandler()
        override def getJsonHandler: JsonHandler = jsonHandler
      }

      val transientErrorEngine = new CustomEngine()
      val txn = getUpdateTxn(transientErrorEngine, tablePath)

      val exMsg = intercept[CommitStateUnknownException] {
        commitTransaction(txn, transientErrorEngine, emptyIterable())
      }.getMessage
      assert(exMsg.contains("Commit attempt 2 for version 1 failed due to a concurrent write " +
        "conflict after a previous retry."))
    }
  }

  // TODO: Transaction will fail on CFE(isRetryable=false, isConflict=true/false). The default
  //       committer doesn't throw this error type. We could test this with a custom committer, but
  //       currently our API to create transactions just use Table::getLatestSnapshot(), and is not
  //       yet properly connected to the SnapshotBuilder.withCommitter code.

  test("Txn will *not* retry on non-IOException RuntimeException") {
    withTempDirAndEngine { (tablePath, engine) =>
      val initialTxn = getCreateTxn(engine, tablePath, testSchema)
      commitTransaction(initialTxn, engine, emptyIterable()) // 000.json

      class CustomJsonHandler extends DefaultJsonHandler(fileIO) {
        override def writeJsonFileAtomically(
            filePath: String,
            data: CloseableIterator[Row],
            overwrite: Boolean): Unit = {
          // The default committer doesn't explicitly turn this into a CFE
          throw new RuntimeException("Non-retryable error")
        }
      }

      class CustomEngine extends DefaultEngine(fileIO) {
        val jsonHandler = new CustomJsonHandler()
        override def getJsonHandler: JsonHandler = jsonHandler
      }

      val alwaysFailingEngine = new CustomEngine()

      val txn = getUpdateTxn(alwaysFailingEngine, tablePath)

      val ex = intercept[RuntimeException] {
        commitTransaction(txn, alwaysFailingEngine, emptyIterable())
      }
      assert(ex.getMessage.contains("Non-retryable error"))
    }
  }

  test("On successive conflicts, each rebase pass reads only new winning commits") {
    withTempDirAndEngine { (tablePath, engine) =>
      commitTransaction(getCreateTxn(engine, tablePath, testSchema), engine, emptyIterable())

      // The losing transaction reads at version 0.
      val kernelTxn = getUpdateTxn(engine, tablePath, maxRetries = 5)

      // Pre-write v1 so that the first write attempt conflicts immediately.
      appendData(engine, tablePath, data = Seq.empty) // v1

      // Track every path passed to listFrom so we can inspect which version each
      // conflict-resolution pass starts from.
      val listFromPaths = scala.collection.mutable.ArrayBuffer[String]()

      class TrackingFileIO extends HadoopFileIO(new Configuration()) {
        override def listFrom(filePath: String): CloseableIterator[FileStatus] = {
          listFromPaths += filePath
          super.listFrom(filePath)
        }
      }
      val trackingFileIO = new TrackingFileIO()

      // The handler throws FAEE for the first two write attempts and succeeds on the third
      var writeAttempts = 0
      class TrackingEngine extends DefaultEngine(trackingFileIO) {
        override def getJsonHandler: JsonHandler = new DefaultJsonHandler(trackingFileIO) {
          override def writeJsonFileAtomically(
              filePath: String,
              data: CloseableIterator[Row],
              overwrite: Boolean): Unit = {
            writeAttempts += 1
            writeAttempts match {
              case 1 =>
                data.close()
                // Attempt at v1 (pre-existing) — throw conflict to trigger pass 1.
                throw new FileAlreadyExistsException(filePath)
              case 2 =>
                data.close()
                // After pass 1 rebased to v2, commit a real v2 via the plain engine before
                // throwing, so the second pass's listing from v2 finds a real commit file.
                appendData(engine, tablePath, data = Seq.empty)
                throw new FileAlreadyExistsException(filePath)
              case _ =>
                // Attempt at v3 — write successfully.
                super.writeJsonFileAtomically(filePath, data, overwrite)
            }
          }
        }
      }

      val trackingEngine = new TrackingEngine()
      val result = commitTransaction(kernelTxn, trackingEngine, emptyIterable())
      assert(result.getVersion == 3, s"Expected committed to v3, got v${result.getVersion}")
      assert(result.getTransactionReport.getTransactionMetrics.getNumCommitAttempts == 3)

      val conflictListPaths = listFromPaths
        .filter(p => p.contains("_delta_log") && p.endsWith(".json"))

      assert(
        conflictListPaths.length >= 2,
        s"Expected >= 2 conflict-resolution listFrom calls, got: $conflictListPaths")

      // With the fix: pass 1 lists from 001.json (attemptVersion=1), pass 2 lists from 002.json
      // (attemptVersion=2).  Without the fix, pass 2 would also list from 001.json.
      // The final listing call must not start from 001.json.
      val lastListPath = conflictListPaths.last
      assert(
        !lastListPath.endsWith("001.json"),
        s"Second conflict resolution listed from $lastListPath — expected 002.json")
    }
  }

}
