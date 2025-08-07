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

import io.delta.kernel.Table
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.engine.{DefaultEngine, DefaultJsonHandler}
import io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO
import io.delta.kernel.engine.JsonHandler
import io.delta.kernel.exceptions.MaxCommitRetryLimitReachedException
import io.delta.kernel.utils.CloseableIterable.emptyIterable
import io.delta.kernel.utils.CloseableIterator

import org.apache.hadoop.conf.Configuration

class TransactionCommitLoopSuite extends DeltaTableWriteSuiteBase {

  // TODO: Refactor this test suite to use both Table.forPath().getLatestSnapshot() and
  //       TableManager.loadSnapshot()

  private val fileIO = new HadoopFileIO(new Configuration())

  test("Txn will attempt to re-commit *next* version on CFE(isRetryable=true, isConflict=true)") {
    import org.apache.spark.sql.functions.col
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialTxn = createWriteTxnBuilder(table)
        .withSchema(engine, testSchema)
        .build(engine)
      commitTransaction(initialTxn, engine, emptyIterable()) // 000.json

      val kernelTxn = createWriteTxnBuilder(table).withMaxRetries(5).build(engine)

      // Create 001.json. This will make the engine throw a FileAlreadyExistsException when trying
      // to write 001.json. The default committer will turn this into a
      // CFE(isRetryable=true, isConflict=true).

      spark.range(0, 10)
        .select(col("id").cast("int"))
        .write.format("delta").mode("append").save(tablePath)

      val result = commitTransaction(kernelTxn, engine, emptyIterable())

      assert(result.getVersion == 2)
      assert(result.getTransactionReport.getTransactionMetrics.getNumCommitAttempts == 2)
    }
  }

  test("Txn will attempt to re-commit *same* version on CFE(isRetryable=true, isConflict=false)") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialTxn = createWriteTxnBuilder(table).withSchema(engine, testSchema).build(engine)
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
            // The default committer will turn this into a CFE(isRetryable=true, isConflict=true)
            throw new java.io.IOException("Transient network error")
          }
          super.writeJsonFileAtomically(filePath, data, overwrite)
        }
      }

      class CustomEngine extends DefaultEngine(fileIO) {
        override def getJsonHandler: JsonHandler = new CustomJsonHandler()
      }

      val transientErrorEngine = new CustomEngine()
      val txn = createWriteTxnBuilder(table).build(transientErrorEngine)
      val result = commitTransaction(txn, transientErrorEngine, emptyIterable())

      assert(result.getVersion == 1)
      assert(attemptCount == attemptNumberToSucceedAt)
      assert(attemptedFilePaths.size == 1) // we should only be attempting to write 001.json
      assert(result.getTransactionReport.getTransactionMetrics.getNumCommitAttempts ==
        attemptNumberToSucceedAt)
    }
  }

  // TODO: Transaction will fail on CFE(isRetryable=false, isConflict=true/false). The default
  //       committer doesn't throw this error type. We could test this with a custom committer, but
  //       currently our API to create transactions just use Table::getLatestSnapshot(), and is not
  //       yet properly connected to the SnapshotBuilder.withCommitter code.

  test("Txn will throw MaxCommitRetryLimitReachedException on too many retries") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialTxn = createWriteTxnBuilder(table).withSchema(engine, testSchema).build(engine)
      commitTransaction(initialTxn, engine, emptyIterable()) // 000.json

      class AlwaysFailingJsonHandler extends DefaultJsonHandler(fileIO) {
        override def writeJsonFileAtomically(
            filePath: String,
            data: CloseableIterator[Row],
            overwrite: Boolean): Unit = {
          throw new java.io.IOException("Transient network error")
        }
      }

      class AlwaysFailingEngine extends DefaultEngine(fileIO) {
        override def getJsonHandler: JsonHandler = new AlwaysFailingJsonHandler()
      }

      val alwaysFailingEngine = new AlwaysFailingEngine()
      val txn = createWriteTxnBuilder(table).withMaxRetries(10).build(alwaysFailingEngine)

      val exMsg = intercept[MaxCommitRetryLimitReachedException] {
        commitTransaction(txn, alwaysFailingEngine, emptyIterable())
      }.getMessage

      assert(exMsg.contains("Commit attempt for version 1 failed with a retryable exception but " +
        "will not be retried because the maximum number of retries (10) has been reached."))
    }
  }

  test("Txn will throw if it sees a CFE(true, false) followed by a CFE(true, true)") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialTxn = createWriteTxnBuilder(table).withSchema(engine, testSchema).build(engine)
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
            // The default committer will turn this into a CFE(isRetryable=true, isConflict=true)
            throw new java.io.IOException("Transient network error")
          } else {
            throw new FileAlreadyExistsException("001.json already exists")
          }
        }
      }

      class CustomEngine extends DefaultEngine(fileIO) {
        override def getJsonHandler: JsonHandler = new CustomJsonHandler()
      }

      val transientErrorEngine = new CustomEngine()
      val txn = createWriteTxnBuilder(table).build(transientErrorEngine)

      val ex = intercept[Exception] {
        commitTransaction(txn, transientErrorEngine, emptyIterable())
      }
      assert(ex.getMessage.contains("Commit attempt 2 for table version 1 failed due to a " +
        "concurrent write conflict. However, a previous commit attempt, call it C, also failed " +
        "without conflict, and was then retried."))
    }
  }

}
