/*
 * Copyright (2021) The Delta Lake Project Authors.
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
package org.apache.spark.sql.delta.optimize

import java.io.File

import scala.concurrent.duration.Duration

import org.apache.spark.SparkException
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.concurrency.PhaseLockingTestMixin
import org.apache.spark.sql.delta.concurrency.TransactionExecutionTestMixin
import org.apache.spark.sql.delta.fuzzer.{OptimisticTransactionPhases, PhaseLockingTransactionExecutionObserver}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

class OptimizeConflictSuite extends QueryTest
  with SharedSparkSession
  with PhaseLockingTestMixin
  with TransactionExecutionTestMixin
  with DeltaSQLCommandTest {

  protected def appendRows(dir: File, numRows: Int, numFiles: Int): Unit = {
    spark.range(start = 0, end = numRows, step = 1, numPartitions = numFiles)
      .write.format("delta").mode("append").save(dir.getAbsolutePath)
  }

  test("conflict handling between Optimize and Business Txn") {
    withTempDir { tempDir =>

      // Create table with 100 rows.
      appendRows(tempDir, numRows = 100, numFiles = 10)

      // Enable DVs.
      sql(s"ALTER TABLE delta.`${tempDir.toString}` " +
        "SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);")
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tempDir.getAbsolutePath)

      def optimizeTxn(): Array[Row] = {
        deltaTable.optimize().executeCompaction()
        Array.empty
      }

      def deleteTxn(): Array[Row] = {
        // Delete 50% of the rows.
        sql(s"DELETE FROM delta.`${tempDir}` WHERE id%2 = 0").collect()
      }

      val Seq(future) = runFunctionsWithOrderingFromObserver(Seq(optimizeTxn)) {
        case (optimizeObserver :: Nil) =>
          // Create a replacement observer for the retry thread of Optimize.
          val retryObserver = new PhaseLockingTransactionExecutionObserver(
            OptimisticTransactionPhases.forName("test-replacement-txn"))

          // Block Optimize during the first commit attempt.
          optimizeObserver.setNextObserver(retryObserver, autoAdvance = true)
          unblockUntilPreCommit(optimizeObserver)
          busyWaitFor(optimizeObserver.phases.preparePhase.hasEntered, timeout)

          // Delete starts and finishes
          deleteTxn()

          // Allow Optimize to resume.
          unblockCommit(optimizeObserver)
          busyWaitFor(optimizeObserver.phases.commitPhase.hasLeft, timeout)
          optimizeObserver.phases.postCommitPhase.exitBarrier.unblock()

          // The first txn will not commit as there was a conflict commit
          // (deleteTxn). Optimize will attempt to auto resolve and retry
          // Wait for the retry txn to finish.
          // Resume the retry txn.
          unblockAllPhases(retryObserver)
      }
      val e = intercept[SparkException] {
        ThreadUtils.awaitResult(future, timeout)
      }
      // The retry txn should fail as the same files are modified(DVs added) by
      // the delete txn.
      assert(e.getCause.getMessage.contains("DELTA_CONCURRENT_DELETE_READ"))
      assert(sql(s"SELECT * FROM delta.`${tempDir}`").count() == 50)
    }
  }
}
