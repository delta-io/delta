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

package org.apache.spark.sql.delta.concurrency

import scala.concurrent.duration._

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.fuzzer.{AtomicBarrier, IllegalStateTransitionException, OptimisticTransactionPhases, PhaseLockingTransactionExecutionObserver}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import io.delta.tables.{DeltaTable => IODeltaTable}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Check that [[TransactionExecutionObserver]] is invoked correctly by transactions
 * and commands.
 *
 * Also check the testing tools that use this API.
 */
class TransactionExecutionObserverSuite extends QueryTest with SharedSparkSession
  with DeltaSQLCommandTest
  with PhaseLockingTestMixin {

  val timeout: FiniteDuration = 10000.millis

  test("Phase Locking - sequential") {
    withTempDir { tempFile =>

      val tempPath = tempFile.toString

      spark.range(100).write.format("delta").save(tempPath)

      val observer = new PhaseLockingTransactionExecutionObserver(
        OptimisticTransactionPhases.forName("test-txn"))
      val deltaLog = DeltaLog.forTable(spark, tempPath)

      // get things started
      observer.phases.initialPhase.entryBarrier.unblock()

      assert(!observer.phases.initialPhase.hasEntered)
      TransactionExecutionObserver.withObserver(observer) {
        deltaLog.withNewTransaction { txn =>
          assert(observer.phases.initialPhase.hasEntered)
          assert(observer.phases.initialPhase.hasLeft)
          assert(!observer.phases.preparePhase.hasEntered)
          assert(!observer.phases.commitPhase.hasEntered)

          // allow things to progress
          observer.phases.preparePhase.entryBarrier.unblock()
          observer.phases.commitPhase.entryBarrier.unblock()
          val removedFiles = txn.snapshot.allFiles.collect().map(_.remove).toSeq
          txn.commit(removedFiles, DeltaOperations.ManualUpdate)

          assert(observer.phases.preparePhase.hasEntered)
          assert(observer.phases.preparePhase.hasLeft)
          assert(observer.phases.commitPhase.hasEntered)
          assert(observer.phases.commitPhase.hasLeft)
        }
      }
      val res = spark.read.format("delta").load(tempPath).collect()
      assert(res.isEmpty)
    }
  }

  test("Phase Locking - parallel") {
    withTempDir { tempFile =>

      val tempPath = tempFile.toString

      spark.range(100).write.format("delta").save(tempPath)

      val observer = new PhaseLockingTransactionExecutionObserver(
        OptimisticTransactionPhases.forName("test-txn"))
      val deltaLog = DeltaLog.forTable(spark, tempPath)

      val testThread = new Thread(() => {
        // make sure the transaction will use our observer
        TransactionExecutionObserver.withObserver(observer) {
          deltaLog.withNewTransaction { txn =>
            val removedFiles = txn.snapshot.allFiles.collect().map(_.remove).toSeq
            txn.commit(removedFiles, DeltaOperations.ManualUpdate)
          }
        }
      })
      testThread.start()

      busyWaitForState(
        observer.phases.initialPhase.entryBarrier, AtomicBarrier.State.Requested, timeout)

      // get things started
      observer.phases.initialPhase.entryBarrier.unblock()

      busyWaitFor(observer.phases.initialPhase.hasEntered, timeout)
      busyWaitFor(observer.phases.initialPhase.hasLeft, timeout)
      assert(!observer.phases.preparePhase.hasEntered)

      observer.phases.preparePhase.entryBarrier.unblock()
      busyWaitFor(observer.phases.preparePhase.hasEntered, timeout)
      busyWaitFor(observer.phases.preparePhase.hasLeft, timeout)
      assert(!observer.phases.commitPhase.hasEntered)

      observer.phases.commitPhase.entryBarrier.unblock()
      busyWaitFor(observer.phases.commitPhase.hasEntered, timeout)
      busyWaitFor(observer.phases.commitPhase.hasLeft, timeout)
      testThread.join(timeout.toMillis)
      assert(!testThread.isAlive) // should have passed the barrier and completed

      val res = spark.read.format("delta").load(tempPath).collect()
      assert(res.isEmpty)
    }
  }

  test("Phase Locking - no reusing observer") {
    withTempDir { tempFile =>

      val tempPath = tempFile.toString

      spark.range(100).write.format("delta").save(tempPath)

      val observer = new PhaseLockingTransactionExecutionObserver(
        OptimisticTransactionPhases.forName("test-txn"))
      val deltaLog = DeltaLog.forTable(spark, tempPath)

      // get things started
      observer.phases.initialPhase.entryBarrier.unblock()

      assert(!observer.phases.initialPhase.hasEntered)
      TransactionExecutionObserver.withObserver(observer) {
        deltaLog.withNewTransaction { txn =>
          // allow things to progress
          observer.phases.preparePhase.entryBarrier.unblock()
          observer.phases.commitPhase.entryBarrier.unblock()
          val removedFiles = txn.snapshot.allFiles.collect().map(_.remove).toSeq
          txn.commit(removedFiles, DeltaOperations.ManualUpdate)
        }
        // Check that we fail trying to re-unblock the barrier
        assertThrows[IllegalStateTransitionException] {
          deltaLog.withNewTransaction { txn =>
            // allow things to progress
            observer.phases.preparePhase.entryBarrier.unblock()
            observer.phases.commitPhase.entryBarrier.unblock()
            val removedFiles = txn.snapshot.allFiles.collect().map(_.remove).toSeq
            txn.commit(removedFiles, DeltaOperations.ManualUpdate)
          }
        }
        // Check that we fail just waiting on the passed barrier
        assertThrows[IllegalStateTransitionException] {
          deltaLog.withNewTransaction { txn =>
            val removedFiles = txn.snapshot.allFiles.collect().map(_.remove).toSeq
            txn.commit(removedFiles, DeltaOperations.ManualUpdate)
          }
        }
      }
      val res = spark.read.format("delta").load(tempPath).collect()
      assert(res.isEmpty)
    }
  }

  test("Phase Locking - delete command") {
    withTempDir { tempFile =>

      val tempPath = tempFile.toString

      spark.range(100).write.format("delta").save(tempPath)

      val observer = new PhaseLockingTransactionExecutionObserver(
        OptimisticTransactionPhases.forName("test-txn"))
      val deltaLog = DeltaLog.forTable(spark, tempPath)
      val deltaTable = IODeltaTable.forPath(spark, tempPath)

      def assertOperationNotVisible(): Unit =
        assert(deltaTable.toDF.count() === 100)

      val testThread = new Thread(() => {
        // make sure the transaction will use our observer
        TransactionExecutionObserver.withObserver(observer) {
          deltaTable.delete()
        }
      })
      testThread.start()

      busyWaitForState(
        observer.phases.initialPhase.entryBarrier, AtomicBarrier.State.Requested, timeout)

      assertOperationNotVisible()

      // get things started
      observer.phases.initialPhase.entryBarrier.unblock()

      busyWaitFor(observer.phases.initialPhase.hasLeft, timeout)

      assertOperationNotVisible()

      observer.phases.preparePhase.entryBarrier.unblock()
      busyWaitFor(observer.phases.preparePhase.hasLeft, timeout)
      assert(!observer.phases.commitPhase.hasEntered)

      assertOperationNotVisible()

      observer.phases.commitPhase.entryBarrier.unblock()
      busyWaitFor(observer.phases.commitPhase.hasLeft, timeout)
      testThread.join(timeout.toMillis)
      assert(!testThread.isAlive) // should have passed the barrier and completed

      val res = spark.read.format("delta").load(tempPath).collect()
      assert(res.isEmpty)
    }
  }
}
