/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.internal.compatibility.tests

import java.util.ConcurrentModificationException

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.{actions => OSSActions, DeltaLog => OSSDeltaLog, DeltaOperations, OptimisticTransaction => OSSOptTxn}
import org.apache.spark.sql.test.SharedSparkSession

import io.delta.standalone.{actions => StandaloneActions, DeltaLog => StandaloneDeltaLog, Operation => StandaloneOperation, OptimisticTransaction => StandaloneOptTxn}

import io.delta.standalone.internal.util.{OSSUtil, StandaloneUtil}

trait OssCompatibilitySuiteBase extends QueryTest with SharedSparkSession {

  protected val now = System.currentTimeMillis()
  protected val ss = new StandaloneUtil(now)
  protected val oo = new OSSUtil(now)

  private val standaloneConflictOp = new StandaloneOperation(StandaloneOperation.Name.MANUAL_UPDATE)
  private val ossConflictOp = DeltaOperations.ManualUpdate

  /**
   * Tests a DELTA STANDALONE transaction getting conflicted by a DELTA OSS commit (i.e. during the
   * DSW transaction, a Delta OSS commit occurs and wins).
   *
   * Check whether the test transaction conflict with the concurrent writes by executing the
   * given params in the following order:
   *  - setup (including setting table isolation level
   *  - reads
   *  - concurrentWrites
   *  - actions
   *
   * When `conflicts` == true, this function checks to make sure the commit of `actions` fails with
   * [[java.util.ConcurrentModificationException]], otherwise checks that the commit is successful.
   *
   * @param testName            test name
   * @param conflicts           should test transaction is expected to conflict or not
   * @param setup               sets up the initial delta log state (set schema, partitioning, etc.)
   * @param reads               reads made in the test transaction
   * @param concurrentOSSWrites writes made by concurrent transactions after the test txn reads
   * @param actions             actions to be committed by the test transaction
   * @param exceptionClass      A substring to expect in the exception class name
   */
  protected def checkStandalone(
      testName: String,
      conflicts: Boolean,
      setup: Seq[StandaloneActions.Action] =
        Seq(ss.conflict.metadata_colXY, new StandaloneActions.Protocol(1, 2)),
      reads: Seq[StandaloneOptTxn => Unit],
      concurrentOSSWrites: Seq[OSSActions.Action],
      actions: Seq[StandaloneActions.Action],
      errorMessageHint: Option[Seq[String]] = None,
      exceptionClass: Option[String] = None): Unit = {

    val concurrentTxn: OSSOptTxn => Unit =
      (opt: OSSOptTxn) => opt.commit(concurrentOSSWrites, ossConflictOp)

    def initialSetup(log: StandaloneDeltaLog): Unit = {
      setup.foreach { action =>
        log.startTransaction().commit(Seq(action).asJava, standaloneConflictOp, ss.engineInfo)
      }
    }

    val conflictMsg = if (conflicts) "should conflict" else "should not conflict"
    test(s"checkStandalone - $testName - $conflictMsg") {
      withTempDir { tempDir =>
        // Standalone loses
        val losingLog =
          StandaloneDeltaLog.forTable(new Configuration(), new Path(tempDir.getCanonicalPath))

        // OSS wins
        val winningLog = OSSDeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))

        // Setup the log
        initialSetup(losingLog)

        // Perform reads
        val standaloneTxn = losingLog.startTransaction()
        reads.foreach(_ (standaloneTxn))

        // Execute concurrent txn while current transaction is active
        concurrentTxn(winningLog.startTransaction())

        // Try commit and check expected conflict behavior
        if (conflicts) {
          val e = intercept[ConcurrentModificationException] {
            standaloneTxn.commit(actions.asJava, standaloneConflictOp, ss.engineInfo)
          }
          errorMessageHint.foreach { expectedParts =>
            assert(expectedParts.forall(part => e.getMessage.contains(part)))
          }
          if (exceptionClass.nonEmpty) {
            assert(e.getClass.getName.contains(exceptionClass.get))
          }
        } else {
          standaloneTxn.commit(actions.asJava, standaloneConflictOp, ss.engineInfo)
        }
      }
    }
  }

  /**
   * Tests a DELTA OSS transaction getting conflicted by a DELTA STANDALONE commit (i.e. during the
   * Delta OSS transaction, a Delta Standalone commit occurs and wins).
   *
   * Check whether the test transaction conflict with the concurrent writes by executing the
   * given params in the following order:
   *  - setup (including setting table isolation level
   *  - reads
   *  - concurrentWrites
   *  - actions
   *
   * When `conflicts` == true, this function checks to make sure the commit of `actions` fails with
   * [[java.util.ConcurrentModificationException]], otherwise checks that the commit is successful.
   *
   * @param testName            test name
   * @param conflicts           should test transaction is expected to conflict or not
   * @param setup               sets up the initial delta log state (set schema, partitioning, etc.)
   * @param reads               reads made in the test transaction
   * @param concurrentStandaloneWrites writes made by concurrent transactions after the test txn
   *                                   reads
   * @param actions             actions to be committed by the test transaction
   * @param exceptionClass      A substring to expect in the exception class name
   */
  protected def checkOSS(
      testName: String,
      conflicts: Boolean,
      setup: Seq[OSSActions.Action] = Seq(OSSActions.Metadata(), OSSActions.Protocol(1, 2)),
      reads: Seq[OSSOptTxn => Unit],
      concurrentStandaloneWrites: Seq[StandaloneActions.Action], // winning Delta Standalone writes
      actions: Seq[OSSActions.Action],
      errorMessageHint: Option[Seq[String]] = None,
      exceptionClass: Option[String] = None): Unit = {
    val concurrentTxn: StandaloneOptTxn => Unit =
      (opt: StandaloneOptTxn) =>
        opt.commit(concurrentStandaloneWrites.asJava, standaloneConflictOp, ss.engineInfo)

    def initialSetup(log: OSSDeltaLog): Unit = {
      setup.foreach { action =>
        log.startTransaction().commit(Seq(action), ossConflictOp)
      }
    }

    val conflictMsg = if (conflicts) "should conflict" else "should not conflict"
    test(s"checkOSS - $testName - $conflictMsg") {
      withTempDir { tempDir =>
        // OSS loses
        val losingLog = OSSDeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))

        // Standalone wins
        val winningLog =
          StandaloneDeltaLog.forTable(new Configuration(), new Path(tempDir.getCanonicalPath))

        // Setup the log
        initialSetup(losingLog)

        // Perform reads
        val ossTxn = losingLog.startTransaction()
        reads.foreach(_ (ossTxn))

        // Execute concurrent txn while current transaction is active
        concurrentTxn(winningLog.startTransaction())

        // Try commit and check expected conflict behavior
        if (conflicts) {
          val e = intercept[ConcurrentModificationException] {
            ossTxn.commit(actions, ossConflictOp)
          }
          errorMessageHint.foreach { expectedParts =>
            assert(expectedParts.forall(part => e.getMessage.contains(part)))
          }
          if (exceptionClass.nonEmpty) {
            assert(e.getClass.getName.contains(exceptionClass.get))
          }
        } else {
          ossTxn.commit(actions, ossConflictOp)
        }
      }
    }
  }

}
