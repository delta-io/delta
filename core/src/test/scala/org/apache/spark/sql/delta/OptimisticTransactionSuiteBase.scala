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

package org.apache.spark.sql.delta

import java.util.ConcurrentModificationException

import org.apache.spark.sql.delta.DeltaOperations.{ManualUpdate, Truncate}
import org.apache.spark.sql.delta.actions.{Action, Metadata, Protocol}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

trait OptimisticTransactionSuiteBase
  extends QueryTest
    with SharedSparkSession {


  /**
   * Check whether the test transaction conflict with the concurrent writes by executing the
   * given params in the following order:
   *  - setup (including setting table isolation level
   *  - reads
   *  - concurrentWrites
   *  - actions
   *
   * When `conflicts` == true, this function checks to make sure the commit of `actions` fails with
   * [[ConcurrentModificationException]], otherwise checks that the commit is successful.
   *
   * @param name                test name
   * @param conflicts           should test transaction is expected to conflict or not
   * @param setup               sets up the initial delta log state (set schema, partitioning, etc.)
   * @param reads               reads made in the test transaction
   * @param concurrentWrites    writes made by concurrent transactions after the test txn reads
   * @param actions             actions to be committed by the test transaction
   * @param errorMessageHint    What to expect in the error message
   * @param exceptionClass      A substring to expect in the exception class name
   */
  protected def check(
      name: String,
      conflicts: Boolean,
      setup: Seq[Action] = Seq(Metadata(), Action.supportedProtocolVersion()),
      reads: Seq[OptimisticTransaction => Unit],
      concurrentWrites: Seq[Action],
      actions: Seq[Action],
      errorMessageHint: Option[Seq[String]] = None,
      exceptionClass: Option[String] = None): Unit = {

    val concurrentTxn: OptimisticTransaction => Unit =
      (opt: OptimisticTransaction) => opt.commit(concurrentWrites, Truncate())

    def initialSetup(log: DeltaLog): Unit = {
      // Setup the log
      setup.foreach { action =>
        log.startTransaction().commit(Seq(action), ManualUpdate)
      }
    }
    check(
      name,
      conflicts,
      initialSetup _,
      reads,
      Seq(concurrentTxn),
      actions,
      operation = Truncate(), // a data-changing operation
      errorMessageHint = errorMessageHint,
      exceptionClass = exceptionClass,
      additionalSQLConfs = Seq.empty
    )
  }

  /**
   * Check whether the test transaction conflict with the concurrent writes by executing the
   * given params in the following order:
   *  - sets up the initial delta log state using `initialSetup` (set schema, partitioning, etc.)
   *  - reads
   *  - concurrentWrites
   *  - actions
   *
   * When `conflicts` == true, this function checks to make sure the commit of `actions` fails with
   * [[ConcurrentModificationException]], otherwise checks that the commit is successful.
   *
   * @param name                test name
   * @param conflicts           should test transaction is expected to conflict or not
   * @param initialSetup        sets up the initial delta log state (set schema, partitioning, etc.)
   * @param reads               reads made in the test transaction
   * @param concurrentTxns      concurrent txns that may write data after the test txn reads
   * @param actions             actions to be committed by the test transaction
   * @param errorMessageHint    What to expect in the error message
   * @param exceptionClass      A substring to expect in the exception class name
   */
  protected def check(
      name: String,
      conflicts: Boolean,
      initialSetup: DeltaLog => Unit,
      reads: Seq[OptimisticTransaction => Unit],
      concurrentTxns: Seq[OptimisticTransaction => Unit],
      actions: Seq[Action],
      operation: DeltaOperations.Operation,
      errorMessageHint: Option[Seq[String]],
      exceptionClass: Option[String],
      additionalSQLConfs: Seq[(String, String)]): Unit = {

    val conflict = if (conflicts) "should conflict" else "should not conflict"
    test(s"$name - $conflict") {
      withSQLConf(additionalSQLConfs: _*) {
        val tempDir = Utils.createTempDir()
        val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))

        // Setup the log
        initialSetup(log)

        // Perform reads
        val txn = log.startTransaction()
        reads.foreach(_ (txn))

        // Execute concurrent txn while current transaction is active
        concurrentTxns.foreach(txn => txn(log.startTransaction()))

        // Try commit and check expected conflict behavior
        if (conflicts) {
          val e = intercept[ConcurrentModificationException] {
            txn.commit(actions, operation)
          }
          errorMessageHint.foreach { expectedParts =>
            assert(expectedParts.forall(part => e.getMessage.contains(part)))
          }
          if (exceptionClass.nonEmpty) {
            assert(e.getClass.getName.contains(exceptionClass.get))
          }
        } else {
          txn.commit(actions, operation)
        }
      }
    }
  }
}
