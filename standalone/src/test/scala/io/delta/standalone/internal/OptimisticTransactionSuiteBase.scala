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

package io.delta.standalone.internal

import java.util.ConcurrentModificationException

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

import io.delta.standalone.{DeltaLog, Operation, OptimisticTransaction}
import io.delta.standalone.actions.{Action => ActionJ, Metadata => MetadataJ, Protocol => ProtocolJ}

import io.delta.standalone.internal.util.TestUtils._

trait OptimisticTransactionSuiteBase extends FunSuite {

  val op = new Operation(Operation.Name.MANUAL_UPDATE)
  val engineInfo = "test-engine-info"

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
      setup: Seq[ActionJ] = Seq(MetadataJ.builder().build(), new ProtocolJ()),
      reads: Seq[OptimisticTransaction => Unit],
      concurrentWrites: Seq[ActionJ],
      actions: Seq[ActionJ],
      errorMessageHint: Option[Seq[String]] = None,
      exceptionClass: Option[String] = None): Unit = {

    val concurrentTxn: OptimisticTransaction => Unit =
      (opt: OptimisticTransaction) =>
        opt.commit(concurrentWrites.asJava, op, engineInfo)

    def initialSetup(log: DeltaLog): Unit = {
      // Setup the log
      setup.foreach { action =>
        log.startTransaction().commit(Seq(action).asJava, op, engineInfo)
      }
    }
    check(
      name,
      conflicts,
      initialSetup _,
      reads,
      Seq(concurrentTxn),
      actions.asJava,
      errorMessageHint,
      exceptionClass
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
      actions: java.util.List[ActionJ],
      errorMessageHint: Option[Seq[String]],
      exceptionClass: Option[String]): Unit = {

    val conflict = if (conflicts) "should conflict" else "should not conflict"
    test(s"$name - $conflict") {
      withTempDir { tempDir =>
        val log = DeltaLog.forTable(new Configuration(), new Path(tempDir.getCanonicalPath))

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
            txn.commit(actions, op, engineInfo)
          }
          errorMessageHint.foreach { expectedParts =>
            assert(expectedParts.forall(part => e.getMessage.contains(part)))
          }
          if (exceptionClass.nonEmpty) {
            assert(e.getClass.getName.contains(exceptionClass.get))
          }
        } else {
          txn.commit(actions, op, engineInfo)
        }
      }
    }
  }
}
