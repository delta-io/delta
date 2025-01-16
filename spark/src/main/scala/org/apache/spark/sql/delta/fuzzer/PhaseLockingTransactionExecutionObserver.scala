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

package org.apache.spark.sql.delta.fuzzer

import org.apache.spark.sql.delta.{OptimisticTransaction, TransactionExecutionObserver}

private[delta] class PhaseLockingTransactionExecutionObserver(
    val phases: OptimisticTransactionPhases)
  extends TransactionExecutionObserver
  with PhaseLockingExecutionObserver {

  override val phaseLocks: Seq[ExecutionPhaseLock] = Seq(
    phases.initialPhase,
    phases.preparePhase,
    phases.commitPhase,
    phases.backfillPhase)

  override def createChild(): TransactionExecutionObserver = {
    // Just return the current thread observer.
    // This is equivalent to the behaviour of the use-site before introduction of
    // `createChild`.
    TransactionExecutionObserver.getObserver
  }

  /**
   * When set to true this observer will automatically update the thread's current observer to
   * the next one. Also, it will not unblock the exit barrier of the commit phase automatically.
   * Instead, the caller will have to automatically unblock it. This allows writing tests that
   * can capture errors caused by code written between the end of the last txn and the start of
   * the next txn.
   */
  @volatile protected var autoAdvanceNextObserver: Boolean = false

  override def startingTransaction(f: => OptimisticTransaction): OptimisticTransaction =
    phases.initialPhase.execute(f)

  override def preparingCommit[T](f: => T): T = phases.preparePhase.execute(f)

  override def beginDoCommit(): Unit = {
    phases.commitPhase.waitToEnter()
  }

  override def beginBackfill(): Unit = {
    phases.commitPhase.leave()
    phases.backfillPhase.waitToEnter()
  }

  override def transactionCommitted(): Unit = {
    if (nextObserver.nonEmpty && autoAdvanceNextObserver) {
      waitForCommitPhaseAndAdvanceToNextObserver()
    } else {
      phases.backfillPhase.leave()
    }
  }

  override def transactionAborted(): Unit = {
    if (!phases.commitPhase.hasLeft) {
      if (!phases.commitPhase.hasEntered) {
        phases.commitPhase.waitToEnter()
      }
      phases.commitPhase.leave()
    }
    if (!phases.backfillPhase.hasEntered) {
      phases.backfillPhase.waitToEnter()
    }
    if (nextObserver.nonEmpty && autoAdvanceNextObserver) {
      waitForCommitPhaseAndAdvanceToNextObserver()
    } else {
      phases.backfillPhase.leave()
    }
  }

  /*
   * Wait for the backfill phase to pass but do not unblock it so that callers can write tests
   * that capture errors caused by code between the end of the last txn and the start of the
   * new txn. After the commit phase is passed, update the thread observer of the thread to
   * the next observer.
   */
  def waitForCommitPhaseAndAdvanceToNextObserver(): Unit = {
    require(nextObserver.nonEmpty)
    phases.backfillPhase.waitToLeave()
    advanceToNextThreadObserver()
  }

  /**
   * Set the next observer, which will replace the txn observer on the thread after a successful
   * commit. This method only works as expected if we haven't entered the commit phase yet.
   *
   * Note that when a next observer is set, the caller needs to manually unblock the exit barrier
   * of the commit phase.
   *
   * For example, see [[waitForCommitPhaseAndAdvanceToNextObserver]].
   */
  def setNextObserver(
      nextTxnObserver: TransactionExecutionObserver,
      autoAdvance: Boolean): Unit = {
    setNextObserver(nextTxnObserver)
    autoAdvanceNextObserver = autoAdvance
  }

  override def advanceToNextThreadObserver(): Unit = super.advanceToNextThreadObserver()
}
