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
  extends TransactionExecutionObserver {

  override def startingTransaction(f: => OptimisticTransaction): OptimisticTransaction =
    phases.initialPhase.execute(f)

  override def preparingCommit[T](f: => T): T = phases.preparePhase.execute(f)

  override def beginDoCommit(): Unit = phases.commitPhase.waitToEnter()

  override def transactionCommitted(): Unit = phases.commitPhase.leave()

  override def transactionAborted(): Unit = {
    if (!phases.commitPhase.hasEntered) {
      // If an Exception was thrown earlier we may not have called `beginDoCommit`, yet.
      phases.commitPhase.passThrough()
    } else {
      phases.commitPhase.leave()
    }
  }
}
