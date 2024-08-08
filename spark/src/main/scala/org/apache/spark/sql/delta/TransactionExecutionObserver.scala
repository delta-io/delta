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

trait ChainableExecutionObserver[O] {
  /**
   * The next txn observer for this thread.
   * The next observer is used to test threads that perform multiple transactions, i.e.
   * commands that perform multiple commits.
   */
  @volatile protected var nextObserver: Option[O] = None

  /** Set the next observer for this thread. */
  def setNextObserver(nextTxnObserver: O): Unit = {
    nextObserver = Some(nextTxnObserver)
  }

  /** Update the observer of this thread with the next observer. */
  def advanceToNextThreadObserver(): Unit
}

/**
 * Track different stages of the execution of a transaction.
 *
 * This is mostly meant for test instrumentation.
 *
 * The default is a no-op implementation.
 */
trait TransactionExecutionObserver
  extends ChainableExecutionObserver[TransactionExecutionObserver] {

  /**
   * Create a child instance of this observer for use in [[OptimisticTransactionImpl.split()]].
   *
   * It's up to each observer type what state new child needs to hold.
   */
  def createChild(): TransactionExecutionObserver

  /*
   * This is called outside the transaction object,
   * since it wraps its creation.
   */

  /** Wraps transaction creation. */
  def startingTransaction(f: => OptimisticTransaction): OptimisticTransaction

  /*
   * These are called from within the transaction object.
   */

  /** Wraps `prepareCommit`. */
  def preparingCommit[T](f: => T): T

  /*
   * The next three methods before/after-style instead of wrapping like above,
   * because the commit code is large and in a try-catch block,
   * making wrapping impractical.
   */

  /** Called before the first `doCommit` attempt. */
  def beginDoCommit(): Unit

  /** Called after publishing the commit file but before the `backfill` attempt. */
  def beginBackfill(): Unit

  /** Called once a commit succeeded. */
  def transactionCommitted(): Unit

  /**
   * Called once the transaction failed.
   *
   * *Note:* It can happen that [[transactionAborted()]] is called
   *         without [[beginDoCommit()]] being called first.
   *         This occurs when there is an Exception thrown during the transaction's body.
   */
  def transactionAborted(): Unit

  override def advanceToNextThreadObserver(): Unit = {
    TransactionExecutionObserver.setObserver(
      nextObserver.getOrElse(NoOpTransactionExecutionObserver))
  }
}

object TransactionExecutionObserver
  extends ThreadStorageExecutionObserver[TransactionExecutionObserver] {
  override protected val initialValue: TransactionExecutionObserver =
    NoOpTransactionExecutionObserver
}

/** Default observer does nothing. */
object NoOpTransactionExecutionObserver extends TransactionExecutionObserver {
  override def startingTransaction(f: => OptimisticTransaction): OptimisticTransaction = f

  override def preparingCommit[T](f: => T): T = f

  override def beginDoCommit(): Unit = ()

  override def beginBackfill(): Unit = ()

  override def transactionCommitted(): Unit = ()

  override def transactionAborted(): Unit = ()

  override def createChild(): TransactionExecutionObserver = {
    // This mimics the original behaviour of this code.
    TransactionExecutionObserver.getObserver
  }
}
