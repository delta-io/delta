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

package org.apache.spark.sql.delta.commands.backfill

import org.apache.spark.sql.delta.{ChainableExecutionObserver, NoOpTransactionExecutionObserver, ThreadStorageExecutionObserver, TransactionExecutionObserver}

trait BackfillExecutionObserver extends ChainableExecutionObserver[BackfillExecutionObserver] {
  def executeBatch[T](f: => T): T

  override def advanceToNextThreadObserver(): Unit = {
    BackfillExecutionObserver.setObserver(nextObserver.getOrElse(NoOpBackfillExecutionObserver))
  }
}

object BackfillExecutionObserver
  extends ThreadStorageExecutionObserver[BackfillExecutionObserver] {

  override protected val threadObserver: ThreadLocal[BackfillExecutionObserver] =
    new InheritableThreadLocal[BackfillExecutionObserver] {
      override def initialValue(): BackfillExecutionObserver = NoOpBackfillExecutionObserver
    }

  override protected def initialValue: BackfillExecutionObserver = NoOpBackfillExecutionObserver
}

object NoOpBackfillExecutionObserver extends BackfillExecutionObserver {
  def executeBatch[T](f: => T): T = f
}

