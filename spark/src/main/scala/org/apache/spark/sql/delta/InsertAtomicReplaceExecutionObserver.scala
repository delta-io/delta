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

/**
 * Track different stages of the execution of a INSERT REPLACE ON/USING/WHERE.
 *
 * This is mostly meant for test instrumentation.
 *
 * The default is a no-op implementation.
 */
trait InsertAtomicReplaceExecutionObserver
  extends ChainableExecutionObserver[InsertAtomicReplaceExecutionObserver] {

  /** Wraps `delete` removing matching table rows. */
  def delete[T](f: => T): T

  /** Wraps `insert` inserting source query rows. */
  def insert[T](f: => T): T

  /**
   * Wraps the call committing to delta log at the end of the
   * INSERT REPLACE ON/USING/WHERE execution.
   */
  def commit[T](f: => T): T

  override def advanceToNextThreadObserver(): Unit = {
    InsertAtomicReplaceExecutionObserver.setObserver(
      nextObserver.getOrElse(NoOpInsertAtomicReplaceExecutionObserver))
  }
}

object InsertAtomicReplaceExecutionObserver
  extends ThreadStorageExecutionObserver[InsertAtomicReplaceExecutionObserver] {
  override protected val initialValue: InsertAtomicReplaceExecutionObserver =
    NoOpInsertAtomicReplaceExecutionObserver
}

/** Default observer does nothing. */
object NoOpInsertAtomicReplaceExecutionObserver extends InsertAtomicReplaceExecutionObserver {
  override def delete[T](f: => T): T = f

  override def insert[T](f: => T): T = f

  override def commit[T](f: => T): T = f
}
