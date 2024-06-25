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

trait ThreadStorageExecutionObserver[T <: ChainableExecutionObserver[T]] {
  /** Thread-local observer instance loaded by [[T]] */
  protected val threadObserver: ThreadLocal[T] = ThreadLocal.withInitial(() => initialValue)
  protected def initialValue: T

  def getObserver: T = threadObserver.get()

  def setObserver(observer: T): Unit = threadObserver.set(observer)

  /** Instrument all executions created and completed within `thunk` with `newObserver`. */
  def withObserver[S](newObserver: T)(thunk: => S): S = {
    val oldObserver = threadObserver.get()
    threadObserver.set(newObserver)
    try {
      thunk
    } finally {
      // reset
      threadObserver.set(oldObserver)
    }
  }

  /** Update the current thread observer with its next one. */
  def advanceToNextObserver(): Unit = threadObserver.get.advanceToNextThreadObserver()
}
