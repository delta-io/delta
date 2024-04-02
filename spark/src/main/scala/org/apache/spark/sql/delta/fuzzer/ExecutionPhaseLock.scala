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

/**
 * An ExecutionPhaseLock is an abstraction to keep multiple transactions moving in
 * a pre-selected lock-step sequence.
 *
 * In order to pass a phase, we first wait on the `entryBarrier`. Once we are allowed to pass there,
 * we can execute the code that belongs to this phase, and then we unblock the `exitBarrier`.
 *
 * @param name human readable name for debugging
 */
case class ExecutionPhaseLock(
    name: String,
    entryBarrier: AtomicBarrier = new AtomicBarrier(),
    exitBarrier: AtomicBarrier = new AtomicBarrier()) {

  def hasEntered: Boolean = entryBarrier.load() == AtomicBarrier.State.Passed

  def hasLeft: Boolean = {
    val current = exitBarrier.load()
    current == AtomicBarrier.State.Unblocked || current == AtomicBarrier.State.Passed
  }

  /** Blocks at this point until the phase has been entered. */
  def waitToEnter(): Unit = entryBarrier.waitToPass()

  /** Unblock the next dependent phase. */
  def leave(): Unit = exitBarrier.unblock()

  /**
   * Wait to enter this phase, then execute `f`, and leave before returning the result of `f`.
   *
   * @return the result of evaluating `f`
   */
  def execute[T](f: => T): T = {
    waitToEnter()
    try {
      f
    } finally {
      leave()
    }
  }

  /**
   * If there is nothing that needs to be done in this phase,
   * we can leave immediately after entering.
   */
  def passThrough(): Unit = {
    waitToEnter()
    leave()
  }
}
