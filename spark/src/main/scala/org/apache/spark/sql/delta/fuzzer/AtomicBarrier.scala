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

import java.util.concurrent.atomic.AtomicInteger

/**
 * An atomic barrier is similar to a countdown latch,
 * except that the content is a state transition system with semantic meaning
 * instead of a simple counter.
 *
 * It is designed with a single writer ("unblocker") thread and a single reader ("waiter") thread
 * in mind. It is concurrency safe with more writers and readers, but using more is likely to cause
 * race conditions for legal transitions. That is to say, trying to perform an otherwise
 * legal transition twice is illegal and may occur if there is more than one unblocker or
 * waiter thread.
 * Having additional passive state observers that only call [[load()]] is never an issue.
 *
 * Legal transitions are:
 * - BLOCKED -> UNBLOCKED
 * - BLOCKED -> REQUESTED
 * - REQUESTED -> UNBLOCKED
 * - UNBLOCKED -> PASSED
 */
class AtomicBarrier {

  import AtomicBarrier._

  private final val state: AtomicInteger = new AtomicInteger(State.Blocked.ordinal)

  /** Get the current state. */
  def load(): State = {
    val ordinal = state.get()
    // We should never be putting illegal state ordinals into `state`,
    // so this should always succeed.
    stateIndex(ordinal)
  }

  /** Transition to the Unblocked state. */
  def unblock(): Unit = {
    // Just hot-retry this, since it never needs to wait to make progress.
    var successful = false
    while(!successful) {
      val currentValue = state.get()
      if (currentValue == State.Blocked.ordinal || currentValue == State.Requested.ordinal) {
        this.synchronized {
          successful = state.compareAndSet(currentValue, State.Unblocked.ordinal)
          if (successful) {
            this.notifyAll()
          }
        }
      } else {
        // if it's in any other state we will never make progress
        throw new IllegalStateTransitionException(stateIndex(currentValue), State.Unblocked)
      }
    }
  }

  /** Wait until this barrier can be passed and then mark it as Passed. */
  def waitToPass(): Unit = {
    while (true) {
      val currentState = load()
      currentState match {
        case State.Unblocked =>
          val updated = state.compareAndSet(currentState.ordinal, State.Passed.ordinal)
          if (updated) {
            return
          }
        case State.Passed =>
          throw new IllegalStateTransitionException(State.Passed, State.Passed)
        case State.Requested =>
          this.synchronized {
            if (load().ordinal == State.Requested.ordinal) {
              this.wait()
            }
          }
        case State.Blocked =>
          this.synchronized {
            val updated = state.compareAndSet(currentState.ordinal, State.Requested.ordinal)
            if (updated) {
              this.wait()
            }
          } // else (if we didn't succeed) just hot-retry until we do
            // (or more likely pass, since unblocking is the only legal concurrent
            // update with a single concurrent "waiter")
      }
    }
  }

  override def toString: String = s"AtomicBarrier(state=${load()})"
}

object AtomicBarrier {

  sealed trait State {
    def ordinal: Int
  }

  object State {
    case object Blocked extends State {
      override final val ordinal = 0
    }
    case object Unblocked extends State {
      override final val ordinal = 1
    }
    case object Requested extends State {
      override final val ordinal = 2
    }
    case object Passed extends State {
      override final val ordinal = 3
    }
  }

  final val stateIndex: Map[Int, State] =
    List(State.Blocked, State.Unblocked, State.Requested, State.Passed)
      .map(state => state.ordinal -> state)
      .toMap
}

class IllegalStateTransitionException(fromState: AtomicBarrier.State, toState: AtomicBarrier.State)
  extends RuntimeException(s"State transition from $fromState to $toState is illegal.")
