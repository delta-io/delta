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

import scala.concurrent.duration._

object ConcurrencyHelpers {
  /**
   * Keep checking if `check` returns `true` until it's the case or `waitTime` expires.
   *
   * Return `true` when the `check` returned `true`, and `false` if `waitTime` expired.
   *
   * Note: This function is used as a helper function for the Concurrency Testing framework,
   * and should not be used in production code. Production code should not use polling
   * and should instead use signalling to coordinate.
   */
  def busyWaitFor(
      check: => Boolean,
      waitTime: FiniteDuration): Boolean = {
    val DEFAULT_SLEEP_TIME: Duration = 10.millis
    val deadline = waitTime.fromNow

    do {
      if (check) {
        return true
      }
      val sleepTimeMs = DEFAULT_SLEEP_TIME.min(deadline.timeLeft).toMillis
      Thread.sleep(sleepTimeMs)
    } while (deadline.hasTimeLeft())
    false
  }

  def withOptimisticTransaction[T](
    activeTransaction: Option[OptimisticTransaction])(block: => T): T = {
    if (activeTransaction.isDefined) {
      OptimisticTransaction.withActive(activeTransaction.get) {
        block
      }
    } else {
      block
    }
  }
}
