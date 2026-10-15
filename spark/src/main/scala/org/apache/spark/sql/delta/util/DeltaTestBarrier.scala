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

package org.apache.spark.sql.delta.util

import com.databricks.spark.util.{TestBarrier, TestBarrierRegistry}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.internal.SQLConf

/** Entry point for labeled barriers used by Delta tests. */
object DeltaTestBarrier {
  /** Creates and registers a barrier for `label`. */
  def createBarrier(conf: SQLConf, label: String, hitCount: Int = 1): TestBarrier = {
    assert(conf.getConf(DeltaSQLConf.TEST_BARRIER_ENABLED))
    TestBarrierRegistry.createBarrier(label, hitCount, Utils.isTesting)
  }

  /** Blocks the current thread if the barrier for `label` is enabled. */
  def waitIfEnabled(label: String): Unit =
    TestBarrierRegistry.waitIfEnabled(label, Utils.isTesting)

  /** Prevents new threads from blocking without releasing current waiters. */
  def disableBarrierWithoutReleasing(label: String): Unit =
    TestBarrierRegistry.disableBarrierWithoutReleasing(label)

  /** Records hits and destroys the barrier once its target count is reached. */
  def hitBarrier(label: String, times: Int = 1): Unit =
    TestBarrierRegistry.hitBarrier(label, times)

  /** Enables the barrier registered for `label`. */
  def enableBarrier(label: String): Unit = TestBarrierRegistry.enableBarrier(label)

  /** Removes the barrier for `label` and releases all blocked threads. */
  def destroyBarrier(label: String): Unit = TestBarrierRegistry.destroyBarrier(label)

  /** Returns whether a barrier is registered for `label`. */
  def isBarrierEnabled(label: String): Boolean = TestBarrierRegistry.isBarrierEnabled(label)
}
