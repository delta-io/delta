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

import org.apache.spark.util.AccumulatorV2

/**
 * Accumulator to collect distinct elements as a set.
 */
class SetAccumulator[T] extends AccumulatorV2[T, java.util.Set[T]] {
  private var _set: java.util.Set[T] = _

  private def getOrCreate = {
    _set = Option(_set).getOrElse(java.util.Collections.synchronizedSet(new java.util.HashSet[T]()))
    _set
  }

  override def isZero: Boolean = this.synchronized(getOrCreate.isEmpty)

  override def reset(): Unit = this.synchronized {
    _set = null
  }
  override def add(v: T): Unit = this.synchronized(getOrCreate.add(v))

  override def merge(other: AccumulatorV2[T, java.util.Set[T]]): Unit = other match {
    case o: SetAccumulator[T] => this.synchronized(getOrCreate.addAll(o.value))
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: java.util.Set[T] = this.synchronized {
    java.util.Collections.unmodifiableSet(new java.util.HashSet[T](getOrCreate))
  }

  override def copy(): AccumulatorV2[T, java.util.Set[T]] = {
    val newAcc = new SetAccumulator[T]()
    this.synchronized {
      newAcc.getOrCreate.addAll(getOrCreate)
    }
    newAcc
  }
}
