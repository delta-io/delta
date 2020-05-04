/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.stats

import java.io.{ObjectInput, ObjectOutput}

import org.apache.spark.util.AccumulatorV2

/**
 * An accumulator that keeps arrays of counts. Counts from multiple partitions
 * are merged by index. -1 indicates a null and is handled using TVL (-1 + N = -1)
 */
class ArrayAccumulator(val size: Int) extends AccumulatorV2[(Int, Long), Array[Long]] {

  protected val counts = new Array[Long](size)

  override def isZero: Boolean = counts.forall(_ == 0)
  override def copy(): AccumulatorV2[(Int, Long), Array[Long]] = {
    val newCopy = new ArrayAccumulator(size)
    (0 until size).foreach(i => newCopy.counts(i) = counts(i))
    newCopy
  }
  override def reset(): Unit = (0 until size).foreach(counts(_) = 0)
  override def add(v: (Int, Long)): Unit = {
    if (v._2 == -1 || counts(v._1) == -1) {
      counts(v._1) = -1
    } else {
      counts(v._1) += v._2
    }
  }
  override def merge(o: AccumulatorV2[(Int, Long), Array[Long]]): Unit = {
    val other = o.asInstanceOf[ArrayAccumulator]
    assert(size == other.size)

    (0 until size).foreach(i => {
      if (counts(i) == -1 || other.counts(i) == -1) {
        counts(i) = -1
      } else {
        counts(i) += other.counts(i)
      }
    })
  }
  override def value: Array[Long] = counts

}

