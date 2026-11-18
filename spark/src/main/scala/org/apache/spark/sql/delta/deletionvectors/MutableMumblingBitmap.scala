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

package org.apache.spark.sql.delta.deletionvectors

import scala.collection.mutable

import org.apache.spark.sql.delta.deletionvectors.mumbling.{MumblingBitmap, MumblingBitmapWriter}

/**
 * A mutable Mumbling bitmap that retains an immutable base bitmap and records additions
 * separately.
 *
 * Creating an immutable snapshot merges the overlay one container at a time, without materializing
 * the existing set positions.
 */
final class MutableMumblingBitmap(private val base: MumblingBitmap) {

  private val additionContainers = mutable.LongMap.empty[Array[Byte]]
  private var additionCardinality = 0
  private var maxAdditionContainer = -1

  /** Adds `value` to the bitmap. */
  def add(value: Long): Unit = {
    require(
      value >= 0 && value < MumblingBitmapWriter.MAX_POSITION_EXCLUSIVE,
      s"Position out of range for a Mumbling bitmap: $value " +
        s"(must be in [0, ${MumblingBitmapWriter.MAX_POSITION_EXCLUSIVE}))")
    val position = value.toInt
    val containerIndex = position >>> 8
    val block = additionContainers.getOrElseUpdate(
      containerIndex.toLong, new Array[Byte](MutableMumblingBitmap.ContainerBytes))
    val positionInContainer = position & 0xFF
    val byteIndex = positionInContainer >>> 3
    val mask = 1 << (7 - (positionInContainer & 0x7))
    if ((block(byteIndex) & mask) == 0) {
      block(byteIndex) = (block(byteIndex) | mask).toByte
      additionCardinality += 1
      maxAdditionContainer = math.max(maxAdditionContainer, containerIndex)
    }
  }

  private def additionContainerBlocks: Array[Array[Byte]] = {
    val blocks = new Array[Array[Byte]](maxAdditionContainer + 1)
    additionContainers.foreach { case (containerIndex, block) =>
      blocks(containerIndex.toInt) = block
    }
    blocks
  }

  /** Returns an immutable snapshot of the current bitmap. */
  def toMumblingBitmap: MumblingBitmap = {
    if (additionCardinality == 0) {
      base
    } else {
      MumblingBitmapWriter.merge(base, additionContainerBlocks)
    }
  }
}

object MutableMumblingBitmap {
  private val ContainerBytes = 32
}
