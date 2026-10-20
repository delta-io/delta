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

import java.nio.ByteBuffer

import scala.collection.Seq

import org.apache.spark.sql.delta.RowIndexFilterProvider
import org.apache.spark.sql.delta.deletionvectors.mumbling.{MumblingBitmap, MumblingBitmapWriter}
import org.apache.hadoop.fs.Path

/**
 * An immutable bitmap of unsigned positions for the AMT manifest deletion vectors: the masking
 * MDV in `manifest_info.dv` and the CDF `tracking.deleted_positions` /
 * `tracking.replaced_positions` bitmaps.
 */
trait ManifestBitmap {
  /** Returns `true` if `value` is set. */
  def contains(value: Int): Boolean

  /** Returns the number of set positions. */
  def cardinality: Long

  /** Returns `true` if no position is set. */
  def isEmpty: Boolean

  /** Returns a mutable copy that can be updated and converted back to an immutable bitmap. */
  def toMutable: MutableManifestBitmap

  /** Serializes to the on-disk byte form carried in `manifest_info.dv` / the CDF bitmaps. */
  def serializeAsByteArray(): Array[Byte]

  /** Builds a [[RowIndexFilterProvider]] representing this bitmap. */
  def toRowIndexFilterProvider(tableRoot: Path): RowIndexFilterProvider
}

/** A mutable copy of a [[ManifestBitmap]]. */
trait MutableManifestBitmap {
  /** Adds `value` to the bitmap. */
  def add(value: Int): Unit

  /** Returns an immutable snapshot of the current bitmap. */
  def toManifestBitmap: ManifestBitmap
}

object ManifestBitmap {
  /** Returns a bitmap of the given positions. */
  def fromPositions(values: Seq[Int]): ManifestBitmap =
    MumblingManifestBitmap.fromPositions(values)

  /** Deserializes a bitmap previously produced by [[ManifestBitmap.serializeAsByteArray]]. */
  def fromSerializedByteArray(bytes: Array[Byte]): ManifestBitmap =
    MumblingManifestBitmap.fromSerializedByteArray(bytes)
}

/** A [[ManifestBitmap]] backed by an immutable [[MumblingBitmap]]. */
final class MumblingManifestBitmap private[deletionvectors] (
    private val underlying: MumblingBitmap)
  extends ManifestBitmap {

  override def contains(value: Int): Boolean = value >= 0 && underlying.isSet(value)

  override def cardinality: Long = underlying.cardinality().toLong

  override def isEmpty: Boolean = underlying.cardinality() == 0

  override def toMutable: MutableManifestBitmap =
    new MutableMumblingManifestBitmap(underlying.toMutable())

  override def serializeAsByteArray(): Array[Byte] = {
    val buffer = underlying.buffer()
    val bytes = new Array[Byte](buffer.remaining())
    buffer.get(bytes)
    bytes
  }

  override def toRowIndexFilterProvider(tableRoot: Path): RowIndexFilterProvider =
    new MumblingRowIndexFilterProvider(serializeAsByteArray(), cardinality)
}

object MumblingManifestBitmap {
  def fromPositions(values: Seq[Int]): MumblingManifestBitmap = {
    val bytes = MumblingBitmapWriter.serialize(values.distinct.sorted.toArray)
    new MumblingManifestBitmap(new MumblingBitmap(ByteBuffer.wrap(bytes)))
  }

  def fromSerializedByteArray(bytes: Array[Byte]): MumblingManifestBitmap =
    new MumblingManifestBitmap(new MumblingBitmap(ByteBuffer.wrap(bytes)))
}

/** A [[MutableManifestBitmap]] backed by a [[MutableMumblingBitmap]]. */
private final class MutableMumblingManifestBitmap(
    private val underlying: MutableMumblingBitmap)
  extends MutableManifestBitmap {

  override def add(value: Int): Unit = underlying.add(value.toLong)

  override def toManifestBitmap: ManifestBitmap =
    new MumblingManifestBitmap(underlying.toMumblingBitmap)
}
