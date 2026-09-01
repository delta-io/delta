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

/**
 * A mutable bitmap of unsigned positions for the AMT manifest deletion vectors: the masking MDV in
 * `manifest_info.dv` and the CDF `tracking.deleted_positions` / `tracking.replaced_positions`
 * bitmaps.
 *
 * This trait is the seam over the concrete bitmap format written to disk. The only implementation
 * today is [[RoaringManifestBitmap]] (Roaring portable), so behavior is unchanged; the trait exists
 * so an alternative compressed format can later be dropped in behind the [[ManifestBitmap]] factory
 * and [[ManifestBitmap.readFrom]] without touching the AMT write path.
 */
trait ManifestBitmap {
  /** Adds `value` to the bitmap. */
  def add(value: Long): Unit

  /** Returns `true` if `value` is set. */
  def contains(value: Long): Boolean

  /** Returns the number of set positions. */
  def cardinality: Long

  /** Returns `true` if no position is set. */
  def isEmpty: Boolean

  /** Returns the set positions in ascending order. */
  def toArray: Array[Long]

  /** Serializes to the on-disk byte form carried in `manifest_info.dv` / the CDF bitmaps. */
  def serializeAsByteArray(): Array[Byte]
}

object ManifestBitmap {
  /** Returns an empty bitmap. */
  def empty(): ManifestBitmap = RoaringManifestBitmap.empty()

  /** Returns a bitmap of the given positions. */
  def apply(values: Long*): ManifestBitmap = RoaringManifestBitmap(values: _*)

  /** Deserializes a bitmap previously produced by [[ManifestBitmap.serializeAsByteArray]]. */
  def readFrom(bytes: Array[Byte]): ManifestBitmap = RoaringManifestBitmap.readFrom(bytes)
}

/** A [[ManifestBitmap]] backed by a [[RoaringBitmapArray]] serialized in the portable format. */
final class RoaringManifestBitmap private (private val underlying: RoaringBitmapArray)
  extends ManifestBitmap {

  override def add(value: Long): Unit = underlying.add(value)

  override def contains(value: Long): Boolean = underlying.contains(value)

  override def cardinality: Long = underlying.cardinality

  override def isEmpty: Boolean = underlying.isEmpty

  override def toArray: Array[Long] = underlying.toArray

  override def serializeAsByteArray(): Array[Byte] =
    underlying.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
}

object RoaringManifestBitmap {
  def empty(): RoaringManifestBitmap = new RoaringManifestBitmap(new RoaringBitmapArray)

  def apply(values: Long*): RoaringManifestBitmap =
    new RoaringManifestBitmap(RoaringBitmapArray(values: _*))

  def readFrom(bytes: Array[Byte]): RoaringManifestBitmap =
    new RoaringManifestBitmap(RoaringBitmapArray.readFrom(bytes))
}
