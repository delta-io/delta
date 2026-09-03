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

import org.apache.spark.util.Utils

/**
 * A mutable bitmap of unsigned positions for the AMT manifest deletion vectors: the masking MDV
 * in `manifest_info.dv` and the CDF `tracking.deleted_positions` / `tracking.replaced_positions`
 * bitmaps.
 */
trait ManifestBitmap {
  /** Adds `value` to the bitmap. */
  def add(value: Int): Unit

  /** Returns `true` if `value` is set. */
  def contains(value: Int): Boolean

  /** Returns the number of set positions. */
  def cardinality: Long

  /** Returns `true` if no position is set. */
  def isEmpty: Boolean

  /** Serializes to the on-disk byte form carried in `manifest_info.dv` / the CDF bitmaps. */
  def serializeAsByteArray(): Array[Byte]

  /** The set positions in ascending order, for tests only. */
  final def toArrayForTesting: Array[Long] = {
    assert(Utils.isTesting)
    toArrayForTestingImpl
  }

  protected def toArrayForTestingImpl: Array[Long]

}

object ManifestBitmap {
  /** Returns a bitmap of the given positions. */
  def fromPositions(values: Seq[Int]): ManifestBitmap = RoaringManifestBitmap.fromPositions(values)

  /** Deserializes a bitmap previously produced by [[ManifestBitmap.serializeAsByteArray]]. */
  def fromSerializedByteArray(bytes: Array[Byte]): ManifestBitmap =
    RoaringManifestBitmap.fromSerializedByteArray(bytes)
}

/**
 * A [[ManifestBitmap]] backed by a [[RoaringBitmapArray]] in the portable format.
 */
final class RoaringManifestBitmap private (
    private val serializedBytesOrPositions: Either[Array[Byte], Seq[Int]])
  extends ManifestBitmap {

  // Materialized on first bit-level use: decoded from the on-disk bytes, or built from the
  // positions.
  private lazy val roaringBitmapArray: RoaringBitmapArray =
    serializedBytesOrPositions match {
      case Left(bytes) => RoaringBitmapArray.readFrom(bytes)
      case Right(positions) => RoaringBitmapArray(positions.map(_.toLong): _*)
    }

  // Set once a position is added, so serialization re-encodes from the decoded (mutated) bitmap
  // instead of returning the now-stale `serializedBytesOrPositions`.
  private var mutated = false

  override def add(value: Int): Unit = {
    mutated = true
    roaringBitmapArray.add(value.toLong)
  }

  override def contains(value: Int): Boolean = roaringBitmapArray.contains(value.toLong)

  override def cardinality: Long = roaringBitmapArray.cardinality

  override def isEmpty: Boolean = roaringBitmapArray.isEmpty

  override protected def toArrayForTestingImpl: Array[Long] = roaringBitmapArray.toArray

  override def serializeAsByteArray(): Array[Byte] =
    serializedBytesOrPositions match {
      // Defensive copy: Don't expose internal buffer for a caller to mutate.
      case Left(bytes) if !mutated => bytes.clone()
      case _ => roaringBitmapArray.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
    }

}

object RoaringManifestBitmap {
  def fromPositions(values: Seq[Int]): RoaringManifestBitmap =
    new RoaringManifestBitmap(Right(values))

  def fromSerializedByteArray(serializedBytes: Array[Byte]): RoaringManifestBitmap =
    new RoaringManifestBitmap(Left(serializedBytes))
}
