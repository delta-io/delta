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

import java.io.IOException
import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.immutable.NumericRange

import com.google.common.primitives.{Ints, UnsignedInts}
import org.roaringbitmap.{RelativeRangeConsumer, RoaringBitmap}

/**
 * A 64-bit extension of [[RoaringBitmap]] that is optimized for cases that usually fit within
 * a 32-bit bitmap, but may run over by a few bits on occasion.
 *
 * This focus makes it different from [[org.roaringbitmap.longlong.Roaring64NavigableMap]] and
 * [[org.roaringbitmap.longlong.Roaring64Bitmap]] which focus on sparse bitmaps over the whole
 * 64-bit range.
 *
 * Structurally, this implementation simply uses the most-significant 4 bytes to index into
 * an array of 32-bit [[RoaringBitmap]] instances.
 * The array is grown as necessary to accommodate the largest value in the bitmap.
 *
 * *Note:* As opposed to the other two 64-bit bitmap implementations mentioned above,
 *         this implementation cannot accommodate `Long` values where the most significant
 *         bit is non-zero (i.e., negative `Long` values).
 *         It cannot even accommodate values where the 4 high-order bytes are `Int.MaxValue`,
 *         because then the length of the `bitmaps` array would be a negative number
 *         (`Int.MaxValue + 1`).
 */
final class RoaringBitmapArray extends Equals {
  import RoaringBitmapArray._

  private var bitmaps: Array[RoaringBitmap] = Array.empty

  /**
   * Add the value to the container (set the value to `true`),
   * whether it already appears or not.
   */
  def add(value: Long): Unit = {
    require(value >= 0 && value <= MAX_REPRESENTABLE_VALUE)
    val (high, low) = decomposeHighLowBytes(value)
    if (high >= bitmaps.length) {
      extendBitmaps(newLength = high + 1)
    }
    val highBitmap = bitmaps(high)
    highBitmap.add(low)
  }

  /** Add all `values` to the container. For testing purposes only. */
  protected[delta] def addAll(values: Long*): Unit = values.foreach(add)

  /** Add all values in `range` to the container. */
  protected[delta] def addRange(range: Range): Unit = {
    require(0 <= range.start && range.start <= range.end)
    if (range.isEmpty) return // Nothing to do.
    if (range.step != 1) {
      // Can't optimize in this case.
      range.foreach(i => add(UnsignedInts.toLong(i)))
      return
    }
    // This is an Int range, so it must fit completely into the first bitmap.
    if (bitmaps.isEmpty) {
      extendBitmaps(newLength = 1)
    }
    val end = if (range.isInclusive) range.end + 1 else range.end
    bitmaps.head.add(range.start, end)
  }

  /** Add all values in `range` to the container. */
  protected[delta] def addRange(range: NumericRange[Long]): Unit = {
    require(0L <= range.start && range.start <= range.end && range.end <= MAX_REPRESENTABLE_VALUE)
    if (range.isEmpty) return // Nothing to do.
    if (range.step != 1L) {
      // Can't optimize in this case.
      range.foreach(add)
      return
    }
    // Decompose into sub-ranges that target a single bitmap,
    // to use the range adds within a bitmap for efficiency.
    val (startHigh, startLow) = decomposeHighLowBytes(range.start)
    val (endHigh, endLow) = decomposeHighLowBytes(range.end)
    val lastHigh = if (endLow == 0 && !range.isInclusive) endHigh - 1 else endHigh
    if (lastHigh >= bitmaps.length) {
      extendBitmaps(newLength = lastHigh + 1)
    }
    var currentHigh = startHigh
    while (currentHigh <= lastHigh) {
      val start = if (currentHigh == startHigh) UnsignedInts.toLong(startLow) else 0L
      // RoaringBitmap.add is exclusive the end boundary.
      val end = if (currentHigh == endHigh) {
        if (range.isInclusive) UnsignedInts.toLong(endLow) + 1L else UnsignedInts.toLong(endLow)
      } else {
        0xFFFFFFFFL + 1L
      }
      bitmaps(currentHigh).add(start, end)
      currentHigh += 1
    }
  }

  /**
   * If present, remove the `value` (effectively, sets its bit value to false).
   *
   * @param value The index in a bitmap.
   */
  protected[deletionvectors] def remove(value: Long): Unit = {
    require(value >= 0 && value <= MAX_REPRESENTABLE_VALUE)
    val (high, low) = decomposeHighLowBytes(value)
    if (high < bitmaps.length) {
      val highBitmap = bitmaps(high)
      highBitmap.remove(low)
      if (highBitmap.isEmpty) {
        // Clean up all bitmaps that are now empty (from the end).
        var latestNonEmpty = bitmaps.length - 1
        var done = false
        while (!done && latestNonEmpty >= 0) {
          if (bitmaps(latestNonEmpty).isEmpty) {
            latestNonEmpty -= 1
          } else {
            done = true
          }
        }
        shrinkBitmaps(latestNonEmpty + 1)
      }
    }
  }

  /** Remove all values from the bitmap. */
  def clear(): Unit = {
    bitmaps = Array.empty
  }

  /**
   * Checks whether the value is included,
   * which is equivalent to checking if the corresponding bit is set.
   */
  def contains(value: Long): Boolean = {
    require(value >= 0 && value <= MAX_REPRESENTABLE_VALUE)
    val high = highBytes(value)
    if (high >= bitmaps.length) {
      false
    } else {
      val highBitmap = bitmaps(high)
      val low = lowBytes(value)
      highBitmap.contains(low)
    }
  }

  /**
   * Return the set values as an array, if the cardinality is smaller than 2147483648.
   *
   * The integer values are in sorted order.
   */
  def toArray: Array[Long] = {
    val cardinality = this.cardinality
    require(cardinality <= Int.MaxValue)
    val values = Array.ofDim[Long](cardinality.toInt)
    var valuesIndex = 0
    for ((bitmap, bitmapIndex) <- bitmaps.zipWithIndex) {
      bitmap.forEach((value: Int) => {
        values(valuesIndex) = composeFromHighLowBytes(bitmapIndex, value)
        valuesIndex += 1
      })
    }
    values
  }

  /** Materialise the whole set into an array */
  def values: Array[Long] = toArray

  /** Returns the number of distinct integers added to the bitmap (e.g., number of bits set). */
  def cardinality: Long = bitmaps.foldLeft(0L)((sum, bitmap) => sum + bitmap.getLongCardinality)

  /** Tests whether the bitmap is empty. */
  def isEmpty: Boolean = bitmaps.forall(_.isEmpty)

  /**
   * Use a run-length encoding where it is more space efficient.
   *
   * @return `true` if a change was applied
   */
  def runOptimize(): Boolean = {
    var changeApplied = false
    for (bitmap <- bitmaps) {
      changeApplied |= bitmap.runOptimize()
    }
    changeApplied
  }

  /**
   * Remove run-length encoding even when it is more space efficient.
   *
   * @return `true` if a change was applied
   */
  def removeRunCompression(): Boolean = {
    var changeApplied = false
    for (bitmap <- bitmaps) {
      changeApplied |= bitmap.removeRunCompression()
    }
    changeApplied
  }

  /**
   * In-place bitwise OR (union) operation.
   *
   * The current bitmap is modified.
   */
  def or(that: RoaringBitmapArray): Unit = {
    if (this.bitmaps.length < that.bitmaps.length) {
      extendBitmaps(newLength = that.bitmaps.length)
    }
    for (index <- that.bitmaps.indices) {
      val thisBitmap = this.bitmaps(index)
      val thatBitmap = that.bitmaps(index)
      thisBitmap.or(thatBitmap)
    }
  }

  /** Merges the `other` set into this one. */
  def merge(other: RoaringBitmapArray): Unit = this.or(other)

  /** Get values in `this` but not `that`. */
  def diff(other: RoaringBitmapArray): Unit = this.andNot(other)

  /** Copy `this` along with underlying bitmaps to a new instance. */
  def copy(): RoaringBitmapArray = {
    val newBitmap = new RoaringBitmapArray()
    newBitmap.merge(this)
    newBitmap
  }

  /**
   * In-place bitwise AND (this & that) operation.
   *
   * The current bitmap is modified.
   */
  def and(that: RoaringBitmapArray): Unit = {
    for (index <- 0 until this.bitmaps.length) {
      val thisBitmap = this.bitmaps(index)
      if (index < that.bitmaps.length) {
        val thatBitmap = that.bitmaps(index)
        thisBitmap.and(thatBitmap)
      } else {
        thisBitmap.clear()
      }
    }
  }

  /**
   * In-place bitwise AND-NOT (this & ~that) operation.
   *
   * The current bitmap is modified.
   */
  def andNot(that: RoaringBitmapArray): Unit = {
    val validLength = math.min(this.bitmaps.length, that.bitmaps.length)
    for (index <- 0 until validLength) {
      val thisBitmap = this.bitmaps(index)
      val thatBitmap = that.bitmaps(index)
      thisBitmap.andNot(thatBitmap)
    }
  }

  /**
   * Report the number of bytes required to serialize this bitmap.
   *
   * This is the number of bytes written out when using the [[serialize]] method.
   */
  def serializedSizeInBytes(format: RoaringBitmapArrayFormat.Value): Long = {
    val magicNumberSize = 4

    val serializedBitmapsSize = format.formatImpl.serializedSizeInBytes(bitmaps)

    magicNumberSize + serializedBitmapsSize
  }

  /**
   * Serialize this [[RoaringBitmapArray]] into the `buffer`.
   *
   * == Format ==
   * - A Magic Number indicating the format used (4 bytes)
   * - The actual data as specified by the format.
   *
   */
  def serialize(buffer: ByteBuffer, format: RoaringBitmapArrayFormat.Value): Unit = {
    require(ByteOrder.LITTLE_ENDIAN == buffer.order(),
      "RoaringBitmapArray has to be serialized using a little endian buffer")
    // Magic number to make sure we don't try to deserialize a simple RoaringBitmap or the wrong
    // format later.
    buffer.putInt(format.formatImpl.MAGIC_NUMBER)
    format.formatImpl.serialize(bitmaps, buffer)
  }

  /** Serializes this [[RoaringBitmapArray]] and returns the serialized form as a byte array. */
  def serializeAsByteArray(format: RoaringBitmapArrayFormat.Value): Array[Byte] = {
    val size = serializedSizeInBytes(format)
    if (!size.isValidInt) {
      throw new IOException(
        s"A bitmap was too big to be serialized into an array ($size bytes)")
    }
    val buffer = ByteBuffer.allocate(size.toInt)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    // This is faster than Java serialization.
    // See: https://richardstartin.github.io/posts/roaringbitmap-performance-tricks#serialisation
    serialize(buffer, format)
    buffer.array()
  }

  /**
   * Deserialize the contents of `buffer` into this [[RoaringBitmapArray]].
   *
   * All existing content will be discarded!
   *
   * See [[serialize]] for the expected serialization format.
   */
  def deserialize(buffer: ByteBuffer): Unit = {
    require(ByteOrder.LITTLE_ENDIAN == buffer.order(),
      "RoaringBitmapArray has to be deserialized using a little endian buffer")

    val magicNumber = buffer.getInt
    val serializationFormat = magicNumber match {
      case NativeRoaringBitmapArraySerializationFormat.MAGIC_NUMBER =>
        NativeRoaringBitmapArraySerializationFormat
      case PortableRoaringBitmapArraySerializationFormat.MAGIC_NUMBER =>
        PortableRoaringBitmapArraySerializationFormat
      case _ =>
        throw new IOException(s"Unexpected RoaringBitmapArray magic number $magicNumber")
    }
    bitmaps = serializationFormat.deserialize(buffer)
  }

  /**
   * Consume presence information for all values in the range `[start, start + length)`.
   *
   * @param start Lower bound of values to consume.
   * @param length Maximum number of values to consume.
   * @param rrc Code to be executed for each present or absent value.
   */
  def forAllInRange(start: Long, length: Int, consumer: RelativeRangeConsumer): Unit = {
    // This one is complicated and deserves its own PR,
    // when we actually want to enable it.
    throw new UnsupportedOperationException
  }

  /** Execute the `consume` function for every value in the set represented by this bitmap. */
  def forEach(consume: Long => Unit): Unit = {
    for ((bitmap, high) <- bitmaps.zipWithIndex) {
      bitmap.forEach { low: Int =>
        val value = composeFromHighLowBytes(high, low)
        consume(value)
      }
    }
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[RoaringBitmapArray]

  override def equals(other: Any): Boolean = {
    other match {
      case that: RoaringBitmapArray =>
        (this eq that) || // don't need to check canEqual because class is final
          java.util.Arrays.deepEquals(
            this.bitmaps.asInstanceOf[Array[AnyRef]],
            that.bitmaps.asInstanceOf[Array[AnyRef]])
      case _ => false
    }
  }

  override def hashCode: Int = 131 * java.util.Arrays.deepHashCode(
    bitmaps.asInstanceOf[Array[AnyRef]])

  def mkString(start: String = "", sep: String = "", end: String = ""): String =
    toArray.mkString(start, sep, end)

  def first: Option[Long] = {
    for ((bitmap, high) <- bitmaps.zipWithIndex) {
      if (!bitmap.isEmpty) {
        val low = bitmap.first()
        return Some(composeFromHighLowBytes(high, low))
      }
    }
    None
  }

  def last: Option[Long] = {
    for ((bitmap, high) <- bitmaps.zipWithIndex.reverse) {
      if (!bitmap.isEmpty) {
        val low = bitmap.last()
        return Some(composeFromHighLowBytes(high, low))
      }
    }
    None
  }

  /**
   * Utility method to extend the array of [[RoaringBitmap]] to given length, keeping
   * the existing elements in place.
   */
  private def extendBitmaps(newLength: Int): Unit = {
    // Optimization for the most common case
    if (bitmaps.isEmpty && newLength == 1) {
      bitmaps = Array(new RoaringBitmap())
      return
    }
    val newBitmaps = Array.ofDim[RoaringBitmap](newLength)
    System.arraycopy(
      bitmaps, // source
      0, // source start pos
      newBitmaps, // dest
      0, // dest start pos
      bitmaps.length) // number of entries to copy
    for (i <- bitmaps.length until newLength) {
      newBitmaps(i) = new RoaringBitmap()
    }
    bitmaps = newBitmaps
  }

  /** Utility method to shrink the array of [[RoaringBitmap]] to given length. */
  private def shrinkBitmaps(newLength: Int): Unit = {
    if (newLength == 0) {
      bitmaps = Array.empty
    } else {
      val newBitmaps = Array.ofDim[RoaringBitmap](newLength)
      System.arraycopy(
        bitmaps, // source
        0, // source start pos
        newBitmaps, // dest
        0, // dest start pos
        newLength) // number of entries to copy
      bitmaps = newBitmaps
    }
  }

  // For testing purposes
  protected[delta] def toBitmap32Bit(): RoaringBitmap = {
    val bitmap32 = new RoaringBitmap()
    forEach { value =>
      val value32 = Ints.checkedCast(value)
      bitmap32.add(value32)
    }
    bitmap32.runOptimize()
    bitmap32
  }
}

object RoaringBitmapArray {

  /** The largest value a [[RoaringBitmapArray]] can possibly represent. */
  final val MAX_REPRESENTABLE_VALUE: Long = composeFromHighLowBytes(Int.MaxValue - 1, Int.MinValue)
  final val MAX_BITMAP_CARDINALITY: Long = 1L << 32

  /** Create a new [[RoaringBitmapArray]] with the given `values`. */
  def apply(values: Long*): RoaringBitmapArray = {
    val bitmap = new RoaringBitmapArray
    bitmap.addAll(values: _*)
    bitmap
  }

  /**
   *
   * @param value Any `Long`; positive or negative.
   * @return An `Int` holding the 4 high-order bytes of information of the input `value`.
   */
  def highBytes(value: Long): Int = (value >> 32).toInt

  /**
   *
   * @param value Any `Long`; positive or negative.
   * @return An `Int` holding the 4 low-order bytes of information of the input `value`.
   */
  def lowBytes(value: Long): Int = value.toInt

  /** Separate high and low 4 bytes into a pair of `Int`s (high, low). */
  def decomposeHighLowBytes(value: Long): (Int, Int) = (highBytes(value), lowBytes(value))

  /**
   * Combine high and low 4 bytes of a pair of `Int`s into a `Long`.
   *
   * This is essentially the inverse of [[decomposeHighLowBytes()]].
   *
   * @param high An `Int` representing the 4 high-order bytes of the output `Long`
   * @param low An `Int` representing the 4 low-order bytes of the output `Long`
   * @return A `Long` composing the `high` and `low` bytes.
   */
  def composeFromHighLowBytes(high: Int, low: Int): Long =
    (high.toLong << 32) | (low.toLong & 0xFFFFFFFFL) // Must bitmask to avoid sign extension.

  /** Deserialize the right instance from the given bytes */
  def readFrom(bytes: Array[Byte]): RoaringBitmapArray = {
    val buffer = ByteBuffer.wrap(bytes)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    val bitmap = new RoaringBitmapArray()
    bitmap.deserialize(buffer)
    bitmap
  }
}

/**
 * Abstracts out how to (de-)serialize the array.
 *
 * All formats are indicated by a magic number in the first 4-bytes,
 * which must be add/stripped by the *caller*.
 */
private[deletionvectors] sealed trait RoaringBitmapArraySerializationFormat {
  /** Magic number prefix for serialization with this format. */
  val MAGIC_NUMBER: Int
  /** The number of bytes written out when using the [[serialize]] method. */
  def serializedSizeInBytes(bitmaps: Array[RoaringBitmap]): Long
  /** Serialize `bitmaps` into `buffer`. */
  def serialize(bitmaps: Array[RoaringBitmap], buffer: ByteBuffer): Unit
  /** Deserialize all bitmaps from the `buffer` into a fresh array. */
  def deserialize(buffer: ByteBuffer): Array[RoaringBitmap]
}

/** Legal values for the serialization format for [[RoaringBitmapArray]]. */
object RoaringBitmapArrayFormat extends Enumeration {
  protected case class Format(formatImpl: RoaringBitmapArraySerializationFormat)
    extends super.Val

  import scala.language.implicitConversions
  implicit def valueToFormat(x: Value): Format = x.asInstanceOf[Format]

  val Native = Format(NativeRoaringBitmapArraySerializationFormat)
  val Portable = Format(PortableRoaringBitmapArraySerializationFormat)
}

private[deletionvectors] object NativeRoaringBitmapArraySerializationFormat
  extends RoaringBitmapArraySerializationFormat {

  override val MAGIC_NUMBER: Int = 1681511376

  override def serializedSizeInBytes(bitmaps: Array[RoaringBitmap]): Long = {
    val roaringBitmapsCountSize = 4

    val roaringBitmapLengthSize = 4
    val roaringBitmapsSize = bitmaps.foldLeft(0L) { (sum, bitmap) =>
      sum + bitmap.serializedSizeInBytes() + roaringBitmapLengthSize
    }

    roaringBitmapsCountSize + roaringBitmapsSize
  }

  /**
   * Serialize `bitmaps` into the `buffer`.
   *
   * == Format ==
   * - Number of bitmaps (4 bytes)
   * - For each individual bitmap:
   *    - Length of the serialized bitmap
   *    - Serialized bitmap data using the standard format
   *      (see https://github.com/RoaringBitmap/RoaringFormatSpec)
   */
  override def serialize(bitmaps: Array[RoaringBitmap], buffer: ByteBuffer): Unit = {
    buffer.putInt(bitmaps.length)
    for (bitmap <- bitmaps) {
      val placeholderPos = buffer.position()
      buffer.putInt(-1) // Placeholder for the serialized size
      val startPos = placeholderPos + 4
      bitmap.serialize(buffer)
      val endPos = buffer.position()
      val writtenBytes = endPos - startPos
      buffer.putInt(placeholderPos, writtenBytes)
    }
  }

  override def deserialize(buffer: ByteBuffer): Array[RoaringBitmap] = {
    val numberOfBitmaps = buffer.getInt
    if (numberOfBitmaps < 0) {
      throw new IOException(s"Invalid RoaringBitmapArray length" +
        s" ($numberOfBitmaps < 0)")
    }
    val bitmaps = Array.fill(numberOfBitmaps)(new RoaringBitmap())
    for (index <- 0 until numberOfBitmaps) {
      val bitmapSize = buffer.getInt
      bitmaps(index).deserialize(buffer)
      // RoaringBitmap.deserialize doesn't move the buffer's pointer
      buffer.position(buffer.position() + bitmapSize)
    }
    bitmaps
  }
}

/**
 * This is the "official" portable format defined in the spec.
 *
 * See [[https://github.com/RoaringBitmap/RoaringFormatSpec#extention-for-64-bit-implementations]]
 */
private[sql] object PortableRoaringBitmapArraySerializationFormat
  extends RoaringBitmapArraySerializationFormat {

  override val MAGIC_NUMBER: Int = 1681511377

  override def serializedSizeInBytes(bitmaps: Array[RoaringBitmap]): Long = {
    val bitmapCountSize = 8

    val individualBitmapKeySize = 4
    val bitmapSizes = bitmaps.foldLeft(0L) { (sum, bitmap) =>
      sum + bitmap.serializedSizeInBytes() + individualBitmapKeySize
    }

    bitmapCountSize + bitmapSizes
  }

  /**
   * Serialize `bitmaps` into the `buffer`.
   *
   * ==Format==
   *   - Number of bitmaps (8 bytes, upper 4 are basically padding)
   *   - For each individual bitmap, in increasing key order (unsigned, technically, but
   *     RoaringBitmapArray doesn't support negative keys anyway.):
   *     - key of the bitmap (upper 32 bit)
   *     - Serialized bitmap data using the standard format (see
   *       https://github.com/RoaringBitmap/RoaringFormatSpec)
   */
  override def serialize(bitmaps: Array[RoaringBitmap], buffer: ByteBuffer): Unit = {
    buffer.putLong(bitmaps.length.toLong)
    // Iterate in index-order, so that the keys are ascending as required by spec.
    for ((bitmap, index) <- bitmaps.zipWithIndex) {
      // In our array-based implementation the index is the key.
      buffer.putInt(index)
      bitmap.serialize(buffer)
    }
  }
  override def deserialize(buffer: ByteBuffer): Array[RoaringBitmap] = {
    val numberOfBitmaps = buffer.getLong
    // These cases are allowed by the format, but out implementation doesn't support them.
    if (numberOfBitmaps < 0L) {
      throw new IOException(s"Invalid RoaringBitmapArray length ($numberOfBitmaps < 0)")
    }
    if (numberOfBitmaps > Int.MaxValue) {
      throw new IOException(
        s"Invalid RoaringBitmapArray length ($numberOfBitmaps > ${Int.MaxValue})")
    }
    // This format is designed for sparse bitmaps, so numberOfBitmaps is only a lower bound for the
    // actual size of the array.
    val minimumArraySize = numberOfBitmaps.toInt
    val bitmaps = Array.newBuilder[RoaringBitmap]
    bitmaps.sizeHint(minimumArraySize)
    var lastIndex = 0
    for (_ <- 0L until numberOfBitmaps) {
      val key = buffer.getInt
      if (key < 0L) {
        throw new IOException(s"Invalid unsigned entry in RoaringBitmapArray ($key)")
      }
      assert(key >= lastIndex, "Keys are required to be sorted in ascending order.")
      // Fill gaps in sparse data.
      while (lastIndex < key) {
        bitmaps += new RoaringBitmap()
        lastIndex += 1
      }
      val bitmap = new RoaringBitmap()
      bitmap.deserialize(buffer)
      bitmaps += bitmap
      lastIndex += 1
      // RoaringBitmap.deserialize doesn't move the buffer's pointer
      buffer.position(buffer.position() + bitmap.serializedSizeInBytes())
    }
    bitmaps.result()
  }
}

