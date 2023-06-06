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

import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.immutable.TreeSet

import com.google.common.primitives.Ints

import org.apache.spark.SparkFunSuite

class RoaringBitmapArraySuite extends SparkFunSuite {

  final val BITMAP2_NUMBER = Int.MaxValue.toLong * 3L
  /** RoaringBitmap containers mostly use `Char` constants internally, so this is consistent. */
  final val CONTAINER_BOUNDARY = Char.MaxValue.toLong + 1L
  final val BITMAP_BOUNDARY = 0xFFFFFFFFL + 1L

  private def testEquality(referenceResult: Seq[Long])(
    testOps: (RoaringBitmapArray => Unit)*): Unit = {
    val referenceBitmap = RoaringBitmapArray(referenceResult: _*)
    val testBitmap = RoaringBitmapArray()
    testOps.foreach(op => op(testBitmap))
    assert(testBitmap === referenceBitmap)
    assert(testBitmap.## === referenceBitmap.##)
    assert(testBitmap.toArray === referenceBitmap.toArray)
  }

  test("equality") {
    testEquality(Seq(1))(_.add(1))
    testEquality(Nil)(_.add(1), _.remove(1))
    testEquality(Seq(1))(_.add(1), _.add(1))
    testEquality(Nil)(_.add(1), _.add(1), _.remove(1))
    testEquality(Nil)(_.add(1), _.remove(1), _.remove(1))
    testEquality(Nil)(_.add(1), _.add(1), _.remove(1), _.remove(1))
    testEquality(Seq(1))(_.add(1), _.remove(1), _.add(1))
    testEquality(Nil)(_.add(1), _.remove(1), _.add(1), _.remove(1))

    testEquality(Seq(BITMAP2_NUMBER))(_.add(BITMAP2_NUMBER))
    testEquality(Nil)(_.add(BITMAP2_NUMBER), _.remove(BITMAP2_NUMBER))
    testEquality(Seq(BITMAP2_NUMBER))(_.add(BITMAP2_NUMBER), _.add(BITMAP2_NUMBER))
    testEquality(Nil)(_.add(BITMAP2_NUMBER), _.add(BITMAP2_NUMBER), _.remove(BITMAP2_NUMBER))
    testEquality(Nil)(_.add(BITMAP2_NUMBER), _.remove(BITMAP2_NUMBER), _.remove(BITMAP2_NUMBER))
    testEquality(Nil)(
      _.add(BITMAP2_NUMBER),
      _.add(BITMAP2_NUMBER),
      _.remove(BITMAP2_NUMBER),
      _.remove(BITMAP2_NUMBER))
    testEquality(Seq(BITMAP2_NUMBER))(
      _.add(BITMAP2_NUMBER),
      _.remove(BITMAP2_NUMBER),
      _.add(BITMAP2_NUMBER))
    testEquality(Nil)(
      _.add(BITMAP2_NUMBER),
      _.remove(BITMAP2_NUMBER),
      _.add(BITMAP2_NUMBER),
      _.remove(BITMAP2_NUMBER))

    testEquality(Seq(1, BITMAP2_NUMBER))(_.add(1), _.add(BITMAP2_NUMBER))
    testEquality(Seq(BITMAP2_NUMBER))(_.add(1), _.add(BITMAP2_NUMBER), _.remove(1))
    testEquality(Seq(1, BITMAP2_NUMBER))(_.add(BITMAP2_NUMBER), _.add(1))
    testEquality(Seq(BITMAP2_NUMBER))(_.add(BITMAP2_NUMBER), _.add(1), _.remove(1))
    testEquality(Seq(BITMAP2_NUMBER))(_.add(1), _.remove(1), _.add(BITMAP2_NUMBER))
    testEquality(Nil)(_.add(1), _.remove(1), _.add(BITMAP2_NUMBER), _.remove(BITMAP2_NUMBER))
    testEquality(Nil)(_.add(1), _.add(BITMAP2_NUMBER), _.remove(1), _.remove(BITMAP2_NUMBER))
    testEquality(Nil)(_.add(BITMAP2_NUMBER), _.add(1), _.remove(1), _.remove(BITMAP2_NUMBER))
    testEquality(Nil)(_.add(BITMAP2_NUMBER), _.add(1), _.remove(BITMAP2_NUMBER), _.remove(1))

    val denseSequence = 1L to (3L * CONTAINER_BOUNDARY)
    def addAll(v: Long): RoaringBitmapArray => Unit = rb => rb.add(v)
    testEquality(denseSequence)(denseSequence.map(addAll): _*)
    testEquality(denseSequence)(denseSequence.reverse.map(addAll): _*)

    val sparseSequence = 1L to BITMAP2_NUMBER by CONTAINER_BOUNDARY
    testEquality(sparseSequence)(sparseSequence.map(addAll): _*)
    testEquality(sparseSequence)(sparseSequence.reverse.map(addAll): _*)
  }

  /**
   * A [[RoaringBitmapArray]] that contains all 3 container types
   * in two [[org.roaringbitmap.RoaringBitmap]] instances.
   */
  lazy val allContainerTypesBitmap: RoaringBitmapArray = {
    val bitmap = RoaringBitmapArray()
    // RoaringBitmap 1 Container 1 (Array)
    bitmap.addAll(1L, 17L, 63000L, CONTAINER_BOUNDARY - 1)
    // RoaringBitmap 1 Container 2 (RLE)
    bitmap.addRange((CONTAINER_BOUNDARY + 500L) until (CONTAINER_BOUNDARY + 1200L))
    // RoaringBitmap 1 Container 3 (Bitset)
    bitmap.addRange((2L * CONTAINER_BOUNDARY) until (3L * CONTAINER_BOUNDARY - 1L) by 3L)

    // RoaringBitmap 2 Container 1 (Array)
    bitmap.addAll(
      BITMAP_BOUNDARY, BITMAP_BOUNDARY + 17L,
      BITMAP_BOUNDARY + 63000L,
      BITMAP_BOUNDARY + CONTAINER_BOUNDARY - 1)
    // RoaringBitmap 2 Container 2 (RLE)
    bitmap.addRange((BITMAP_BOUNDARY + CONTAINER_BOUNDARY + 500L) until
      (BITMAP_BOUNDARY + CONTAINER_BOUNDARY + 1200L))
    // RoaringBitmap 2 Container 3 (Bitset)
    bitmap.addRange((BITMAP_BOUNDARY + 2L * CONTAINER_BOUNDARY) until
      (BITMAP_BOUNDARY + 3L * CONTAINER_BOUNDARY - 1L) by 3L)

    // Check that RLE containers are actually created.
    assert(bitmap.runOptimize())

    bitmap
  }

  for (serializationFormat <- RoaringBitmapArrayFormat.values) {
    test(s"serialization - $serializationFormat") {
      checkSerializeDeserialize(RoaringBitmapArray(), serializationFormat)
      checkSerializeDeserialize(RoaringBitmapArray(1L), serializationFormat)
      checkSerializeDeserialize(RoaringBitmapArray(BITMAP2_NUMBER), serializationFormat)
      checkSerializeDeserialize(RoaringBitmapArray(1L, BITMAP2_NUMBER), serializationFormat)
      checkSerializeDeserialize(allContainerTypesBitmap, serializationFormat)
    }
  }

  private def checkSerializeDeserialize(
      input: RoaringBitmapArray,
      format: RoaringBitmapArrayFormat.Value): Unit = {
    val serializedSize = Ints.checkedCast(input.serializedSizeInBytes(format))
    val buffer = ByteBuffer.allocate(serializedSize).order(ByteOrder.LITTLE_ENDIAN)
    input.serialize(buffer, format)
    val output = RoaringBitmapArray()
    buffer.flip()
    output.deserialize(buffer)
    assert(input === output)
  }

  for (serializationFormat <- RoaringBitmapArrayFormat.values) {
    test(
      s"serialization and deserialization with big endian buffers throws - $serializationFormat") {
      val roaringBitmapArray = RoaringBitmapArray(1L)
      val bigEndianBuffer = ByteBuffer
        .allocate(roaringBitmapArray.serializedSizeInBytes(serializationFormat).toInt)
        .order(ByteOrder.BIG_ENDIAN)

      assertThrows[IllegalArgumentException] {
        roaringBitmapArray.serialize(bigEndianBuffer, serializationFormat)
      }

      assertThrows[IllegalArgumentException] {
        roaringBitmapArray.deserialize(bigEndianBuffer)
      }
    }
  }

  test("empty") {
    val bitmap = RoaringBitmapArray()
    assert(bitmap.isEmpty)
    assert(bitmap.cardinality === 0L)
    assert(!bitmap.contains(0L))
    assert(bitmap.toArray === Array.empty[Long])
    var hadValue = false
    bitmap.forEach(_ => hadValue = true)
    assert(!hadValue)
  }

  test("special values") {
    testSpecialValue(0L)
    testSpecialValue(Int.MaxValue.toLong)
    testSpecialValue(CONTAINER_BOUNDARY - 1L)
    testSpecialValue(CONTAINER_BOUNDARY)
    testSpecialValue(BITMAP_BOUNDARY - 1L)
    testSpecialValue(BITMAP_BOUNDARY)
    testSpecialValue(3L * BITMAP_BOUNDARY + 42L)
  }

  private def testSpecialValue(value: Long): Unit = {
    val bitmap = RoaringBitmapArray(value)
    assert(bitmap.cardinality === 1L)
    assert(bitmap.contains(value))
    assert(bitmap.toArray === Array(value))
    var valueCount = 0
    bitmap.forEach { v =>
      valueCount += 1
      assert(v === value)
    }
    assert(valueCount === 1)
    bitmap.remove(value)
    assert(!bitmap.contains(value))
    assert(bitmap.cardinality === 0L)
  }

  test("negative numbers") {
    assertThrows[IllegalArgumentException] {
      val bitmap = RoaringBitmapArray()
      bitmap.add(-1L)
    }
    assertThrows[IllegalArgumentException] {
      RoaringBitmapArray(-1L)
    }
    assertThrows[IllegalArgumentException] {
      val bitmap = RoaringBitmapArray(1L)
      bitmap.remove(-1L)
    }
    assertThrows[IllegalArgumentException] {
      val bitmap = RoaringBitmapArray()
      bitmap.add(Long.MaxValue)
    }
    assertThrows[IllegalArgumentException] {
      RoaringBitmapArray(Long.MaxValue)
    }
    assertThrows[IllegalArgumentException] {
      val bitmap = RoaringBitmapArray(1L)
      bitmap.remove(Long.MaxValue)
    }
    assertThrows[IllegalArgumentException] {
      val bitmap = RoaringBitmapArray()
      bitmap.addAll(-1L, 1L)
    }
    assertThrows[IllegalArgumentException] {
      val bitmap = RoaringBitmapArray()
      bitmap.addRange(-3 to 1)
    }
    assertThrows[IllegalArgumentException] {
      val bitmap = RoaringBitmapArray()
      bitmap.addRange(-3L to 1L)
    }
  }

  private def testContainsButNoSimilarValues(value: Long, bitmap: RoaringBitmapArray): Unit = {
    assert(bitmap.contains(value))
    for (i <- 1 to 3) {
      assert(!bitmap.contains(value + i * CONTAINER_BOUNDARY))
      assert(!bitmap.contains(value + i * BITMAP_BOUNDARY))
    }
  }

  test("small integers") {
    val bitmap = RoaringBitmapArray(
      3L, 4L, CONTAINER_BOUNDARY - 1L, CONTAINER_BOUNDARY, Int.MaxValue.toLong)
    assert(bitmap.cardinality === 5L)
    testContainsButNoSimilarValues(3L, bitmap)
    testContainsButNoSimilarValues(4L, bitmap)
    testContainsButNoSimilarValues(CONTAINER_BOUNDARY - 1L, bitmap)
    testContainsButNoSimilarValues(CONTAINER_BOUNDARY, bitmap)
    testContainsButNoSimilarValues(Int.MaxValue.toLong, bitmap)
    assert(bitmap.toArray ===
      Array(3L, 4L, CONTAINER_BOUNDARY - 1L, CONTAINER_BOUNDARY, Int.MaxValue.toLong))
    var values: List[Long] = Nil
    bitmap.forEach { value =>
      values ::= value
    }
    assert(values.reverse ===
      List(3L, 4L, CONTAINER_BOUNDARY - 1L, CONTAINER_BOUNDARY, Int.MaxValue.toLong))
    bitmap.remove(CONTAINER_BOUNDARY)
    assert(!bitmap.contains(CONTAINER_BOUNDARY))
    assert(bitmap.cardinality === 4L)
    testContainsButNoSimilarValues(3L, bitmap)
    testContainsButNoSimilarValues(4L, bitmap)
    testContainsButNoSimilarValues(CONTAINER_BOUNDARY - 1L, bitmap)
    testContainsButNoSimilarValues(Int.MaxValue.toLong, bitmap)
  }

  test("large integers") {
    val container1Number = Int.MaxValue.toLong + 1L
    val container3Number = 2 * BITMAP_BOUNDARY + 1L
    val bitmap = RoaringBitmapArray(
      3L, 4L, container1Number, BITMAP_BOUNDARY, BITMAP2_NUMBER, container3Number)
    assert(bitmap.cardinality === 6L)
    testContainsButNoSimilarValues(3L, bitmap)
    testContainsButNoSimilarValues(4L, bitmap)
    testContainsButNoSimilarValues(container1Number, bitmap)
    testContainsButNoSimilarValues(BITMAP_BOUNDARY, bitmap)
    testContainsButNoSimilarValues(BITMAP2_NUMBER, bitmap)
    testContainsButNoSimilarValues(container3Number, bitmap)
    assert(bitmap.toArray ===
      Array(3L, 4L, container1Number, BITMAP_BOUNDARY, BITMAP2_NUMBER, container3Number))
    var values: List[Long] = Nil
    bitmap.forEach { value =>
      values ::= value
    }
    assert(values.reverse ===
      List(3L, 4L, container1Number, BITMAP_BOUNDARY, BITMAP2_NUMBER, container3Number))
    bitmap.remove(BITMAP_BOUNDARY)
    assert(!bitmap.contains(BITMAP_BOUNDARY))
    assert(bitmap.cardinality === 5L)
    testContainsButNoSimilarValues(3L, bitmap)
    testContainsButNoSimilarValues(4L, bitmap)
    testContainsButNoSimilarValues(container1Number, bitmap)
    testContainsButNoSimilarValues(BITMAP2_NUMBER, bitmap)
    testContainsButNoSimilarValues(container3Number, bitmap)
  }

  test("add/remove round-trip") {
    // Single value in the second bitmap
    val bitmap = RoaringBitmapArray(BITMAP2_NUMBER)
    assert(bitmap.contains(BITMAP2_NUMBER))
    bitmap.remove(BITMAP2_NUMBER)
    assert(!bitmap.contains(BITMAP2_NUMBER))
    bitmap.add(BITMAP2_NUMBER)
    assert(bitmap.contains(BITMAP2_NUMBER))

    // Two values in two bitmaps
    bitmap.add(CONTAINER_BOUNDARY)
    assert(bitmap.contains(CONTAINER_BOUNDARY))
    assert(bitmap.contains(BITMAP2_NUMBER))
    bitmap.remove(CONTAINER_BOUNDARY)
    assert(!bitmap.contains(CONTAINER_BOUNDARY))
    assert(bitmap.contains(BITMAP2_NUMBER))
    bitmap.add(CONTAINER_BOUNDARY)
    assert(bitmap.contains(CONTAINER_BOUNDARY))
    assert(bitmap.contains(BITMAP2_NUMBER))
  }

  test("or") {
    testOr(left = TreeSet.empty, right = TreeSet.empty)
    testOr(left = TreeSet(1L), right = TreeSet.empty)
    testOr(left = TreeSet.empty, right = TreeSet(1L))
    testOr(left = TreeSet(0L, CONTAINER_BOUNDARY), right = TreeSet(1L, BITMAP_BOUNDARY - 1L))
    testOr(
      left = TreeSet(0L, CONTAINER_BOUNDARY, BITMAP2_NUMBER),
      right = TreeSet(1L, BITMAP_BOUNDARY - 1L))
    testOr(
      left = TreeSet(0L, CONTAINER_BOUNDARY),
      right = TreeSet(1L, BITMAP_BOUNDARY - 1L, BITMAP2_NUMBER))
  }

  private def testOr(left: TreeSet[Long], right: TreeSet[Long]): Unit = {
    val leftBitmap = RoaringBitmapArray(left.toSeq: _*)
    val rightBitmap = RoaringBitmapArray(right.toSeq: _*)

    val expected = left.union(right).toSeq

    leftBitmap.or(rightBitmap)

    assert(leftBitmap.toArray.toSeq === expected)
  }

  test("andNot") {
    testAndNot(left = TreeSet.empty, right = TreeSet.empty)
    testAndNot(left = TreeSet(1L), right = TreeSet.empty)
    testAndNot(left = TreeSet.empty, right = TreeSet(1L))
    testAndNot(left = TreeSet(0L, CONTAINER_BOUNDARY), right = TreeSet(1L, BITMAP_BOUNDARY - 1L))
    testAndNot(
      left = TreeSet(0L, CONTAINER_BOUNDARY, BITMAP2_NUMBER),
      right = TreeSet(1L, BITMAP_BOUNDARY - 1L))
    testAndNot(
      left = TreeSet(0L, CONTAINER_BOUNDARY),
      right = TreeSet(1L, BITMAP_BOUNDARY - 1L, BITMAP2_NUMBER))
  }

  private def testAndNot(left: TreeSet[Long], right: TreeSet[Long]): Unit = {
    val leftBitmap = RoaringBitmapArray()
    left.foreach(leftBitmap.add)
    val rightBitmap = RoaringBitmapArray()
    right.foreach(rightBitmap.add)

    val expected = left.diff(right).toArray

    leftBitmap.andNot(rightBitmap)

    assert(leftBitmap.toArray === expected)
  }

  test("and") {
    // Empty result
    testAnd(left = TreeSet.empty, right = TreeSet.empty)
    testAnd(left = TreeSet.empty, right = TreeSet(1L))
    testAnd(left = TreeSet.empty, right = TreeSet(1L, BITMAP_BOUNDARY - 1L))
    testAnd(left = TreeSet.empty, right = TreeSet(0L, CONTAINER_BOUNDARY, BITMAP2_NUMBER))
    testAnd(left = TreeSet(1L), right = TreeSet.empty)
    testAnd(left = TreeSet(1L), right = TreeSet(BITMAP_BOUNDARY))
    testAnd(left = TreeSet(1L), right = TreeSet(CONTAINER_BOUNDARY))
    testAnd(left = TreeSet(1L, BITMAP_BOUNDARY - 1L), right = TreeSet.empty)
    testAnd(left = TreeSet(0L, CONTAINER_BOUNDARY, BITMAP2_NUMBER), right = TreeSet.empty)
    testAnd(
      left = TreeSet(0L, CONTAINER_BOUNDARY, BITMAP2_NUMBER),
      right = TreeSet(1L, BITMAP_BOUNDARY - 1L))
    testAnd(
      left = TreeSet(0L, CONTAINER_BOUNDARY),
      right = TreeSet(1L, BITMAP_BOUNDARY - 1L, BITMAP2_NUMBER))

    // Non empty result
    testAnd(left = TreeSet(0L, 5L, 10L), right = TreeSet(5L, 15L))
    testAnd(
      left = TreeSet(0L, CONTAINER_BOUNDARY, BITMAP2_NUMBER),
      right = TreeSet(1L, BITMAP2_NUMBER))
    testAnd(
      left = TreeSet(1L, BITMAP_BOUNDARY, CONTAINER_BOUNDARY),
      right = TreeSet(1L, BITMAP_BOUNDARY, CONTAINER_BOUNDARY))
  }

  private def testAnd(left: TreeSet[Long], right: TreeSet[Long]): Unit = {
    val leftBitmap = RoaringBitmapArray()
    leftBitmap.addAll(left.toSeq: _*)
    val rightBitmap = RoaringBitmapArray()
    rightBitmap.addAll(right.toSeq: _*)

    leftBitmap.and(rightBitmap)
    val expected = left.intersect(right)
    assert(leftBitmap.toArray === expected.toArray)
  }

  test("clear") {
    testEquality(Nil)(_.add(1), _.clear())
    testEquality(Nil)(_.add(1), _.add(1), _.clear())
    testEquality(Nil)(_.add(1), _.clear(), _.clear())
    testEquality(Nil)(_.add(1), _.add(1), _.clear(), _.clear())
    testEquality(Seq(1))(_.add(1), _.clear(), _.add(1))
    testEquality(Nil)(_.add(1), _.clear(), _.add(1), _.clear())

    testEquality(Nil)(_.add(BITMAP2_NUMBER), _.clear())
    testEquality(Nil)(_.add(BITMAP2_NUMBER), _.add(BITMAP2_NUMBER), _.clear())
    testEquality(Nil)(_.add(BITMAP2_NUMBER), _.clear(), _.clear())
    testEquality(Nil)(_.add(BITMAP2_NUMBER), _.add(BITMAP2_NUMBER), _.clear(), _.clear())
    testEquality(Seq(BITMAP2_NUMBER))(_.add(BITMAP2_NUMBER), _.clear(), _.add(BITMAP2_NUMBER))
    testEquality(Nil)(_.add(BITMAP2_NUMBER), _.clear(), _.add(BITMAP2_NUMBER), _.clear())

    testEquality(Nil)(_.add(1), _.add(BITMAP2_NUMBER), _.clear())
    testEquality(Nil)(_.add(BITMAP2_NUMBER), _.add(1), _.clear())
    testEquality(Seq(BITMAP2_NUMBER))(_.add(1), _.clear(), _.add(BITMAP2_NUMBER))
    testEquality(Nil)(_.add(1), _.clear(), _.add(BITMAP2_NUMBER), _.clear())
    testEquality(Nil)(_.add(1), _.add(BITMAP2_NUMBER), _.clear(), _.clear())

    val denseSequence = 1L to (3L * CONTAINER_BOUNDARY)
    testEquality(Nil)(_.addAll(denseSequence: _*), _.clear())

    val sparseSequence = 1L to BITMAP2_NUMBER by CONTAINER_BOUNDARY
    testEquality(Nil)(_.addAll(sparseSequence: _*), _.clear())
  }

  test("bulk adds") {

    def testArrayEquality(referenceResult: Seq[Long], command: RoaringBitmapArray => Unit): Unit = {
      val testBitmap = RoaringBitmapArray()
      command(testBitmap)
      assert(testBitmap.toArray.toSeq === referenceResult)
    }

    val bitmap = RoaringBitmapArray(1L, 5L, CONTAINER_BOUNDARY, BITMAP_BOUNDARY)
    assert(bitmap.toArray.toSeq === Seq(1L, 5L, CONTAINER_BOUNDARY, BITMAP_BOUNDARY))

    testArrayEquality(
      referenceResult = Seq(1L, 5L, CONTAINER_BOUNDARY, BITMAP_BOUNDARY),
      command = _.addAll(1L, 5L, CONTAINER_BOUNDARY, BITMAP_BOUNDARY))

    testArrayEquality(
      referenceResult = (CONTAINER_BOUNDARY - 5L) to (CONTAINER_BOUNDARY + 5L),
      command = _.addRange((CONTAINER_BOUNDARY - 5L) to (CONTAINER_BOUNDARY + 5L)))

    testArrayEquality(
      referenceResult = (CONTAINER_BOUNDARY - 5L) to (CONTAINER_BOUNDARY + 5L) by 3L,
      command = _.addRange((CONTAINER_BOUNDARY - 5L) to (CONTAINER_BOUNDARY + 5L) by 3L))

    // Int ranges call a different method.
    testArrayEquality(
      referenceResult = (CONTAINER_BOUNDARY - 5L) to (CONTAINER_BOUNDARY + 5L),
      command = _.addRange((CONTAINER_BOUNDARY - 5L).toInt to (CONTAINER_BOUNDARY + 5L).toInt))

    testArrayEquality(
      referenceResult = (CONTAINER_BOUNDARY - 5L) to (CONTAINER_BOUNDARY + 5L) by 3L,
      command = _.addRange((CONTAINER_BOUNDARY - 5L).toInt to (CONTAINER_BOUNDARY + 5L).toInt by 3))

    testArrayEquality(
      referenceResult = (BITMAP_BOUNDARY - 5L) to BITMAP_BOUNDARY,
      command = _.addRange((BITMAP_BOUNDARY - 5L) to BITMAP_BOUNDARY))

    testArrayEquality(
      referenceResult = (BITMAP_BOUNDARY - 5L) to (BITMAP_BOUNDARY + 5L),
      command = _.addRange((BITMAP_BOUNDARY - 5L) to (BITMAP_BOUNDARY + 5L)))

    testArrayEquality(
      referenceResult = BITMAP_BOUNDARY to (BITMAP_BOUNDARY + 5L),
      command = _.addRange(BITMAP_BOUNDARY to (BITMAP_BOUNDARY + 5L)))
  }

  test("large cardinality") {
    val bitmap = RoaringBitmapArray()
    // We can't produce ranges in Scala whose lengths would be greater than Int.MaxValue
    // so we add them in stages of Int.MaxValue / 2 instead.
    for (index <- 0 until 6) {
      val start = index.toLong * Int.MaxValue.toLong / 2L
      val end = (index.toLong + 1L) * Int.MaxValue.toLong / 2L
      bitmap.addRange(start until end)
    }
    assert(bitmap.cardinality === (3L * Int.MaxValue.toLong))
    for (index <- 0 until 6) {
      val start = index.toLong * Int.MaxValue.toLong / 2L
      val end = (index.toLong + 1L) * Int.MaxValue.toLong / 2L
      val stride = 1023
      for (pos <- start until end by stride) {
        assert(bitmap.contains(pos))
      }
    }
    assert(!bitmap.contains(3L * Int.MaxValue.toLong))
    assert(!bitmap.contains(3L * Int.MaxValue.toLong + 42L))
  }

  test("first/last") {
    {
      val bitmap = RoaringBitmapArray()
      assert(bitmap.first.isEmpty)
      assert(bitmap.last.isEmpty)
    }
    // Single value bitmaps.
    val valuesOfInterest = Seq(0L, 1L, 64L, CONTAINER_BOUNDARY, BITMAP_BOUNDARY, BITMAP2_NUMBER)
    for (v <- valuesOfInterest) {
      val bitmap = RoaringBitmapArray(v)
      assert(bitmap.first === Some(v))
      assert(bitmap.last === Some(v))
    }
    // Two value bitmaps.
    for {
      start <- valuesOfInterest
      end <- valuesOfInterest
      if start < end
    } {
      val bitmap = RoaringBitmapArray(start, end)
      assert(bitmap.first === Some(start))
      assert(bitmap.last === Some(end))
    }
  }

}
