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

import scala.util.Random

import org.apache.spark.sql.delta.deletionvectors.mumbling.{MumblingBitmap, MumblingBitmapWriter}

import org.apache.spark.SparkFunSuite

class MutableMumblingBitmapSuite extends SparkFunSuite {

  private def newMutableBitmap(positions: Long*): MutableMumblingBitmap = {
    val empty = new MumblingBitmap(
      ByteBuffer.wrap(MumblingBitmapWriter.serialize(Array.emptyIntArray)))
    val bitmap = new MutableMumblingBitmap(empty)
    positions.foreach(bitmap.add)
    bitmap
  }

  private def bufferBytes(bitmap: MumblingBitmap): Array[Byte] = {
    val buffer = bitmap.buffer()
    val bytes = new Array[Byte](buffer.remaining())
    buffer.get(bytes)
    bytes
  }

  private def merge(basePositions: Seq[Int], additions: Int*): MumblingBitmap = {
    val base = new MumblingBitmap(
      ByteBuffer.wrap(MumblingBitmapWriter.serialize(basePositions.toArray)))
    val mutableBitmap = base.toMutable()
    additions.foreach(position => mutableBitmap.add(position.toLong))
    mutableBitmap.toMumblingBitmap
  }

  private def singleContainerDescriptor(bitmap: MumblingBitmap): Int = {
    val bytes = bufferBytes(bitmap)
    assert(bytes(4) == 1 && bytes(5) == 0, "expected exactly one container")
    // PFOR stores a single descriptor as the chunk minimum at byte 8.
    bytes(8) & 0xff
  }

  /** Serialize as Mumbling, read back, and assert the positions survive a full round-trip. */
  private def roundTrip(positions: Seq[Long], name: String): Unit = {
    val decoded = newMutableBitmap(positions: _*).toMumblingBitmap
    val bytes = bufferBytes(decoded)
    // A Mumbling bitmap always begins with the version byte 1.
    assert(bytes.nonEmpty && bytes(0) == 1, s"$name: expected the Mumbling version byte")
    val expected = positions.toSet
    assert(decoded.cardinality() == expected.size, s"$name: cardinality")
    expected.foreach(p => assert(decoded.isSet(p.toInt), s"$name: should contain $p"))
    // Spot-check absent positions around the populated range.
    val maxPos = if (expected.isEmpty) 0L else expected.max
    (0L to math.min(maxPos + 300L, 3000L)).foreach { p =>
      assert(decoded.isSet(p.toInt) == expected.contains(p), s"$name: contains mismatch at $p")
    }
  }

  test("round-trip across sparse, dense, mixed, and container boundaries") {
    roundTrip(Seq.empty, "empty")
    roundTrip(Seq(0L), "single-0")
    roundTrip(Seq(255L), "single-255")
    roundTrip(Seq(0L, 5L, 100L, 255L), "sparse")
    roundTrip((0 until 31).map(_.toLong * 8L), "full-sparse-31")
    roundTrip((0 until 32).map(_.toLong), "dense-32")
    roundTrip((0 until 256).map(_.toLong), "dense-256")
    roundTrip((0 until 128).map(_.toLong * 2L), "dense-even-positions")
    roundTrip(Seq(0L, 255L, 256L, 257L, 512L), "cross-container")
    roundTrip(Seq(5L, 522L), "gap-with-empty-middle-container")
    roundTrip((0 until 32).map(_.toLong) :+ 257L, "mixed-dense-and-sparse")
    roundTrip(
      Seq(MumblingBitmapWriter.MAX_POSITION_EXCLUSIVE.toLong - 1L), "maximum-position")
  }

  test("empty bitmap serializes to a 6-byte header and reads back empty") {
    val bitmap = newMutableBitmap().toMumblingBitmap
    val bytes = bufferBytes(bitmap)
    assert(bytes.length == 6)
    assert(bitmap.cardinality() == 0)
  }

  test("mutable bitmap produces independent immutable snapshots") {
    val mutable = newMutableBitmap()
    mutable.add(10L)
    mutable.add(300L)
    mutable.add(10L) // duplicate
    val firstSnapshot = mutable.toMumblingBitmap

    mutable.add(11L)
    val secondSnapshot = mutable.toMumblingBitmap

    assert(firstSnapshot.cardinality() == 2)
    assert(firstSnapshot.isSet(10) && firstSnapshot.isSet(300))
    assert(!firstSnapshot.isSet(11))
    assert(secondSnapshot.cardinality() == 3)
    assert(secondSnapshot.isSet(11))
  }

  test("reader rejects Roaring-serialized bitmaps") {
    val roaringBytes = RoaringBitmapArray(0L, 7L, 300L)
      .serializeAsByteArray(RoaringBitmapArrayFormat.Portable)

    val error = intercept[UnsupportedOperationException] {
      new MumblingBitmap(ByteBuffer.wrap(roaringBytes))
    }
    assert(error.getMessage.startsWith("Unsupported Mumbling bitmap version:"))
  }

  test("immutable bitmap owns its serialized bytes") {
    val bytes = MumblingBitmapWriter.serialize(Array(10, 300))
    val bitmap = new MumblingBitmap(ByteBuffer.wrap(bytes))

    java.util.Arrays.fill(bytes, 0.toByte)

    assert(bitmap.cardinality() == 2)
    assert(bitmap.isSet(10))
    assert(bitmap.isSet(300))
  }

  test("add rejects positions outside [0, container-index ceiling)") {
    val bitmap = newMutableBitmap()
    intercept[IllegalArgumentException](bitmap.add(-1L))
    intercept[IllegalArgumentException](
      bitmap.add(MumblingBitmapWriter.MAX_POSITION_EXCLUSIVE.toLong))
    // The largest representable position is allowed.
    bitmap.add(MumblingBitmapWriter.MAX_POSITION_EXCLUSIVE.toLong - 1L)
    assert(bitmap.toMumblingBitmap.cardinality() == 1)
  }

  test("merge adds positions without changing existing positions") {
    val existing = newMutableBitmap(
      0L, 2L, 30L, 31L, 255L, 256L, 522L).toMumblingBitmap
    val mutableBitmap = existing.toMutable()
    Seq(1024L, 300L, 2L, 256L, 32L).foreach(mutableBitmap.add)
    val bitmap = mutableBitmap.toMumblingBitmap
    val expected = Seq(0, 2, 30, 31, 32, 255, 256, 300, 522, 1024)

    assert(bitmap.cardinality() == expected.size)
    expected.foreach(position => assert(bitmap.isSet(position)))
    Seq(1, 33, 257, 521, 523, 1023).foreach(position => assert(!bitmap.isSet(position)))
  }

  test("merge keeps a sparse base container sparse") {
    val bitmap = merge(0 until 30, 30)

    assert(bitmap.cardinality() == 31)
    assert(singleContainerDescriptor(bitmap) == 31)
  }

  test("merge keeps a dense base container dense") {
    val bitmap = merge(0 until 32, 32)

    assert(bitmap.cardinality() == 33)
    assert(singleContainerDescriptor(bitmap) == 32)
  }

  test("merge promotes a sparse base container to dense") {
    val bitmap = merge(0 until 31, 31)

    assert(bitmap.cardinality() == 32)
    assert(singleContainerDescriptor(bitmap) == 32)
  }

  test("merge fast path 1 copies a source container without additions") {
    val basePositions = 0 until 32
    val bitmap = merge(basePositions, 256)
    val expected = MumblingBitmapWriter.serialize((basePositions :+ 256).toArray)

    assert(bufferBytes(bitmap).sameElements(expected))
  }

  test("merge fast path 2 emits an empty container gap") {
    val bitmap = merge(Seq(1), 512)
    val expected = MumblingBitmapWriter.serialize(Array(1, 512))

    assert(bufferBytes(bitmap).sameElements(expected))
  }

  test("merge handles intermingled sparse, dense, promotion, and fast-path containers") {
    def positions(container: Int, offsets: Seq[Int]): Seq[Int] =
      offsets.map(offset => container * 256 + offset)

    // Container layout:
    //   0, 5: sparse stays sparse
    //   1, 4: fast path 1 copies untouched source containers
    //   2, 7: sparse becomes dense
    //   3, 6: dense stays dense
    //   8, 10: fast path 2 emits empty gaps beyond the source containers
    val sparseStaysSparse = Seq(0, 5)
    val fastPath1 = Seq(1, 4)
    val sparseBecomesDense = Seq(2, 7)
    val denseStaysDense = Seq(3, 6)
    val fastPath2 = Seq(8, 10)

    val basePositions = (
      sparseStaysSparse.flatMap(positions(_, 0 until 30)) ++
        positions(fastPath1.head, Seq(1, 7)) ++
        positions(fastPath1.last, 0 until 32) ++
        sparseBecomesDense.flatMap(positions(_, 0 until 31)) ++
        denseStaysDense.flatMap(positions(_, 0 until 32))).sorted
    // New containers 9 and 11 leave fast-path-2 gaps after the last base container.
    val additions = (
      sparseStaysSparse.map(_ * 256 + 30) ++
        sparseBecomesDense.map(_ * 256 + 31) ++
        denseStaysDense.map(_ * 256 + 32) ++
        Seq(9 * 256 + 9, 11 * 256 + 11)).sorted
    val expectedPositions = (basePositions ++ additions).distinct.sorted

    val bitmap = merge(basePositions, additions: _*)
    val expectedBytes = MumblingBitmapWriter.serialize(expectedPositions.toArray)

    assert(bitmap.cardinality() == expectedPositions.size)
    expectedPositions.foreach(position => assert(bitmap.isSet(position)))
    assert(bufferBytes(bitmap).sameElements(expectedBytes))
    fastPath2.foreach { container =>
      assert(!(0 until 256).exists(offset => bitmap.isSet(container * 256 + offset)))
    }
  }

  test("merge preserves and updates dense base containers") {
    val basePositions = (0 until 64) ++ (256 until 288) ++ Seq(600)
    val base = new MumblingBitmap(
      ByteBuffer.wrap(MumblingBitmapWriter.serialize(basePositions.toArray)))
    val mutableBitmap = base.toMutable()
    val additions = Seq(10, 64, 601, 1024)
    additions.foreach(position => mutableBitmap.add(position.toLong))

    val bitmap = mutableBitmap.toMumblingBitmap
    val expected = (basePositions ++ additions).toSet
    assert(bitmap.cardinality() == expected.size)
    expected.foreach(position => assert(bitmap.isSet(position)))
    Seq(65, 300, 602, 1023).foreach(position => assert(!bitmap.isSet(position)))
  }

  private val fuzzTestSeed = Random.nextLong()

  test(s"fuzz: random position sets round-trip through the Mumbling format " +
      s"(seed: $fuzzTestSeed)") {
    val rng = new Random(fuzzTestSeed)
    for (iteration <- 0 until 50) {
      val positions = (0 until rng.nextInt(2000)).map(_ => rng.nextInt(49999).toLong).toSet.toSeq
      roundTrip(positions, s"iteration $iteration")
    }
  }

  test("reader honors a non-zero ByteBuffer position") {
    val encoded = MumblingBitmapWriter.serialize(Array(42))
    val padded = ByteBuffer.allocate(encoded.length + 4)
    padded.position(4)
    padded.put(encoded)
    padded.position(4)

    val bitmap = new MumblingBitmap(padded)
    assert(bitmap.cardinality() == 1)
    assert(!bitmap.isSet(41))
    assert(bitmap.isSet(42))
    assert(!bitmap.isSet(43))
    assert(bufferBytes(bitmap).sameElements(encoded))
  }

  test("reader decodes a mixed descriptor array with a PFOR exception") {
    val sparsePositions = (0 until 15).filter(_ != 7).map(i => i * 256 + i)
    val densePositions = (0 until 32).map(7 * 256 + _)
    val positions = (sparsePositions ++ densePositions).sorted.toArray
    val bitmap = new MumblingBitmap(ByteBuffer.wrap(MumblingBitmapWriter.serialize(positions)))

    assert(bitmap.cardinality() == positions.length)
    positions.foreach(pos => assert(bitmap.isSet(pos)))
    assert(!bitmap.isSet(7 * 256 - 1))
    assert(!bitmap.isSet(7 * 256 + 32))
  }

  test("reader rejects headers beyond the Mumbling limits") {
    val excessiveContainers = hexBytes("01 00 00 00 01 20")
    val excessiveCardinality = hexBytes("01 01 00 20 00 00")

    intercept[IllegalStateException](new MumblingBitmap(ByteBuffer.wrap(excessiveContainers)))
    intercept[IllegalStateException](new MumblingBitmap(ByteBuffer.wrap(excessiveCardinality)))
  }

  test("reader rejects descriptors that are neither sparse nor dense") {
    val invalidDescriptor = hexBytes("01 00 00 00 01 00  00 00 40")
    val bitmap = new MumblingBitmap(ByteBuffer.wrap(invalidDescriptor))

    val error = intercept[IllegalStateException](bitmap.isSet(0))
    assert(error.getMessage.contains("Invalid descriptor"))
  }

  test("merge accepts and canonicalizes dense descriptors with low bits set") {
    val nonCanonicalDense =
      hexBytes("01 20 00 00 01 00  00 00 21  FF FF FF FF") ++ Array.fill(28)(0.toByte)
    val mutableBitmap = new MumblingBitmap(ByteBuffer.wrap(nonCanonicalDense)).toMutable()
    mutableBitmap.add(32L)

    val bitmap = mutableBitmap.toMumblingBitmap
    assert(bitmap.cardinality() == 33)
    assert(bitmap.isSet(0))
    assert(bitmap.isSet(31))
    assert(bitmap.isSet(32))
    assert(bufferBytes(bitmap).slice(6, 9).sameElements(hexBytes("00 00 20")))
  }

  // ---- On-disk format vectors (Mumbling spec conformance) ----
  // Assert the exact serialized bytes, so a change that still round-trips but deviates from the
  // spec layout (bit order, endianness, descriptor encoding) is caught.

  private def hexBytes(hex: String): Array[Byte] =
    hex.split("\\s+").filter(_.nonEmpty).map(Integer.parseInt(_, 16).toByte)

  private def assertSerialized(positions: Array[Int], expected: Array[Byte], name: String): Unit = {
    def show(bs: Array[Byte]) = bs.map(b => f"${b & 0xff}%02x").mkString(" ")
    val actual = MumblingBitmapWriter.serialize(positions)
    assert(actual.sameElements(expected),
      s"$name:\n  expected ${show(expected)}\n  actual   ${show(actual)}")
    // Cross-check: the bytes decode back to the input positions.
    val bitmap = new MumblingBitmap(ByteBuffer.wrap(actual))
    assert(bitmap.cardinality() == positions.length)
    positions.foreach(position => assert(bitmap.isSet(position)))
  }

  test("format: empty bitmap is a 6-byte header (version=1, cardinality=0, count=0)") {
    assertSerialized(Array.empty[Int], hexBytes("01 00 00 00 00 00"), "empty")
  }

  test("format: sparse container matches the spec layout") {
    // Spec sparse example: descriptor 3 -> container 00 22 FF (positions 0, 34, 255).
    // header ver=01 card=03 00 00 count=01 00 | PFOR of [3] = 00 00 03 | container 00 22 FF
    assertSerialized(Array(0, 34, 255),
      hexBytes("01 03 00 00 01 00  00 00 03  00 22 FF"), "sparse 0/34/255")
  }

  test("format: dense container for 0..31 matches the spec layout") {
    // Spec dense example: descriptor 0x20 -> FF FF FF FF 00...00 (positions 0-31).
    val header = hexBytes("01 20 00 00 01 00") // cardinality = 32.
    val descriptors = hexBytes("00 00 20") // PFOR of [32].
    val container = hexBytes("FF FF FF FF") ++ Array.fill(28)(0.toByte)
    assertSerialized((0 until 32).toArray, header ++ descriptors ++ container, "dense 0..31")
  }

  test("format: dense container for 0..32 sets the MSB of byte 4 (spec example)") {
    // Spec: FF FF FF FF 80 ... 00  (positions 0-32; position 32 is the MSB of byte 4).
    val header = hexBytes("01 21 00 00 01 00") // cardinality = 33.
    val descriptors = hexBytes("00 00 20")
    val container = hexBytes("FF FF FF FF 80") ++ Array.fill(27)(0.toByte)
    assertSerialized((0 to 32).toArray, header ++ descriptors ++ container, "dense 0..32")
  }

  test("format: dense container of even positions is 0xAA repeated (spec example)") {
    // Spec: AA AA ... AA  (even positions 0, 2, 4, ...).
    val header = hexBytes("01 80 00 00 01 00") // cardinality = 128.
    val descriptors = hexBytes("00 00 20")
    val container = Array.fill(32)(0xAA.toByte)
    assertSerialized((0 until 256 by 2).toArray, header ++ descriptors ++ container, "dense even")
  }

  test("format: multi-container descriptor array is PFOR-encoded") {
    // positions 5, 522 -> containers c0={5}, c1={} (empty), c2={522 & 0xff = 10}
    // descriptors [1, 0, 1] PFOR-encode with b1=1 (01 00 00) + primary A0 (bits 1,0,1 MSB-first).
    val header = hexBytes("01 02 00 00 03 00") // cardinality = 2, count = 3.
    val descriptors = hexBytes("01 00 00 A0")
    val containers = hexBytes("05 0A")
    assertSerialized(Array(5, 522), header ++ descriptors ++ containers, "multi-container")
  }
}
