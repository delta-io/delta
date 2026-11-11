/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.amt

import java.io.ByteArrayOutputStream
import java.nio.{ByteBuffer, ByteOrder}

import scala.util.Random

import org.apache.spark.sql.delta.amt.AMTBinaryJsonCodec.{
  encodedLength,
  FORMAT_VERSION_HEADER_SIZE,
  MalformedInput,
  PayloadTooLarge
}
import org.apache.spark.sql.delta.amt.AMTBinaryJsonCodec.ConstantsForFormatVersion1.{
  MAX_ENCODED_LENGTH,
  MAX_PAYLOAD_BYTES,
  TOTAL_HEADER_SIZE
}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.sources.DeltaSQLConf.AMTBinaryJsonCompressionPolicy.{
  DISABLED,
  IF_BENEFICIAL,
  VALUES
}
import org.apache.spark.sql.delta.util.Codec.Base85Codec
import com.github.luben.zstd.{Zstd, ZstdOutputStreamNoFinalizer}

import org.apache.spark.{SparkConf, SparkIllegalArgumentException}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class AMTBinaryJsonCodecFormatVersion1Suite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.AMT_BINARY_JSON_FORMAT_VERSION.key, "1")

  private val formatVersion = DeltaSQLConf.AMT_BINARY_JSON_FORMAT_VERSION.key
  private val compressionPolicy = DeltaSQLConf.AMT_BINARY_JSON_COMPRESSION_POLICY.key

  private def testAllPolicies(name: String)(f: String => Unit): Unit = {
    VALUES.toSeq.sorted.foreach { policy =>
      test(s"$name (policy=$policy)") {
        f(policy)
      }
    }
  }

  private def encodeBytes(input: Array[Byte], policy: String): String = {
    withSQLConf(compressionPolicy -> policy) {
      AMTBinaryJsonCodec.encode(spark, input) match {
        case Right(encodedStr) => encodedStr
        case Left(error) => fail(s"Encoding failed: $error")
      }
    }
  }

  private def assertDecoded(encodedStr: String, expected: Array[Byte]): Unit = {
    AMTBinaryJsonCodec.decode(encodedStr) match {
      case Right(actual) => assert(actual.sameElements(expected))
      case Left(error) => fail(s"Decoding failed: $error")
    }
  }

  private def assertMalformed(encodedStr: String, expectedReason: String): Unit = {
    assert(AMTBinaryJsonCodec.decode(encodedStr) == Left(MalformedInput(expectedReason)))
  }

  private def randomBytes(length: Int): Array[Byte] = {
    val bytes = new Array[Byte](length)
    new Random(42L).nextBytes(bytes)
    bytes
  }

  /** Construct the header explicitly so decoder tests do not depend on the AMT encoder. */
  private def envelope(signedLength: Int, payload: Array[Byte], version: Int = 1): String = {
    val header = Array(version.toByte, (signedLength >> 8).toByte, signedLength.toByte)
    Base85Codec.encodeBytes(header ++ payload)
  }

  private val ZSTD_COMPRESSION_LEVEL = 1
  private val ZSTD_RAW_FRAME_OVERHEAD_BYTES = 12

  /**
   * Wraps the unchanged input in a standard ZSTD frame with one uncompressed (raw) block.
   * The frame is exactly originalBytes.length + ZSTD_RAW_FRAME_OVERHEAD_BYTES bytes, allowing
   * tests to hit the stored-payload limit and every padding alignment without relying on
   * compressors. Decompression returns the original bytes and length.
   *
   * Example input: [01 02 03] (3 bytes). Output frame (15 bytes):
   * {{{
   * [28 B5 2F FD]  ZSTD magic
   * [A0]           single segment, four-byte content size, no dictionary or checksum
   * [03 00 00 00]  original length = 3, little-endian
   * [19 00 00]     final raw block, containing 3 bytes
   * [01 02 03]     unchanged payload
   * }}}
   *
   * Decompression returns [01 02 03].
   */
  private def wrapInZstdFrameWithFixedOverhead(originalBytes: Array[Byte]): Array[Byte] = {
    require(originalBytes.length <= 128 * 1024)
    val buffer = ByteBuffer
      .allocate(ZSTD_RAW_FRAME_OVERHEAD_BYTES + originalBytes.length)
      .order(ByteOrder.LITTLE_ENDIAN)
    buffer.putInt(0xfd2fb528) // ZSTD magic.
    buffer.put(0xa0.toByte) // Single segment, four-byte content size.
    buffer.putInt(originalBytes.length)
    val blockHeader = (originalBytes.length << 3) | 1 // Last block, raw block type.
    buffer.put(blockHeader.toByte)
    buffer.put((blockHeader >> 8).toByte)
    buffer.put((blockHeader >> 16).toByte)
    buffer.put(originalBytes)
    val framedBytes = buffer.array()
    assert(framedBytes.length - originalBytes.length == ZSTD_RAW_FRAME_OVERHEAD_BYTES)
    val decompressedBytes = Zstd.decompress(framedBytes, originalBytes.length)
    assert(decompressedBytes.sameElements(originalBytes))
    framedBytes
  }

  test("format version config defaults to 1 and rejects unsupported values") {
    assert(DeltaSQLConf.AMT_BINARY_JSON_FORMAT_VERSION.defaultValue.contains(1))
    for (version <- Seq(0, 2, 0xff)) {
      val e = intercept[IllegalArgumentException] {
        spark.conf.set(formatVersion, version.toString)
      }
      assert(e.getMessage ==
        s"The value of $formatVersion should be one of 1, but was $version")
    }
  }

  testAllPolicies("configured version 1 writes a version 1 envelope") { policy =>
    withSQLConf(formatVersion -> "1") {
      val originalBytes = new Array[Byte](1024)
      val encodedStr = encodeBytes(originalBytes, policy)
      assert(Base85Codec.decodeAlignedBytes(encodedStr).head == 1)
      assertDecoded(encodedStr, originalBytes)
    }
  }

  testAllPolicies("empty input has a canonical raw encoding") { policy =>
    val encodedStr = encodeBytes(Array.emptyByteArray, policy)
    // [01 version][00 00 length][empty payload][00 padding] -> "0rr91".
    assert(encodedStr == "0rr91")
    assertDecoded(encodedStr, Array.emptyByteArray)
  }

  test("raw length header is big-endian and counts only payload bytes") {
    val originalBytes = Array.tabulate[Byte](256)(_.toByte)
    val encodedStr = encodeBytes(originalBytes, DISABLED)
    val decodedBytes = Base85Codec.decodeAlignedBytes(encodedStr)
    // [01 version][01 00 length]: 256 = 0x0100, excluding the version, length and padding.
    assert(decodedBytes.take(TOTAL_HEADER_SIZE).sameElements(Array[Byte](1, 1, 0)))
    val decodedLength = ByteBuffer
      .wrap(decodedBytes)
      .order(ByteOrder.BIG_ENDIAN)
      .getShort( /* index = */ FORMAT_VERSION_HEADER_SIZE)
    assert(decodedLength == originalBytes.length)
    assert(encodedStr == envelope(256, originalBytes))
    assertDecoded(encodedStr, originalBytes)
  }

  for (length <- 1 to 8) {
    test(s"raw round trip preserves trailing zeros at every alignment (length=$length)") {
      // Before Z85: [version][length][payload    ][padding ]
      // length 1:   [01     ][00 01 ][7f         ][        ]
      // length 2:   [01     ][00 02 ][7f 00      ][00 00 00]
      // length 3:   [01     ][00 03 ][7f 00 00   ][00 00   ]
      // length 4:   [01     ][00 04 ][7f 00 00 00][00      ]
      // Lengths 5-8 repeat these alignments. Preserve payload zeros and discard padding.
      val originalBytes = Array.tabulate[Byte](length)(i => if (i == 0) 0x7f.toByte else 0.toByte)
      val encodedStr = encodeBytes(originalBytes, DISABLED)
      assert(encodedStr == envelope(length, originalBytes))
      val padding = Base85Codec.decodeAlignedBytes(encodedStr).drop(TOTAL_HEADER_SIZE + length)
      assert(padding.length == (4 - (TOTAL_HEADER_SIZE + length) % 4) % 4)
      assert(padding.forall(_ == 0))
      assertDecoded(encodedStr, originalBytes)
    }
  }

  testAllPolicies("round trip supports every byte value with trailing zeros") { policy =>
    val originalBytes = Array.tabulate[Byte](256)(_.toByte) ++ Array[Byte](0, 0, 0)
    assertDecoded(encodeBytes(originalBytes, policy), originalBytes)
  }

  test("disabled policy stores compressible input as raw bytes") {
    val originalBytes = new Array[Byte](4096)
    val encodedStr = encodeBytes(originalBytes, DISABLED)
    assert(encodedStr == envelope(originalBytes.length, originalBytes))
    assertDecoded(encodedStr, originalBytes)
  }

  test("beneficial compression stores a negative compressed length") {
    val originalBytes = new Array[Byte](8192)
    val compressedBytes = Zstd.compress(originalBytes, ZSTD_COMPRESSION_LEVEL)
    val encodedStr = encodeBytes(originalBytes, IF_BENEFICIAL)
    assert(encodedStr == envelope(-compressedBytes.length, compressedBytes))
    assert(encodedStr.length < envelope(originalBytes.length, originalBytes).length)
    assertDecoded(encodedStr, originalBytes)
  }

  test("if-beneficial stores incompressible and tiny input as raw bytes") {
    for (originalBytes <- Seq(Array[Byte](1), randomBytes(4096))) {
      val compressedBytes = Zstd.compress(originalBytes, ZSTD_COMPRESSION_LEVEL)
      val rawEncodedStr = envelope(originalBytes.length, originalBytes)
      val compressedEncodedStr = envelope(-compressedBytes.length, compressedBytes)
      assert(compressedEncodedStr.length >= rawEncodedStr.length,
        s"Expected no encoded-size savings for ${originalBytes.length} input bytes")
      val encodedStr = encodeBytes(originalBytes, IF_BENEFICIAL)
      assert(encodedStr == rawEncodedStr)
      assertDecoded(encodedStr, originalBytes)
    }
  }

  test("if-beneficial keeps raw bytes when padded Z85 sizes tie") {
    // This 19-byte payload compresses to 18 bytes; both envelopes pad to 24 bytes.
    val magicPayloadBytes = Array[Byte](0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1)
    val compressedBytes = Zstd.compress(magicPayloadBytes, ZSTD_COMPRESSION_LEVEL)
    assert(compressedBytes.length < magicPayloadBytes.length)
    assert(encodedLength(magicPayloadBytes.length, TOTAL_HEADER_SIZE) ==
      encodedLength(compressedBytes.length, TOTAL_HEADER_SIZE))

    val encodedStr = encodeBytes(magicPayloadBytes, IF_BENEFICIAL)
    assert(encodedStr == envelope(magicPayloadBytes.length, magicPayloadBytes))
    assertDecoded(encodedStr, magicPayloadBytes)
  }

  test("compression can encode original bytes larger than the raw payload limit") {
    val originalBytes = new Array[Byte](2 * MAX_PAYLOAD_BYTES)
    val encodedStr = encodeBytes(originalBytes, IF_BENEFICIAL)
    val decodedBytes = Base85Codec.decodeAlignedBytes(encodedStr)
    assert(decodedBytes.head == 1) // format version
    // Signed length header is negative because the payload is compressed.
    assert(ByteBuffer.wrap(decodedBytes).getShort( /* index = */ FORMAT_VERSION_HEADER_SIZE) < 0)
    assertDecoded(encodedStr, originalBytes)
  }

  test("raw payload supports maximum encoded length") {
    val originalBytes = randomBytes(MAX_PAYLOAD_BYTES)
    val encodedStr = encodeBytes(originalBytes, DISABLED)
    assert(encodedStr.length == MAX_ENCODED_LENGTH)
    assert(encodedStr == envelope(MAX_PAYLOAD_BYTES, originalBytes))
    assertDecoded(encodedStr, originalBytes)
  }

  test("disabled policy rejects oversized input without wrapping the header") {
    withSQLConf(compressionPolicy -> DISABLED) {
      for (length <- Seq(MAX_PAYLOAD_BYTES + 1, 2 * MAX_PAYLOAD_BYTES)) {
        assert(AMTBinaryJsonCodec.encode(spark, new Array[Byte](length)) ==
          Left(PayloadTooLarge(length, MAX_PAYLOAD_BYTES)))
      }
    }
  }

  test("if-beneficial rejects input when neither representation fits") {
    val originalBytes = randomBytes(2 * MAX_PAYLOAD_BYTES)
    val compressedBytes = Zstd.compress(originalBytes, ZSTD_COMPRESSION_LEVEL)
    assert(compressedBytes.length > MAX_PAYLOAD_BYTES)
    withSQLConf(compressionPolicy -> IF_BENEFICIAL) {
      assert(AMTBinaryJsonCodec.encode(spark, originalBytes).isLeft)
    }
  }

  test("decoder accepts a raw envelope with exactly enough remaining bytes") {
    val originalBytes = Array[Byte](0x7f, 0, 0, 0, 0)
    // [01 version][00 05 length][7f 00 00 00 00 payload] fills two Z85 blocks without padding.
    val encodedStr = Base85Codec.encodeBytes(Array[Byte](1, 0, 5) ++ originalBytes)
    assertDecoded(encodedStr, originalBytes)
  }

  for (length <- 256 to 259) {
    test(s"compressed decoder strips padding but preserves trailing zeros (length=$length)") {
      val originalBytes = randomBytes(length - 3) ++ Array[Byte](0, 0, 0)
      val frame = wrapInZstdFrameWithFixedOverhead(originalBytes)
      assertDecoded(envelope(-frame.length, frame), originalBytes)
    }
  }

  test("decoder accepts a compressed payload at the shared payload limit") {
    val originalBytes = randomBytes(MAX_PAYLOAD_BYTES - ZSTD_RAW_FRAME_OVERHEAD_BYTES)
    val frame = wrapInZstdFrameWithFixedOverhead(originalBytes)
    assert(frame.length == MAX_PAYLOAD_BYTES)
    val encodedStr = envelope(-MAX_PAYLOAD_BYTES, frame)
    assert(encodedStr.length == MAX_ENCODED_LENGTH)
    assertDecoded(encodedStr, originalBytes)
  }

  test("decoder rejects the minimum signed header above the shared payload limit") {
    val originalBytes = randomBytes(MAX_PAYLOAD_BYTES + 1 - ZSTD_RAW_FRAME_OVERHEAD_BYTES)
    val frame = wrapInZstdFrameWithFixedOverhead(originalBytes)
    assert(frame.length == MAX_PAYLOAD_BYTES + 1)
    assertMalformed(
      envelope(Short.MinValue.toInt, frame),
      expectedReason = "Invalid stored payload length"
    )
  }

  test("decoder accepts compressed input when writer compression policy is disabled") {
    val originalBytes = new Array[Byte](1024)
    val encodedStr = encodeBytes(originalBytes, IF_BENEFICIAL)
    withSQLConf(compressionPolicy -> DISABLED) {
      assertDecoded(encodedStr, originalBytes)
    }
  }

  test("decoder rejects null and empty encoded input") {
    for (encodedStr <- Seq(null, "")) {
      assertMalformed(encodedStr, expectedReason = "Invalid encoded string length")
    }
  }

  test("version 1 decoder rejects oversized envelopes") {
    // Keep a V1 header and exceed the cap by one complete Z85 block, preserving alignment.
    val oversizedEncodedStr = envelope(0, Array.emptyByteArray) + ("0" * MAX_ENCODED_LENGTH.toInt)
    assertMalformed(oversizedEncodedStr, expectedReason = "Invalid decoded envelope length")
  }

  test("decoder returns malformed input for Z85 failures") {
    val nonAscii = "0000" + 0x80.toChar
    for (encodedStr <- Seq("0", "0000", "000000", "0000_", "0000\"", "0000 ", nonAscii)) {
      assertMalformed(encodedStr, expectedReason = "Z85 decoding failure")
    }
  }

  test("decoder rejects unsupported versions before interpreting the length") {
    for (version <- Seq(0, 2, 0xff)) {
      // The invalid compressed payload must not be decoded under an unknown version.
      val encodedStr = envelope(-1, Array[Byte](0), version)
      assertMalformed(encodedStr, expectedReason = s"Unsupported envelope format version: $version")
    }
  }

  test("decoder rejects raw and compressed lengths larger than the remaining bytes") {
    // The three-byte header and one-byte payload fill a block; no padding can satisfy the length.
    for (signedLength <- Seq(2, -2, MAX_PAYLOAD_BYTES, -MAX_PAYLOAD_BYTES)) {
      assertMalformed(
        envelope(signedLength, Array[Byte](1)),
        expectedReason = "Invalid decoded envelope length"
      )
    }
  }

  test("decoder rejects extra blocks beyond raw and compressed envelope padding") {
    for (length <- 0 to 4) {
      val originalBytes = new Array[Byte](length)
      val frame = wrapInZstdFrameWithFixedOverhead(originalBytes)
      for (encodedStr <- Seq(envelope(length, originalBytes), envelope(-frame.length, frame))) {
        assertMalformed(encodedStr + "00000", expectedReason = "Invalid decoded envelope length")
      }
    }
  }

  /** Produces a standard streaming frame without a Frame Content Size. */
  private def compressWithoutContentSize(originalBytes: Array[Byte]): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val compressor = new ZstdOutputStreamNoFinalizer(output, ZSTD_COMPRESSION_LEVEL)
    try {
      compressor.write(originalBytes)
    } finally {
      compressor.close()
    }
    val compressedBytes = output.toByteArray
    assert(Zstd.getFrameContentSize(compressedBytes) < 0)
    compressedBytes
  }

  test("decoder rejects ZSTD frames without a content size") {
    val originalBytes = randomBytes(8192) ++ Array[Byte](0, 0, 0)
    val compressedBytes = compressWithoutContentSize(originalBytes)
    assertMalformed(
      envelope(-compressedBytes.length, compressedBytes),
      expectedReason = "ZSTD decompression failure"
    )
  }

  test("decoder accepts a ZSTD frame with zero content size") {
    val compressedBytes = Zstd.compress(Array.emptyByteArray, ZSTD_COMPRESSION_LEVEL)
    assert(Zstd.getFrameContentSize(compressedBytes) == 0)
    assertDecoded(envelope(-compressedBytes.length, compressedBytes), Array.emptyByteArray)
  }

  test("decoder rejects ZSTD frames whose content size differs from the output length") {
    val originalBytes = randomBytes(256)
    // The fixture's four-byte content size follows the four-byte magic and one-byte descriptor.
    val contentSizeOffset = java.lang.Integer.BYTES + java.lang.Byte.BYTES
    for (incorrectContentSize <- Seq(originalBytes.length - 1, originalBytes.length + 1)) {
      val compressedBytes = wrapInZstdFrameWithFixedOverhead(originalBytes)
      ByteBuffer
        .wrap(compressedBytes)
        .order(ByteOrder.LITTLE_ENDIAN) // Zstd's content size is little-endian.
        .putInt(contentSizeOffset, incorrectContentSize)
      assert(Zstd.getFrameContentSize(compressedBytes) == incorrectContentSize)
      assertMalformed(
        envelope(-compressedBytes.length, compressedBytes),
        expectedReason = "ZSTD decompression failure"
      )
    }
  }

  test("decoder returns malformed input for invalid or truncated ZSTD payloads") {
    val frame = wrapInZstdFrameWithFixedOverhead(randomBytes(256))
    val invalidMagic = frame.clone()
    invalidMagic(0) = 0
    for (payload <- Seq(Array[Byte](0), invalidMagic, frame.dropRight(1))) {
      assertMalformed(
        envelope(-payload.length, payload),
        expectedReason = "ZSTD decompression failure"
      )
    }
  }
}
