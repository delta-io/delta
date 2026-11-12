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

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Arrays

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.sources.DeltaSQLConf.AMTBinaryJsonCompressionPolicy
import org.apache.spark.sql.delta.util.Codec.Base85Codec
import com.github.luben.zstd.Zstd

import org.apache.spark.sql.SparkSession

/**
 * Encodes/decodes AMT bitmap and key-metadata bytes into/from JSON strings with Z85 encoding.
 *
 * The full payload (array of bytes) through the Z85 round-trip is a self-describing envelope.
 * The envelope looks like the following:
 *   [format version][format-specific header][actual payload][padded zeros]
 *    - format version: an unsigned byte that identifies the format version
 *    - format-specific header: a header that describes the metadata of the actual payload, e.g.,
 *                              length, compressed or not, etc. Header's length is format-specific.
 *    - actual payload: the actual bytes of the bitmap/key-metadata, maybe compressed.
 *    - padded zeros: zero bytes (0x00) padded to make the envelope a multiple of 4 bytes.
 *
 * The first byte denotes the format version, which determines header layout and compression format.
 * Unsupported versions return [[MalformedInput]].
 *
 * Z85 encodes the entire envelope, plus 0~3 zero bytes padded at the end for alignment, so that the
 * envelope becomes a multiple of 4 bytes. This is required by the Z85 encoding algorithm.
 */
private[delta] object AMTBinaryJsonCodec {

  /** A payload exceeds its signed length header's capacity under the configured policy. */
  case class PayloadTooLarge(storedLength: Int, maxStoredLength: Int)

  sealed trait DecodeError

  /** Envelope validation or an underlying decoder rejected the input. */
  case class MalformedInput(reason: String) extends DecodeError

  /** The size of the format version header. */
  private[amt] val FORMAT_VERSION_HEADER_SIZE = 1

  /**
   * Encodes original bytes using the session's format version and compression policy.
   *
   * @return the Z85-encoded string, or a [[PayloadTooLarge]] signaling an encoding failure due to
   *         payload size limits according to the session's configured format.
   */
  def encode(spark: SparkSession, originalBytes: Array[Byte]): Either[PayloadTooLarge, String] = {
    spark.conf.get(DeltaSQLConf.AMT_BINARY_JSON_FORMAT_VERSION) match {
      case 1 => encodeFormatVersion1(spark, originalBytes)
      case version =>
        throw new IllegalArgumentException(s"Unsupported AMT binary JSON format version: $version")
    }
  }

  /**
   * Header layout and stored-payload limits for format version 1.
   *
   * Header layout: [payload length]
   *  - Header size: 2 bytes.
   *  - Header characteristics: signed, big-endian.
   *  - Sign: non-negative means the payload is raw; negative means the payload is compressed.
   *  - Magnitude: the length of the actual payload being stored (raw or compressed), excluding
   *               headers and padded zeros.
   *  - Compression format: a single ZSTD frame with Frame Content Size present; the current
   *                        writer uses level 1.
   *
   * Payload size limit: raw and compressed payloads can contain at most 32767 stored bytes.
   * The length header value -32768 is invalid.
   *
   * The full envelope looks like the following:
   *   [0x01: format version][non-negative short: payload length][raw payload][padded zeros]
   * or
   *   [0x01: format version][negative short: payload length][compressed payload][padded zeros]
   */
  private[amt] object ConstantsForFormatVersion1 {
    val PAYLOAD_LENGTH_HEADER_SIZE = java.lang.Short.BYTES // 2 bytes
    val TOTAL_HEADER_SIZE = FORMAT_VERSION_HEADER_SIZE + PAYLOAD_LENGTH_HEADER_SIZE
    val MAX_PAYLOAD_BYTES = Short.MaxValue.toInt
    val MAX_ENCODED_LENGTH = encodedLength(MAX_PAYLOAD_BYTES, TOTAL_HEADER_SIZE)
    // Each five-character Z85 block decodes to four envelope bytes, including padding.
    val MAX_PADDED_ENVELOPE_BYTES = (MAX_ENCODED_LENGTH / 5) * 4
  }

  /**
   * Encodes original bytes into a string using format version 1.
   * See details in [[ConstantsForFormatVersion1]].
   */
  private def encodeFormatVersion1(
      spark: SparkSession,
      originalBytes: Array[Byte]): Either[PayloadTooLarge, String] = {
    import ConstantsForFormatVersion1._

    /** Frames the selected payload to store with the V1 header envelope and encodes it as Z85. */
    def encodePayload(
        payload: Array[Byte],
        isCompressed: Boolean): Either[PayloadTooLarge, String] = {
      if (payload.length > MAX_PAYLOAD_BYTES) {
        return Left(PayloadTooLarge(payload.length, MAX_PAYLOAD_BYTES))
      }
      val envelope = ByteBuffer
        .allocate(TOTAL_HEADER_SIZE + payload.length)
        .order(ByteOrder.BIG_ENDIAN)
      val formatVersionHeader = 1
      val signedLengthHeader = if (isCompressed) -payload.length else payload.length
      envelope.put(formatVersionHeader.toByte)
      envelope.putShort(signedLengthHeader.toShort)
      envelope.put(payload)
      Right(Base85Codec.encodeBytes(envelope.array()))
    }

    if (originalBytes.isEmpty) {
      return encodePayload(originalBytes, isCompressed = false)
    }

    spark.conf.get(DeltaSQLConf.AMT_BINARY_JSON_COMPRESSION_POLICY) match {
      case AMTBinaryJsonCompressionPolicy.DISABLED =>
        encodePayload(originalBytes, isCompressed = false)
      case AMTBinaryJsonCompressionPolicy.IF_BENEFICIAL =>
        val compressedBytes = ZstdCompression.compress(originalBytes)
        // Compression is beneficial if the calculated encoded length of the compressed payload is
        // less than that of the original payload. If not, we store the raw payload instead to avoid
        // the overhead of decompressing during decoding.
        val isCompressionBeneficial = encodedLength(compressedBytes.length, TOTAL_HEADER_SIZE) <
          encodedLength(originalBytes.length, TOTAL_HEADER_SIZE)
        if (isCompressionBeneficial) {
          encodePayload(compressedBytes, isCompressed = true)
        } else {
          encodePayload(originalBytes, isCompressed = false)
        }
    }
  }

  /** Calculates the Z85-encoded length of the full envelope (payload + header). */
  private[amt] def encodedLength(storedLength: Int, headerSize: Int): Long =
    // Each 4-byte envelope block will be encoded into a 5-character Z85 block. If the envelope is
    // not a multiple of 4, we will pad the last block with zero bytes until it is.
    5L * ((headerSize.toLong + storedLength + (4 - 1)) / 4)

  /** Calculates the Z85-decoded length of the encoded string. */
  private[amt] def decodedLength(encodedLength: Long): Long = {
    require(encodedLength % 5 == 0, "Encoded length must be a multiple of 5")
    (encodedLength / 5) * 4
  }

  /**
   * Decodes the encoded string into an envelope of bytes, and uses the headers' information to
   * extract the original bytes.
   *
   * @return the original bytes, or a [[DecodeError]] signaling any decoding failure.
   */
  def decode(encoded: String): Either[DecodeError, Array[Byte]] = {
    if (encoded == null || encoded.isEmpty) {
      return Left(MalformedInput("Invalid encoded string length"))
    }

    val decodedEnvelope: Array[Byte] = try {
      Base85Codec.decodeAlignedBytes(encoded)
    } catch {
      case NonFatal(_) => return Left(MalformedInput("Z85 decoding failure"))
    }

    val formatVersion = java.lang.Byte.toUnsignedInt(decodedEnvelope.head)
    formatVersion match {
      case 1 => decodeFormatVersion1(decodedEnvelope)
      case _ => Left(MalformedInput(s"Unsupported envelope format version: $formatVersion"))
    }
  }

  /**
   * Decodes the envelope of bytes into the original bytes using format version 1.
   * See details in [[ConstantsForFormatVersion1]].
   */
  private def decodeFormatVersion1(
      decodedEnvelope: Array[Byte]): Either[DecodeError, Array[Byte]] = {
    import ConstantsForFormatVersion1._

    if (decodedEnvelope.length > MAX_PADDED_ENVELOPE_BYTES) {
      return Left(MalformedInput("Invalid decoded envelope length"))
    }

    val buffer = ByteBuffer.wrap(decodedEnvelope).order(ByteOrder.BIG_ENDIAN)
    buffer.position(FORMAT_VERSION_HEADER_SIZE)
    val signedLengthHeader = buffer.getShort.toInt
    val isCompressed = signedLengthHeader < 0
    val payloadLengthFromHeader = math.abs(signedLengthHeader)
    if (payloadLengthFromHeader > MAX_PAYLOAD_BYTES) {
      return Left(MalformedInput("Invalid stored payload length"))
    }
    val expectedDecodedEnvelopeLength =
      decodedLength(encodedLength(payloadLengthFromHeader, TOTAL_HEADER_SIZE))
    // [decoded envelope] = [header] + [payload] + [0~3 padded zeros]
    assert(expectedDecodedEnvelopeLength >= TOTAL_HEADER_SIZE + payloadLengthFromHeader)
    if (decodedEnvelope.length != expectedDecodedEnvelopeLength) {
      return Left(MalformedInput("Invalid decoded envelope length"))
    }

    val storedPayloadBytes = Arrays.copyOfRange(
      /* original = */ decodedEnvelope,
      /* from = */ TOTAL_HEADER_SIZE,
      /* to = */ TOTAL_HEADER_SIZE + payloadLengthFromHeader)
    if (!isCompressed) {
      return Right(storedPayloadBytes)
    }

    try {
      Right(ZstdCompression.decompress(storedPayloadBytes))
    } catch {
      case NonFatal(_) => Left(MalformedInput("ZSTD decompression failure"))
    }
  }

  private object ZstdCompression {
    private val COMPRESSION_LEVEL = 1

    /** Writes a single ZSTD frame containing the original byte count. */
    def compress(originalBytes: Array[Byte]): Array[Byte] = {
      Zstd.compress(originalBytes, COMPRESSION_LEVEL)
    }

    /** Decompresses in one pass using the required Frame Content Size. */
    def decompress(compressedBytes: Array[Byte]): Array[Byte] = {
      val contentSize = Zstd.getFrameContentSize(compressedBytes)
      require(
        contentSize >= 0 && contentSize <= Int.MaxValue,
        "ZSTD Frame Content Size must be present and fit in a byte array"
      )
      val originalBytes = Zstd.decompress(compressedBytes, contentSize.toInt)
      require(
        originalBytes.length == contentSize,
        "ZSTD output length differs from Frame Content Size"
      )
      originalBytes
    }
  }
}
