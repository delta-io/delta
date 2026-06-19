/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.checksum

import java.util
import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel.data.{ColumnarBatch, ColumnVector, Row}
import io.delta.kernel.internal.actions.{DomainMetadata, Format, Metadata, Protocol}
import io.delta.kernel.internal.checksum.CRCInfo.{CRC_FILE_READ_SCHEMA, CRC_FILE_SCHEMA}
import io.delta.kernel.internal.data.GenericColumnVector
import io.delta.kernel.internal.stats.FileSizeHistogram
import io.delta.kernel.internal.types.DataTypeJsonSerDe
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.internal.util.VectorUtils.{buildArrayValue, stringStringMapValue}
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.types.{DataType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests that CRCInfo.fromColumnarBatch correctly reads the file size histogram from CRC files
 * written using the legacy field name "histogramOpt" or the spec-compliant "fileSizeHistogram".
 */
class CRCInfoReadCompatSuite extends AnyFunSuite with VectorTestUtils {

  private val testProtocol =
    new Protocol(1, 2, Collections.emptySet(), Collections.emptySet())

  private val testMetadata = new Metadata(
    "id",
    Optional.of("name"),
    Optional.of("description"),
    new Format("parquet", Collections.emptyMap()),
    DataTypeJsonSerDe.serializeDataType(new StructType()),
    new StructType(),
    buildArrayValue(util.Arrays.asList("c3"), StringType.STRING),
    Optional.of(123),
    stringStringMapValue(new util.HashMap[String, String]() {
      put("delta.appendOnly", "true")
    }))

  /** Creates a simple histogram with distinct values for identification in tests. */
  private def createTestHistogram(fileCount: Long): FileSizeHistogram = {
    val boundaries = Array(0L, 1024L)
    val counts = Array(fileCount, 0L)
    val bytes = Array(fileCount * 100, 0L)
    new FileSizeHistogram(boundaries, counts, bytes)
  }

  /**
   * Builds a ColumnVector for a FileSizeHistogram struct field. If histogram is None, the vector
   * is null at row 0.
   */
  private def histogramColumnVector(
      histogram: Option[FileSizeHistogram]): ColumnVector = {
    val rowValue: Row = histogram.map(_.toRow()).orNull
    new GenericColumnVector(
      util.Arrays.asList(rowValue),
      FileSizeHistogram.FULL_SCHEMA)
  }

  /**
   * Build a ColumnarBatch with the given schema and histogram column vectors at the appropriate
   * positions.
   */
  private def buildBatch(
      schema: StructType,
      fileSizeHistogram: Option[FileSizeHistogram],
      histogramOpt: Option[FileSizeHistogram]): ColumnarBatch = {
    val protocolColVector =
      new GenericColumnVector(
        util.Arrays.asList(testProtocol.toRow()),
        Protocol.FULL_SCHEMA)
    val metadataColVector =
      new GenericColumnVector(
        util.Arrays.asList(testMetadata.toRow()),
        Metadata.FULL_SCHEMA)

    new ColumnarBatch {
      override def getSchema: StructType = schema
      override def getSize: Int = 1

      override def getColumnVector(ordinal: Int): ColumnVector = {
        val fieldName = schema.at(ordinal).getName
        fieldName match {
          case "tableSizeBytes" => longVector(Seq(1000L))
          case "numFiles" => longVector(Seq(10L))
          case "numMetadata" => longVector(Seq(1L))
          case "numProtocol" => longVector(Seq(1L))
          case "metadata" => metadataColVector
          case "protocol" => protocolColVector
          case "txnId" => stringVector(Seq(null))
          case "domainMetadata" => nullColumnVector(
              CRC_FILE_SCHEMA.get("domainMetadata").getDataType)
          case "fileSizeHistogram" => histogramColumnVector(fileSizeHistogram)
          case "histogramOpt" => histogramColumnVector(histogramOpt)
          case _ =>
            throw new IllegalArgumentException(s"Unknown field: $fieldName")
        }
      }
    }
  }

  /** Creates a column vector that is null at every row. */
  private def nullColumnVector(dataType: DataType): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = dataType
      override def getSize: Int = 1
      override def close(): Unit = {}
      override def isNullAt(rowId: Int): Boolean = true
    }
  }

  test("reads fileSizeHistogram when only spec-compliant field is present") {
    val histogram = createTestHistogram(fileCount = 42)
    val batch = buildBatch(
      CRC_FILE_READ_SCHEMA,
      fileSizeHistogram = Some(histogram),
      histogramOpt = None)

    val crcInfo = CRCInfo.fromColumnarBatch(1L, batch, 0, "test.crc")
    assert(crcInfo.isPresent)
    assert(crcInfo.get().getFileSizeHistogram.isPresent)
    assert(crcInfo.get().getFileSizeHistogram.get() === histogram)
  }

  test("reads histogramOpt when only legacy field is present") {
    val histogram = createTestHistogram(fileCount = 99)
    val batch = buildBatch(
      CRC_FILE_READ_SCHEMA,
      fileSizeHistogram = None,
      histogramOpt = Some(histogram))

    val crcInfo = CRCInfo.fromColumnarBatch(1L, batch, 0, "test.crc")
    assert(crcInfo.isPresent)
    assert(crcInfo.get().getFileSizeHistogram.isPresent)
    assert(crcInfo.get().getFileSizeHistogram.get() === histogram)
  }

  test("prefers fileSizeHistogram when both fields are present") {
    val specHistogram = createTestHistogram(fileCount = 10)
    val legacyHistogram = createTestHistogram(fileCount = 20)
    val batch = buildBatch(
      CRC_FILE_READ_SCHEMA,
      fileSizeHistogram = Some(specHistogram),
      histogramOpt = Some(legacyHistogram))

    val crcInfo = CRCInfo.fromColumnarBatch(1L, batch, 0, "test.crc")
    assert(crcInfo.isPresent)
    assert(crcInfo.get().getFileSizeHistogram.isPresent)
    assert(crcInfo.get().getFileSizeHistogram.get() === specHistogram)
  }

  test("returns empty histogram when neither field is present") {
    val batch = buildBatch(
      CRC_FILE_READ_SCHEMA,
      fileSizeHistogram = None,
      histogramOpt = None)

    val crcInfo = CRCInfo.fromColumnarBatch(1L, batch, 0, "test.crc")
    assert(crcInfo.isPresent)
    assert(!crcInfo.get().getFileSizeHistogram.isPresent)
  }

  test("safely skips fallback when batch uses original CRC_FILE_SCHEMA") {
    // When fromColumnarBatch is called with a batch using the original schema
    // (without histogramOpt), the fallback should be safely skipped.
    val protocolColVector =
      new GenericColumnVector(
        util.Arrays.asList(testProtocol.toRow()),
        Protocol.FULL_SCHEMA)
    val metadataColVector =
      new GenericColumnVector(
        util.Arrays.asList(testMetadata.toRow()),
        Metadata.FULL_SCHEMA)

    val batch = new ColumnarBatch {
      override def getSchema: StructType = CRC_FILE_SCHEMA
      override def getSize: Int = 1
      override def getColumnVector(ordinal: Int): ColumnVector = {
        val fieldName = CRC_FILE_SCHEMA.at(ordinal).getName
        fieldName match {
          case "tableSizeBytes" => longVector(Seq(1000L))
          case "numFiles" => longVector(Seq(10L))
          case "numMetadata" => longVector(Seq(1L))
          case "numProtocol" => longVector(Seq(1L))
          case "metadata" => metadataColVector
          case "protocol" => protocolColVector
          case "txnId" => stringVector(Seq(null))
          case "domainMetadata" => nullColumnVector(
              CRC_FILE_SCHEMA.get("domainMetadata").getDataType)
          case "fileSizeHistogram" => histogramColumnVector(None)
          case _ =>
            throw new IllegalArgumentException(s"Unknown field: $fieldName")
        }
      }
    }

    val crcInfo = CRCInfo.fromColumnarBatch(1L, batch, 0, "test.crc")
    assert(crcInfo.isPresent)
    assert(!crcInfo.get().getFileSizeHistogram.isPresent)
  }
}
