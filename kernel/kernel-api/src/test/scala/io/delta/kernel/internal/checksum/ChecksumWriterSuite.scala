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

import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}

import io.delta.kernel.data.Row
import io.delta.kernel.exceptions.TableNotFoundException
import io.delta.kernel.internal.actions.{DomainMetadata, Format, Metadata, Protocol}
import io.delta.kernel.internal.checksum.CRCInfo.CRC_FILE_SCHEMA
import io.delta.kernel.internal.data.{GenericRow, StructRow}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.types.DataTypeJsonSerDe
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.internal.util.VectorUtils.{buildArrayValue, buildColumnVector, stringStringMapValue}
import io.delta.kernel.test.{BaseMockJsonHandler, MockEngineUtils}
import io.delta.kernel.types.{StringType, StructType}
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite for ChecksumWriter functionality.
 */
class ChecksumWriterSuite extends AnyFunSuite with MockEngineUtils {

  private val FAKE_DELTA_LOG_PATH = new Path("/path/to/delta/log")

  // Schema field indices in crc file
  private val TABLE_SIZE_BYTES_IDX = CRC_FILE_SCHEMA.indexOf("tableSizeBytes")
  private val NUM_FILES_IDX = CRC_FILE_SCHEMA.indexOf("numFiles")
  private val NUM_METADATA_IDX = CRC_FILE_SCHEMA.indexOf("numMetadata")
  private val NUM_PROTOCOL_IDX = CRC_FILE_SCHEMA.indexOf("numProtocol")
  private val TXN_ID_IDX = CRC_FILE_SCHEMA.indexOf("txnId")
  private val DOMAIN_METADATA_IDX = CRC_FILE_SCHEMA.indexOf("domainMetadata")
  private val METADATA_IDX = CRC_FILE_SCHEMA.indexOf("metadata")
  private val PROTOCOL_IDX = CRC_FILE_SCHEMA.indexOf("protocol")
  private val FILE_SIZE_HISTOGRAM_IDX = CRC_FILE_SCHEMA.indexOf("fileSizeHistogram")

  test("write checksum") {
    val jsonHandler = new MockCheckSumFileJsonWriter()
    val checksumWriter = new ChecksumWriter(FAKE_DELTA_LOG_PATH)
    val protocol = createTestProtocol()
    val metadata = createTestMetadata()

    def testChecksumWrite(
        txn: Optional[String],
        domainMetadata: Optional[util.List[DomainMetadata]]): Unit = {
      val version = 1L
      val tableSizeBytes = 100L
      val numFiles = 1L

      // TODO when we support writing fileSizeHistogram as part of CRC update this to be non-empty
      checksumWriter.writeCheckSum(
        mockEngine(jsonHandler = jsonHandler),
        new CRCInfo(
          version,
          metadata,
          protocol,
          tableSizeBytes,
          numFiles,
          txn,
          domainMetadata,
          Optional.empty()))

      verifyChecksumFile(jsonHandler, version)
      verifyChecksumContent(
        jsonHandler.capturedCrcRow.get,
        tableSizeBytes,
        numFiles,
        metadata,
        protocol,
        txn,
        domainMetadata)
    }

    // Test with and without transaction ID, domain metadata
    testChecksumWrite(Optional.of("txn"), Optional.empty())
    testChecksumWrite(Optional.empty(), Optional.empty())
    testChecksumWrite(
      Optional.empty(),
      Optional.of(Seq(
        new DomainMetadata("domain1", "", false /* removed */ ),
        new DomainMetadata("domain2", "", false /* removed */ )).asJava))
    // Per protocol, domain metadata list should exclude tombstone.
    intercept[IllegalArgumentException] {
      testChecksumWrite(
        Optional.empty(),
        Optional.of(Seq(
          new DomainMetadata("domain1", "", true /* removed */ ),
          new DomainMetadata("domain2", "", false /* removed */ )).asJava))
    }
  }

  private def verifyChecksumFile(jsonHandler: MockCheckSumFileJsonWriter, version: Long): Unit = {
    assert(jsonHandler.checksumFilePath == s"$FAKE_DELTA_LOG_PATH/${"%020d".format(version)}.crc")
    assert(jsonHandler.capturedCrcRow.isDefined)
    assert(jsonHandler.capturedCrcRow.get.getSchema == CRC_FILE_SCHEMA)
  }

  private def verifyChecksumContent(
      actualCheckSumRow: Row,
      expectedTableSizeBytes: Long,
      expectedNumFiles: Long,
      expectedMetadata: Metadata,
      expectedProtocol: Protocol,
      expectedTxnId: Optional[String],
      domainMetadata: Optional[util.List[DomainMetadata]]): Unit = {
    assert(!actualCheckSumRow.isNullAt(TABLE_SIZE_BYTES_IDX) && actualCheckSumRow.getLong(
      TABLE_SIZE_BYTES_IDX) == expectedTableSizeBytes)
    assert(!actualCheckSumRow.isNullAt(
      NUM_FILES_IDX) && actualCheckSumRow.getLong(NUM_FILES_IDX) == expectedNumFiles)
    assert(!actualCheckSumRow.isNullAt(
      NUM_METADATA_IDX) && actualCheckSumRow.getLong(NUM_METADATA_IDX) == 1L)
    assert(!actualCheckSumRow.isNullAt(
      NUM_PROTOCOL_IDX) && actualCheckSumRow.getLong(NUM_PROTOCOL_IDX) == 1L)
    assert(expectedProtocol === Protocol.fromRow(actualCheckSumRow.getStruct(PROTOCOL_IDX)))
    assert(expectedMetadata === Metadata.fromRow(actualCheckSumRow.getStruct(METADATA_IDX)))

    if (expectedTxnId.isPresent) {
      assert(actualCheckSumRow.getString(TXN_ID_IDX) == expectedTxnId.get())
    } else {
      assert(actualCheckSumRow.isNullAt(TXN_ID_IDX))
    }

    if (domainMetadata.isPresent) {
      assert(VectorUtils.toJavaList[Row](actualCheckSumRow.getArray(DOMAIN_METADATA_IDX)).asScala
        .map(DomainMetadata.fromRow)
        === domainMetadata.get().asScala)
    } else {
      assert(actualCheckSumRow.isNullAt(DOMAIN_METADATA_IDX))
    }

    // TODO once we support writing fileSizeHistogram as part of CRC check it here
    assert(actualCheckSumRow.isNullAt(FILE_SIZE_HISTOGRAM_IDX))
  }

  private def createTestMetadata(): Metadata = {
    new Metadata(
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
  }

  private def createTestProtocol(): Protocol = {
    new Protocol(
      /* minReaderVersion= */ 1,
      /* minWriterVersion= */ 2,
      Collections.emptySet(),
      Collections.emptySet())
  }
}

/**
 * Mock implementation of JsonHandler for testing checksum file writing.
 */
class MockCheckSumFileJsonWriter extends BaseMockJsonHandler {
  var capturedCrcRow: Option[Row] = None
  var checksumFilePath: String = ""

  override def writeJsonFileAtomically(
      filePath: String,
      data: CloseableIterator[Row],
      overwrite: Boolean): Unit = {
    checksumFilePath = filePath
    assert(data.hasNext, "Expected data iterator to contain exactly one row")
    capturedCrcRow = Some(data.next())
    assert(!data.hasNext, "Expected data iterator to contain exactly one row")
  }
}
