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

import io.delta.kernel.data.Row
import io.delta.kernel.internal.actions.{Format, Metadata, Protocol}
import io.delta.kernel.internal.checksum.CRCInfo.CRC_FILE_SCHEMA
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.internal.util.VectorUtils.{stringArrayValue, stringStringMapValue}
import io.delta.kernel.test.{BaseMockJsonHandler, MockEngineUtils}
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterator

import java.util
import java.util.{Collections, Optional, OptionalLong}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite for ChecksumWriter functionality.
 */
class ChecksumWriterSuite extends AnyFunSuite with MockEngineUtils {

  private val FAKE_DELTA_LOG_PATH = new Path("/path/to/delta/log")

  // Schema field indices in crc file
  private val TABLE_SIZE_BYTES = CRC_FILE_SCHEMA.indexOf("tableSizeBytes")
  private val NUM_FILES = CRC_FILE_SCHEMA.indexOf("numFiles")
  private val NUM_METADATA = CRC_FILE_SCHEMA.indexOf("numMetadata")
  private val NUM_PROTOCOL = CRC_FILE_SCHEMA.indexOf("numProtocol")
  private val TXN_ID = CRC_FILE_SCHEMA.indexOf("txnId")
  private val METADATA = CRC_FILE_SCHEMA.indexOf("metadata")
  private val PROTOCOL = CRC_FILE_SCHEMA.indexOf("protocol")

  test("write checksum") {
    val jsonHandler = new MockCheckSumFileJsonWriter()
    val checksumWriter = new ChecksumWriter(FAKE_DELTA_LOG_PATH)
    val protocol = createTestProtocol()
    val metadata = createTestMetadata()

    def testChecksumWrite(txn: Optional[String]): Unit = {
      val version = 1L
      val tableSizeBytes = 100L
      val numFiles = 1L

      checksumWriter.writeCheckSum(
        mockEngine(jsonHandler = jsonHandler),
        new CRCInfo(version, metadata, protocol, tableSizeBytes, numFiles, txn)
      )

      verifyChecksumFile(jsonHandler, version)
      verifyChecksumContent(jsonHandler.capturedCrcRow, tableSizeBytes, numFiles, txn)
      verifyMetadataAndProtocol(jsonHandler.capturedCrcRow, metadata, protocol)
    }

    // Test with and without transaction ID
    testChecksumWrite(Optional.of("txn"))
    testChecksumWrite(Optional.empty())
  }

  private def verifyChecksumFile(jsonHandler: MockCheckSumFileJsonWriter, version: Long): Unit = {
    assert(jsonHandler.checksumFilePath == s"$FAKE_DELTA_LOG_PATH/${"%020d".format(version)}.crc")
    assert(jsonHandler.capturedCrcRow.getSchema == CRC_FILE_SCHEMA)
  }

  private def verifyChecksumContent(
      row: Row,
      tableSizeBytes: Long,
      numFiles: Long,
      txn: Optional[String]): Unit = {
    assert(row.getLong(TABLE_SIZE_BYTES) == tableSizeBytes)
    assert(row.getLong(NUM_FILES) == numFiles)
    assert(row.getLong(NUM_METADATA) == 1L)
    assert(row.getLong(NUM_PROTOCOL) == 1L)

    if (txn.isPresent) {
      assert(row.getString(TXN_ID) == txn.get())
    } else {
      assert(row.isNullAt(TXN_ID))
    }
  }

  private def verifyMetadataAndProtocol(row: Row, metadata: Metadata, protocol: Protocol): Unit = {
    checkMetadata(metadata, row.getStruct(METADATA))
    checkProtocol(protocol, row.getStruct(PROTOCOL))
  }

  private def createTestMetadata(): Metadata = {
    new Metadata(
      "id",
      Optional.of("name"),
      Optional.of("description"),
      new Format("parquet", Collections.emptyMap()),
      "schemaString",
      new StructType(),
      stringArrayValue(util.Arrays.asList("c3")),
      Optional.of(123),
      stringStringMapValue(new util.HashMap[String, String]() {
        put("delta.appendOnly", "true")
      })
    )
  }

  private def createTestProtocol(): Protocol = {
    new Protocol(
      /* minReaderVersion= */ 1,
      /* minWriterVersion= */ 2,
      Collections.emptyList(),
      Collections.emptyList()
    )
  }

  private def checkMetadata(metadata: Metadata, metadataRow: Row): Unit = {
    assert(metadataRow.getSchema == Metadata.FULL_SCHEMA)

    def getOptionalString(field: String): Optional[String] =
      Optional.ofNullable(metadataRow.getString(Metadata.FULL_SCHEMA.indexOf(field)))

    assert(metadataRow.getString(Metadata.FULL_SCHEMA.indexOf("id")) == metadata.getId)
    assert(getOptionalString("name") == metadata.getName)
    assert(getOptionalString("description") == metadata.getDescription)

    val formatRow = metadataRow.getStruct(Metadata.FULL_SCHEMA.indexOf("format"))
    assert(
      formatRow.getString(Format.FULL_SCHEMA.indexOf("provider")) == metadata.getFormat.getProvider
    )

    assert(
      metadataRow
        .getString(Metadata.FULL_SCHEMA.indexOf("schemaString")) == metadata.getSchemaString
    )
    assert(
      metadataRow
        .getArray(Metadata.FULL_SCHEMA.indexOf("partitionColumns")) == metadata.getPartitionColumns
    )
    assert(
      Optional
        .ofNullable(metadataRow.getLong(Metadata.FULL_SCHEMA.indexOf("createdTime")))
      == metadata.getCreatedTime
    )
    assert(
      VectorUtils
        .toJavaMap(metadataRow.getMap(Metadata.FULL_SCHEMA.indexOf("configuration")))
      == metadata.getConfiguration
    )
  }

  private def checkProtocol(protocol: Protocol, protocolRow: Row): Unit = {
    assert(protocolRow.getSchema == Protocol.FULL_SCHEMA)
    assert(
      protocol.getMinReaderVersion == protocolRow
        .getInt(Protocol.FULL_SCHEMA.indexOf("minReaderVersion"))
    )
    assert(
      protocol.getMinWriterVersion == protocolRow
        .getInt(Protocol.FULL_SCHEMA.indexOf("minWriterVersion"))
    )
  }
}

/**
 * Mock implementation of JsonHandler for testing checksum file writing.
 */
class MockCheckSumFileJsonWriter extends BaseMockJsonHandler {
  var capturedCrcRow: Row = new GenericRow(new StructType(), new util.HashMap[Integer, AnyRef])
  var checksumFilePath: String = ""

  override def writeJsonFileAtomically(
      filePath: String,
      data: CloseableIterator[Row],
      overwrite: Boolean): Unit = {
    checksumFilePath = filePath
    assert(data.hasNext, "Expected data iterator to contain exactly one row")
    capturedCrcRow = data.next()
    assert(!data.hasNext, "Expected data iterator to contain exactly one row")
  }
}
