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
import io.delta.kernel.internal.checksum.ChecksumUtils.CRC_FILE_SCHEMA
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.internal.util.VectorUtils.{stringArrayValue, stringStringMapValue}
import io.delta.kernel.test.{BaseMockJsonHandler, MockEngineUtils}
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterator
import org.scalatest.funsuite.AnyFunSuite

import java.util
import java.util.{Collections, Optional, OptionalLong}

class ChecksumWriterSuite extends AnyFunSuite with MockEngineUtils {

  private val FAKE_DELTA_LOG_PATH = new Path("/path/to/delta/log")

  test("write checksum") {
    val jsonHandler = new MockCheckSumFileJsonWriter()
    val checksumWriter = new ChecksumWriter(FAKE_DELTA_LOG_PATH)
    val protocol = createTestProtocol()
    val metadata = createTestMetadata()

    def testChecksumWrite(txn: Optional[String]): Unit = {
      checksumWriter.writeCheckSum(
        mockEngine(jsonHandler = jsonHandler),
        new CRCInfo(1L, metadata, protocol, 100L, 1L, txn)
      )

      assert(jsonHandler.checksumFilePath == "/path/to/delta/log/00000000000000000001.crc")
      assert(jsonHandler.capturedCrcRow.getSchema == CRC_FILE_SCHEMA)
      assert(
        jsonHandler.capturedCrcRow
          .getLong(CRC_FILE_SCHEMA.indexOf("tableSizeBytes")) == 100L
      )
      assert(
        jsonHandler.capturedCrcRow.getLong(CRC_FILE_SCHEMA.indexOf("numFiles")) == 1L
      )
      assert(
        jsonHandler.capturedCrcRow
          .getLong(CRC_FILE_SCHEMA.indexOf("numMetadata")) == 1L
      )
      assert(
        jsonHandler.capturedCrcRow
          .getLong(CRC_FILE_SCHEMA.indexOf("numProtocol")) == 1L
      )

      if (txn.isPresent) {
        assert(
          jsonHandler.capturedCrcRow.getString(
            CRC_FILE_SCHEMA.indexOf("txnId")
          ) == txn.get()
        )
      } else {
        assert(jsonHandler.capturedCrcRow.isNullAt(CRC_FILE_SCHEMA.indexOf("txnId")))
      }

      checkMetadata(
        metadata,
        jsonHandler.capturedCrcRow.getStruct(CRC_FILE_SCHEMA.indexOf("metadata"))
      )
      checkProtocol(
        protocol,
        jsonHandler.capturedCrcRow.getStruct(CRC_FILE_SCHEMA.indexOf("protocol"))
      )

    }
    testChecksumWrite(Optional.of("txn"));
    testChecksumWrite(Optional.empty());
  }

  def createTestMetadata(): Metadata = {
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

  def createTestProtocol(): Protocol = {
    new Protocol(
      /* minReaderVersion= */ 1,
      /* minWriterVersion= */ 2,
      Collections.emptyList(),
      Collections.emptyList()
    )
  }

  def checkMetadata(metadata: Metadata, metadataRow: Row): Unit = {
    assert(metadataRow.getSchema == Metadata.FULL_SCHEMA)
    assert(metadataRow.getString(Metadata.FULL_SCHEMA.indexOf("id")) == metadata.getId)
    assert(
      Optional
        .ofNullable(metadataRow.getString(Metadata.FULL_SCHEMA.indexOf("name"))) == metadata.getName
    )
    assert(
      Optional.ofNullable(metadataRow.getString(Metadata.FULL_SCHEMA.indexOf("description")))
      == metadata.getDescription
    )
    assert(
      metadataRow
        .getStruct(
          Metadata.FULL_SCHEMA.indexOf("format")
        )
        .getString(Format.FULL_SCHEMA.indexOf("provider")) == metadata.getFormat.getProvider
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
        .ofNullable(metadataRow.getLong(Metadata.FULL_SCHEMA.indexOf("createdTime"))) == metadata.getCreatedTime
    )
    assert(
      VectorUtils
        .toJavaMap(metadataRow.getMap(Metadata.FULL_SCHEMA.indexOf("configuration"))) == metadata.getConfiguration
    )
  }

  def checkProtocol(protocol: Protocol, protocolRow: Row): Unit = {
    assert(protocolRow.getSchema == Protocol.FULL_SCHEMA)
    assert(
      protocol.getMinReaderVersion ==
      protocolRow.getInt(Protocol.FULL_SCHEMA.indexOf("minReaderVersion"))
    )
    assert(
      protocol.getMinWriterVersion ==
      protocolRow.getInt(Protocol.FULL_SCHEMA.indexOf("minWriterVersion"))
    )
  }
}

class MockCheckSumFileJsonWriter extends BaseMockJsonHandler {
  var capturedCrcRow: Row = new GenericRow(new StructType(), new util.HashMap[Integer, AnyRef]);
  var checksumFilePath = ""

  override def writeJsonFileAtomically(
      filePath: String,
      data: CloseableIterator[Row],
      overwrite: Boolean): Unit = {
    checksumFilePath = filePath
    assert(data.hasNext)
    capturedCrcRow = data.next()
    assert(!data.hasNext)
  }

}
