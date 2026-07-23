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
package io.delta.kernel.defaults

import java.util
import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel.defaults.internal.json.JsonUtils
import io.delta.kernel.internal.actions.{Format, Metadata, Protocol, SetTransaction}
import io.delta.kernel.internal.checksum.CRCInfo
import io.delta.kernel.internal.types.DataTypeJsonSerDe
import io.delta.kernel.internal.util.VectorUtils.{buildArrayValue, stringStringMapValue}
import io.delta.kernel.types.{StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

/**
 * Golden-string tests that pin the raw serialized `.crc` JSON produced by kernel
 * (CRCInfo.toRow -> JsonUtils.rowToJson, the same path ChecksumWriter uses via
 * DefaultJsonHandler.writeJsonFileAtomically).
 */
class CRCInfoSerializationSuite extends AnyFunSuite {

  private def testProtocol(): Protocol =
    new Protocol(1, 2, Collections.emptySet(), Collections.emptySet())

  // A fully-deterministic Metadata: fixed id, fixed createdTime, single fixed table property.
  private def testMetadata(): Metadata =
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

  private def crcJson(crcInfo: CRCInfo): String = JsonUtils.rowToJson(crcInfo.toRow())

  test("serialized CRC omits setTransactions when absent") {
    val crcInfo = new CRCInfo(
      1L,
      testMetadata(),
      testProtocol(),
      100L, /* tableSizeBytes */
      1L, /* numFiles */
      Optional.empty(), /* txnId */
      Optional.empty(), /* domainMetadata */
      Optional.empty()
    ) /* fileSizeHistogram */

    val expected =
      """{"tableSizeBytes":100,"numFiles":1,"numMetadata":1,"numProtocol":1,""" +
        """"metadata":{"id":"id","name":"name","description":"description",""" +
        """"format":{"provider":"parquet","options":{}},""" +
        """"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":["c3"],""" +
        """"createdTime":123,"configuration":{"delta.appendOnly":"true"}},""" +
        """"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"""

    assert(crcJson(crcInfo) === expected)
  }

  test("serialized CRC includes an empty setTransactions array") {
    val crcInfo = new CRCInfo(
      2L,
      testMetadata(),
      testProtocol(),
      100L,
      1L,
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.empty(), /* inCommitTimestamp */
      Optional.of(Collections.emptyList[SetTransaction]())
    ) /* setTransactions */

    val expected =
      """{"tableSizeBytes":100,"numFiles":1,"numMetadata":1,"numProtocol":1,""" +
        """"metadata":{"id":"id","name":"name","description":"description",""" +
        """"format":{"provider":"parquet","options":{}},""" +
        """"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":["c3"],""" +
        """"createdTime":123,"configuration":{"delta.appendOnly":"true"}},""" +
        """"protocol":{"minReaderVersion":1,"minWriterVersion":2},"setTransactions":[]}"""

    assert(crcJson(crcInfo) === expected)
  }

  test("serialized CRC includes populated setTransactions") {
    val txns = util.Arrays.asList(
      new SetTransaction("app1", 5L, Optional.of(java.lang.Long.valueOf(100L))),
      new SetTransaction("app2", 9L, Optional.empty()))
    val crcInfo = new CRCInfo(
      3L,
      testMetadata(),
      testProtocol(),
      100L,
      1L,
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.of(txns))

    val expected =
      """{"tableSizeBytes":100,"numFiles":1,"numMetadata":1,"numProtocol":1,""" +
        """"metadata":{"id":"id","name":"name","description":"description",""" +
        """"format":{"provider":"parquet","options":{}},""" +
        """"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":["c3"],""" +
        """"createdTime":123,"configuration":{"delta.appendOnly":"true"}},""" +
        """"protocol":{"minReaderVersion":1,"minWriterVersion":2},""" +
        """"setTransactions":[{"appId":"app1","version":5,"lastUpdated":100},""" +
        """{"appId":"app2","version":9}]}"""

    assert(crcJson(crcInfo) === expected)
  }
}
