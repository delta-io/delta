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

import io.delta.kernel.data.Row
import io.delta.kernel.internal.actions.{DomainMetadata, Format, Metadata, Protocol}
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.stats.FileSizeHistogram
import io.delta.kernel.internal.types.DataTypeJsonSerDe
import io.delta.kernel.internal.util.VectorUtils.{buildArrayValue, stringStringMapValue}
import io.delta.kernel.types.{StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests that [[CRCInfo.fromRow]] inverts [[CRCInfo.toRow]]: a CRCInfo round-tripped through its Row
 * representation is equal to the original, across every combination of the optional fields.
 */
class CRCInfoFromRowSuite extends AnyFunSuite {

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

  private def createTestHistogram(fileCount: Long): FileSizeHistogram = {
    val boundaries = Array(0L, 1024L)
    val counts = Array(fileCount, 0L)
    val bytes = Array(fileCount * 100, 0L)
    new FileSizeHistogram(boundaries, counts, bytes)
  }

  private def domainMetadataSet(entries: (String, String)*): util.Set[DomainMetadata] = {
    val set = new util.HashSet[DomainMetadata]()
    entries.foreach { case (domain, config) =>
      set.add(new DomainMetadata(domain, config, false))
    }
    set
  }

  /** Round-trips a CRCInfo through toRow -> fromRow and asserts equality. */
  private def assertRoundTrips(crcInfo: CRCInfo): Unit = {
    val row: Row = crcInfo.toRow()
    val reconstructed = CRCInfo.fromRow(crcInfo.getVersion, row)
    assert(reconstructed === crcInfo)
  }

  test("round-trips with only required fields (no optional fields)") {
    assertRoundTrips(new CRCInfo(
      5L,
      testMetadata,
      testProtocol,
      /* tableSizeBytes */ 1000L,
      /* numFiles */ 10L,
      /* txnId */ Optional.empty(),
      /* domainMetadata */ Optional.empty(),
      /* fileSizeHistogram */ Optional.empty()))
  }

  test("round-trips with txnId present") {
    assertRoundTrips(new CRCInfo(
      7L,
      testMetadata,
      testProtocol,
      2000L,
      20L,
      Optional.of("txn-abc"),
      Optional.empty(),
      Optional.empty()))
  }

  test("round-trips with domainMetadata present") {
    assertRoundTrips(new CRCInfo(
      9L,
      testMetadata,
      testProtocol,
      3000L,
      30L,
      Optional.empty(),
      Optional.of(domainMetadataSet("delta.rowTracking" -> "{\"hwm\":42}", "d2" -> "{}")),
      Optional.empty()))
  }

  test("round-trips with fileSizeHistogram present") {
    assertRoundTrips(new CRCInfo(
      11L,
      testMetadata,
      testProtocol,
      4000L,
      40L,
      Optional.empty(),
      Optional.empty(),
      Optional.of(createTestHistogram(fileCount = 40))))
  }

  test("round-trips with inCommitTimestamp present") {
    assertRoundTrips(new CRCInfo(
      12L,
      testMetadata,
      testProtocol,
      4500L,
      45L,
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.of(java.lang.Long.valueOf(1749830855993L))))
  }

  test("round-trips with all optional fields present") {
    assertRoundTrips(new CRCInfo(
      13L,
      testMetadata,
      testProtocol,
      5000L,
      50L,
      Optional.of("txn-xyz"),
      Optional.of(domainMetadataSet("delta.clustering" -> "{\"cols\":[\"c1\"]}")),
      Optional.of(createTestHistogram(fileCount = 50)),
      Optional.of(java.lang.Long.valueOf(1749830871085L))))
  }

  test("preserves the supplied version, independent of the row (toRow omits version)") {
    val crcInfo = new CRCInfo(
      99L,
      testMetadata,
      testProtocol,
      6000L,
      60L,
      Optional.empty(),
      Optional.empty(),
      Optional.empty())
    val reconstructed = CRCInfo.fromRow(123L, crcInfo.toRow())
    assert(reconstructed.getVersion === 123L)
    assert(reconstructed.getMetadata === crcInfo.getMetadata)
    assert(reconstructed.getProtocol === crcInfo.getProtocol)
    assert(reconstructed.getTableSizeBytes === crcInfo.getTableSizeBytes)
    assert(reconstructed.getNumFiles === crcInfo.getNumFiles)
  }

  test("rejects a row whose schema is not CRC_FILE_SCHEMA") {
    val wrongRow: Row = testMetadata.toRow()
    val ex = intercept[IllegalArgumentException] {
      CRCInfo.fromRow(1L, wrongRow)
    }
    assert(ex.getMessage.contains("Expected schema"))
  }

  test("multiple domainMetadata entries round-trip as a set (order-independent)") {
    val entries = domainMetadataSet(
      "delta.rowTracking" -> "{\"hwm\":42}",
      "delta.clustering" -> "{\"cols\":[\"c1\"]}",
      "d3" -> "{}")
    val crcInfo = new CRCInfo(
      15L,
      testMetadata,
      testProtocol,
      7000L,
      70L,
      Optional.empty(),
      Optional.of(entries),
      Optional.empty())
    val reconstructed = CRCInfo.fromRow(crcInfo.getVersion, crcInfo.toRow())
    assert(reconstructed === crcInfo)
    // The array in the row is reassembled into a Set, so all entries survive regardless of order.
    assert(reconstructed.getDomainMetadata.isPresent)
    assert(reconstructed.getDomainMetadata.get === entries)
    assert(reconstructed.getDomainMetadata.get.size === 3)
  }

  // A row with CRC_FILE_SCHEMA whose fields come from `values`. Any field absent from the map reads
  // back as null.
  private def crcRow(values: util.Map[Integer, Object]): Row =
    new GenericRow(CRCInfo.CRC_FILE_SCHEMA, values)

  private def idx(field: String): Integer = Int.box(CRCInfo.CRC_FILE_SCHEMA.indexOf(field))

  /** A value map with every required field of CRC_FILE_SCHEMA populated. */
  private def requiredValues(): util.HashMap[Integer, Object] = {
    val values = new util.HashMap[Integer, Object]()
    values.put(idx("tableSizeBytes"), Long.box(1000L))
    values.put(idx("numFiles"), Long.box(10L))
    values.put(idx("numMetadata"), Long.box(1L))
    values.put(idx("numProtocol"), Long.box(1L))
    values.put(idx("metadata"), testMetadata.toRow())
    values.put(idx("protocol"), testProtocol.toRow())
    values
  }

  Seq("metadata", "protocol", "tableSizeBytes", "numFiles").foreach { field =>
    test(s"throws when required field '$field' is null") {
      val values = requiredValues()
      values.remove(idx(field))
      val ex = intercept[IllegalArgumentException] {
        CRCInfo.fromRow(1L, crcRow(values))
      }
      assert(ex.getMessage.contains(field))
    }
  }

  test("throws on a fully-null row (all fields null)") {
    // Schema matches CRC_FILE_SCHEMA, but every value is absent -> null. fromRow must reject it
    val ex = intercept[IllegalArgumentException] {
      CRCInfo.fromRow(1L, crcRow(new util.HashMap[Integer, Object]()))
    }
    // The first required field
    assert(ex.getMessage.contains("metadata"))
  }
}
