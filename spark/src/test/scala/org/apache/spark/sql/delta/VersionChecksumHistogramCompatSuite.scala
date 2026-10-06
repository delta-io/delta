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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.FileSizeHistogram
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for backward-compatible deserialization of the VersionChecksum histogram field.
 *
 * Delta spec and Kernel (Java/Rust) write CRC files using "fileSizeHistogram" as the JSON field
 * name, while Delta-Spark historically used "histogramOpt". The `@JsonAlias` on
 * [[VersionChecksum.fileSizeHistogram]] allows reading both field names so that CRC files written
 * by either Kernel or Delta-Spark are compatible.
 */
class VersionChecksumHistogramCompatSuite
  extends QueryTest
  with DeltaSQLCommandTest
  with SharedSparkSession {

  import testImplicits._

  test("CRC with spec-compliant fileSizeHistogram field (Kernel format) is readable") {
    // Delta spec and Kernel (Java/Rust) use "fileSizeHistogram" as the JSON field name.
    // Delta-Spark historically used "histogramOpt". This test verifies that Delta-Spark
    // can read CRC files written by Kernel (i.e., JSON with "fileSizeHistogram" key).

    // Part 1: hardcoded JSON (unit-level deserialization check)
    val kernelWrittenJson =
      """{
        |  "txnId": "kernel-txn-id",
        |  "tableSizeBytes": 2000,
        |  "numFiles": 5,
        |  "numDeletedRecordsOpt": null,
        |  "numDeletionVectorsOpt": null,
        |  "numMetadata": 1,
        |  "numProtocol": 1,
        |  "inCommitTimestampOpt": null,
        |  "setTransactions": null,
        |  "domainMetadata": null,
        |  "metadata": {"id": "kernel-test-table-id", "format": {"provider": "parquet"},
        |    "partitionColumns": [], "configuration": {}},
        |  "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
        |  "fileSizeHistogram": {
        |    "sortedBinBoundaries": [0, 1024, 10240, 102400, 1048576, 10485760],
        |    "fileCounts": [2, 1, 0, 1, 1, 0],
        |    "totalBytes": [1000, 5000, 0, 200000, 2000000, 0]
        |  },
        |  "deletedRecordCountsHistogramOpt": null,
        |  "allFiles": null
        |}""".stripMargin

    val parsedChecksum = JsonUtils.mapper.readValue[VersionChecksum](kernelWrittenJson)
    assert(parsedChecksum.fileSizeHistogram.isDefined,
      "fileSizeHistogram should be populated from the fileSizeHistogram JSON field")
    val parsedHistogram = parsedChecksum.fileSizeHistogram.get
    assert(parsedHistogram.sortedBinBoundaries ===
      IndexedSeq(0L, 1024L, 10240L, 102400L, 1048576L, 10485760L))
    assert(parsedHistogram.fileCounts.toSeq === Seq(2L, 1L, 0L, 1L, 1L, 0L))
    assert(parsedHistogram.totalBytes.toSeq === Seq(1000L, 5000L, 0L, 200000L, 2000000L, 0L))

    // Part 2: integration check via real Delta table and DeltaLog
    withTempDir { dir =>
      spark.range(10).write.format("delta").save(dir.getAbsolutePath)
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      val version = log.snapshot.version

      // Scenario A: persist the hardcoded test JSON as the CRC and read it back via DeltaLog.
      // Re-serialize as compact JSON (single line) because store.read() splits by newline and
      // readChecksum takes only the first line.
      log.store.write(
        FileNames.checksumFile(log.logPath, version),
        Iterator(JsonUtils.toJson(parsedChecksum)),
        overwrite = true)
      DeltaLog.clearCache()
      val snapshotA = DeltaLog.forTable(spark, dir.getAbsolutePath).snapshot
      assert(snapshotA.checksumOpt.isDefined)
      assert(snapshotA.checksumOpt.get.fileSizeHistogram.isDefined,
        "Scenario A: fileSizeHistogram should be populated from the fileSizeHistogram JSON field")
      assert(snapshotA.checksumOpt.get.fileSizeHistogram.get === parsedHistogram)

      // Scenario B: read the real CRC produced by Delta-Spark, replace "fileSizeHistogram" key
      // with "histogramOpt" (simulating a CRC written by legacy Delta-Spark), and read it back.
      val realChecksum = log.readChecksum(version).get
      assert(realChecksum.fileSizeHistogram.isDefined, "expected histogram in real CRC")
      val realHistogram = realChecksum.fileSizeHistogram.get
      val legacyFormatJson = JsonUtils.toJson(realChecksum)
        .replace("\"fileSizeHistogram\":", "\"histogramOpt\":")
      log.store.write(
        FileNames.checksumFile(log.logPath, version),
        Iterator(legacyFormatJson),
        overwrite = true)
      DeltaLog.clearCache()
      val snapshotB = DeltaLog.forTable(spark, dir.getAbsolutePath).snapshot
      assert(snapshotB.checksumOpt.isDefined)
      assert(snapshotB.checksumOpt.get.fileSizeHistogram.isDefined,
        "Scenario B: fileSizeHistogram should be populated from the histogramOpt JSON field")
      assert(snapshotB.checksumOpt.get.fileSizeHistogram.get === realHistogram)
    }
  }

  test("CRC missing both histogramOpt and fileSizeHistogram fields deserializes without error") {
    // CRC files written before histogram support was added have neither field.
    // Readers must gracefully return None for fileSizeHistogram.
    val noHistogramJson =
      """{
        |  "txnId": "old-txn-id",
        |  "tableSizeBytes": 500,
        |  "numFiles": 2,
        |  "numDeletedRecordsOpt": null,
        |  "numDeletionVectorsOpt": null,
        |  "numMetadata": 1,
        |  "numProtocol": 1,
        |  "inCommitTimestampOpt": null,
        |  "setTransactions": null,
        |  "domainMetadata": null,
        |  "metadata": null,
        |  "protocol": null,
        |  "deletedRecordCountsHistogramOpt": null,
        |  "allFiles": null
        |}""".stripMargin

    val checksum = JsonUtils.mapper.readValue[VersionChecksum](noHistogramJson)
    assert(checksum.fileSizeHistogram.isEmpty,
      "fileSizeHistogram should be None when neither histogramOpt nor fileSizeHistogram is present")
  }

  test("CRC with both histogramOpt and fileSizeHistogram - last field in JSON takes priority") {
    // In practice a CRC will only contain one of these fields (Delta-Spark writes
    // histogramOpt, Kernel writes fileSizeHistogram). However, if both are present,
    // Jackson maps both to the same
    // VersionChecksum.fileSizeHistogram field (via @JsonAlias) and processes them sequentially,
    // so the LAST occurrence in the JSON wins. This test documents that behavior.

    // Part 1: hardcoded JSON (unit-level deserialization check)
    // histogramOpt fileCounts = [10, 20, 30] - distinguishable "Delta-Spark value"
    // fileSizeHistogram fileCounts = [1, 2, 3] - used as a distinguishable "Kernel value"

    // Case 1: fileSizeHistogram appears last -> fileSizeHistogram value wins
    val fileSizeHistogramLast =
      """{
        |  "txnId": "txn-1",
        |  "tableSizeBytes": 1000,
        |  "numFiles": 3,
        |  "numDeletedRecordsOpt": null,
        |  "numDeletionVectorsOpt": null,
        |  "numMetadata": 1,
        |  "numProtocol": 1,
        |  "inCommitTimestampOpt": null,
        |  "setTransactions": null,
        |  "domainMetadata": null,
        |  "metadata": {"id": "kernel-test-table-id", "format": {"provider": "parquet"},
        |    "partitionColumns": [], "configuration": {}},
        |  "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
        |  "histogramOpt": {
        |    "sortedBinBoundaries": [0, 1024, 10240],
        |    "fileCounts": [10, 20, 30],
        |    "totalBytes": [100, 200, 300]
        |  },
        |  "fileSizeHistogram": {
        |    "sortedBinBoundaries": [0, 1024, 10240],
        |    "fileCounts": [1, 2, 3],
        |    "totalBytes": [10, 20, 30]
        |  },
        |  "deletedRecordCountsHistogramOpt": null,
        |  "allFiles": null
        |}""".stripMargin

    val checksumCase1 = JsonUtils.mapper.readValue[VersionChecksum](fileSizeHistogramLast)
    assert(checksumCase1.fileSizeHistogram.get.fileCounts.toSeq === Seq(1L, 2L, 3L),
      "fileSizeHistogram (last in JSON) should win over histogramOpt")

    // Case 2: histogramOpt appears last -> histogramOpt value wins
    val histogramOptLast =
      """{
        |  "txnId": "txn-2",
        |  "tableSizeBytes": 1000,
        |  "numFiles": 3,
        |  "numDeletedRecordsOpt": null,
        |  "numDeletionVectorsOpt": null,
        |  "numMetadata": 1,
        |  "numProtocol": 1,
        |  "inCommitTimestampOpt": null,
        |  "setTransactions": null,
        |  "domainMetadata": null,
        |  "metadata": {"id": "kernel-test-table-id", "format": {"provider": "parquet"},
        |    "partitionColumns": [], "configuration": {}},
        |  "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
        |  "fileSizeHistogram": {
        |    "sortedBinBoundaries": [0, 1024, 10240],
        |    "fileCounts": [1, 2, 3],
        |    "totalBytes": [10, 20, 30]
        |  },
        |  "histogramOpt": {
        |    "sortedBinBoundaries": [0, 1024, 10240],
        |    "fileCounts": [10, 20, 30],
        |    "totalBytes": [100, 200, 300]
        |  },
        |  "deletedRecordCountsHistogramOpt": null,
        |  "allFiles": null
        |}""".stripMargin

    val checksumCase2 = JsonUtils.mapper.readValue[VersionChecksum](histogramOptLast)
    assert(checksumCase2.fileSizeHistogram.get.fileCounts.toSeq === Seq(10L, 20L, 30L),
      "histogramOpt (last in JSON) should win over fileSizeHistogram")

    // Part 2: integration check via real Delta table and DeltaLog
    withTempDir { dir =>
      spark.range(10).write.format("delta").save(dir.getAbsolutePath)
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      val version = log.snapshot.version

      // Scenario A: persist the hardcoded test JSON (both fields, fileSizeHistogram last)
      // and verify fileSizeHistogram wins when read back via DeltaLog.
      // Use readTree->writeValueAsString to compact to a single line (store.read() splits
      // by newline and readChecksum takes only .head), while preserving both JSON fields.
      val compactBothFields = JsonUtils.mapper.writeValueAsString(
        JsonUtils.mapper.readTree(fileSizeHistogramLast))
      log.store.write(
        FileNames.checksumFile(log.logPath, version),
        Iterator(compactBothFields),
        overwrite = true)
      DeltaLog.clearCache()
      val snapshotA = DeltaLog.forTable(spark, dir.getAbsolutePath).snapshot
      assert(snapshotA.checksumOpt.isDefined)
      assert(snapshotA.checksumOpt.get.fileSizeHistogram.get.fileCounts.toSeq === Seq(1L, 2L, 3L),
        "Scenario A: fileSizeHistogram (last in JSON) should win over histogramOpt")

      // Scenario B: read the real CRC produced by Delta-Spark, inject both fields by
      // appending a second "histogramOpt" (with bumped fileCounts) after the existing
      // "fileSizeHistogram", and verify histogramOpt wins when it appears last.
      val realChecksum = log.readChecksum(version).get
      assert(realChecksum.fileSizeHistogram.isDefined, "expected histogram in real CRC")
      val realHistogramJson = JsonUtils.toJson(realChecksum.fileSizeHistogram.get)
      val altHistogram = realChecksum.fileSizeHistogram.get.copy(
        fileCounts = realChecksum.fileSizeHistogram.get.fileCounts.map(_ + 1))
      val altHistogramJson = JsonUtils.toJson(altHistogram)
      // Insert histogramOpt after fileSizeHistogram so it appears last and wins.
      val bothFieldsHistogramOptLast = JsonUtils.toJson(realChecksum).replace(
        s""""fileSizeHistogram":$realHistogramJson""",
        s""""fileSizeHistogram":$realHistogramJson,"histogramOpt":$altHistogramJson""")
      log.store.write(
        FileNames.checksumFile(log.logPath, version),
        Iterator(bothFieldsHistogramOptLast),
        overwrite = true)
      DeltaLog.clearCache()
      val snapshotB = DeltaLog.forTable(spark, dir.getAbsolutePath).snapshot
      assert(snapshotB.checksumOpt.isDefined)
      assert(snapshotB.checksumOpt.get.fileSizeHistogram.get === altHistogram,
        "Scenario B: histogramOpt (last in JSON) should win over fileSizeHistogram")
    }
  }


  test("writeChecksumFile writes correct field name based on conf") {
    withTempDir { dir =>
      spark.range(10).write.format("delta").save(dir.getAbsolutePath)
      val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)
      val testHistogram = FileSizeHistogram(
        sortedBinBoundaries = Vector(0L, 1024L, 10240L),
        fileCounts = Array(5L, 10L, 15L),
        totalBytes = Array(100L, 200L, 300L))
      val checksum = deltaLog.snapshot.checksumOpt.get.copy(fileSizeHistogram = Some(testHistogram))

      val currentSpark = spark
      val currentLog = deltaLog
      val writer = new RecordChecksum {
        override val deltaLog: DeltaLog = currentLog
        override protected def spark: org.apache.spark.sql.SparkSession = currentSpark
        def writeChecksum(version: Long, cs: VersionChecksum): Unit =
          writeChecksumFile(version, cs)
      }

      // Write with flag OFF (default) -- should use histogramOpt
      val versionOff = deltaLog.snapshot.version + 1
      withSQLConf(DeltaSQLConf.DELTA_CHECKSUM_HISTOGRAM_FIELD_FOLLOWS_PROTOCOL.key -> "false") {
        writer.writeChecksum(versionOff, checksum)
      }
      val crcJsonOff =
        deltaLog.store.read(FileNames.checksumFile(deltaLog.logPath, versionOff)).head
      assert(crcJsonOff.contains("\"histogramOpt\":"),
        "Flag OFF: CRC should contain histogramOpt")
      assert(!crcJsonOff.contains("\"fileSizeHistogram\":"),
        "Flag OFF: CRC should not contain fileSizeHistogram")

      // Write with flag ON -- should use fileSizeHistogram
      val versionOn = versionOff + 1
      withSQLConf(DeltaSQLConf.DELTA_CHECKSUM_HISTOGRAM_FIELD_FOLLOWS_PROTOCOL.key -> "true") {
        writer.writeChecksum(versionOn, checksum)
      }
      val crcJsonOn =
        deltaLog.store.read(FileNames.checksumFile(deltaLog.logPath, versionOn)).head
      assert(crcJsonOn.contains("\"fileSizeHistogram\":"),
        "Flag ON: CRC should contain fileSizeHistogram")
      assert(!crcJsonOn.contains("\"histogramOpt\":"),
        "Flag ON: CRC should not contain histogramOpt")

      // Both CRCs should be readable and produce the same histogram
      val checksumOff = JsonUtils.mapper.readValue[VersionChecksum](crcJsonOff)
      val checksumOn = JsonUtils.mapper.readValue[VersionChecksum](crcJsonOn)
      assert(checksumOff.fileSizeHistogram.get === testHistogram,
        "Flag OFF: read-back histogram should match the test histogram")
      assert(checksumOn.fileSizeHistogram.get === testHistogram,
        "Flag ON: read-back histogram should match the test histogram")
    }
  }

  test("VersionChecksumLegacy fields match VersionChecksum") {
    // Use reflection to ensure the two classes stay in sync. If someone adds a field to
    // VersionChecksum but forgets VersionChecksumLegacy, this test will catch it.
    val checksumFields = classOf[VersionChecksum].getDeclaredFields
      .map(f => (f.getName, f.getType)).toSet
    val legacyFields = classOf[VersionChecksumLegacy].getDeclaredFields
      .map(f => (f.getName, f.getType)).toSet

    // The only difference should be fileSizeHistogram vs histogramOpt (same type).
    val expectedOnlyInChecksum = Set(("fileSizeHistogram", classOf[Option[_]]))
    val expectedOnlyInLegacy = Set(("histogramOpt", classOf[Option[_]]))

    val onlyInChecksum = checksumFields -- legacyFields
    val onlyInLegacy = legacyFields -- checksumFields

    assert(onlyInChecksum === expectedOnlyInChecksum,
      s"Unexpected fields only in VersionChecksum: $onlyInChecksum. " +
        "Did you add a new field to VersionChecksum without updating " +
        "VersionChecksumLegacy?")
    assert(onlyInLegacy === expectedOnlyInLegacy,
      s"Unexpected fields only in VersionChecksumLegacy: $onlyInLegacy. " +
        "Did you add a new field to VersionChecksumLegacy without updating " +
        "VersionChecksum?")
  }
}
