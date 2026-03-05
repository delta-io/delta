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

import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for backward-compatible deserialization of the VersionChecksum histogram field.
 *
 * Delta spec and Kernel (Java/Rust) write CRC files using "fileSizeHistogram" as the JSON field
 * name, while Delta-Spark historically used "histogramOpt". The `@JsonAlias` on
 * [[VersionChecksum.histogramOpt]] allows reading both field names so that CRC files written by
 * either Kernel or Delta-Spark are compatible.
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
    assert(parsedChecksum.histogramOpt.isDefined,
      "histogramOpt should be populated from the fileSizeHistogram JSON field")
    val parsedHistogram = parsedChecksum.histogramOpt.get
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
      assert(snapshotA.checksumOpt.get.histogramOpt.isDefined,
        "Scenario A: histogramOpt should be populated from the fileSizeHistogram JSON field")
      assert(snapshotA.checksumOpt.get.histogramOpt.get === parsedHistogram)

      // Scenario B: read the real CRC produced by Delta-Spark, replace "histogramOpt" key
      // with "fileSizeHistogram" (simulating a CRC rewritten by Kernel), and read it back
      val realChecksum = log.readChecksum(version).get
      assert(realChecksum.histogramOpt.isDefined, "expected histogram in real CRC")
      val realHistogram = realChecksum.histogramOpt.get
      val kernelFormatJson = JsonUtils.toJson(realChecksum)
        .replace("\"histogramOpt\":", "\"fileSizeHistogram\":")
      log.store.write(
        FileNames.checksumFile(log.logPath, version),
        Iterator(kernelFormatJson),
        overwrite = true)
      DeltaLog.clearCache()
      val snapshotB = DeltaLog.forTable(spark, dir.getAbsolutePath).snapshot
      assert(snapshotB.checksumOpt.isDefined)
      assert(snapshotB.checksumOpt.get.histogramOpt.isDefined,
        "Scenario B: histogramOpt should be populated from the fileSizeHistogram JSON field")
      assert(snapshotB.checksumOpt.get.histogramOpt.get === realHistogram)
    }
  }

  test("CRC missing both histogramOpt and fileSizeHistogram fields deserializes without error") {
    // CRC files written before histogram support was added have neither field.
    // Readers must gracefully return None for histogramOpt.
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
    assert(checksum.histogramOpt.isEmpty,
      "histogramOpt should be None when neither histogramOpt nor fileSizeHistogram is present")
  }

  test("CRC with both histogramOpt and fileSizeHistogram - last field in JSON takes priority") {
    // In practice a CRC will only contain one of these fields (Delta-Spark writes
    // histogramOpt, Kernel writes fileSizeHistogram). However, if both are present,
    // Jackson maps both to the same
    // VersionChecksum.histogramOpt field (via @JsonAlias) and processes them sequentially,
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
    assert(checksumCase1.histogramOpt.get.fileCounts.toSeq === Seq(1L, 2L, 3L),
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
    assert(checksumCase2.histogramOpt.get.fileCounts.toSeq === Seq(10L, 20L, 30L),
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
      assert(snapshotA.checksumOpt.get.histogramOpt.get.fileCounts.toSeq === Seq(1L, 2L, 3L),
        "Scenario A: fileSizeHistogram (last in JSON) should win over histogramOpt")

      // Scenario B: read the real CRC produced by Delta-Spark, inject both fields by
      // appending "fileSizeHistogram" (with bumped fileCounts) after the existing
      // "histogramOpt", and verify fileSizeHistogram wins when read back via DeltaLog
      val realChecksum = log.readChecksum(version).get
      assert(realChecksum.histogramOpt.isDefined, "expected histogram in real CRC")
      val realHistogramJson = JsonUtils.toJson(realChecksum.histogramOpt.get)
      val altHistogram = realChecksum.histogramOpt.get.copy(
        fileCounts = realChecksum.histogramOpt.get.fileCounts.map(_ + 1))
      val altHistogramJson = JsonUtils.toJson(altHistogram)
      // Insert fileSizeHistogram after histogramOpt so it appears last and wins
      val bothFieldsFSHLast = JsonUtils.toJson(realChecksum).replace(
        s""""histogramOpt":$realHistogramJson""",
        s""""histogramOpt":$realHistogramJson,"fileSizeHistogram":$altHistogramJson""")
      log.store.write(
        FileNames.checksumFile(log.logPath, version),
        Iterator(bothFieldsFSHLast),
        overwrite = true)
      DeltaLog.clearCache()
      val snapshotB = DeltaLog.forTable(spark, dir.getAbsolutePath).snapshot
      assert(snapshotB.checksumOpt.isDefined)
      assert(snapshotB.checksumOpt.get.histogramOpt.get === altHistogram,
        "Scenario B: fileSizeHistogram (last in JSON) should win over histogramOpt")
    }
  }
}
