/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.metrics

import java.util.{Optional, UUID}

import io.delta.kernel.expressions.{Column, Literal, Predicate}
import io.delta.kernel.metrics.{ScanReport, SnapshotReport, TransactionReport}
import io.delta.kernel.types.{IntegerType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class MetricsReportSerializerSuite extends AnyFunSuite {

  private def optionToString[T](option: Optional[T]): String = {
    if (option.isPresent) {
      if (option.get().isInstanceOf[String]) {
        s""""${option.get()}"""" // For string objects wrap with quotes
      } else {
        option.get().toString
      }
    } else {
      "null"
    }
  }

  private def testSnapshotReport(snapshotReport: SnapshotReport): Unit = {
    val timestampToVersionResolutionDuration = optionToString(
      snapshotReport.getSnapshotMetrics().getTimestampToVersionResolutionDurationNs())
    val loadProtocolAndMetadataDuration =
      snapshotReport.getSnapshotMetrics().getLoadInitialDeltaActionsDurationNs()
    val exception: Optional[String] = snapshotReport.getException().map(_.toString)
    val expectedJson =
      s"""
         |{"tablePath":"${snapshotReport.getTablePath()}",
         |"operationType":"Snapshot",
         |"reportUUID":"${snapshotReport.getReportUUID()}",
         |"exception":${optionToString(exception)},
         |"version":${optionToString(snapshotReport.getVersion())},
         |"providedTimestamp":${optionToString(snapshotReport.getProvidedTimestamp())},
         |"snapshotMetrics":{
         |"timestampToVersionResolutionDurationNs":${timestampToVersionResolutionDuration},
         |"loadInitialDeltaActionsDurationNs":${loadProtocolAndMetadataDuration}
         |}
         |}
         |""".stripMargin.replaceAll("\n", "")
    assert(expectedJson == MetricsReportSerializers.serializeSnapshotReport(snapshotReport))
  }

  test("SnapshotReport serializer") {
    val snapshotContext1 = SnapshotQueryContext.forTimestampSnapshot("/table/path", 0)
    snapshotContext1.getSnapshotMetrics.timestampToVersionResolutionTimer.record(10)
    snapshotContext1.getSnapshotMetrics.loadInitialDeltaActionsTimer.record(1000)
    snapshotContext1.setVersion(1)
    val exception = new RuntimeException("something something failed")

    val snapshotReport1 = SnapshotReportImpl.forError(
      snapshotContext1,
      exception)

    // Manually check expected JSON
    val expectedJson =
      s"""
        |{"tablePath":"/table/path",
        |"operationType":"Snapshot",
        |"reportUUID":"${snapshotReport1.getReportUUID()}",
        |"exception":"$exception",
        |"version":1,
        |"providedTimestamp":0,
        |"snapshotMetrics":{
        |"timestampToVersionResolutionDurationNs":10,
        |"loadInitialDeltaActionsDurationNs":1000
        |}
        |}
        |""".stripMargin.replaceAll("\n", "")
    assert(expectedJson == MetricsReportSerializers.serializeSnapshotReport(snapshotReport1))

    // Check with test function
    testSnapshotReport(snapshotReport1)

    // Empty options for all possible fields (version, providedTimestamp and exception)
    val snapshotContext2 = SnapshotQueryContext.forLatestSnapshot("/table/path")
    val snapshotReport2 = SnapshotReportImpl.forSuccess(snapshotContext2)
    testSnapshotReport(snapshotReport2)
  }

  private def testTransactionReport(transactionReport: TransactionReport): Unit = {
    val exception: Optional[String] = transactionReport.getException().map(_.toString)
    val snapshotReportUUID: Optional[String] =
      transactionReport.getSnapshotReportUUID().map(_.toString)
    val transactionMetrics = transactionReport.getTransactionMetrics

    val expectedJson =
      s"""
         |{"tablePath":"${transactionReport.getTablePath()}",
         |"operationType":"Transaction",
         |"reportUUID":"${transactionReport.getReportUUID()}",
         |"exception":${optionToString(exception)},
         |"operation":"${transactionReport.getOperation()}",
         |"engineInfo":"${transactionReport.getEngineInfo()}",
         |"baseSnapshotVersion":${transactionReport.getBaseSnapshotVersion()},
         |"snapshotReportUUID":${optionToString(snapshotReportUUID)},
         |"committedVersion":${optionToString(transactionReport.getCommittedVersion())},
         |"transactionMetrics":{
         |"totalCommitDurationNs":${transactionMetrics.getTotalCommitDurationNs},
         |"numCommitAttempts":${transactionMetrics.getNumCommitAttempts},
         |"numAddFiles":${transactionMetrics.getNumAddFiles},
         |"numRemoveFiles":${transactionMetrics.getNumRemoveFiles},
         |"numTotalActions":${transactionMetrics.getNumTotalActions},
         |"totalAddFilesSizeInBytes":${transactionMetrics.getTotalAddFilesSizeInBytes},
         |"totalRemoveFilesSizeInBytes":${transactionMetrics.getTotalRemoveFilesSizeInBytes}
         |}
         |}
         |""".stripMargin.replaceAll("\n", "")
    assert(expectedJson == MetricsReportSerializers.serializeTransactionReport(transactionReport))
  }

  test("TransactionReport serializer") {
    val snapshotReport1 = SnapshotReportImpl.forSuccess(
      SnapshotQueryContext.forVersionSnapshot("/table/path", 1))
    val exception = new RuntimeException("something something failed")

    // Initialize transaction metrics and record some values
    val transactionMetrics1 = TransactionMetrics.forNewTable();
    transactionMetrics1.totalCommitTimer.record(200)
    transactionMetrics1.commitAttemptsCounter.increment(2)
    transactionMetrics1.updateForAddFile(1000)
    transactionMetrics1.updateForAddFile(100)
    transactionMetrics1.updateForRemoveFile(1000)
    transactionMetrics1.totalActionsCounter.increment(90)

    val transactionReport1 = new TransactionReportImpl(
      "/table/path",
      "test-operation",
      "test-engine",
      Optional.of(2), /* committedVersion */
      transactionMetrics1,
      snapshotReport1,
      Optional.of(exception))

    // Manually check expected JSON
    val expectedJson =
      s"""
         |{"tablePath":"/table/path",
         |"operationType":"Transaction",
         |"reportUUID":"${transactionReport1.getReportUUID()}",
         |"exception":"$exception",
         |"operation":"test-operation",
         |"engineInfo":"test-engine",
         |"baseSnapshotVersion":1,
         |"snapshotReportUUID":"${snapshotReport1.getReportUUID}",
         |"committedVersion":2,
         |"transactionMetrics":{
         |"totalCommitDurationNs":200,
         |"numCommitAttempts":2,
         |"numAddFiles":2,
         |"numRemoveFiles":1,
         |"numTotalActions":90,
         |"totalAddFilesSizeInBytes":1100,
         |"totalRemoveFilesSizeInBytes":1000
         |}
         |}
         |""".stripMargin.replaceAll("\n", "")
    assert(expectedJson == MetricsReportSerializers.serializeTransactionReport(transactionReport1))
    // Check with test function
    testTransactionReport(transactionReport1)

    // Initialize snapshot report for the empty table case
    val snapshotReport2 = SnapshotReportImpl.forSuccess(
      SnapshotQueryContext.forVersionSnapshot("/table/path", -1))
    // Empty option for all possible fields (committedVersion, exception)
    val transactionReport2 = new TransactionReportImpl(
      "/table/path",
      "test-operation-2",
      "test-engine-2",
      Optional.empty(), /* committedVersion */
      // empty/un-incremented transaction metrics
      TransactionMetrics.withExistingTableFileSizeHistogram(Optional.empty()),
      snapshotReport2,
      Optional.empty() /* exception */
    )
    testTransactionReport(transactionReport2)
  }

  private def testScanReport(scanReport: ScanReport): Unit = {
    val exception: Optional[String] = scanReport.getException().map(_.toString)
    val filter: Optional[String] = scanReport.getFilter.map(_.toString)
    val partitionPredicate: Optional[String] = scanReport.getPartitionPredicate().map(_.toString)
    val dataSkippingFilter: Optional[String] = scanReport.getDataSkippingFilter().map(_.toString)
    val scanMetrics = scanReport.getScanMetrics

    val expectedJson =
      s"""
         |{"tablePath":"${scanReport.getTablePath()}",
         |"operationType":"Scan",
         |"reportUUID":"${scanReport.getReportUUID()}",
         |"exception":${optionToString(exception)},
         |"tableVersion":${scanReport.getTableVersion()},
         |"tableSchema":"${scanReport.getTableSchema()}",
         |"snapshotReportUUID":"${scanReport.getSnapshotReportUUID}",
         |"filter":${optionToString(filter)},
         |"readSchema":"${scanReport.getReadSchema}",
         |"partitionPredicate":${optionToString(partitionPredicate)},
         |"dataSkippingFilter":${optionToString(dataSkippingFilter)},
         |"isFullyConsumed":${scanReport.getIsFullyConsumed},
         |"scanMetrics":{
         |"totalPlanningDurationNs":${scanMetrics.getTotalPlanningDurationNs},
         |"numAddFilesSeen":${scanMetrics.getNumAddFilesSeen},
         |"numAddFilesSeenFromDeltaFiles":${scanMetrics.getNumAddFilesSeenFromDeltaFiles},
         |"numActiveAddFiles":${scanMetrics.getNumActiveAddFiles},
         |"numDuplicateAddFiles":${scanMetrics.getNumDuplicateAddFiles},
         |"numRemoveFilesSeenFromDeltaFiles":${scanMetrics.getNumRemoveFilesSeenFromDeltaFiles}
         |}
         |}
         |""".stripMargin.replaceAll("\n", "")
    assert(expectedJson == MetricsReportSerializers.serializeScanReport(scanReport))
  }

  test("ScanReport serializer") {
    val snapshotReportUUID = UUID.randomUUID()
    val tableSchema = new StructType()
      .add("part", IntegerType.INTEGER)
      .add("id", IntegerType.INTEGER)
    val partitionPredicate = new Predicate(">", new Column("part"), Literal.ofInt(1))
    val exception = new RuntimeException("something something failed")

    // Initialize transaction metrics and record some values
    val scanMetrics = new ScanMetrics()
    scanMetrics.totalPlanningTimer.record(200)
    scanMetrics.addFilesCounter.increment(100)
    scanMetrics.addFilesFromDeltaFilesCounter.increment(90)
    scanMetrics.activeAddFilesCounter.increment(10)
    scanMetrics.removeFilesFromDeltaFilesCounter.increment(10)

    val scanReport1 = new ScanReportImpl(
      "/table/path",
      1,
      tableSchema,
      snapshotReportUUID,
      Optional.of(partitionPredicate),
      new StructType().add("id", IntegerType.INTEGER),
      Optional.of(partitionPredicate),
      Optional.empty(),
      true,
      scanMetrics,
      Optional.of(exception))

    // Manually check expected JSON
    val expectedJson =
      s"""
         |{"tablePath":"/table/path",
         |"operationType":"Scan",
         |"reportUUID":"${scanReport1.getReportUUID}",
         |"exception":"$exception",
         |"tableVersion":1,
         |"tableSchema":"struct(StructField(name=part,type=integer,nullable=true,metadata={}),
         | StructField(name=id,type=integer,nullable=true,metadata={}))",
         |"snapshotReportUUID":"$snapshotReportUUID",
         |"filter":"(column(`part`) > 1)",
         |"readSchema":"struct(StructField(name=id,type=integer,nullable=true,metadata={}))",
         |"partitionPredicate":"(column(`part`) > 1)",
         |"dataSkippingFilter":null,
         |"isFullyConsumed":true,
         |"scanMetrics":{
         |"totalPlanningDurationNs":200,
         |"numAddFilesSeen":100,
         |"numAddFilesSeenFromDeltaFiles":90,
         |"numActiveAddFiles":10,
         |"numDuplicateAddFiles":0,
         |"numRemoveFilesSeenFromDeltaFiles":10
         |}
         |}
         |""".stripMargin.replaceAll("\n", "")
    assert(expectedJson == MetricsReportSerializers.serializeScanReport(scanReport1))

    // Check with test function
    testScanReport(scanReport1)

    // Empty options for all possible fields (version, providedTimestamp and exception)
    val scanReport2 = new ScanReportImpl(
      "/table/path",
      1,
      tableSchema,
      snapshotReportUUID,
      Optional.empty(),
      tableSchema,
      Optional.empty(),
      Optional.empty(),
      false, // isFullyConsumed
      new ScanMetrics(),
      Optional.empty())
    testScanReport(scanReport2)
  }
}
