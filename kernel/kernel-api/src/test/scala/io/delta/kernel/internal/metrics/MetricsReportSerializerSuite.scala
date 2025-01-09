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

import java.util.Optional

import io.delta.kernel.metrics.SnapshotReport
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
      exception
    )

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
}
