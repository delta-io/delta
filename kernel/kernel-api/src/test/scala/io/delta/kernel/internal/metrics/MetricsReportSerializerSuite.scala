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

  def testSnapshotReport(snapshotReport: SnapshotReport): Unit = {
    val timestampToVersionResolutionDuration = optionToString(
      snapshotReport.snapshotMetrics().timestampToVersionResolutionDuration())
    val loadProtocolAndMetadataDuration =
      snapshotReport.snapshotMetrics().loadInitialDeltaActionsDuration()
    val exception: Optional[String] = snapshotReport.exception().map(_.toString)
    val expectedJson =
      s"""
         |{"tablePath":"${snapshotReport.tablePath()}",
         |"operationType":"Snapshot",
         |"reportUUID":"${snapshotReport.reportUUID()}",
         |"version":${optionToString(snapshotReport.version())},
         |"providedTimestamp":${optionToString(snapshotReport.providedTimestamp())},
         |"snapshotMetrics":{
         |"timestampToVersionResolutionDuration":${timestampToVersionResolutionDuration},
         |"loadProtocolAndMetadataDuration":${loadProtocolAndMetadataDuration}
         |},
         |"exception":${optionToString(exception)}
         |}
         |""".stripMargin.replaceAll("\n", "")
    assert(expectedJson == MetricsReportSerializers.serializeSnapshotReport(snapshotReport))
  }

  test("SnapshotReport serializer") {
    val snapshotMetrics1 = new SnapshotMetrics()
    snapshotMetrics1.timestampToVersionResolutionDuration.record(10)
    snapshotMetrics1.loadProtocolAndMetadataDuration.record(1000)
    val exception = new RuntimeException("something something failed")

    val snapshotReport1 = new SnapshotReportImpl(
      "/table/path",
      Optional.of(1),
      Optional.of(0),
      snapshotMetrics1,
      Optional.of(exception)
    )

    // Manually check expected JSON
    val expectedJson =
      s"""
        |{"tablePath":"/table/path",
        |"operationType":"Snapshot",
        |"reportUUID":"${snapshotReport1.reportUUID()}",
        |"version":1,
        |"providedTimestamp":0,
        |"snapshotMetrics":{
        |"timestampToVersionResolutionDuration":10,
        |"loadProtocolAndMetadataDuration":1000
        |},
        |"exception":"$exception"
        |}
        |""".stripMargin.replaceAll("\n", "")
    assert(expectedJson == MetricsReportSerializers.serializeSnapshotReport(snapshotReport1))

    // Check with test function
    testSnapshotReport(snapshotReport1)

    // Empty options for all possible fields
    val snapshotMetrics2 = new SnapshotMetrics()
    val snapshotReport2 = new SnapshotReportImpl(
      "/table/path",
      Optional.empty(),
      Optional.empty(),
      snapshotMetrics2,
      Optional.empty()
    )
    testSnapshotReport(snapshotReport2)
  }
}
