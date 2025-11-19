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

package io.delta.unity

import io.delta.kernel.Operation
import io.delta.kernel.commit.PublishFailedException
import io.delta.kernel.test.MockFileSystemClientUtils
import io.delta.kernel.utils.CloseableIterable
import io.delta.unity.metrics.UcPublishTelemetry

import org.scalatest.funsuite.AnyFunSuite

class UcPublishTelemetrySuite
    extends AnyFunSuite
    with UCCatalogManagedTestUtils
    with MockFileSystemClientUtils {

  test("publish metrics for successful publish operations") {
    withTempDirAndEngine { case (tablePathUnresolved, _) =>
      // ===== GIVEN =====
      val reporter = new CapturingMetricsReporter[UcPublishTelemetry#Report]
      val engine = createEngineWithMetricsCapture(reporter)
      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)
      val (ucClient, ucCatalogManagedClient) = createUCClientAndCatalogManagedClient()

      val result0 = ucCatalogManagedClient
        .buildCreateTableTransaction("ucTableId", tablePath, testSchema, "test-engine")
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable())

      initializeUCTable(ucClient, "ucTableId")

      val resultV1 = result0.getPostCommitSnapshot.get()
        .buildUpdateTableTransaction("engineInfo", Operation.MANUAL_UPDATE)
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable())

      val resultV2 = resultV1.getPostCommitSnapshot.get()
        .buildUpdateTableTransaction("engineInfo", Operation.MANUAL_UPDATE)
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable())

      reporter.reports.clear()

      // ===== WHEN =====
      resultV1.getPostCommitSnapshot.get().publish(engine) // publishes 01.uuid.json -> 01.json
      resultV2.getPostCommitSnapshot.get().publish(engine) // publishes 02.uuid.json -> 02.json

      // ===== THEN =====
      assert(reporter.reports.size === 2)

      val firstPublish = reporter.reports(0)
      assert(firstPublish.operationType === "UcPublish")
      assert(firstPublish.ucTableId === "ucTableId")
      assert(firstPublish.snapshotVersion === 1)
      assert(firstPublish.numCommitsToPublish === 1)
      assert(firstPublish.metrics.numCommitsPublished === 1)
      assert(firstPublish.metrics.numCommitsAlreadyPublished === 0)

      val secondPublish = reporter.reports(1)
      assert(secondPublish.operationType === "UcPublish")
      assert(secondPublish.ucTableId === "ucTableId")
      assert(secondPublish.snapshotVersion === 2)
      assert(secondPublish.numCommitsToPublish === 2) // Both 01.uuid.json and 02.uuid.json
      assert(secondPublish.metrics.numCommitsPublished === 1) // Only 02.uuid.json
      assert(secondPublish.metrics.numCommitsAlreadyPublished === 1) // 01.uuid.json was already!
    }
  }

  test("JSON serialization: success report") {
    val telemetry = new UcPublishTelemetry("ucTableId", "ucTablePath", 5, 3)
    val collector = telemetry.getMetricsCollector
    collector.totalPublishTimer.record(500)
    collector.incrementCommitsPublished()
    collector.incrementCommitsPublished()
    collector.incrementCommitsAlreadyPublished()

    val report = telemetry.createSuccessReport()

    // scalastyle:off line.size.limit
    val expectedJson =
      s"""
         |{"operationType":"UcPublish",
         |"reportUUID":"${report.reportUUID}",
         |"ucTableId":"ucTableId",
         |"ucTablePath":"ucTablePath",
         |"snapshotVersion":5,
         |"numCommitsToPublish":3,
         |"metrics":{"totalPublishDurationNs":500,"numCommitsPublished":2,"numCommitsAlreadyPublished":1},
         |"exception":null}
         |""".stripMargin.replaceAll("\n", "")
    // scalastyle:on line.size.limit

    assert(report.toJson() === expectedJson)
  }

  test("JSON serialization: failure report") {
    val telemetry = new UcPublishTelemetry("ucTableId", "ucTablePath", 3, 2)
    val collector = telemetry.getMetricsCollector
    collector.totalPublishTimer.record(300)
    collector.incrementCommitsPublished()

    val exception = new PublishFailedException("Failed to publish")
    val report = telemetry.createFailureReport(exception)

    // scalastyle:off line.size.limit
    val expectedJson =
      s"""
         |{"operationType":"UcPublish",
         |"reportUUID":"${report.reportUUID}",
         |"ucTableId":"ucTableId",
         |"ucTablePath":"ucTablePath",
         |"snapshotVersion":3,
         |"numCommitsToPublish":2,
         |"metrics":{"totalPublishDurationNs":300,"numCommitsPublished":1,"numCommitsAlreadyPublished":0},
         |"exception":"io.delta.kernel.commit.PublishFailedException: Failed to publish"}
         |""".stripMargin.replaceAll("\n", "")
    // scalastyle:on line.size.limit

    assert(report.toJson() === expectedJson)
  }
}
