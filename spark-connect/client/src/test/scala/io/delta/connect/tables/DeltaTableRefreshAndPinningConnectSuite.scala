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

package io.delta.tables

import io.delta.tables.shared.{DeltaCacheTableRefreshTests, DeltaJoinRefreshTests, DeltaRepeatedAccessRefreshTests, DeltaTempViewRefreshTests}

import org.apache.spark.SparkException
import org.apache.spark.sql.test.DeltaQueryTest

/**
 * Spark Connect variant of the table refresh and version pinning tests.
 *
 * Key behavioral differences from classic (local) mode:
 *   - In Connect, Dataset is re-analyzed on each execution, so collect() and show() behave
 *     the same: both always see the latest data and schema.
 *   - Temp views created from Dataset capture the plan, and in Connect temp views with stored
 *     plans behave the same as classic for column-mapping schema changes (they throw
 *     DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS).
 *
 * These tests document the "OSS Delta (connect)" column from the
 * "Refreshing and pinning tables in Spark" design doc.
 */
trait DeltaTableRefreshAndPinningConnectSuiteBase
  extends DeltaQueryTest with RemoteSparkSession
  with DeltaTableRefreshConnectTestBase
  with DeltaTempViewRefreshTests
  with DeltaRepeatedAccessRefreshTests
  with DeltaJoinRefreshTests
  with DeltaCacheTableRefreshTests

/** Same-session writes (default). */
class DeltaTableRefreshAndPinningConnectSuite
  extends DeltaTableRefreshAndPinningConnectSuiteBase

/**
 * Writes go through spark.newSession(). In Connect, this creates a new client session
 * to the same server. The server shares a single DeltaLog instance cache, so writes
 * from either session update the same snapshot. Verifies behavior is identical to
 * same-session writes. See trait scaladoc for details.
 */
class DeltaTableRefreshAndPinningConnectExternalSessionSuite
  extends DeltaTableRefreshAndPinningConnectSuiteBase {
  override protected def useExternalSession: Boolean = true
}

/**
 * V2_ENABLE_MODE = STRICT with Connect. STRICT engages the Delta Kernel V2 connector, which
 * needs delta-kernel-defaults (DefaultEngine). The Connect test server classpath cannot load it,
 * so STRICT cannot run over Connect today. We still register the suites (setting STRICT via the
 * server's startup config) and cancel each test with a clear reason, so the gap stays visible and
 * the suites will light up if the harness gains kernel support. Real (non DefaultEngine) failures
 * still surface.
 */
trait DeltaTableRefreshAndPinningConnectStrictModeBase
  extends DeltaTableRefreshAndPinningConnectSuiteBase {

  override protected def serverConfig: Map[String, String] =
    super.serverConfig + ("spark.databricks.delta.v2.enableMode" -> "STRICT")

  override def test(
      testName: String,
      testTags: org.scalatest.Tag*)(
      testFun: => Any)(implicit
      pos: org.scalactic.source.Position): Unit = {
    super.test(testName, testTags: _*) {
      try {
        testFun
      } catch {
        case e: SparkException if e.getMessage.contains("DefaultEngine") =>
          cancel("STRICT mode requires delta-kernel-defaults (DefaultEngine) on the Connect " +
            "server classpath, which is not available in the test harness.")
      }
    }(pos)
  }
}

/** V2_ENABLE_MODE = STRICT with Connect, same-session writes. */
class DeltaTableRefreshAndPinningConnectStrictModeSuite
  extends DeltaTableRefreshAndPinningConnectStrictModeBase

/** V2_ENABLE_MODE = STRICT with Connect, external session writes. */
class DeltaTableRefreshAndPinningConnectStrictModeExternalSessionSuite
  extends DeltaTableRefreshAndPinningConnectStrictModeBase {
  override protected def useExternalSession: Boolean = true
}
