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

import io.delta.tables.shared.DeltaRepeatedAccessRefreshTests

import org.apache.spark.sql.test.DeltaQueryTest

/**
 * Spark Connect variant of the repeated table access refresh tests.
 *
 * Key behavioral differences from classic (local) mode:
 *   - In Connect, Dataset is re-analyzed on each execution, so repeated reads always see the
 *     latest data and schema.
 *
 * These tests document the "OSS Delta (connect)" column from the
 * "Refreshing and pinning tables in Spark" design doc.
 */
trait DeltaTableRefreshAndPinningConnectSuiteBase
  extends DeltaQueryTest with RemoteSparkSession
  with DeltaTableRefreshConnectTestBase
  with DeltaRepeatedAccessRefreshTests

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
 * V2_ENABLE_MODE = STRICT with Connect. STRICT engages the Delta Kernel V2 connector, set on the
 * server at startup via [[serverConfig]] (the connect analog of overriding sparkConf, since the
 * server runs in a separate JVM).
 *
 * TODO: full V2 connector support is still in progress. The current behavior matches the default
 * modes (repeated `sql()` access reflects the latest snapshot), so these suites assert the same
 * refresh behavior. Revisit if STRICT diverges.
 */
trait DeltaTableRefreshAndPinningConnectStrictModeBase
  extends DeltaTableRefreshAndPinningConnectSuiteBase {

  // Set STRICT on the server (startup config) and mirror it in the client-side field so the
  // shared trait's `v2EnableMode == "STRICT"` branch applies on the connect side too.
  override protected def v2EnableMode: String = "STRICT"

  override protected def serverConfig: Map[String, String] =
    super.serverConfig + ("spark.databricks.delta.v2.enableMode" -> "STRICT")
}

/** V2_ENABLE_MODE = STRICT with Connect, same-session writes. */
class DeltaTableRefreshAndPinningConnectStrictModeSuite
  extends DeltaTableRefreshAndPinningConnectStrictModeBase

/** V2_ENABLE_MODE = STRICT with Connect, external session writes. */
class DeltaTableRefreshAndPinningConnectStrictModeExternalSessionSuite
  extends DeltaTableRefreshAndPinningConnectStrictModeBase {
  override protected def useExternalSession: Boolean = true
}
