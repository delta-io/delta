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

// The conf key is a literal because the connect client test module does not depend on delta-spark,
// so DeltaSQLConf.V2_ENABLE_MODE.key is not importable here.

/**
 * Spark Connect base for the repeated table access refresh tests. In Connect the Dataset is
 * re-analyzed on each execution, so repeated reads always see the latest data and schema.
 * Concrete suites cover V2_ENABLE_MODE = AUTO and STRICT, set on the server at startup via
 * [[serverConfig]] (the connect analog of sparkConf, since the server runs in a separate JVM).
 */
trait DeltaTableRefreshConnectSuiteBase
  extends DeltaQueryTest with RemoteSparkSession
  with DeltaTableRefreshConnectTestBase
  with DeltaRepeatedAccessRefreshTests

/** V2_ENABLE_MODE = AUTO (the product default). */
class DeltaTableRefreshConnectAutoModeSuite
  extends DeltaTableRefreshConnectSuiteBase {
  override protected def v2EnableMode: String = "AUTO"

  override protected def serverConfig: Map[String, String] =
    super.serverConfig + ("spark.databricks.delta.v2.enableMode" -> "AUTO")
}

/**
 * V2_ENABLE_MODE = STRICT with Connect, which engages the Delta Kernel V2 connector. The mode is
 * set on the server and mirrored in the client-side `v2EnableMode` field that drives the shared
 * trait's STRICT branch.
 *
 * TODO: full V2 connector support is in progress. The behavior currently matches AUTO, except that
 * an INSERT right after an in-session ADD COLUMN resolves against the schema cached at table lookup
 * (see scenario 2's STRICT branch in [[DeltaRepeatedAccessRefreshTests]]). Revisit if STRICT
 * diverges further.
 */
class DeltaTableRefreshConnectStrictModeSuite
  extends DeltaTableRefreshConnectSuiteBase {
  override protected def v2EnableMode: String = "STRICT"

  override protected def serverConfig: Map[String, String] =
    super.serverConfig + ("spark.databricks.delta.v2.enableMode" -> "STRICT")
}
