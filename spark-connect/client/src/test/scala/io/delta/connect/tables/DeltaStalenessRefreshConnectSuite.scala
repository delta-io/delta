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

package io.delta.tables

import io.delta.tables.shared.{DeltaStalenessRepeatedAccessTests, DeltaStalenessTempViewTests}

import org.apache.spark.sql.test.DeltaQueryTest

/**
 * Runs the shared staleness tests over a remote Spark Connect session. The external `_delta_log`
 * commits are visible server-side because the Connect client and server share the filesystem, and
 * the staleness window is a server session conf set through `spark.conf.set`.
 */
trait DeltaStalenessRefreshConnectSuiteBase
  extends DeltaQueryTest
  with RemoteSparkSession
  with DeltaStalenessRepeatedAccessTests
  with DeltaStalenessTempViewTests {

  // The conf key is a literal because the Spark Connect thin client does not depend on Spark
  // Classic, which is where the Delta SQL configs live, so DeltaSQLConf.V2_ENABLE_MODE.key is
  // not importable here.
  override protected def serverConfigs: Map[String, String] =
    super.serverConfigs + ("spark.databricks.delta.v2.enableMode" -> v2EnableMode)
}

/**
 * V2_ENABLE_MODE = AUTO, the product default: the Delta Kernel V2 connector is used only where it
 * is supported and falls back to the legacy V1 path otherwise.
 */
class DeltaStalenessRefreshConnectAutoModeSuite
  extends DeltaStalenessRefreshConnectSuiteBase {
  override protected def v2EnableMode: String = "AUTO"
}

/**
 * V2_ENABLE_MODE = STRICT: always engages the Delta Kernel V2 connector with no V1 fallback. Unlike
 * AUTO, this surfaces gaps in the V2 connector instead of silently routing around them.
 */
class DeltaStalenessRefreshConnectStrictModeSuite
  extends DeltaStalenessRefreshConnectSuiteBase {
  override protected def v2EnableMode: String = "STRICT"
}
