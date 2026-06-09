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

import io.delta.tables.shared.{
  DeltaCacheTableTests, DeltaRepeatedAccessRefreshTests, DeltaTempViewStoredPlanRefreshTests}

import org.apache.spark.sql.test.DeltaQueryTest

/**
 * Runs the shared refresh and cache tests over a remote Spark Connect session. In Connect the
 * Dataset is re-analyzed on each execution, so repeated reads always see the latest data and
 * schema.
 */
trait DeltaTableRefreshConnectSuiteBase
  extends DeltaQueryTest
  with RemoteSparkSession
  with DeltaRepeatedAccessRefreshTests
  with DeltaCacheTableTests
  with DeltaTempViewStoredPlanRefreshTests {

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
class DeltaTableRefreshConnectAutoModeSuite
  extends DeltaTableRefreshConnectSuiteBase {
  override protected def v2EnableMode: String = "AUTO"
}

/**
 * V2_ENABLE_MODE = STRICT: always engages the Delta Kernel V2 connector with no V1 fallback. Unlike
 * AUTO, this surfaces gaps in the V2 connector instead of silently routing around them.
 *
 * TODO: full V2 connector support is in progress; ADD COLUMN is not supported in V2 yet.
 */
class DeltaTableRefreshConnectStrictModeSuite
  extends DeltaTableRefreshConnectSuiteBase {
  override protected def v2EnableMode: String = "STRICT"
}
