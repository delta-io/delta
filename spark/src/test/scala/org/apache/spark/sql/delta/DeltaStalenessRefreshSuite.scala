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

import io.delta.tables.shared.{DeltaStalenessRepeatedAccessTests, DeltaStalenessTempViewTests}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/** Tests the read-staleness window against external changes (design-doc sections [1] and [2]). */
trait DeltaStalenessRefreshSuiteBase
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeltaStalenessRepeatedAccessTests
  with DeltaStalenessTempViewTests {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaSQLConf.DELTA_ALTER_TABLE_DROP_COLUMN_ENABLED.key, "true")
      .set(DeltaSQLConf.V2_ENABLE_MODE.key, v2EnableMode)
  }
}

/**
 * V2_ENABLE_MODE = AUTO, the product default: the Delta Kernel V2 connector is used only where it
 * is supported and falls back to the legacy V1 path otherwise.
 */
class DeltaStalenessRefreshAutoModeSuite
  extends DeltaStalenessRefreshSuiteBase {
  override protected def v2EnableMode: String = "AUTO"
}

/**
 * V2_ENABLE_MODE = STRICT: always engages the Delta Kernel V2 connector with no V1 fallback. Unlike
 * AUTO, this surfaces gaps in the V2 connector instead of silently routing around them.
 */
class DeltaStalenessRefreshStrictModeSuite
  extends DeltaStalenessRefreshSuiteBase {
  override protected def v2EnableMode: String = "STRICT"
}
