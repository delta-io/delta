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

import io.delta.tables.shared.DeltaRepeatedAccessRefreshTests
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests existing Delta behavior for repeated table access with external changes (Section [2] of
 * the "Refreshing and pinning tables in Spark" design doc), mixing in
 * [[DeltaRepeatedAccessRefreshTests]]. Concrete suites cover V2_ENABLE_MODE = AUTO and STRICT.
 */
trait DeltaTableRefreshSuiteBase
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeltaTableRefreshTestBase
  with DeltaRepeatedAccessRefreshTests {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaSQLConf.DELTA_ALTER_TABLE_DROP_COLUMN_ENABLED.key, "true")
      .set(DeltaSQLConf.V2_ENABLE_MODE.key, v2EnableMode)
  }
}

/** V2_ENABLE_MODE = AUTO (the product default). */
class DeltaTableRefreshAutoModeSuite
  extends DeltaTableRefreshSuiteBase {
  override protected def v2EnableMode: String = "AUTO"
}

/**
 * V2_ENABLE_MODE = STRICT, which engages the V2 Kernel connector path.
 *
 * TODO: full V2 connector support is still in progress. For repeated `sql()` access the current
 * behavior matches AUTO (the table is re-resolved on each access and reflects the latest
 * snapshot), except that an INSERT right after an in-session ADD COLUMN still resolves against
 * the schema cached at table lookup (see the STRICT branch in
 * [[DeltaRepeatedAccessRefreshTests]] scenario 2). Revisit if STRICT diverges further.
 */
class DeltaTableRefreshStrictModeSuite
  extends DeltaTableRefreshSuiteBase {
  override protected def v2EnableMode: String = "STRICT"
}
