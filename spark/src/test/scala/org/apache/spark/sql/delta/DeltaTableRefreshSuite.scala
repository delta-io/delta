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
 * Tests existing Delta behavior for repeated table access with external changes, mixing in
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
 * TODO: full V2 connector support is in progress. Repeated `sql()` access behaves the same as AUTO,
 * with one exception: ADD COLUMN is not supported in V2 yet, so an INSERT issued right after an
 * in-session ALTER TABLE ADD COLUMN still resolves against the schema cached at table lookup and
 * fails with an arity mismatch (see scenario 2's STRICT branch in
 * [[DeltaRepeatedAccessRefreshTests]]). Revisit once the connector refreshes its cached schema.
 */
class DeltaTableRefreshStrictModeSuite
  extends DeltaTableRefreshSuiteBase {
  override protected def v2EnableMode: String = "STRICT"
}
