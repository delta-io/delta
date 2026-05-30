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
  with DeltaDatasetReanalysisConnectTests
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
 * V2_ENABLE_MODE = STRICT with Connect, same-session writes.
 *
 * Known failures: STRICT mode triggers the Delta Kernel V2 connector, which requires
 * DefaultEngine from delta-kernel-defaults. The Connect test server (RemoteSparkSession)
 * classpath does not include kernel jars, so all operations fail with ClassNotFoundException.
 */
class DeltaTableRefreshAndPinningConnectStrictModeSuite
  extends DeltaTableRefreshAndPinningConnectSuiteBase {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("SET spark.databricks.delta.v2.enableMode=STRICT")
  }

  // All tests fail because Connect server lacks delta-kernel-defaults jar.
  // Wrap every test to assert the expected ClassNotFoundException.
  override def test(
      testName: String,
      testTags: org.scalatest.Tag*)(
      testFun: => Any)(implicit
      pos: org.scalactic.source.Position): Unit = {
    super.test(testName, testTags: _*) {
      val exception = intercept[SparkException] { testFun }
      assert(exception.getMessage.contains("DefaultEngine"),
        s"Expected DefaultEngine error but got: ${exception.getMessage}")
    }(pos)
  }
}

/**
 * V2_ENABLE_MODE = STRICT with Connect, external session writes.
 *
 * Known failures: Same kernel jar classpath issue as
 * DeltaTableRefreshAndPinningConnectStrictModeSuite.
 */
class DeltaTableRefreshAndPinningConnectStrictModeExternalSessionSuite
  extends DeltaTableRefreshAndPinningConnectSuiteBase {
  override protected def useExternalSession: Boolean = true
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("SET spark.databricks.delta.v2.enableMode=STRICT")
  }

  // All tests fail because Connect server lacks delta-kernel-defaults jar.
  override def test(
      testName: String,
      testTags: org.scalatest.Tag*)(
      testFun: => Any)(implicit
      pos: org.scalactic.source.Position): Unit = {
    super.test(testName, testTags: _*) {
      val exception = intercept[SparkException] { testFun }
      assert(exception.getMessage.contains("DefaultEngine"),
        s"Expected DefaultEngine error but got: ${exception.getMessage}")
    }(pos)
  }
}
