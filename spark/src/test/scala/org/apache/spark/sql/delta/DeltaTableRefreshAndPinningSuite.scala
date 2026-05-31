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
 * Tests that document and verify existing Delta behavior for repeated table access with
 * external changes. These tests cover scenarios from the
 * "Refreshing and pinning tables in Spark" design doc.
 *
 * The suite mixes in a single category trait:
 *   2. [[DeltaRepeatedAccessRefreshTests]]: Repeated table access with external changes
 *
 * The base trait is parameterized by:
 *   - V2_ENABLE_MODE (NONE, AUTO) for connector mode coverage
 *   - useExternalSession: when true, writes go through spark.newSession()
 *
 * Important notes on parameterization:
 *
 * External session (spark.newSession()): In a single JVM, all SparkSessions share the same
 * DeltaLog instance cache (a static Guava Cache on the DeltaLog companion object). When any
 * session commits a write, the DeltaLog.currentSnapshot is updated in place and all other
 * sessions immediately see the new version. This means spark.newSession() is NOT equivalent
 * to a true external writer (separate JVM/cluster). We parameterize with it anyway to verify
 * that the behavior is indeed identical, which documents that Delta's refresh mechanism is
 * driven by the shared DeltaLog, not by Spark's session-level catalog state.
 */
trait DeltaTableRefreshAndPinningSuiteBase
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

// ---------------------------------------------------------------------------
// Concrete test suites parameterized by V2 mode and session type
// ---------------------------------------------------------------------------

/** V2_ENABLE_MODE = NONE, same-session writes. */
class DeltaTableRefreshAndPinningSuite
  extends DeltaTableRefreshAndPinningSuiteBase {
  override protected def v2EnableMode: String = "NONE"
}

/** V2_ENABLE_MODE = AUTO (default), same-session writes. */
class DeltaTableRefreshAndPinningAutoModeSuite
  extends DeltaTableRefreshAndPinningSuiteBase {
  override protected def v2EnableMode: String = "AUTO"
}

/**
 * Writes go through spark.newSession(). Verifies that behavior is identical to
 * same-session writes because all sessions in a single JVM share the same DeltaLog
 * instance cache. The DeltaLog.currentSnapshot is updated in place by the writer,
 * so the reader always sees fresh data regardless of which session performed the write.
 */
class DeltaTableRefreshAndPinningExternalSessionSuite
  extends DeltaTableRefreshAndPinningSuiteBase {
  override protected def useExternalSession: Boolean = true
}

/**
 * AUTO mode with external session writes. Combines both parameterization axes to
 * verify the v2 kernel connector also behaves identically with newSession() writes.
 */
class DeltaTableRefreshAndPinningAutoModeExternalSessionSuite
  extends DeltaTableRefreshAndPinningSuiteBase {
  override protected def v2EnableMode: String = "AUTO"
  override protected def useExternalSession: Boolean = true
}

/**
 * V2_ENABLE_MODE = STRICT, same-session writes. STRICT engages the V2 Kernel connector path.
 *
 * TODO: full V2 connector support is still in progress. For repeated `sql()` access the current
 * behavior matches NONE/AUTO (the table is re-resolved on each access and reflects the latest
 * snapshot), so this suite asserts the same refresh behavior. Revisit if STRICT diverges.
 */
class DeltaTableRefreshAndPinningStrictModeSuite
  extends DeltaTableRefreshAndPinningSuiteBase {
  override protected def v2EnableMode: String = "STRICT"
}

/**
 * V2_ENABLE_MODE = STRICT with external session writes. Combines the STRICT V2 connector path
 * with spark.newSession() writes.
 *
 * TODO: full V2 connector support is still in progress. The current behavior matches the other
 * modes (repeated `sql()` access reflects the latest snapshot regardless of the writing session),
 * so this suite asserts the same refresh behavior. Revisit if STRICT diverges.
 */
class DeltaTableRefreshAndPinningStrictModeExternalSessionSuite
  extends DeltaTableRefreshAndPinningSuiteBase {
  override protected def v2EnableMode: String = "STRICT"
  override protected def useExternalSession: Boolean = true
}
