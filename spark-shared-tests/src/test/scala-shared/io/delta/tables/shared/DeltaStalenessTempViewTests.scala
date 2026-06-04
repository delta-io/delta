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

package io.delta.tables.shared

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.Row

/**
 * Design-doc section [[1]] (temp views with a stored plan) exercised with a large `stalenessLimit`.
 *
 * The view captures a fully resolved Dataset plan whose scan re-reads the snapshot through the V1
 * `deltaLog.update(stalenessAcceptable = true)` path. With the window open, the view serves the
 * cached snapshot after an external commit (the doc's "Connector w/ cache" column). Because the
 * captured plan reuses its `DeltaLog` instance, `REFRESH TABLE` is effectively a no-op for it;
 * instead shrinking the window to "0s" forces the next access to refresh synchronously. The V2
 * Kernel connector (STRICT) ignores the window and reads fresh.
 *
 * Covered: external add-row (1.2), add-column (2.2), and drop/recreate (4.2). The add-column case
 * is mode-aware: V1 (AUTO) serves the stale snapshot and keeps the captured two-column projection
 * after REFRESH, while V2 (STRICT) re-resolves the captured plan and rejects the added column with
 * INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION. The external column-drop and
 * type-widening scenarios (3.2/5.2/6.2/7) need a column-mapping-aware external-drop helper and are
 * left as follow-up.
 */
trait DeltaStalenessTempViewTests
  extends DeltaTableRefreshSharedBase { self: AnyFunSuite =>

  /** STRICT forces the V2 connector, which ignores the staleness window and reads fresh. */
  private def v2IgnoresStaleness: Boolean = v2EnableMode == "STRICT"

  test("scenario 1 external staleness: stored-plan view stale read picks up data after REFRESH") {
    withExternalTable { path =>
      createStoredPlanView("tmp", "t", "salary < 999")
      // Baseline: the view sees the seeded row.
      assertFinalTableState("tmp", Seq(Row(1, 100)))
      externalDataWrite(path, Seq((2, 200)))
      setStalenessLimit("1h")
      if (v2IgnoresStaleness) {
        // The V2 connector ignores the window, so the view's scan reads fresh immediately.
        assertFinalTableState("tmp", Seq(Row(1, 100), Row(2, 200)))
      } else {
        // The view's scan serves the cached snapshot, so the external row is not yet visible.
        assertFinalTableState("tmp", Seq(Row(1, 100)))
      }
      writerSql("REFRESH TABLE t")
      assertFinalTableState("tmp", Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("scenario 2 external staleness: stored-plan view and an external add column") {
    withExternalTable { path =>
      createStoredPlanView("tmp", "t", "salary < 999")
      assertFinalTableState("tmp", Seq(Row(1, 100)))
      externalAddColumnAndWrite(path, Seq((2, 200, -1)))
      setStalenessLimit("1h")
      if (v2IgnoresStaleness) {
        // STRICT re-resolves the captured plan against the live table on each access and rejects
        // the externally added column instead of serving it.
        checkError(
          exception = intercept[SparkThrowable] {
            spark.sql("SELECT * FROM tmp").collect()
          },
          condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION")
      } else {
        // The cached snapshot still carries the old two-column schema and lacks the new row.
        assertFinalTableState("tmp", Seq(Row(1, 100)))
        writerSql("REFRESH TABLE t")
        // The captured plan keeps its two-column projection, so new_column is not surfaced.
        assertFinalTableState("tmp", Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  test("scenario 4 external staleness: stored-plan view and external drop/recreate") {
    withExternalTable { path =>
      createStoredPlanView("tmp", "t", "salary < 999")
      assertFinalTableState("tmp", Seq(Row(1, 100)))
      externalDropAndRecreate(path)
      setStalenessLimit("1h")
      // The recreate keeps the same columns, so no column-change validation trips; the table is
      // just empty. STRICT reads that fresh immediately; the V1 stale read still serves the cached
      // row until REFRESH.
      if (v2IgnoresStaleness) {
        assertFinalTableState("tmp", Seq.empty)
      } else {
        assertFinalTableState("tmp", Seq(Row(1, 100)))
        writerSql("REFRESH TABLE t")
        assertFinalTableState("tmp", Seq.empty)
      }
    }
  }
}
