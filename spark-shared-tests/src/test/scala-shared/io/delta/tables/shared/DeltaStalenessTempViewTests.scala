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

import org.apache.spark.sql.Row

/**
 * Design-doc section [[1]] (temp views with a stored plan) exercised with a large `stalenessLimit`.
 *
 * The view captures a fully resolved Dataset plan whose scan re-reads the snapshot through the V1
 * `deltaLog.update(stalenessAcceptable = true)` path. With the window open, the view serves the
 * cached snapshot after an external commit (the doc's "Connector w/ cache" column), then picks up
 * the change after REFRESH TABLE.
 *
 * These tests assert only the version-stable V1 (AUTO) path. The V2 (STRICT) stored-plan-view
 * behavior is exactly what the design doc is still defining and it varies across Spark versions and
 * between classic and Connect (4.0 leaves the captured plan an orphan that never refreshes, 4.1
 * re-resolves and may reject schema changes, 4.2 changed that again), so STRICT is canceled here
 * and tracked separately. The external column-drop and type-widening scenarios (3.2/5.2/6.2/7) need
 * a column-mapping-aware external-drop helper and are left as follow-up.
 */
trait DeltaStalenessTempViewTests
  extends DeltaTableRefreshSharedBase { self: AnyFunSuite =>

  /** STRICT (V2) stored-plan-view behavior is version/mode-dependent, so it is not asserted. */
  private def cancelUnderV2(): Unit =
    if (v2EnableMode == "STRICT") {
      cancel("V2 stored-plan-view refresh behavior is Spark-version-dependent")
    }

  test("scenario 1 external staleness: stored-plan view stale read picks up data after REFRESH") {
    cancelUnderV2()
    withExternalTable { path =>
      createStoredPlanView("tmp", "t", "salary < 999")
      // Baseline: the view sees the seeded row.
      assertFinalTableState("tmp", Seq(Row(1, 100)))
      externalDataWrite(path, Seq((2, 200)))
      setStalenessLimit("1h")
      // The view's scan serves the cached snapshot, so the external row is not yet visible.
      assertFinalTableState("tmp", Seq(Row(1, 100)))
      writerSql("REFRESH TABLE t")
      assertFinalTableState("tmp", Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("scenario 2 external staleness: stored-plan view and an external add column") {
    cancelUnderV2()
    withExternalTable { path =>
      createStoredPlanView("tmp", "t", "salary < 999")
      assertFinalTableState("tmp", Seq(Row(1, 100)))
      externalAddColumnAndWrite(path, Seq((2, 200, -1)))
      setStalenessLimit("1h")
      // The cached snapshot still carries the old two-column schema and lacks the new row.
      assertFinalTableState("tmp", Seq(Row(1, 100)))
      writerSql("REFRESH TABLE t")
      // The captured plan keeps its two-column projection, so new_column is not surfaced.
      assertFinalTableState("tmp", Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("scenario 4 external staleness: stored-plan view and external drop/recreate") {
    cancelUnderV2()
    withExternalTable { path =>
      createStoredPlanView("tmp", "t", "salary < 999")
      assertFinalTableState("tmp", Seq(Row(1, 100)))
      externalDropAndRecreate(path)
      setStalenessLimit("1h")
      // The recreate keeps the same columns, so the table is just empty. The V1 stale read still
      // serves the cached row until REFRESH.
      assertFinalTableState("tmp", Seq(Row(1, 100)))
      writerSql("REFRESH TABLE t")
      assertFinalTableState("tmp", Seq.empty)
    }
  }
}
