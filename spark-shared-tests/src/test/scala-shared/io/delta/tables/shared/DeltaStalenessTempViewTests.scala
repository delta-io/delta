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
 * cached snapshot after an external commit (the doc's "Connector w/ cache" column). Because the
 * captured plan reuses its `DeltaLog` instance, `REFRESH TABLE` is effectively a no-op for it;
 * instead shrinking the window to "0s" forces the next access to refresh synchronously. The V2
 * Kernel connector (STRICT) ignores the window and reads fresh.
 *
 * Only the data-only scenario is covered here: it is identical across classic (which freezes the
 * captured plan) and Connect (which re-analyzes it) because no schema change is involved. The
 * schema-change and drop/recreate scenarios diverge between the two and are left as follow-up.
 */
trait DeltaStalenessTempViewTests
  extends DeltaTableRefreshSharedBase { self: AnyFunSuite =>

  /** STRICT forces the V2 connector, which ignores the staleness window and reads fresh. */
  private def v2IgnoresStaleness: Boolean = v2EnableMode == "STRICT"

  test("scenario 1 external staleness: stored-plan view stays stale until window closes") {
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
      // Shrinking the window forces a synchronous fresh read on the next access.
      setStalenessLimit("0s")
      assertFinalTableState("tmp", Seq(Row(1, 100), Row(2, 200)))
    }
  }
}
