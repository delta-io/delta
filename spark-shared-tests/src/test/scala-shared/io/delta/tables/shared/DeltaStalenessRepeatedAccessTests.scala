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
 * Design-doc section [[2]] (repeated table access with external changes) exercised with a large
 * `stalenessLimit`.
 *
 * `stalenessLimit` is a V1 `DeltaLog` mechanism. On the V1 read path
 * (`deltaLog.update(stalenessAcceptable = true)`), an open window serves the cached snapshot and
 * refreshes asynchronously, so a read right after an external commit reproduces the doc's
 * "Connector w/ cache" column (stale data); `REFRESH TABLE` then forces a fresh read. The V2
 * Kernel connector (STRICT) ignores the window and always reads fresh (the "Connector w/o cache"
 * column), so under STRICT the first read already reflects the external change.
 *
 * Staleness is set inside each test, after the seed read, so the seed never schedules an async
 * refresh that the stale assertion could race against.
 */
trait DeltaStalenessRepeatedAccessTests
  extends DeltaTableRefreshSharedBase { self: AnyFunSuite =>

  /** STRICT forces the V2 connector, which ignores the staleness window and reads fresh. */
  private def v2IgnoresStaleness: Boolean = v2EnableMode == "STRICT"

  test("scenario 1 external staleness: stale read serves cached data until REFRESH") {
    withExternalTable { path =>
      externalDataWrite(path, Seq((2, 200)))
      setStalenessLimit("1h")
      if (v2IgnoresStaleness) {
        assertFinalTableState("t", Seq(Row(1, 100), Row(2, 200)))
      } else {
        // Within the window the cached snapshot is served, so the external row is not yet visible.
        assertFinalTableState("t", Seq(Row(1, 100)))
      }
      writerSql("REFRESH TABLE t")
      assertFinalTableState("t", Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("scenario 2 external staleness: stale read hides an external schema change until REFRESH") {
    withExternalTable { path =>
      externalAddColumnAndWrite(path, Seq((2, 200, -1)))
      setStalenessLimit("1h")
      if (v2IgnoresStaleness) {
        assertFinalTableState("t", Seq(Row(1, 100, null), Row(2, 200, -1)))
      } else {
        // The cached snapshot still carries the old two-column schema and lacks the new row.
        assertFinalTableState("t", Seq(Row(1, 100)))
      }
      writerSql("REFRESH TABLE t")
      assertFinalTableState("t", Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }

  test("scenario 3 external staleness: stale read hides an external drop/recreate until REFRESH") {
    withExternalTable { path =>
      externalDropAndRecreate(path)
      setStalenessLimit("1h")
      if (v2IgnoresStaleness) {
        assertFinalTableState("t", Seq.empty)
      } else {
        assertFinalTableState("t", Seq(Row(1, 100)))
      }
      writerSql("REFRESH TABLE t")
      assertFinalTableState("t", Seq.empty)
    }
  }
}
