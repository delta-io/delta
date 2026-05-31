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
 * Section [2]: Repeated table access with external changes. Shared across classic and Connect:
 * repeated `sql()` calls reflect both session and external mutations (data writes, schema
 * changes, drop/recreate). The one place STRICT diverges today (scenario 2 session write) is
 * asserted explicitly with a TODO.
 */
trait DeltaRepeatedAccessRefreshTests
  extends DeltaTableRefreshSharedBase { self: AnyFunSuite =>

  /** Creates the standard 2 column table, seeds it with `(1, 100)`, and asserts that first read. */
  private def withSeededTable(tableRef: String)(body: => Unit): Unit = {
    createSimpleTable(tableRef)
    insertInitialData(tableRef)
    checkAnswer(spark.sql(s"SELECT * FROM $tableRef"), Seq(Row(1, 100)))
    body
  }

  /** Asserts the full table contents (ordered by id) match `expectedRows`. */
  private def assertFinalTableState(tableRef: String, expectedRows: Seq[Row]): Unit =
    checkAnswer(spark.sql(s"SELECT * FROM $tableRef ORDER BY id"), expectedRows)

  // ---------------------------------------------------------------------------
  // Section [2]: Repeated table access (session writes)
  // ---------------------------------------------------------------------------

  test("[2] scenario 1: repeated access picks up new data") {
    withTable("t") {
      withSeededTable("t") {
        writerSql("INSERT INTO t VALUES (2, 200)")
        assertFinalTableState("t", Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  test("[2] scenario 2: repeated access reflects schema changes") {
    withTable("t") {
      withSeededTable("t") {
        writerSql("ALTER TABLE t ADD COLUMN new_column INT")
        // TODO: Under STRICT the V2 connector resolves the INSERT against the schema cached at
        // table lookup, so the just-added column is not yet visible and the 3 value INSERT fails
        // with an arity mismatch. Once the connector refreshes its cached schema this should match
        // the non-STRICT branch, where the additive INSERT succeeds and the new column reads back.
        if (v2EnableMode == "STRICT") {
          assertArityMismatchError { writerSql("INSERT INTO t VALUES (2, 200, -1)") }
        } else {
          writerSql("INSERT INTO t VALUES (2, 200, -1)")
          assertFinalTableState("t", Seq(Row(1, 100, null), Row(2, 200, -1)))
        }
      }
    }
  }

  test("[2] scenario 3: repeated access after drop and recreate") {
    withTable("t") {
      withSeededTable("t") {
        writerSql("DROP TABLE t")
        writerSql("CREATE TABLE t (id INT, salary INT) USING delta")
        assertFinalTableState("t", Seq.empty)
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Section [2] external: Repeated table access with external modifications
  // ---------------------------------------------------------------------------

  test("[2] scenario 1 external: repeated access picks up external data") {
    withRefreshTable { tableRef =>
      withSeededTable(tableRef) {
        externalDataWrite(tableRef, Seq((2, 200)))
        assertFinalTableState(tableRef, Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  test("[2] scenario 2 external: repeated access reflects external schema change") {
    withRefreshTable { tableRef =>
      withSeededTable(tableRef) {
        externalAddColumnAndWrite(tableRef, Seq((2, 200, -1)))
        assertFinalTableState(tableRef, Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  test("[2] scenario 3 external: repeated access after external DROP and recreate") {
    withRefreshTable { tableRef =>
      withSeededTable(tableRef) {
        externalDropAndRecreate(tableRef)
        assertFinalTableState(tableRef, Seq.empty)
      }
    }
  }
}
