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

import org.apache.spark.sql.{AnalysisException, Row}

/**
 * Repeated `sql()` calls reflect both session and external mutations (data writes, schema changes,
 * drop/recreate). Shared across classic and Connect.
 */
trait DeltaRepeatedAccessRefreshTests
  extends DeltaTableRefreshSharedBase { self: AnyFunSuite =>

  // Scenario 1: data changes via writes

  test("scenario 1: repeated sql() reflects session write") {
    withInitialTable { tableRef =>
      writerSql(s"INSERT INTO $tableRef VALUES (2, 200)")
      assertFinalTableState(tableRef, Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("scenario 1 external: repeated sql() reflects external write") {
    withExternalTable { path =>
      externalDataWrite(path, Seq((2, 200)))
      // The default synchronous staleness window re-resolves the snapshot on each read, so the
      // external commit is already visible before REFRESH; REFRESH then keeps it consistent.
      assertFinalTableState("t", Seq(Row(1, 100), Row(2, 200)))
      writerSql("REFRESH TABLE t")
      assertFinalTableState("t", Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // Scenario 2: schema changes

  test("scenario 2: repeated sql() reflects session schema change") {
    withInitialTable { tableRef =>
      writerSql(s"ALTER TABLE $tableRef ADD COLUMN new_column INT")
      // TODO: STRICT mode does not support schema evolution yet, so the wider INSERT fails with an
      // arity mismatch; the non-STRICT branch succeeds.
      if (v2EnableMode == "STRICT") {
        checkError(
          exception = intercept[AnalysisException] {
            writerSql(s"INSERT INTO $tableRef VALUES (2, 200, -1)")
          },
          condition = "INSERT_COLUMN_ARITY_MISMATCH")
      } else {
        writerSql(s"INSERT INTO $tableRef VALUES (2, 200, -1)")
        assertFinalTableState(tableRef, Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  test("scenario 2 external: repeated sql() reflects external schema change") {
    withExternalTable { path =>
      externalAddColumnAndWrite(path, Seq((2, 200, -1)))
      // The default synchronous staleness window re-resolves the snapshot on each read, so the
      // external schema change is already visible before REFRESH; REFRESH then keeps it consistent.
      assertFinalTableState("t", Seq(Row(1, 100, null), Row(2, 200, -1)))
      writerSql("REFRESH TABLE t")
      assertFinalTableState("t", Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }

  // Scenario 3: drop and recreate table

  test("scenario 3: repeated sql() reflects session drop/recreate") {
    withInitialTable { tableRef =>
      writerSql(s"DROP TABLE $tableRef")
      writerSql(s"CREATE TABLE $tableRef (id INT, salary INT) USING delta")
      assertFinalTableState(tableRef, Seq.empty)
    }
  }

  test("scenario 3 external: repeated sql() reflects external drop/recreate") {
    withExternalTable { path =>
      externalDropAndRecreate(path)
      // The default synchronous staleness window re-resolves the snapshot on each read, so the
      // external drop/recreate is already visible before REFRESH; REFRESH then keeps it consistent.
      assertFinalTableState("t", Seq.empty)
      writerSql("REFRESH TABLE t")
      assertFinalTableState("t", Seq.empty)
    }
  }
}
