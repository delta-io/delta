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
 * Section [2]: Repeated table access with external changes.
 *
 * Shared across classic and Connect. Repeated `sql()` calls reflect both session and
 * external mutations (data writes, schema changes, drop/recreate). [[isConnect]]
 * differentiates the classic STRICT-mode branches.
 */
trait DeltaRepeatedAccessRefreshTests
  extends DeltaTableRefreshSharedBase { self: AnyFunSuite =>

  // ---------------------------------------------------------------------------
  // Section [2]: Repeated table access (session writes)
  // ---------------------------------------------------------------------------

  test("[2] scenario 1: repeated access picks up new data") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      checkAnswer(spark.sql("SELECT * FROM t"), Seq(Row(1, 100)))
      writerSql("INSERT INTO t VALUES (2, 200)")
      checkAnswer(spark.sql("SELECT * FROM t ORDER BY id"), Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[2] scenario 2: repeated access reflects schema changes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      checkAnswer(spark.sql("SELECT * FROM t"), Seq(Row(1, 100)))
      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      if (v2EnableMode == "STRICT") {
        assertArityMismatchError { writerSql("INSERT INTO t VALUES (2, 200, -1)") }
      } else {
        writerSql("INSERT INTO t VALUES (2, 200, -1)")
        checkAnswer(
          spark.sql("SELECT * FROM t ORDER BY id"),
          Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  test("[2] scenario 3: repeated access after drop and recreate") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      checkAnswer(spark.sql("SELECT * FROM t"), Seq(Row(1, 100)))
      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")
      checkAnswer(spark.sql("SELECT * FROM t"), Seq.empty)
    }
  }

  // ---------------------------------------------------------------------------
  // Section [2] external: Repeated table access with external modifications
  // ---------------------------------------------------------------------------

  test("[2] scenario 1 external: repeated access picks up external data") {
    withRefreshTable { tableRef =>
      createSimpleTable(tableRef)
      insertInitialData(tableRef)
      checkAnswer(spark.sql(s"SELECT * FROM $tableRef"), Seq(Row(1, 100)))
      withExternalWrite(externalDataWrite(tableRef, Seq((2, 200)))) {
        checkAnswer(
          spark.sql(s"SELECT * FROM $tableRef ORDER BY id"),
          Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  test("[2] scenario 2 external: repeated access reflects external schema change") {
    withRefreshTable { tableRef =>
      createSimpleTable(tableRef)
      insertInitialData(tableRef)
      checkAnswer(spark.sql(s"SELECT * FROM $tableRef"), Seq(Row(1, 100)))
      withExternalWrite(externalAddColumnAndWrite(tableRef, Seq((2, 200, -1)))) {
        checkAnswer(
          spark.sql(s"SELECT * FROM $tableRef ORDER BY id"),
          Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  test("[2] scenario 3 external: repeated access after external DROP and recreate") {
    withRefreshTable { tableRef =>
      createSimpleTable(tableRef)
      insertInitialData(tableRef)
      checkAnswer(spark.sql(s"SELECT * FROM $tableRef"), Seq(Row(1, 100)))
      withExternalWrite(externalDropAndRecreate(tableRef, columnMapping = false)) {
        checkAnswer(spark.sql(s"SELECT * FROM $tableRef"), Seq.empty)
      }
    }
  }
}
