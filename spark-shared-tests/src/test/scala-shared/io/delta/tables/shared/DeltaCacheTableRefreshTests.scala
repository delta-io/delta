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
 * Section [5]: CACHE TABLE impact on reads.
 *
 * Shared across classic and Connect. In classic, scenarios 1-4 use TRUE external
 * writes (LogStore, bypassing DeltaLog) so CACHE pins data; in Connect those scenarios
 * use same-JVM writes (the Connect client cannot bypass the shared server DeltaLog), so
 * the writes are visible. [[isConnect]] differentiates both the mechanism and the
 * expectation. Scenarios 6b/6c use true external (filesystem/LogStore) writes in both
 * modes and share identical expectations. Scenarios 6d/6e are classic only because
 * Connect's filesystem writes are non-deterministically discovered by the server.
 */
trait DeltaCacheTableRefreshTests extends DeltaTableRefreshSharedBase { self: AnyFunSuite =>

  // ---------------------------------------------------------------------------
  // Section [5]: CACHE TABLE impact on reads
  // ---------------------------------------------------------------------------

  test("[5] scenario 1: CACHE TABLE with concurrent writes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")
      checkAnswer(spark.sql("SELECT * FROM t"), Seq(Row(1, 100)))
      if (isConnect) {
        // Same-JVM write updates the shared DeltaLog, so the cache is bypassed.
        writerSql("INSERT INTO t VALUES (2, 200)")
        checkAnswer(spark.sql("SELECT * FROM t ORDER BY id"), Seq(Row(1, 100), Row(2, 200)))
        spark.sql("UNCACHE TABLE t")
      } else if (v2EnableMode == "STRICT") {
        assertExternalStrictConflict { externalDataWrite("t", Seq((2, 200))) }
        spark.sql("UNCACHE TABLE t")
      } else {
        // True external write bypasses DeltaLog; CACHE pins data (SPARK-54022).
        externalDataWrite("t", Seq((2, 200)))
        checkAnswer(spark.sql("SELECT * FROM t"), Seq(Row(1, 100)))
        spark.sql("UNCACHE TABLE t")
      }
    }
  }

  test("[5] scenario 2: session write invalidates cache then concurrent write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")
      checkAnswer(spark.sql("SELECT * FROM t"), Seq(Row(1, 100)))
      spark.sql("INSERT INTO t VALUES (2, 200)")
      if (isConnect) {
        writerSql("INSERT INTO t VALUES (3, 300)")
        checkAnswer(
          spark.sql("SELECT * FROM t ORDER BY id"),
          Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
        spark.sql("UNCACHE TABLE t")
      } else if (v2EnableMode == "STRICT") {
        assertExternalStrictConflict { externalDataWrite("t", Seq((3, 300))) }
        spark.sql("UNCACHE TABLE t")
      } else {
        externalDataWrite("t", Seq((3, 300)))
        checkAnswer(spark.sql("SELECT * FROM t ORDER BY id"), Seq(Row(1, 100), Row(2, 200)))
        spark.sql("UNCACHE TABLE t")
      }
    }
  }

  test("[5] scenario 3: schema change breaks cache") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")
      checkAnswer(spark.sql("SELECT * FROM t"), Seq(Row(1, 100)))
      if (isConnect) {
        writerSql("ALTER TABLE t ADD COLUMN new_column INT")
        writerSql("INSERT INTO t VALUES (2, 200, -1)")
        checkAnswer(
          spark.sql("SELECT * FROM t ORDER BY id"),
          Seq(Row(1, 100, null), Row(2, 200, -1)))
        spark.sql("UNCACHE TABLE t")
      } else if (v2EnableMode == "STRICT") {
        assertExternalStrictConflict { externalAddColumnAndWrite("t", Seq((2, 200, -1))) }
        spark.sql("UNCACHE TABLE t")
      } else {
        externalAddColumnAndWrite("t", Seq((2, 200, -1)))
        checkAnswer(
          spark.sql("SELECT * FROM t ORDER BY id"),
          Seq(Row(1, 100, null), Row(2, 200, -1)))
        spark.sql("UNCACHE TABLE t")
      }
    }
  }

  test("[5] scenario 4: session schema change with concurrent write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")
      checkAnswer(spark.sql("SELECT * FROM t"), Seq(Row(1, 100)))
      spark.sql("ALTER TABLE t ADD COLUMN new_column INT")
      if (isConnect) {
        writerSql("INSERT INTO t VALUES (2, 200, -1)")
        checkAnswer(
          spark.sql("SELECT * FROM t ORDER BY id"),
          Seq(Row(1, 100, null), Row(2, 200, -1)))
        spark.sql("UNCACHE TABLE t")
      } else if (v2EnableMode == "STRICT") {
        assertExternalStrictConflict { externalDataWriteWide("t", Seq((2, 200, -1))) }
        spark.sql("UNCACHE TABLE t")
      } else {
        externalDataWriteWide("t", Seq((2, 200, -1)))
        checkAnswer(
          spark.sql("SELECT * FROM t ORDER BY id"),
          Seq(Row(1, 100, null), Row(2, 200, -1)))
        spark.sql("UNCACHE TABLE t")
      }
    }
  }

  test("[5] scenario 5: drop and recreate table") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")
      checkAnswer(spark.sql("SELECT * FROM t"), Seq(Row(1, 100)))
      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")
      checkAnswer(spark.sql("SELECT * FROM t"), Seq.empty)
      spark.sql("UNCACHE TABLE IF EXISTS t")
    }
  }

  // ---------------------------------------------------------------------------
  // Section [5] continued: true external write simulation. Both modes use a write
  // that bypasses DeltaLog (classic LogStore, connect filesystem), so expectations
  // are identical: CACHE pins data against the external write.
  // ---------------------------------------------------------------------------

  test("[5] scenario 6b: CACHE TABLE pins data against external writes") {
    withRefreshTable { tableRef =>
      createSimpleTable(tableRef)
      insertInitialData(tableRef)
      val cacheName = if (isConnect) "cached_6b" else tableRef
      if (isConnect) {
        spark.sql(s"CACHE TABLE $cacheName AS SELECT * FROM $tableRef")
      } else {
        spark.sql(s"CACHE TABLE $cacheName")
      }
      checkAnswer(spark.sql(s"SELECT * FROM $cacheName"), Seq(Row(1, 100)))
      if (!isConnect && v2EnableMode == "STRICT") {
        assertExternalStrictConflict { externalDataWrite(tableRef, Seq((2, 200))) }
        spark.sql(s"UNCACHE TABLE IF EXISTS $cacheName")
      } else {
        externalDataWrite(tableRef, Seq((2, 200)))
        // CACHE pins data against the external write.
        checkAnswer(spark.sql(s"SELECT * FROM $cacheName"), Seq(Row(1, 100)))
        spark.sql(s"UNCACHE TABLE IF EXISTS $cacheName")
        // After uncaching, a fresh query discovers the external commit.
        checkAnswer(
          spark.sql(s"SELECT * FROM $tableRef ORDER BY id"),
          Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  test("[5] scenario 6c: session write invalidates, external not visible") {
    withRefreshTable { tableRef =>
      createSimpleTable(tableRef)
      insertInitialData(tableRef)
      val cacheName = if (isConnect) "cached_6c" else tableRef
      if (isConnect) {
        spark.sql(s"CACHE TABLE $cacheName AS SELECT * FROM $tableRef")
      } else {
        spark.sql(s"CACHE TABLE $cacheName")
      }
      checkAnswer(spark.sql(s"SELECT * FROM $cacheName"), Seq(Row(1, 100)))
      spark.sql(s"INSERT INTO $tableRef VALUES (2, 200)")
      if (!isConnect && v2EnableMode == "STRICT") {
        assertExternalStrictConflict { externalDataWrite(tableRef, Seq((3, 300))) }
        spark.sql(s"UNCACHE TABLE IF EXISTS $cacheName")
      } else {
        externalDataWrite(tableRef, Seq((3, 300)))
        // Session write visible, external not.
        checkAnswer(
          spark.sql(s"SELECT * FROM $cacheName ORDER BY id"),
          Seq(Row(1, 100), Row(2, 200)))
        spark.sql(s"UNCACHE TABLE IF EXISTS $cacheName")
        // After uncaching, a fresh query discovers all data including the external write.
        checkAnswer(
          spark.sql(s"SELECT * FROM $tableRef ORDER BY id"),
          Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
      }
    }
  }

  // Scenarios 6d/6e are classic only. In Connect, filesystem writes bypass the server
  // DeltaLog and its async discovery is non-deterministic for these cache scenarios.
  if (!isConnect) {
    test("[5] scenario 6d: external schema change with CACHE") {
      withRefreshTable { tableRef =>
        createSimpleTable(tableRef)
        insertInitialData(tableRef)
        spark.sql(s"CACHE TABLE $tableRef")
        checkAnswer(spark.sql(s"SELECT * FROM $tableRef"), Seq(Row(1, 100)))
        if (v2EnableMode == "STRICT") {
          assertExternalStrictConflict { externalAddColumnAndWrite(tableRef, Seq((2, 200, -1))) }
          spark.sql(s"UNCACHE TABLE IF EXISTS $tableRef")
        } else {
          externalAddColumnAndWrite(tableRef, Seq((2, 200, -1)))
          checkAnswer(
            spark.sql(s"SELECT * FROM $tableRef ORDER BY id"),
            Seq(Row(1, 100, null), Row(2, 200, -1)))
          spark.sql(s"UNCACHE TABLE IF EXISTS $tableRef")
          checkAnswer(
            spark.sql(s"SELECT * FROM $tableRef ORDER BY id"),
            Seq(Row(1, 100, null), Row(2, 200, -1)))
        }
      }
    }

    test("[5] scenario 6e: session schema change then external write") {
      withRefreshTable { tableRef =>
        createSimpleTable(tableRef)
        insertInitialData(tableRef)
        spark.sql(s"CACHE TABLE $tableRef")
        checkAnswer(spark.sql(s"SELECT * FROM $tableRef"), Seq(Row(1, 100)))
        spark.sql(s"ALTER TABLE $tableRef ADD COLUMN new_column INT")
        if (v2EnableMode == "STRICT") {
          assertExternalStrictConflict { externalDataWriteWide(tableRef, Seq((2, 200, -1))) }
          spark.sql(s"UNCACHE TABLE IF EXISTS $tableRef")
        } else {
          externalDataWriteWide(tableRef, Seq((2, 200, -1)))
          checkAnswer(
            spark.sql(s"SELECT * FROM $tableRef ORDER BY id"),
            Seq(Row(1, 100, null), Row(2, 200, -1)))
          spark.sql(s"UNCACHE TABLE IF EXISTS $tableRef")
          checkAnswer(
            spark.sql(s"SELECT * FROM $tableRef ORDER BY id"),
            Seq(Row(1, 100, null), Row(2, 200, -1)))
        }
      }
    }
  }
}
