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

import org.apache.spark.sql.{DataFrame, Row}

/**
 * Section [3]: Incrementally constructed queries (joins).
 *
 * Shared across classic and Connect. Classic captures each DataFrame's schema at
 * analysis time (df1 pins its schema), while Connect re-analyzes both join sides on
 * every execution (effectively a self-join on the latest table state). [[isConnect]]
 * differentiates the expectations.
 *
 * Each scenario uses a no-alias DataFrame self-join: classic resolves and verifies
 * data / detects schema change; Connect throws AMBIGUOUS_COLUMN_OR_FIELD (or succeeds
 * on Spark 4.2+) because Delta's V1 fallback loses PLAN_ID_TAG.
 */
trait DeltaJoinRefreshTests extends DeltaTableRefreshSharedBase { self: AnyFunSuite =>

  /**
   * Connect expectation for a no-alias self-join: AMBIGUOUS_COLUMN_OR_FIELD before Spark 4.2
   * (Delta's V1 fallback loses PLAN_ID_TAG), else the join succeeds with `expectedCount` rows.
   */
  private def checkDfJoinConnect(joined: DataFrame, expectedCount: Int): Unit = {
    if (!ambiguousColumnFixed) {
      assertAmbiguousColumnError { joined.collect() }
    } else {
      assert(joined.collect().length == expectedCount)
    }
  }

  // ---------------------------------------------------------------------------
  // Section [3]: no-alias DataFrame join
  // ---------------------------------------------------------------------------

  test("[3] scenario 1 (no alias): join after write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      val df1 = spark.table("t")
      writerSql("INSERT INTO t VALUES (2, 200)")
      val df2 = spark.table("t")
      val joined = df1.join(df2, df1("id") === df2("id"))
      if (isConnect) {
        checkDfJoinConnect(joined, 2)
      } else {
        checkAnswer(
          joined.select(df1("id"), df1("salary"), df2("id"), df2("salary"))
            .orderBy(df1("id")),
          Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
      }
    }
  }

  test("[3] scenario 2 (no alias): join after ADD COLUMN") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      val df1 = spark.table("t")
      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      if (v2EnableMode == "STRICT") {
        assertArityMismatchError { writerSql("INSERT INTO t VALUES (2, 200, -1)") }
      } else {
        writerSql("INSERT INTO t VALUES (2, 200, -1)")
        val df2 = spark.table("t")
        val joined = df1.join(df2, df1("id") === df2("id"))
        if (isConnect) {
          if (!ambiguousColumnFixed) {
            assertAmbiguousColumnError { joined.collect() }
          } else {
            assert(joined.collect().length == 2)
          }
        } else {
          checkAnswer(df1.orderBy("id"), Seq(Row(1, 100), Row(2, 200)))
          checkAnswer(df2.orderBy("id"), Seq(Row(1, 100, null), Row(2, 200, -1)))
          checkAnswer(
            joined.select(
              df1("id"), df1("salary"),
              df2("id"), df2("salary"), df2("new_column")).orderBy(df1("id")),
            Seq(Row(1, 100, 1, 100, null), Row(2, 200, 2, 200, -1)))
        }
      }
    }
  }

  test("[3] scenario 3 (no alias): join after DROP COLUMN (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")
      val df1 = spark.table("t")
      withColumnMappingDdl("ALTER TABLE t DROP COLUMN salary") {
        val df2 = spark.table("t")
        val joined = df1.join(df2, df1("id") === df2("id"))
        if (isConnect) checkDfJoinConnect(joined, 1)
        else assertSchemaChangeError { joined.collect() }
      }
    }
  }

  test("[3] scenario 4 (no alias): join after DROP/recreate (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")
      val df1 = spark.table("t")
      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
      val df2 = spark.table("t")
      val joined = df1.join(df2, df1("id") === df2("id"))
      if (isConnect) {
        checkDfJoinConnect(joined, 0)
      } else {
        assertSchemaChangeError { joined.collect() }
      }
    }
  }

  test("[3] scenario 4 (no alias): join after DROP/recreate (no column mapping)") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      val df1 = spark.table("t")
      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")
      val df2 = spark.table("t")
      val joined = df1.join(df2, df1("id") === df2("id"))
      if (isConnect) {
        checkDfJoinConnect(joined, 0)
      } else {
        checkAnswer(joined, Seq.empty)
      }
    }
  }

  test("[3] scenario 5 (no alias): join after DROP/ADD same type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")
      val df1 = spark.table("t")
      withColumnMappingDdl("ALTER TABLE t DROP COLUMN salary") {
        writerSql("ALTER TABLE t ADD COLUMN salary INT")
        val df2 = spark.table("t")
        val joined = df1.join(df2, df1("id") === df2("id"))
        if (isConnect) checkDfJoinConnect(joined, 1)
        else assertSchemaChangeError { joined.collect() }
      }
    }
  }

  test("[3] scenario 6 (no alias): join after DROP/ADD different type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")
      val df1 = spark.table("t")
      withColumnMappingDdl("ALTER TABLE t DROP COLUMN salary") {
        writerSql("ALTER TABLE t ADD COLUMN salary STRING")
        val df2 = spark.table("t")
        val joined = df1.join(df2, df1("id") === df2("id"))
        if (isConnect) checkDfJoinConnect(joined, 1)
        else assertSchemaChangeError { joined.collect() }
      }
    }
  }

  test("[3] scenario 7 (no alias): join after ALTER COLUMN TYPE INT to BIGINT " +
      "(type widening)") {
    withTable("t") {
      createTypeWideningTable("t")
      insertInitialData("t")
      val df1 = spark.table("t")
      withColumnMappingDdl("ALTER TABLE t ALTER COLUMN salary TYPE BIGINT") {
        val df2 = spark.table("t")
        val joined = df1.join(df2, df1("id") === df2("id"))
        if (isConnect) checkDfJoinConnect(joined, 1)
        else assertSchemaChangeError { joined.collect() }
      }
    }
  }
}
