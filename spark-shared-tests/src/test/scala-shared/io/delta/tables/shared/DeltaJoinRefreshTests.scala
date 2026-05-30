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
import org.apache.spark.sql.functions.col

/**
 * Section [3]: Incrementally constructed queries (joins).
 *
 * Shared across classic and Connect. Classic captures each DataFrame's schema at
 * analysis time (df1 pins its schema), while Connect re-analyzes both join sides on
 * every execution (effectively a self-join on the latest table state). [[isConnect]]
 * differentiates the expectations.
 *
 * Four forms are exercised per scenario:
 *   - no-alias DataFrame join: classic resolves and verifies data / detects schema
 *     change; Connect throws AMBIGUOUS_COLUMN_OR_FIELD (or succeeds on Spark 4.2+)
 *     because Delta's V1 fallback loses PLAN_ID_TAG.
 *   - SQL JOIN without aliases: classic returns rows; Connect throws ambiguous
 *     (or succeeds on 4.2+). Both assert the same row count on the success path.
 *   - renamed-column DataFrame join: verifies actual data. Classic pins df1's schema,
 *     Connect re-analyzes both sides.
 *   - SQL JOIN with explicit aliases: a fresh SQL query in both modes, so results are
 *     identical (no pinning applies).
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
      if (!isConnect && v2EnableMode == "STRICT") {
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
      writerSql("ALTER TABLE t DROP COLUMN salary")
      val df2 = spark.table("t")
      val joined = df1.join(df2, df1("id") === df2("id"))
      if (isConnect) {
        checkDfJoinConnect(joined, 1)
      } else {
        assertSchemaChangeError { joined.collect() }
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
      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")
      val df2 = spark.table("t")
      val joined = df1.join(df2, df1("id") === df2("id"))
      if (isConnect) {
        checkDfJoinConnect(joined, 1)
      } else {
        assertSchemaChangeError { joined.collect() }
      }
    }
  }

  test("[3] scenario 6 (no alias): join after DROP/ADD different type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")
      val df1 = spark.table("t")
      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")
      val df2 = spark.table("t")
      val joined = df1.join(df2, df1("id") === df2("id"))
      if (isConnect) {
        checkDfJoinConnect(joined, 1)
      } else {
        assertSchemaChangeError { joined.collect() }
      }
    }
  }

  test("[3] scenario 7 (no alias): join after ALTER COLUMN TYPE INT to BIGINT " +
      "(type widening)") {
    withTable("t") {
      createTypeWideningTable("t")
      insertInitialData("t")
      val df1 = spark.table("t")
      writerSql("ALTER TABLE t ALTER COLUMN salary TYPE BIGINT")
      val df2 = spark.table("t")
      val joined = df1.join(df2, df1("id") === df2("id"))
      if (isConnect) {
        checkDfJoinConnect(joined, 1)
      } else {
        assertSchemaChangeError { joined.collect() }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Section [3]: SQL JOIN without aliases.
  // A fresh SQL self-join with duplicate output column names. Classic returns rows;
  // Connect throws AMBIGUOUS_COLUMN_OR_FIELD (Spark < 4.2) or succeeds (4.2+). Both
  // assert the same row count on the success path.
  // ---------------------------------------------------------------------------

  private val sqlJoinNoAlias = "SELECT * FROM t t1 JOIN t t2 ON t1.id = t2.id"

  private def checkSqlJoinNoAlias(expectedCount: Int): Unit = {
    if (isConnect && !ambiguousColumnFixed) {
      assertAmbiguousColumnError { spark.sql(sqlJoinNoAlias).collect() }
    } else {
      assert(spark.sql(sqlJoinNoAlias).collect().length == expectedCount)
    }
  }

  // (name, table setup run inside withTable("t"), expected self-join row count). Scenario 2
  // is registered separately below because it also exercises the classic STRICT arity path.
  private val sqlJoinNoAliasScenarios: Seq[(String, () => Unit, Int)] = Seq(
    ("scenario 1 (SQL JOIN no alias): join after write",
      () => {
        createSimpleTable("t"); insertInitialData("t")
        writerSql("INSERT INTO t VALUES (2, 200)")
      }, 2),
    ("scenario 3 (SQL JOIN no alias): join after DROP COLUMN (column mapping)",
      () => {
        createColumnMappingTable("t"); insertInitialData("t")
        writerSql("ALTER TABLE t DROP COLUMN salary")
      }, 1),
    ("scenario 4 (SQL JOIN no alias): join after DROP/recreate (column mapping)",
      () => {
        createColumnMappingTable("t"); insertInitialData("t")
        writerSql("DROP TABLE t")
        writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
          "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
      }, 0),
    ("scenario 4 (SQL JOIN no alias): join after DROP/recreate (no column mapping)",
      () => {
        createSimpleTable("t"); insertInitialData("t")
        writerSql("DROP TABLE t")
        writerSql("CREATE TABLE t (id INT, salary INT) USING delta")
      }, 0),
    ("scenario 5 (SQL JOIN no alias): join after DROP/ADD same type (column mapping)",
      () => {
        createColumnMappingTable("t"); insertInitialData("t")
        writerSql("ALTER TABLE t DROP COLUMN salary")
        writerSql("ALTER TABLE t ADD COLUMN salary INT")
      }, 1),
    ("scenario 6 (SQL JOIN no alias): join after DROP/ADD different type (column mapping)",
      () => {
        createColumnMappingTable("t"); insertInitialData("t")
        writerSql("ALTER TABLE t DROP COLUMN salary")
        writerSql("ALTER TABLE t ADD COLUMN salary STRING")
      }, 1),
    ("scenario 7 (SQL JOIN no alias): join after type widening (type widening)",
      () => {
        createTypeWideningTable("t"); insertInitialData("t")
        writerSql("ALTER TABLE t ALTER COLUMN salary TYPE BIGINT")
      }, 1))

  sqlJoinNoAliasScenarios.foreach { case (name, setup, expectedCount) =>
    test(s"[3] $name") {
      withTable("t") {
        setup()
        checkSqlJoinNoAlias(expectedCount)
      }
    }
  }

  test("[3] scenario 2 (SQL JOIN no alias): join after ADD COLUMN") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      if (!isConnect && v2EnableMode == "STRICT") {
        assertArityMismatchError { writerSql("INSERT INTO t VALUES (2, 200, -1)") }
      } else {
        writerSql("INSERT INTO t VALUES (2, 200, -1)")
        checkSqlJoinNoAlias(2)
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Section [3]: renamed-column DataFrame join (verifies actual data).
  // Renaming at join time avoids AMBIGUOUS_COLUMN_OR_FIELD in Connect. Classic pins
  // df1's analyzed schema; Connect re-analyzes both sides to the latest schema.
  // ---------------------------------------------------------------------------

  test("[3] scenario 1 (renamed cols): join after write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      val df1 = spark.table("t")
      writerSql("INSERT INTO t VALUES (2, 200)")
      val df2 = spark.table("t")
      val joined = df1.toDF("id1", "salary1")
        .join(df2.toDF("id2", "salary2"), col("id1") === col("id2"))
      checkAnswer(
        joined.orderBy("id1"),
        Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
    }
  }

  test("[3] scenario 2 (renamed cols): join after ADD COLUMN") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      val df1 = spark.table("t")
      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      if (!isConnect && v2EnableMode == "STRICT") {
        assertArityMismatchError { writerSql("INSERT INTO t VALUES (2, 200, -1)") }
      } else {
        writerSql("INSERT INTO t VALUES (2, 200, -1)")
        val df2 = spark.table("t")
        if (isConnect) {
          // Connect re-analyzes df1 to the new 3 column schema.
          val joined = df1.toDF("id1", "salary1", "nc1")
            .join(df2.toDF("id2", "salary2", "nc2"), col("id1") === col("id2"))
          checkAnswer(
            joined.orderBy("id1"),
            Seq(Row(1, 100, null, 1, 100, null), Row(2, 200, -1, 2, 200, -1)))
        } else {
          // Classic pins df1 to its analyzed 2 column schema.
          val joined = df1.toDF("id1", "salary1")
            .join(df2.toDF("id2", "salary2", "nc2"), col("id1") === col("id2"))
          checkAnswer(
            joined.orderBy("id1"),
            Seq(Row(1, 100, 1, 100, null), Row(2, 200, 2, 200, -1)))
        }
      }
    }
  }

  test("[3] scenario 3 (renamed cols): join after DROP COLUMN (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")
      val df1 = spark.table("t")
      writerSql("ALTER TABLE t DROP COLUMN salary")
      val df2 = spark.table("t")
      if (isConnect) {
        val joined = df1.toDF("id1")
          .join(df2.toDF("id2"), col("id1") === col("id2"))
        checkAnswer(joined.orderBy("id1"), Seq(Row(1, 1)))
      } else {
        // Classic pins df1's (id, salary) schema; the column mapping drop is detected.
        val joined = df1.toDF("id1", "salary1")
          .join(df2.toDF("id2"), col("id1") === col("id2"))
        assertSchemaChangeError { joined.collect() }
      }
    }
  }

  test("[3] scenario 4 (renamed cols): join after DROP/recreate (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")
      val df1 = spark.table("t")
      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
      val df2 = spark.table("t")
      val joined = df1.toDF("id1", "salary1")
        .join(df2.toDF("id2", "salary2"), col("id1") === col("id2"))
      if (isConnect) {
        checkAnswer(joined, Seq.empty)
      } else {
        assertSchemaChangeError { joined.collect() }
      }
    }
  }

  test("[3] scenario 4 (renamed cols): join after DROP/recreate (no column mapping)") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      val df1 = spark.table("t")
      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")
      val df2 = spark.table("t")
      val joined = df1.toDF("id1", "salary1")
        .join(df2.toDF("id2", "salary2"), col("id1") === col("id2"))
      checkAnswer(joined, Seq.empty)
    }
  }

  test("[3] scenario 5 (renamed cols): join after DROP/ADD same type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")
      val df1 = spark.table("t")
      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")
      val df2 = spark.table("t")
      val joined = df1.toDF("id1", "salary1")
        .join(df2.toDF("id2", "salary2"), col("id1") === col("id2"))
      if (isConnect) {
        checkAnswer(joined.orderBy("id1"), Seq(Row(1, null, 1, null)))
      } else {
        assertSchemaChangeError { joined.collect() }
      }
    }
  }

  test("[3] scenario 6 (renamed cols): join after DROP/ADD different type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")
      val df1 = spark.table("t")
      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")
      val df2 = spark.table("t")
      val joined = df1.toDF("id1", "salary1")
        .join(df2.toDF("id2", "salary2"), col("id1") === col("id2"))
      if (isConnect) {
        checkAnswer(joined.orderBy("id1"), Seq(Row(1, null, 1, null)))
      } else {
        assertSchemaChangeError { joined.collect() }
      }
    }
  }

  test("[3] scenario 7 (renamed cols): join after type widening (type widening)") {
    withTable("t") {
      createTypeWideningTable("t")
      insertInitialData("t")
      val df1 = spark.table("t")
      writerSql("ALTER TABLE t ALTER COLUMN salary TYPE BIGINT")
      val df2 = spark.table("t")
      val joined = df1.toDF("id1", "salary1")
        .join(df2.toDF("id2", "salary2"), col("id1") === col("id2"))
      if (isConnect) {
        checkAnswer(joined.orderBy("id1"), Seq(Row(1, 100, 1, 100)))
      } else {
        assertSchemaChangeError { joined.collect() }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Section [3]: SQL JOIN with explicit column aliases.
  // A fresh SQL query in both modes (no pinning), so the results are identical.
  // ---------------------------------------------------------------------------

  test("[3] scenario 1 (SQL JOIN): join after write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      writerSql("INSERT INTO t VALUES (2, 200)")
      checkAnswer(
        spark.sql(
          "SELECT t1.id AS id1, t1.salary AS salary1, " +
          "t2.id AS id2, t2.salary AS salary2 " +
          "FROM t t1 JOIN t t2 ON t1.id = t2.id ORDER BY id1"),
        Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
    }
  }

  test("[3] scenario 2 (SQL JOIN): join after ADD COLUMN") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      if (!isConnect && v2EnableMode == "STRICT") {
        assertArityMismatchError { writerSql("INSERT INTO t VALUES (2, 200, -1)") }
      } else {
        writerSql("INSERT INTO t VALUES (2, 200, -1)")
        checkAnswer(
          spark.sql(
            "SELECT t1.id AS id1, t1.salary AS salary1, t1.new_column AS nc1, " +
            "t2.id AS id2, t2.salary AS salary2, t2.new_column AS nc2 " +
            "FROM t t1 JOIN t t2 ON t1.id = t2.id ORDER BY id1"),
          Seq(Row(1, 100, null, 1, 100, null), Row(2, 200, -1, 2, 200, -1)))
      }
    }
  }

  test("[3] scenario 3 (SQL JOIN): join after DROP COLUMN (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")
      writerSql("ALTER TABLE t DROP COLUMN salary")
      checkAnswer(
        spark.sql(
          "SELECT t1.id AS id1, t2.id AS id2 " +
          "FROM t t1 JOIN t t2 ON t1.id = t2.id ORDER BY id1"),
        Seq(Row(1, 1)))
    }
  }

  test("[3] scenario 4 (SQL JOIN): join after DROP/recreate (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")
      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
      checkAnswer(
        spark.sql(
          "SELECT t1.id AS id1, t1.salary AS salary1, " +
          "t2.id AS id2, t2.salary AS salary2 " +
          "FROM t t1 JOIN t t2 ON t1.id = t2.id"),
        Seq.empty)
    }
  }

  test("[3] scenario 4 (SQL JOIN): join after DROP/recreate (no column mapping)") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")
      checkAnswer(
        spark.sql(
          "SELECT t1.id AS id1, t1.salary AS salary1, " +
          "t2.id AS id2, t2.salary AS salary2 " +
          "FROM t t1 JOIN t t2 ON t1.id = t2.id"),
        Seq.empty)
    }
  }

  test("[3] scenario 5 (SQL JOIN): join after DROP/ADD same type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")
      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")
      checkAnswer(
        spark.sql(
          "SELECT t1.id AS id1, t1.salary AS salary1, " +
          "t2.id AS id2, t2.salary AS salary2 " +
          "FROM t t1 JOIN t t2 ON t1.id = t2.id ORDER BY id1"),
        Seq(Row(1, null, 1, null)))
    }
  }

  test("[3] scenario 6 (SQL JOIN): join after DROP/ADD different type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")
      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")
      checkAnswer(
        spark.sql(
          "SELECT t1.id AS id1, t1.salary AS salary1, " +
          "t2.id AS id2, t2.salary AS salary2 " +
          "FROM t t1 JOIN t t2 ON t1.id = t2.id ORDER BY id1"),
        Seq(Row(1, null, 1, null)))
    }
  }

  test("[3] scenario 7 (SQL JOIN): join after type widening (type widening)") {
    withTable("t") {
      createTypeWideningTable("t")
      insertInitialData("t")
      writerSql("ALTER TABLE t ALTER COLUMN salary TYPE BIGINT")
      checkAnswer(
        spark.sql(
          "SELECT t1.id AS id1, t1.salary AS salary1, " +
          "t2.id AS id2, t2.salary AS salary2 " +
          "FROM t t1 JOIN t t2 ON t1.id = t2.id ORDER BY id1"),
        Seq(Row(1, 100, 1, 100)))
    }
  }
}
