/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.tables

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.DeltaQueryTest

/**
 * Section [3]: Incrementally constructed queries (Connect).
 * In Connect, Dataset is re-analyzed on each execution, so both df1 and df2
 * resolve to the latest table state (effectively a self-join).
 *
 * Without aliases, the join throws AMBIGUOUS_COLUMN_OR_FIELD because Delta's
 * V1 fallback (FallbackToV1DeltaRelation) loses PLAN_ID_TAG, preventing
 * Connect's plan-based column disambiguation. The aliased duplicates below
 * verify the actual data.
 */
trait DeltaJoinRefreshConnectTests {
  self: DeltaTableRefreshConnectTestBase with DeltaQueryTest with RemoteSparkSession =>

  // ---------------------------------------------------------------------------
  // Section [3]: Incrementally constructed queries (Connect)
  // In Connect, Dataset is re-analyzed on each execution, so both df1 and df2
  // resolve to the latest table state (effectively a self-join).
  //
  // Without aliases, the join throws AMBIGUOUS_COLUMN_OR_FIELD because Delta's
  // V1 fallback (FallbackToV1DeltaRelation) loses PLAN_ID_TAG, preventing
  // Connect's plan-based column disambiguation. The aliased duplicates below
  // verify the actual data.
  // ---------------------------------------------------------------------------

  test("[3] connect scenario 1 (no alias): self-join after write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("INSERT INTO t VALUES (2, 200)")

      val df2 = spark.table("t")

      val joined = df1.join(df2, df1("id") === df2("id"))
      if (ambiguousColumnFixed) {
        assert(joined.collect().length == 2)
      } else {
        checkError(
          exception = intercept[AnalysisException] { joined.collect() },
          condition = "AMBIGUOUS_COLUMN_OR_FIELD")
      }
    }
  }

  test("[3] connect scenario 2 (no alias): self-join after ADD COLUMN") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      val df2 = spark.table("t")

      val joined = df1.join(df2, df1("id") === df2("id"))
      if (ambiguousColumnFixed) {
        assert(joined.collect().length == 2)
      } else {
        checkError(
          exception = intercept[AnalysisException] { joined.collect() },
          condition = "AMBIGUOUS_COLUMN_OR_FIELD")
      }
    }
  }

  test("[3] connect scenario 3 (no alias): self-join after DROP COLUMN " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")

      val df2 = spark.table("t")

      val joined = df1.join(df2, df1("id") === df2("id"))
      if (ambiguousColumnFixed) {
        assert(joined.collect().length == 1)
      } else {
        checkError(
          exception = intercept[AnalysisException] { joined.collect() },
          condition = "AMBIGUOUS_COLUMN_OR_FIELD")
      }
    }
  }

  test("[3] connect scenario 4 (no alias): self-join after DROP/recreate " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

      val df2 = spark.table("t")

      val joined = df1.join(df2, df1("id") === df2("id"))
      if (ambiguousColumnFixed) {
        assert(joined.collect().length == 0)
      } else {
        checkError(
          exception = intercept[AnalysisException] { joined.collect() },
          condition = "AMBIGUOUS_COLUMN_OR_FIELD")
      }
    }
  }

  test("[3] connect scenario 4 (no alias): self-join after DROP/recreate " +
      "(no column mapping)") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      val df2 = spark.table("t")

      val joined = df1.join(df2, df1("id") === df2("id"))
      if (ambiguousColumnFixed) {
        assert(joined.collect().length == 0)
      } else {
        checkError(
          exception = intercept[AnalysisException] { joined.collect() },
          condition = "AMBIGUOUS_COLUMN_OR_FIELD")
      }
    }
  }

  test("[3] connect scenario 5 (no alias): self-join after DROP/ADD same type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")

      val df2 = spark.table("t")

      val joined = df1.join(df2, df1("id") === df2("id"))
      if (ambiguousColumnFixed) {
        assert(joined.collect().length == 1)
      } else {
        checkError(
          exception = intercept[AnalysisException] { joined.collect() },
          condition = "AMBIGUOUS_COLUMN_OR_FIELD")
      }
    }
  }

  test("[3] connect scenario 6 (no alias): self-join after DROP/ADD different type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")

      val df2 = spark.table("t")

      val joined = df1.join(df2, df1("id") === df2("id"))
      if (ambiguousColumnFixed) {
        assert(joined.collect().length == 1)
      } else {
        checkError(
          exception = intercept[AnalysisException] { joined.collect() },
          condition = "AMBIGUOUS_COLUMN_OR_FIELD")
      }
    }
  }

  test("[3] connect scenario 7 (no alias): self-join after type widening " +
      "(type widening)") {
    withTable("t") {
      spark.sql(
        """CREATE TABLE t (id INT, salary INT) USING delta
          |TBLPROPERTIES (
          |  'delta.columnMapping.mode' = 'name',
          |  'delta.enableTypeWidening' = 'true'
          |)""".stripMargin)
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t ALTER COLUMN salary TYPE BIGINT")

      val df2 = spark.table("t")

      val joined = df1.join(df2, df1("id") === df2("id"))
      if (ambiguousColumnFixed) {
        assert(joined.collect().length == 1)
      } else {
        checkError(
          exception = intercept[AnalysisException] { joined.collect() },
          condition = "AMBIGUOUS_COLUMN_OR_FIELD")
      }
    }
  }

  // Section [3] continued: SQL JOIN without column aliases.
  // On Spark < 4.2, the SQL equivalent of df1.join(df2) also throws
  // AMBIGUOUS_COLUMN_OR_FIELD because the output has duplicate column names.
  // On Spark 4.2+, this is fixed and the join succeeds.

  test("[3] connect scenario 1 (SQL JOIN no alias): self-join after write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      writerSql("INSERT INTO t VALUES (2, 200)")

      val sqlJoin = "SELECT * FROM t t1 JOIN t t2 ON t1.id = t2.id"
      if (ambiguousColumnFixed) {
        assert(spark.sql(sqlJoin).collect().length == 2)
      } else {
        checkError(
          exception = intercept[AnalysisException] { spark.sql(sqlJoin).collect() },
          condition = "AMBIGUOUS_COLUMN_OR_FIELD")
      }
    }
  }

  test("[3] connect scenario 2 (SQL JOIN no alias): self-join after ADD COLUMN") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      val sqlJoin = "SELECT * FROM t t1 JOIN t t2 ON t1.id = t2.id"
      if (ambiguousColumnFixed) {
        assert(spark.sql(sqlJoin).collect().length == 2)
      } else {
        checkError(
          exception = intercept[AnalysisException] { spark.sql(sqlJoin).collect() },
          condition = "AMBIGUOUS_COLUMN_OR_FIELD")
      }
    }
  }

  test("[3] connect scenario 3 (SQL JOIN no alias): self-join after DROP COLUMN " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")

      val sqlJoin = "SELECT * FROM t t1 JOIN t t2 ON t1.id = t2.id"
      if (ambiguousColumnFixed) {
        assert(spark.sql(sqlJoin).collect().length == 1)
      } else {
        checkError(
          exception = intercept[AnalysisException] { spark.sql(sqlJoin).collect() },
          condition = "AMBIGUOUS_COLUMN_OR_FIELD")
      }
    }
  }

  test("[3] connect scenario 4 (SQL JOIN no alias): self-join after DROP/recreate " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

      val sqlJoin = "SELECT * FROM t t1 JOIN t t2 ON t1.id = t2.id"
      if (ambiguousColumnFixed) {
        assert(spark.sql(sqlJoin).collect().length == 0)
      } else {
        checkError(
          exception = intercept[AnalysisException] { spark.sql(sqlJoin).collect() },
          condition = "AMBIGUOUS_COLUMN_OR_FIELD")
      }
    }
  }

  test("[3] connect scenario 4 (SQL JOIN no alias): self-join after DROP/recreate " +
      "(no column mapping)") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      val sqlJoin = "SELECT * FROM t t1 JOIN t t2 ON t1.id = t2.id"
      if (ambiguousColumnFixed) {
        assert(spark.sql(sqlJoin).collect().length == 0)
      } else {
        checkError(
          exception = intercept[AnalysisException] { spark.sql(sqlJoin).collect() },
          condition = "AMBIGUOUS_COLUMN_OR_FIELD")
      }
    }
  }

  test("[3] connect scenario 5 (SQL JOIN no alias): self-join after DROP/ADD same type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")

      val sqlJoin = "SELECT * FROM t t1 JOIN t t2 ON t1.id = t2.id"
      if (ambiguousColumnFixed) {
        assert(spark.sql(sqlJoin).collect().length == 1)
      } else {
        checkError(
          exception = intercept[AnalysisException] { spark.sql(sqlJoin).collect() },
          condition = "AMBIGUOUS_COLUMN_OR_FIELD")
      }
    }
  }

  test("[3] connect scenario 6 (SQL JOIN no alias): self-join after DROP/ADD different " +
      "type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")

      val sqlJoin = "SELECT * FROM t t1 JOIN t t2 ON t1.id = t2.id"
      if (ambiguousColumnFixed) {
        assert(spark.sql(sqlJoin).collect().length == 1)
      } else {
        checkError(
          exception = intercept[AnalysisException] { spark.sql(sqlJoin).collect() },
          condition = "AMBIGUOUS_COLUMN_OR_FIELD")
      }
    }
  }

  test("[3] connect scenario 7 (SQL JOIN no alias): self-join after type widening " +
      "(type widening)") {
    withTable("t") {
      spark.sql(
        """CREATE TABLE t (id INT, salary INT) USING delta
          |TBLPROPERTIES (
          |  'delta.columnMapping.mode' = 'name',
          |  'delta.enableTypeWidening' = 'true'
          |)""".stripMargin)
      insertInitialData("t")

      writerSql("ALTER TABLE t ALTER COLUMN salary TYPE BIGINT")

      val sqlJoin = "SELECT * FROM t t1 JOIN t t2 ON t1.id = t2.id"
      if (ambiguousColumnFixed) {
        assert(spark.sql(sqlJoin).collect().length == 1)
      } else {
        checkError(
          exception = intercept[AnalysisException] { spark.sql(sqlJoin).collect() },
          condition = "AMBIGUOUS_COLUMN_OR_FIELD")
      }
    }
  }

  // Section [3] continued: column-renamed duplicates that verify actual data.
  // Delta's V1 fallback loses PLAN_ID_TAG, so we rename columns to disambiguate
  // instead of using DataFrame aliases (.as("t1")) which still fail.

  test("[3] connect scenario 1 (renamed cols): join after write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("INSERT INTO t VALUES (2, 200)")

      val df2 = spark.table("t")

      // In Connect, both DataFrames re-analyze to the latest version.
      // Both scans see (1,100),(2,200). Self-join matches each row to itself.
      // Rename at join time to avoid AMBIGUOUS_COLUMN_OR_FIELD.
      val joined = df1.toDF("id1", "salary1")
        .join(df2.toDF("id2", "salary2"), col("id1") === col("id2"))
      checkAnswer(
        joined.orderBy("id1"),
        Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
    }
  }

  test("[3] connect scenario 2 (renamed cols): join after ADD COLUMN") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      val df2 = spark.table("t")

      // In Connect, both DataFrames re-analyze to the latest version and schema.
      // Both scans see the new schema (id, salary, new_column).
      // Rename at join time to match the post-change schema.
      val joined = df1.toDF("id1", "salary1", "nc1")
        .join(df2.toDF("id2", "salary2", "nc2"), col("id1") === col("id2"))
      checkAnswer(
        joined.orderBy("id1"),
        Seq(Row(1, 100, null, 1, 100, null), Row(2, 200, -1, 2, 200, -1)))
    }
  }

  test("[3] connect scenario 3 (renamed cols): join after DROP COLUMN (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")

      val df2 = spark.table("t")

      // In Connect, both DataFrames re-analyze to the latest version and schema.
      // Both scans see only (id) after the column drop.
      // Rename at join time to match the post-change schema.
      val joined = df1.toDF("id1")
        .join(df2.toDF("id2"), col("id1") === col("id2"))
      checkAnswer(
        joined.orderBy("id1"),
        Seq(Row(1, 1)))
    }
  }

  test("[3] connect scenario 4 (renamed cols): join after DROP/recreate (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

      val df2 = spark.table("t")

      // In Connect, both DataFrames re-analyze to the new empty table.
      val joined = df1.toDF("id1", "salary1")
        .join(df2.toDF("id2", "salary2"), col("id1") === col("id2"))
      checkAnswer(joined, Seq.empty)
    }
  }

  test("[3] connect scenario 4 (renamed cols): join after DROP/recreate (no column mapping)") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      val df2 = spark.table("t")

      // In Connect, both DataFrames re-analyze to the new empty table.
      val joined = df1.toDF("id1", "salary1")
        .join(df2.toDF("id2", "salary2"), col("id1") === col("id2"))
      checkAnswer(joined, Seq.empty)
    }
  }

  test("[3] connect scenario 5 (renamed cols): join after DROP/ADD same type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")

      val df2 = spark.table("t")

      // In Connect, both DataFrames re-analyze. The new salary column has no
      // data for existing rows (old salary data is gone).
      // Rename at join time to match the post-change schema.
      val joined = df1.toDF("id1", "salary1")
        .join(df2.toDF("id2", "salary2"), col("id1") === col("id2"))
      checkAnswer(
        joined.orderBy("id1"),
        Seq(Row(1, null, 1, null)))
    }
  }

  test("[3] connect scenario 6 (renamed cols): join after DROP/ADD different type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")

      val df2 = spark.table("t")

      // In Connect, both DataFrames re-analyze. Salary is now STRING type,
      // old salary data is gone.
      // Rename at join time to match the post-change schema.
      val joined = df1.toDF("id1", "salary1")
        .join(df2.toDF("id2", "salary2"), col("id1") === col("id2"))
      checkAnswer(
        joined.orderBy("id1"),
        Seq(Row(1, null, 1, null)))
    }
  }

  test("[3] connect scenario 7 (renamed cols): join after type widening (type widening)") {
    withTable("t") {
      spark.sql(
        """CREATE TABLE t (id INT, salary INT) USING delta
          |TBLPROPERTIES (
          |  'delta.columnMapping.mode' = 'name',
          |  'delta.enableTypeWidening' = 'true'
          |)""".stripMargin)
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t ALTER COLUMN salary TYPE BIGINT")

      val df2 = spark.table("t")

      // In Connect, both DataFrames re-analyze. Salary is BIGINT now,
      // original data is still readable.
      // Rename at join time to match the post-change schema.
      val joined = df1.toDF("id1", "salary1")
        .join(df2.toDF("id2", "salary2"), col("id1") === col("id2"))
      checkAnswer(
        joined.orderBy("id1"),
        Seq(Row(1, 100, 1, 100)))
    }
  }

  // Section [3] continued: SQL JOIN tests.
  // SQL queries are analyzed in one go on the server, so PLAN_ID_TAG loss
  // does not apply. These verify the design doc's expected data directly.

  test("[3] connect scenario 1 (SQL JOIN): join after write") {
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

  test("[3] connect scenario 2 (SQL JOIN): join after ADD COLUMN") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      checkAnswer(
        spark.sql(
          "SELECT t1.id AS id1, t1.salary AS salary1, t1.new_column AS nc1, " +
          "t2.id AS id2, t2.salary AS salary2, t2.new_column AS nc2 " +
          "FROM t t1 JOIN t t2 ON t1.id = t2.id ORDER BY id1"),
        Seq(Row(1, 100, null, 1, 100, null), Row(2, 200, -1, 2, 200, -1)))
    }
  }

  test("[3] connect scenario 3 (SQL JOIN): join after DROP COLUMN (column mapping)") {
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

  test("[3] connect scenario 4 (SQL JOIN): join after DROP/recreate (column mapping)") {
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

  test("[3] connect scenario 4 (SQL JOIN): join after DROP/recreate (no column mapping)") {
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

  test("[3] connect scenario 5 (SQL JOIN): join after DROP/ADD same type " +
      "(column mapping)") {
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

  test("[3] connect scenario 6 (SQL JOIN): join after DROP/ADD different type " +
      "(column mapping)") {
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

  test("[3] connect scenario 7 (SQL JOIN): join after type widening (type widening)") {
    withTable("t") {
      spark.sql(
        """CREATE TABLE t (id INT, salary INT) USING delta
          |TBLPROPERTIES (
          |  'delta.columnMapping.mode' = 'name',
          |  'delta.enableTypeWidening' = 'true'
          |)""".stripMargin)
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
