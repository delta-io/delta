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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for INSERT ... REPLACE WHERE with targetAlias option.
 */
class DeltaInsertReplaceWhereTargetAliasSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  import testImplicits._

  var testTableName: String = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    testTableName = s"testTable_${java.util.UUID.randomUUID()}".replace("-", "_")
  }

  // Basic alias resolution: t.col resolves to col, compound conditions, mergeSchema.
  test("replaceWhere with targetAlias - t.col resolves the same as col") {
    withTable(testTableName) {
      Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "data")
        .write.format("delta").mode("overwrite").saveAsTable(testTableName)

      Seq((1, "updated")).toDF("id", "data")
        .write
        .format("delta")
        .mode("overwrite")
        .option("targetAlias", "t")
        .option("replaceWhere", "t.id = 1")
        .saveAsTable(testTableName)

      checkAnswer(
        spark.table(testTableName).orderBy("id"),
        Seq(Row(1, "updated"), Row(2, "b"), Row(3, "c")))
    }
  }

  test("replaceWhere with targetAlias - compound condition using alias") {
    withTable(testTableName) {
      Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")).toDF("id", "data")
        .write.format("delta").mode("overwrite").saveAsTable(testTableName)

      Seq((2, "new2"), (3, "new3")).toDF("id", "data")
        .write
        .format("delta")
        .mode("overwrite")
        .option("targetAlias", "t")
        .option("replaceWhere", "t.id >= 2 AND t.id <= 3")
        .saveAsTable(testTableName)

      checkAnswer(
        spark.table(testTableName).orderBy("id"),
        Seq(Row(1, "a"), Row(2, "new2"), Row(3, "new3"), Row(4, "d"), Row(5, "e")))
    }
  }

  test("replaceWhere with targetAlias and mergeSchema adds new columns") {
    withTable(testTableName) {
      Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "data")
        .write.format("delta").mode("overwrite").saveAsTable(testTableName)

      Seq((1, "updated", 100)).toDF("id", "data", "extra")
        .write
        .format("delta")
        .mode("overwrite")
        .option("targetAlias", "t")
        .option("replaceWhere", "t.id = 1")
        .option("mergeSchema", "true")
        .saveAsTable(testTableName)

      checkAnswer(
        spark.table(testTableName).orderBy("id"),
        Seq(Row(1, "updated", 100), Row(2, "b", null), Row(3, "c", null)))
    }
  }

  // Partition vs data predicate classification with alias.
  test("replaceWhere with targetAlias - alias on partition column") {
    withTable(testTableName) {
      Seq((1, "p1", "a"), (2, "p1", "b"), (3, "p2", "c")).toDF("id", "part", "data")
        .write
        .format("delta")
        .partitionBy("part")
        .saveAsTable(testTableName)

      Seq((1, "p1", "updated")).toDF("id", "part", "data")
        .write
        .format("delta")
        .mode("overwrite")
        .option("targetAlias", "t")
        .option("replaceWhere", "t.part = 'p1'")
        .saveAsTable(testTableName)

      checkAnswer(
        spark.table(testTableName).orderBy("id"),
        Seq(Row(1, "p1", "updated"), Row(3, "p2", "c")))
    }
  }

  test("replaceWhere with targetAlias - metadata-only path (dataColumnsEnabled=false)") {
    // With REPLACEWHERE_DATACOLUMNS_ENABLED=false, only partition predicates are allowed.
    // "t.part = 'p1'" is stripped to "part = 'p1'".
    withSQLConf(
        DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> "false") {
      withTable(testTableName) {
        Seq((1, "p1", "a"), (2, "p1", "b"), (3, "p2", "c"))
          .toDF("id", "part", "data")
          .write.format("delta").partitionBy("part").saveAsTable(testTableName)

        Seq((1, "p1", "updated"), (5, "p1", "new")).toDF("id", "part", "data")
          .write
          .format("delta")
          .mode("overwrite")
          .option("targetAlias", "t")
          .option("replaceWhere", "t.part = 'p1'")
          .saveAsTable(testTableName)

        checkAnswer(
          spark.table(testTableName).orderBy("id"),
          Seq(
            Row(1, "p1", "updated"),
            Row(3, "p2", "c"),
            Row(5, "p1", "new")))
      }
    }
  }

  test("replaceWhere with targetAlias - data predicate rejected (dataColumnsEnabled=false)") {
    // With REPLACEWHERE_DATACOLUMNS_ENABLED=false, non-partition predicates are rejected
    // by verifyPartitionPredicates. "t.id = 1" is stripped to "id = 1", which references
    // a non-partition column and should be rejected.
    withSQLConf(
        DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> "false") {
      withTable(testTableName) {
        Seq((1, "p1", "a"), (2, "p1", "b"), (3, "p2", "c"))
          .toDF("id", "part", "data")
          .write.format("delta").partitionBy("part").saveAsTable(testTableName)

        checkError(
          exception = intercept[AnalysisException] {
            Seq((1, "p1", "updated")).toDF("id", "part", "data")
              .write
              .format("delta")
              .mode("overwrite")
              .option("targetAlias", "t")
              .option("replaceWhere", "t.id = 1")
              .saveAsTable(testTableName)
          },
          condition = "DELTA_NON_PARTITION_COLUMN_REFERENCE",
          parameters = Map("columnName" -> "id", "columnList" -> "part"))
      }
    }
  }

  test("replaceWhere with targetAlias - aliased partition predicate is correctly classified") {
    withSQLConf(
        DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> "true") {
      withTable(testTableName) {
        Seq((1, "p1", "a"), (2, "p1", "b"), (3, "p2", "c"), (4, "p2", "d"))
          .toDF("id", "part", "data")
          .write.format("delta").partitionBy("part").saveAsTable(testTableName)

        // "t.part = 'p1'" is stripped to "part = 'p1'", which is a pure partition predicate
        // (metadata filter). Since there are no data filters, dataChange=false is allowed.
        // Only the p1 partition is replaced; p2 rows are untouched.
        Seq((1, "p1", "updated"), (5, "p1", "new")).toDF("id", "part", "data")
          .write
          .format("delta")
          .mode("overwrite")
          .option("targetAlias", "t")
          .option("replaceWhere", "t.part = 'p1'")
          .option("dataChange", "false")
          .saveAsTable(testTableName)

        checkAnswer(
          spark.table(testTableName).orderBy("id"),
          Seq(
            Row(1, "p1", "updated"),
            Row(3, "p2", "c"),
            Row(4, "p2", "d"),
            Row(5, "p1", "new")))
      }
    }
  }

  test("replaceWhere with targetAlias - mixed predicate correctly splits partition and data") {
    withSQLConf(
        DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> "true") {
      withTable(testTableName) {
        Seq((1, "p1", "a"), (2, "p1", "b"), (3, "p2", "c"))
          .toDF("id", "part", "data")
          .write.format("delta").partitionBy("part").saveAsTable(testTableName)

        // The alias prefix "t." is stripped from the predicates before partition/data
        // splitting. After stripping, "t.part = 'p1'" becomes a partition predicate and
        // "t.id = 1" becomes a data filter. With dataChange=false, data filters are
        // disallowed, so this throws DELTA_REPLACE_WHERE_WITH_FILTER_DATA_CHANGE_UNSET.
        checkError(
          exception = intercept[AnalysisException] {
            Seq((1, "p1", "updated")).toDF("id", "part", "data")
              .write
              .format("delta")
              .mode("overwrite")
              .option("targetAlias", "t")
              .option("replaceWhere", "t.part = 'p1' AND t.id = 1")
              .option("dataChange", "false")
              .saveAsTable(testTableName)
          },
          condition = "DELTA_REPLACE_WHERE_WITH_FILTER_DATA_CHANGE_UNSET",
          parameters = Map("dataFilters" -> "('id = 1)"))
      }
    }
  }

  test("replaceWhere with targetAlias - data-only predicate correctly classified") {
    withSQLConf(
        DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> "true") {
      withTable(testTableName) {
        Seq((1, "p1", "a"), (2, "p1", "b"), (3, "p2", "c"))
          .toDF("id", "part", "data")
          .write.format("delta").partitionBy("part").saveAsTable(testTableName)

        // "t.id = 1" is stripped to "id = 1", a non-partition (data) predicate.
        // With dataChange=false, data filters are disallowed.
        checkError(
          exception = intercept[AnalysisException] {
            Seq((1, "p1", "updated")).toDF("id", "part", "data")
              .write
              .format("delta")
              .mode("overwrite")
              .option("targetAlias", "t")
              .option("replaceWhere", "t.id = 1")
              .option("dataChange", "false")
              .saveAsTable(testTableName)
          },
          condition = "DELTA_REPLACE_WHERE_WITH_FILTER_DATA_CHANGE_UNSET",
          parameters = Map("dataFilters" -> "('id = 1)"))
      }
    }
  }

  // dataChange=false with aliased data predicate.
  test("replaceWhere with targetAlias and nested struct column - dataChange=false rejects") {
    withSQLConf(
        DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> "true") {
      withTable(testTableName) {
        spark.sql(
          s"""CREATE OR REPLACE TABLE $testTableName
             |(id bigint, t struct<id: bigint, value: string>, data string)
             |USING delta PARTITIONED BY (id)""".stripMargin)
        spark.sql(
          s"""INSERT INTO $testTableName VALUES
             |(1, named_struct('id', 10, 'value', 'x'), 'a'),
             |(2, named_struct('id', 20, 'value', 'y'), 'b')""".stripMargin)

        checkError(
          exception = intercept[AnalysisException] {
            spark.sql(
              """SELECT 1L AS id,
                |  named_struct('id', 10L, 'value', 'updated_x') AS t,
                |  'updated_a' AS data""".stripMargin)
              .write
              .format("delta")
              .mode("overwrite")
              .option("targetAlias", "t")
              .option("replaceWhere", "t.t.id = 10")
              .option("dataChange", "false")
              .saveAsTable(testTableName)
          },
          condition = "DELTA_REPLACE_WHERE_WITH_FILTER_DATA_CHANGE_UNSET",
          parameters = Map("dataFilters" -> "('t.id = 10)"))
      }
    }
  }

  // Error cases: missing alias, wrong alias.
  test("replaceWhere with qualified column but user forgot to specify targetAlias") {
    withTable(testTableName) {
      Seq((1, "a"), (2, "b")).toDF("id", "data")
        .write.format("delta").mode("overwrite").saveAsTable(testTableName)

      checkError(
        exception = intercept[AnalysisException] {
          Seq((1, "updated")).toDF("id", "data")
            .write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", "t.id = 1")
            .saveAsTable(testTableName)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = Map(
          "objectName" -> "`t`.`id`",
          "proposal" -> "`id`, `data`"),
        context = ExpectedContext(
          fragment = "t.id",
          start = 0,
          stop = 3))
    }
  }

  test("replaceWhere with targetAlias but predicate uses wrong alias") {
    withTable(testTableName) {
      Seq((1, "a"), (2, "b")).toDF("id", "data")
        .write.format("delta").mode("overwrite").saveAsTable(testTableName)

      checkError(
        exception = intercept[AnalysisException] {
          Seq((1, "updated")).toDF("id", "data")
            .write
            .format("delta")
            .mode("overwrite")
            .option("targetAlias", "t")
            .option("replaceWhere", "wrong.id = 1")
            .saveAsTable(testTableName)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = Map(
          "objectName" -> "`wrong`.`id`",
          "proposal" -> "`t`.`id`, `t`.`data`"),
        context = ExpectedContext(
          fragment = "wrong.id",
          start = 0,
          stop = 7))
    }
  }

  test("replaceWhere with targetAlias same name as struct column") {
    withTable(testTableName) {
      spark.sql(
        s"""CREATE OR REPLACE TABLE $testTableName
           |(id bigint, t struct<id: bigint, value: string>, data string)
           |USING delta PARTITIONED BY (id)""".stripMargin)
      spark.sql(
        s"""INSERT INTO $testTableName VALUES
           |(1, named_struct('id', 10, 'value', 'x'), 'a'),
           |(2, named_struct('id', 20, 'value', 'y'), 'b')""".stripMargin)

      // targetAlias "t" matches the struct column name "t". "t.id = 1" is stripped to
      // "id = 1" (the top-level partition column), not struct field t.id. This replaces
      // the partition where id=1. If stripping didn't happen, "t.id" would resolve to
      // the struct field t.id (bigint) and the predicate would filter on struct values.
      spark.sql(
        """SELECT 1L AS id,
          |  named_struct('id', 10L, 'value', 'updated_x') AS t,
          |  'updated_a' AS data""".stripMargin)
        .write
        .format("delta")
        .mode("overwrite")
        .option("targetAlias", "t")
        .option("replaceWhere", "t.id = 1")
        .saveAsTable(testTableName)

      checkAnswer(
        spark.table(testTableName).orderBy("id"),
        Seq(
          Row(1L, Row(10L, "updated_x"), "updated_a"),
          Row(2L, Row(20L, "y"), "b")))
    }
  }

  test("replaceWhere with targetAlias and nested struct column" +
      " - alias different from struct name") {
    withSQLConf(
        DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> "true") {
      withTable(testTableName) {
        spark.sql(
          s"""CREATE OR REPLACE TABLE $testTableName
             |(id bigint, t struct<id: bigint, value: string>, data string)
             |USING delta PARTITIONED BY (id)""".stripMargin)
        spark.sql(
          s"""INSERT INTO $testTableName VALUES
             |(1, named_struct('id', 10, 'value', 'x'), 'a'),
             |(2, named_struct('id', 20, 'value', 'y'), 'b')""".stripMargin)

        // targetAlias "s" differs from struct column "t". "s.t.id = 10" is stripped to
        // "t.id = 10", which resolves to the struct field t.id. Replaces rows where t.id=10.
        spark.sql(
          """SELECT 1L AS id,
            |  named_struct('id', 10L, 'value', 'updated_x') AS t,
            |  'updated_a' AS data""".stripMargin)
          .write
          .format("delta")
          .mode("overwrite")
          .option("targetAlias", "s")
          .option("replaceWhere", "s.t.id = 10")
          .saveAsTable(testTableName)

        checkAnswer(
          spark.table(testTableName).orderBy("id"),
          Seq(
            Row(1L, Row(10L, "updated_x"), "updated_a"),
            Row(2L, Row(20L, "y"), "b")))
      }
    }
  }

  test("replaceWhere with targetAlias and nested struct column" +
      " - alias same as struct name") {
    // targetAlias "t" matches the struct column name "t". The aliased predicate
    // "t.t.id = 10" resolves in the delete step as: alias t -> column t -> field id.
    // This correctly deletes rows where struct field t.id = 10.
    withSQLConf(
        DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> "true") {
      withTable(testTableName) {
        spark.sql(
          s"""CREATE OR REPLACE TABLE $testTableName
             |(id bigint, t struct<id: bigint, value: string>, data string)
             |USING delta PARTITIONED BY (id)""".stripMargin)
        spark.sql(
          s"""INSERT INTO $testTableName VALUES
             |(1, named_struct('id', 10, 'value', 'x'), 'a'),
             |(2, named_struct('id', 20, 'value', 'y'), 'b')""".stripMargin)

        spark.sql(
          """SELECT 1L AS id,
            |  named_struct('id', 10L, 'value', 'updated_x') AS t,
            |  'updated_a' AS data""".stripMargin)
          .write
          .format("delta")
          .mode("overwrite")
          .option("targetAlias", "t")
          .option("replaceWhere", "t.t.id = 10")
          .saveAsTable(testTableName)

        checkAnswer(
          spark.table(testTableName).orderBy("id"),
          Seq(
            Row(1L, Row(10L, "updated_x"), "updated_a"),
            Row(2L, Row(20L, "y"), "b")))
      }
    }
  }

  // case sensitivity, special aliases.
  test("replaceWhere with targetAlias - single-part column name same as alias") {
    withTable(testTableName) {
      Seq((1, "a"), (2, "b"), (3, "c")).toDF("t", "data")
        .write.format("delta").mode("overwrite").saveAsTable(testTableName)

      // Column "t" has the same name as targetAlias "t". "t >= 2" is a single-part
      // reference so stripping does not apply (only multi-part references are stripped).
      // It resolves directly to column "t" and replaces rows where t >= 2.
      Seq((2, "updated")).toDF("t", "data")
        .write
        .format("delta")
        .mode("overwrite")
        .option("targetAlias", "t")
        .option("replaceWhere", "t >= 2")
        .saveAsTable(testTableName)

      checkAnswer(
        spark.table(testTableName).orderBy("t"),
        Seq(Row(1, "a"), Row(2, "updated")))
    }
  }

  test("replaceWhere with targetAlias - single-part name fails if column does not exist") {
    withTable(testTableName) {
      Seq((1, "a"), (2, "b")).toDF("id", "data")
        .write.format("delta").mode("overwrite").saveAsTable(testTableName)

      checkError(
        exception = intercept[AnalysisException] {
          Seq((1, "updated")).toDF("id", "data")
            .write
            .format("delta")
            .mode("overwrite")
            .option("targetAlias", "t")
            .option("replaceWhere", "t >= 1")
            .saveAsTable(testTableName)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = Map(
          "objectName" -> "`t`",
          "proposal" -> "`id`, `data`"),
        context = ExpectedContext(
          fragment = "t",
          start = 0,
          stop = 0))
    }
  }

  test("replaceWhere with targetAlias - constraint check rejects non-matching rows") {
    withSQLConf(
        DeltaSQLConf.REPLACEWHERE_CONSTRAINT_CHECK_ENABLED.key -> "true",
        DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> "true") {
      withTable(testTableName) {
        Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "data")
          .write.format("delta").mode("overwrite").saveAsTable(testTableName)

        // "t.id = 1" is stripped to "id = 1". The constraint check verifies that all
        // inserted rows satisfy the replaceWhere predicate. Row (99, "bad") violates id=1.
        checkError(
          exception = intercept[AnalysisException] {
            Seq((99, "bad")).toDF("id", "data")
              .write
              .format("delta")
              .mode("overwrite")
              .option("targetAlias", "t")
              .option("replaceWhere", "t.id = 1")
              .saveAsTable(testTableName)
          },
          condition = "DELTA_REPLACE_WHERE_MISMATCH",
          parameters = Map(
            "replaceWhere" -> "t.id = 1",
            "message" -> "(?s).*id : 99.*"),
          matchPVals = true)
      }
    }
  }

  test("replaceWhere with targetAlias - constraint check disabled allows non-matching rows") {
    withSQLConf(
        DeltaSQLConf.REPLACEWHERE_CONSTRAINT_CHECK_ENABLED.key -> "false",
        DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> "true") {
      withTable(testTableName) {
        Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "data")
          .write.format("delta").mode("overwrite").saveAsTable(testTableName)

        // "t.id = 1" is stripped to "id = 1". With constraint check disabled, the
        // non-matching row (99, "bad") is still inserted. The delete step removes id=1,
        // and the write appends (99, "bad").
        Seq((99, "bad")).toDF("id", "data")
          .write
          .format("delta")
          .mode("overwrite")
          .option("targetAlias", "t")
          .option("replaceWhere", "t.id = 1")
          .saveAsTable(testTableName)

        checkAnswer(
          spark.table(testTableName).orderBy("id"),
          Seq(Row(2, "b"), Row(3, "c"), Row(99, "bad")))
      }
    }
  }

  test("replaceWhere with targetAlias - case insensitive alias") {
    withTable(testTableName) {
      Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "data")
        .write.format("delta").mode("overwrite").saveAsTable(testTableName)

      Seq((1, "updated")).toDF("id", "data")
        .write
        .format("delta")
        .mode("overwrite")
        .option("targetAlias", "T")
        .option("replaceWhere", "t.id = 1")
        .saveAsTable(testTableName)

      checkAnswer(
        spark.table(testTableName).orderBy("id"),
        Seq(Row(1, "updated"), Row(2, "b"), Row(3, "c")))
    }
  }

  test("replaceWhere with targetAlias - case sensitive alias mismatch fails") {
    // With case sensitivity enabled, "T" and "t" are different identifiers.
    // The alias "T" does not match "t" in the predicate, so "t.id" is not
    // stripped and fails to resolve as a column.
    withSQLConf(
        SQLConf.CASE_SENSITIVE.key -> "true") {
      withTable(testTableName) {
        Seq((1, "a"), (2, "b")).toDF("id", "data")
          .write.format("delta").mode("overwrite").saveAsTable(testTableName)

        checkError(
          exception = intercept[AnalysisException] {
            Seq((1, "updated")).toDF("id", "data")
              .write
              .format("delta")
              .mode("overwrite")
              .option("targetAlias", "T")
              .option("replaceWhere", "t.id = 1")
              .saveAsTable(testTableName)
          },
          condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
          parameters = Map(
            "objectName" -> "`t`.`id`",
            "proposal" -> "`T`.`id`, `T`.`data`"),
          context = ExpectedContext(
            fragment = "t.id",
            start = 0,
            stop = 3))
      }
    }
  }

  // Various predicate types: IN, OR, NOT.
  test("replaceWhere with targetAlias - IN list expression") {
    withSQLConf(
        DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> "true",
        DeltaSQLConf.REPLACEWHERE_CONSTRAINT_CHECK_ENABLED.key -> "false") {
      withTable(testTableName) {
        Seq((1, "a"), (2, "b"), (3, "c"), (4, "d")).toDF("id", "data")
          .write.format("delta").mode("overwrite").saveAsTable(testTableName)

        Seq((1, "new1"), (3, "new3"), (5, "new5")).toDF("id", "data")
          .write
          .format("delta")
          .mode("overwrite")
          .option("targetAlias", "t")
          .option("replaceWhere", "t.id IN (1, 3)")
          .saveAsTable(testTableName)

        checkAnswer(
          spark.table(testTableName).orderBy("id"),
          Seq(Row(1, "new1"), Row(2, "b"), Row(3, "new3"), Row(4, "d"), Row(5, "new5")))
      }
    }
  }

  test("replaceWhere with targetAlias - OR condition") {
    withSQLConf(
        DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> "true",
        DeltaSQLConf.REPLACEWHERE_CONSTRAINT_CHECK_ENABLED.key -> "false") {
      withTable(testTableName) {
        Seq((1, "a"), (2, "b"), (3, "c"), (4, "d")).toDF("id", "data")
          .write.format("delta").mode("overwrite").saveAsTable(testTableName)

        Seq((1, "new1"), (4, "new4"), (5, "new5")).toDF("id", "data")
          .write
          .format("delta")
          .mode("overwrite")
          .option("targetAlias", "t")
          .option("replaceWhere", "t.id = 1 OR t.id = 4")
          .saveAsTable(testTableName)

        checkAnswer(
          spark.table(testTableName).orderBy("id"),
          Seq(Row(1, "new1"), Row(2, "b"), Row(3, "c"), Row(4, "new4"), Row(5, "new5")))
      }
    }
  }

  test("replaceWhere with targetAlias - NOT condition") {
    withSQLConf(
        DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> "true",
        DeltaSQLConf.REPLACEWHERE_CONSTRAINT_CHECK_ENABLED.key -> "false") {
      withTable(testTableName) {
        Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "data")
          .write.format("delta").mode("overwrite").saveAsTable(testTableName)

        Seq((1, "new1"), (2, "new2"), (3, "new3")).toDF("id", "data")
          .write
          .format("delta")
          .mode("overwrite")
          .option("targetAlias", "t")
          .option("replaceWhere", "NOT (t.id = 2)")
          .saveAsTable(testTableName)

        // Delete removes rows where NOT(id=2), i.e. rows 1 and 3. Row 2 ("b") stays.
        // Insert appends all source rows (1, 2, 3). Row 2 appears twice: original + source.
        checkAnswer(
          spark.table(testTableName).orderBy("id", "data"),
          Seq(Row(1, "new1"), Row(2, "b"), Row(2, "new2"), Row(3, "new3")))
      }
    }
  }

  // Deeply nested structs and special alias names.
  test("replaceWhere with targetAlias - deeply nested struct t.t.t.id (4-part name)") {
    // targetAlias "t" matches the struct column "t". The aliased predicate
    // "t.t.t.id = 10" resolves in the delete step as: alias t -> column t -> field t ->
    // field id. This correctly targets the deeply nested struct field.
    withSQLConf(
        DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> "true") {
      withTable(testTableName) {
        spark.sql(
          s"""CREATE OR REPLACE TABLE $testTableName
             |(id bigint, t struct<t: struct<id: bigint>>, data string)
             |USING delta""".stripMargin)
        spark.sql(
          s"""INSERT INTO $testTableName VALUES
             |(1, named_struct('t', named_struct('id', 10)), 'a'),
             |(2, named_struct('t', named_struct('id', 20)), 'b')""".stripMargin)

        spark.sql(
          """SELECT 1L AS id,
            |  named_struct('t', named_struct('id', 10L)) AS t,
            |  'updated_a' AS data""".stripMargin)
          .write
          .format("delta")
          .mode("overwrite")
          .option("targetAlias", "t")
          .option("replaceWhere", "t.t.t.id = 10")
          .saveAsTable(testTableName)

        checkAnswer(
          spark.table(testTableName).orderBy("id"),
          Seq(
            Row(1L, Row(Row(10L)), "updated_a"),
            Row(2L, Row(Row(20L)), "b")))
      }
    }
  }

  test("replaceWhere with targetAlias - numeric alias with backticks") {
    withTable(testTableName) {
      Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "data")
        .write.format("delta").mode("overwrite").saveAsTable(testTableName)

      Seq((1, "updated")).toDF("id", "data")
        .write
        .format("delta")
        .mode("overwrite")
        .option("targetAlias", "22")
        .option("replaceWhere", "`22`.id = 1")
        .saveAsTable(testTableName)

      checkAnswer(
        spark.table(testTableName).orderBy("id"),
        Seq(Row(1, "updated"), Row(2, "b"), Row(3, "c")))
    }
  }

  test("replaceWhere with targetAlias - space-containing alias with backticks") {
    withTable(testTableName) {
      Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "data")
        .write.format("delta").mode("overwrite").saveAsTable(testTableName)

      Seq((2, "updated")).toDF("id", "data")
        .write
        .format("delta")
        .mode("overwrite")
        .option("targetAlias", "hello world")
        .option("replaceWhere", "`hello world`.id = 2")
        .saveAsTable(testTableName)

      checkAnswer(
        spark.table(testTableName).orderBy("id"),
        Seq(Row(1, "a"), Row(2, "updated"), Row(3, "c")))
    }
  }

  test("replaceWhere with targetAlias - top-level column name same as alias in multi-part ref") {
    withSQLConf(
        DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> "true") {
      withTable(testTableName) {
        Seq((1, 1, "a"), (2, 2, "b"), (3, 3, "c")).toDF("id", "t", "data")
          .write.format("delta").mode("overwrite").saveAsTable(testTableName)

        // Column "t" has the same name as targetAlias "t". "t.t >= 2" is a multi-part
        // reference where the first part matches the alias, so it's stripped to "t >= 2",
        // which resolves to the top-level column "t". Replaces rows where t >= 2.
        Seq((2, 2, "new2"), (3, 3, "new3")).toDF("id", "t", "data")
          .write
          .format("delta")
          .mode("overwrite")
          .option("targetAlias", "t")
          .option("replaceWhere", "t.t >= 2")
          .saveAsTable(testTableName)

        checkAnswer(
          spark.table(testTableName).orderBy("id"),
          Seq(Row(1, 1, "a"), Row(2, 2, "new2"), Row(3, 3, "new3")))
      }
    }
  }

  test("replaceWhere with targetAlias - OR on partition columns") {
    withTable(testTableName) {
      Seq((1, "p1", "a"), (2, "p2", "b"), (3, "p3", "c")).toDF("id", "part", "data")
        .write
        .format("delta")
        .partitionBy("part")
        .saveAsTable(testTableName)

      Seq((1, "p1", "new1"), (2, "p2", "new2")).toDF("id", "part", "data")
        .write
        .format("delta")
        .mode("overwrite")
        .option("targetAlias", "t")
        .option("replaceWhere", "t.part = 'p1' OR t.part = 'p2'")
        .saveAsTable(testTableName)

      checkAnswer(
        spark.table(testTableName).orderBy("id"),
        Seq(Row(1, "p1", "new1"), Row(2, "p2", "new2"), Row(3, "p3", "c")))
    }
  }


  // Constraint check with alias in predicates.
  test("replaceWhere with targetAlias - constraint check rejects with IS NULL") {
    withSQLConf(
        DeltaSQLConf.REPLACEWHERE_CONSTRAINT_CHECK_ENABLED.key -> "true",
        DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> "true") {
      withTable(testTableName) {
        Seq((1, Some("a")), (2, None), (3, Some("c"))).toDF("id", "data")
          .write.format("delta").mode("overwrite").saveAsTable(testTableName)

        // Row (5, "non-null") has data IS NOT NULL, violating the IS NULL predicate.
        checkError(
          exception = intercept[AnalysisException] {
            Seq((5, Some("non-null"))).toDF("id", "data")
              .write
              .format("delta")
              .mode("overwrite")
              .option("targetAlias", "t")
              .option("replaceWhere", "t.data IS NULL")
              .saveAsTable(testTableName)
          },
          condition = "DELTA_REPLACE_WHERE_MISMATCH",
          parameters = Map(
            "replaceWhere" -> "t.data IS NULL",
            "message" -> "(?s).*data : non-null.*"),
          matchPVals = true)
      }
    }
  }

  test("replaceWhere with targetAlias - CDF change records are correct") {
    withSQLConf(
        DeltaSQLConf.REPLACEWHERE_CONSTRAINT_CHECK_ENABLED.key -> "false") {
      withTable(testTableName) {
        Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "data")
          .write.format("delta")
          .option(DeltaConfigs.CHANGE_DATA_FEED.key, "true")
          .mode("overwrite")
          .saveAsTable(testTableName)

        Seq((1, "updated"), (4, "new")).toDF("id", "data")
          .write
          .format("delta")
          .mode("overwrite")
          .option("targetAlias", "t")
          .option("replaceWhere", "t.id = 1")
          .saveAsTable(testTableName)

        checkAnswer(
          spark.table(testTableName).orderBy("id"),
          Seq(Row(1, "updated"), Row(2, "b"), Row(3, "c"), Row(4, "new")))

        val changes = spark.read.format("delta")
          .option("readChangeFeed", "true")
          .option("startingVersion", 1)
          .table(testTableName)
          .select("id", "data", "_change_type")
          .orderBy("id", "_change_type")
        checkAnswer(changes, Seq(
          Row(1, "a", "delete"),
          Row(1, "updated", "insert"),
          Row(4, "new", "insert")))
      }
    }
  }
  // Struct column disambiguation: alias vs struct field with same name.
  test("replaceWhere on struct column t.id without targetAlias") {
    withTable(testTableName) {
      spark.sql(
        s"""CREATE OR REPLACE TABLE $testTableName
           |(id bigint, t struct<id: bigint, value: string>, data string)
           |USING delta""".stripMargin)
      spark.sql(
        s"""INSERT INTO $testTableName VALUES
           |(1, named_struct('id', 10, 'value', 'x'), 'a'),
           |(2, named_struct('id', 20, 'value', 'y'), 'b')""".stripMargin)

      // Without targetAlias, "t.id = 10" resolves to the struct field t.id (not a table
      // alias). This correctly filters on the nested column and replaces only matching rows.
      // If t.id were incorrectly resolved as a top-level column, the predicate would fail
      // analysis since there is no top-level column named "t".
      spark.sql(
        s"""SELECT 1L AS id,
           |  named_struct('id', 10L, 'value', 'updated_x') AS t,
           |  'updated_a' AS data""".stripMargin)
        .write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", "t.id = 10")
        .saveAsTable(testTableName)

      checkAnswer(
        spark.table(testTableName).orderBy("id"),
        Seq(
          Row(1L, Row(10L, "updated_x"), "updated_a"),
          Row(2L, Row(20L, "y"), "b")))
    }
  }

}
