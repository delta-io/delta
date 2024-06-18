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

package org.apache.spark.sql.delta.typewidening

import org.apache.spark.sql.delta.DeltaAnalysisException
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.types._

/**
 * Suite covering changing the type of columns referenced by constraints, e.g. CHECK constraints or
 * NOT NULL constraints.
 */
class TypeWideningConstraintsSuite
  extends QueryTest
    with TypeWideningTestMixin
    with TypeWideningConstraintsTests

trait TypeWideningConstraintsTests { self: QueryTest with TypeWideningTestMixin =>

  test("not null constraint with type change") {
    withTable("t") {
      sql("CREATE TABLE t (a byte NOT NULL) USING DELTA")
      sql("INSERT INTO t VALUES (1)")
      checkAnswer(sql("SELECT * FROM t"), Row(1))

      // Changing the type of a column with a NOT NULL constraint is allowed.
      sql("ALTER TABLE t CHANGE COLUMN a TYPE SMALLINT")
      assert(sql("SELECT * FROM t").schema("a").dataType === ShortType)

      sql("INSERT INTO t VALUES (2)")
      checkAnswer(sql("SELECT * FROM t"), Seq(Row(1), Row(2)))
    }
  }

  test("check constraint with type change") {
    withTable("t") {
      sql("CREATE TABLE t (a byte, b byte) USING DELTA")
      sql("ALTER TABLE t ADD CONSTRAINT ck CHECK (hash(a) > 0)")
      sql("INSERT INTO t VALUES (2, 2)")
      checkAnswer(sql("SELECT hash(a) FROM t"), Row(1765031574))

      // Changing the type of a column that a CHECK constraint depends on is not allowed.
      checkError(
        exception = intercept[DeltaAnalysisException] {
          sql("ALTER TABLE t CHANGE COLUMN a TYPE SMALLINT")
        },
        errorClass = "DELTA_CONSTRAINT_DEPENDENT_COLUMN_CHANGE",
        parameters = Map(
          "columnName" -> "a",
          "constraints" -> "delta.constraints.ck -> hash ( a ) > 0"
      ))

      // Changing the type of `b` is allowed as it's not referenced by the constraint.
      sql("ALTER TABLE t CHANGE COLUMN b TYPE SMALLINT")
      assert(sql("SELECT * FROM t").schema("b").dataType === ShortType)
      checkAnswer(sql("SELECT * FROM t"), Row(2, 2))
    }
  }

  test("check constraint on nested field with type change") {
    withTable("t") {
      sql("CREATE TABLE t (a struct<x: byte, y: byte>) USING DELTA")
      sql("ALTER TABLE t ADD CONSTRAINT ck CHECK (hash(a.x) > 0)")
      sql("INSERT INTO t (a) VALUES (named_struct('x', 2, 'y', 3))")
      checkAnswer(sql("SELECT hash(a.x) FROM t"), Row(1765031574))

      checkError(
        exception = intercept[DeltaAnalysisException] {
          sql("ALTER TABLE t CHANGE COLUMN a.x TYPE SMALLINT")
        },
        errorClass = "DELTA_CONSTRAINT_DEPENDENT_COLUMN_CHANGE",
        parameters = Map(
          "columnName" -> "a.x",
          "constraints" -> "delta.constraints.ck -> hash ( a . x ) > 0"
      ))

      // Changing the type of a.y is allowed since it's not referenced by the CHECK constraint.
      sql("ALTER TABLE t CHANGE COLUMN a.y TYPE SMALLINT")
      checkAnswer(sql("SELECT * FROM t"), Row(Row(2, 3)))
    }
  }

  test(s"check constraint with type evolution") {
    withTable("t") {
      sql(s"CREATE TABLE t (a byte) USING DELTA")
      sql("ALTER TABLE t ADD CONSTRAINT ck CHECK (hash(a) > 0)")
      sql("INSERT INTO t VALUES (2)")
      checkAnswer(sql("SELECT hash(a) FROM t"), Row(1765031574))

      withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
        checkError(
          exception = intercept[DeltaAnalysisException] {
            sql("INSERT INTO t VALUES (200)")
          },
          errorClass = "DELTA_CONSTRAINT_DATA_TYPE_MISMATCH",
          parameters = Map(
            "columnName" -> "a",
            "columnType" -> "TINYINT",
            "dataType" -> "INT",
            "constraints" -> "delta.constraints.ck -> hash ( a ) > 0"
        ))
      }
    }
  }

  test("check constraint on nested field with type evolution") {
    withTable("t") {
      sql("CREATE TABLE t (a struct<x: byte, y: byte>) USING DELTA")
      sql("ALTER TABLE t ADD CONSTRAINT ck CHECK (hash(a.x) > 0)")
      sql("INSERT INTO t (a) VALUES (named_struct('x', 2, 'y', 3))")
      checkAnswer(sql("SELECT hash(a.x) FROM t"), Row(1765031574))

      withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
        checkError(
          exception = intercept[DeltaAnalysisException] {
            sql("INSERT INTO t (a) VALUES (named_struct('x', 200, 'y', CAST(5 AS byte)))")
          },
          errorClass = "DELTA_CONSTRAINT_DATA_TYPE_MISMATCH",
          parameters = Map(
            "columnName" -> "a",
            "columnType" -> "STRUCT<x: TINYINT, y: TINYINT>",
            "dataType" -> "STRUCT<x: INT, y: TINYINT>",
            "constraints" -> "delta.constraints.ck -> hash ( a . x ) > 0"
        ))

        // We're currently too strict and reject changing the type of struct field a.y even though
        // it's not the field referenced by the CHECK constraint.
        checkError(
          exception = intercept[DeltaAnalysisException] {
            sql("INSERT INTO t (a) VALUES (named_struct('x', CAST(2 AS byte), 'y', 500))")
          },
          errorClass = "DELTA_CONSTRAINT_DATA_TYPE_MISMATCH",
          parameters = Map(
            "columnName" -> "a",
            "columnType" -> "STRUCT<x: TINYINT, y: TINYINT>",
            "dataType" -> "STRUCT<x: TINYINT, y: INT>",
            "constraints" -> "delta.constraints.ck -> hash ( a . x ) > 0"
        ))
      }
    }
  }

  test("add constraint after type change then RESTORE") {
    withTable("t") {
      sql("CREATE TABLE t (a byte) USING DELTA")
      sql("INSERT INTO t VALUES (2)")
      sql("ALTER TABLE t CHANGE COLUMN a TYPE INT")
      sql("INSERT INTO t VALUES (5)")
      checkAnswer(sql("SELECT a, hash(a) FROM t"), Seq(Row(2, 1765031574), Row(5, 1023896466)))
      sql("ALTER TABLE t ADD CONSTRAINT ck CHECK (hash(a) > 0)")
      // Constraints are stored in the table metadata, RESTORE removes the constraint so the type
      // change can't get in the way.
      sql(s"RESTORE TABLE t VERSION AS OF 1")
      sql("INSERT INTO t VALUES (1)")
      checkAnswer(sql("SELECT a, hash(a) FROM t"), Seq(Row(2, 1765031574), Row(1, -559580957)))
    }
  }
}
