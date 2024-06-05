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

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.types._

/**
 * Suite covering changing the type of columns referenced by generated columns.
 */
class TypeWideningGeneratedColumnsSuite
  extends QueryTest
    with TypeWideningTestMixin
    with GeneratedColumnTest
    with TypeWideningGeneratedColumnTests

trait TypeWideningGeneratedColumnTests extends GeneratedColumnTest {
  self: QueryTest with TypeWideningTestMixin =>

  test("generated column with type change") {
    withTable("t") {
      createTable(
        tableName = "t",
        path = None,
        schemaString = "a byte, b byte, gen int",
        generatedColumns = Map("gen" -> "hash(a)"),
        partitionColumns = Seq.empty
      )
      sql("INSERT INTO t (a, b) VALUES (2, 2)")
      checkAnswer(sql("SELECT hash(a) FROM t"), Row(1765031574))

      // Changing the type of a column that a generated column depends on is not allowed.
      checkError(
        exception = intercept[DeltaAnalysisException] {
          sql("ALTER TABLE t CHANGE COLUMN a TYPE SMALLINT")
        },
        errorClass = "DELTA_GENERATED_COLUMNS_DEPENDENT_COLUMN_CHANGE",
        parameters = Map(
          "columnName" -> "a",
          "generatedColumns" -> "gen -> hash(a)"
        ))

      // Changing the type of `b` is allowed as it's not referenced by the generated column.
      sql("ALTER TABLE t CHANGE COLUMN b TYPE SMALLINT")
      assert(sql("SELECT * FROM t").schema("b").dataType === ShortType)
      checkAnswer(sql("SELECT * FROM t"), Row(2, 2, 1765031574))
    }
  }

  test("generated column on nested field with type change") {
    withTable("t") {
      createTable(
        tableName = "t",
        path = None,
        schemaString = "a struct<x: byte, y: byte>, gen int",
        generatedColumns = Map("gen" -> "hash(a.x)"),
        partitionColumns = Seq.empty
      )
      sql("INSERT INTO t (a) VALUES (named_struct('x', 2, 'y', 3))")
      checkAnswer(sql("SELECT hash(a.x) FROM t"), Row(1765031574))

      checkError(
        exception = intercept[DeltaAnalysisException] {
          sql("ALTER TABLE t CHANGE COLUMN a.x TYPE SMALLINT")
        },
        errorClass = "DELTA_GENERATED_COLUMNS_DEPENDENT_COLUMN_CHANGE",
        parameters = Map(
          "columnName" -> "a.x",
          "generatedColumns" -> "gen -> hash(a.x)"
      ))

      // Changing the type of a.y is allowed since it's not referenced by the CHECK constraint.
      sql("ALTER TABLE t CHANGE COLUMN a.y TYPE SMALLINT")
      checkAnswer(sql("SELECT * FROM t"), Row(Row(2, 3), 1765031574) :: Nil)
    }
  }

  test("generated column with type evolution") {
    withTable("t") {
      createTable(
        tableName = "t",
        path = None,
        schemaString = "a byte, gen int",
        generatedColumns = Map("gen" -> "hash(a)"),
        partitionColumns = Seq.empty
      )
      sql("INSERT INTO t (a) VALUES (2)")
      checkAnswer(sql("SELECT hash(a) FROM t"), Row(1765031574))

      withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
        checkError(
        exception = intercept[DeltaAnalysisException] {
          sql("INSERT INTO t (a) VALUES (200)")
        },
        errorClass = "DELTA_GENERATED_COLUMNS_DATA_TYPE_MISMATCH",
        parameters = Map(
          "columnName" -> "a",
          "columnType" -> "TINYINT",
          "dataType" -> "INT",
          "generatedColumns" -> "gen -> hash(a)"
        ))
      }
    }
  }

  test("generated column on nested field with type evolution") {
    withTable("t") {
      createTable(
        tableName = "t",
        path = None,
        schemaString = "a struct<x: byte, y: byte>, gen int",
        generatedColumns = Map("gen" -> "hash(a.x)"),
        partitionColumns = Seq.empty
      )
      sql("INSERT INTO t (a) VALUES (named_struct('x', 2, 'y', 3))")
      checkAnswer(sql("SELECT hash(a.x) FROM t"), Row(1765031574))

      withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
        checkError(
          exception = intercept[DeltaAnalysisException] {
            sql("INSERT INTO t (a) VALUES (named_struct('x', 200, 'y', CAST(5 AS byte)))")
          },
          errorClass = "DELTA_GENERATED_COLUMNS_DATA_TYPE_MISMATCH",
          parameters = Map(
            "columnName" -> "a",
            "columnType" -> "STRUCT<x: TINYINT, y: TINYINT>",
            "dataType" -> "STRUCT<x: INT, y: TINYINT>",
            "generatedColumns" -> "gen -> hash(a.x)"
        ))

        // We're currently too strict and reject changing the type of struct field a.y even though
        // it's not the field referenced by the generated column.
        checkError(
          exception = intercept[DeltaAnalysisException] {
            sql("INSERT INTO t (a) VALUES (named_struct('x', CAST(2 AS byte), 'y', 200))")
          },
          errorClass = "DELTA_GENERATED_COLUMNS_DATA_TYPE_MISMATCH",
          parameters = Map(
            "columnName" -> "a",
            "columnType" -> "STRUCT<x: TINYINT, y: TINYINT>",
            "dataType" -> "STRUCT<x: TINYINT, y: INT>",
            "generatedColumns" -> "gen -> hash(a.x)"
        ))
      }
    }
  }
}
