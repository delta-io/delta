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
import org.apache.spark.sql.functions.col
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
        intercept[DeltaAnalysisException] {
          sql("ALTER TABLE t CHANGE COLUMN a TYPE SMALLINT")
        },
        "DELTA_GENERATED_COLUMNS_DEPENDENT_COLUMN_CHANGE",
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
        intercept[DeltaAnalysisException] {
          sql("ALTER TABLE t CHANGE COLUMN a.x TYPE SMALLINT")
        },
        "DELTA_GENERATED_COLUMNS_DEPENDENT_COLUMN_CHANGE",
        parameters = Map(
          "columnName" -> "a.x",
          "generatedColumns" -> "gen -> hash(a.x)"
      ))

      // Changing the type of a.y is allowed since it's not referenced by the CHECK constraint.
      sql("ALTER TABLE t CHANGE COLUMN a.y TYPE SMALLINT")
      checkAnswer(sql("SELECT * FROM t"), Row(Row(2, 3), 1765031574) :: Nil)
    }
  }

  test("generated column on arrays and maps with type change") {
    withTable("t") {
      createTable(
        tableName = "t",
        path = None,
        schemaString = "a array<struct<f: byte, g: byte>>, gen tinyint",
        generatedColumns = Map("gen" -> "a[0].f"),
        partitionColumns = Seq.empty
      )
      sql("INSERT INTO t (a) VALUES (array(named_struct('f', 7, 'g', 8)))")
      checkAnswer(sql("SELECT gen FROM t"), Row(7))

      sql("ALTER TABLE t CHANGE COLUMN a.element.g TYPE SMALLINT")
      checkError(
        intercept[DeltaAnalysisException] {
          sql("ALTER TABLE t CHANGE COLUMN a.element.f TYPE SMALLINT")
        },
        "DELTA_GENERATED_COLUMNS_DEPENDENT_COLUMN_CHANGE",
        parameters = Map(
          "columnName" -> "a.element.f",
          "generatedColumns" -> "gen -> a[0].f"
        ))

      checkAnswer(sql("SELECT gen FROM t"), Row(7))
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
        intercept[DeltaAnalysisException] {
          sql("INSERT INTO t (a) VALUES (200)")
        },
        "DELTA_GENERATED_COLUMNS_DATA_TYPE_MISMATCH",
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
      checkAnswer(sql("SELECT gen FROM t"), Row(1765031574))

      withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
        checkError(
          intercept[DeltaAnalysisException] {
            sql("INSERT INTO t (a) VALUES (named_struct('x', 200, 'y', CAST(5 AS byte)))")
          },
          "DELTA_GENERATED_COLUMNS_DATA_TYPE_MISMATCH",
          parameters = Map(
            "columnName" -> "a.x",
            "columnType" -> "TINYINT",
            "dataType" -> "INT",
            "generatedColumns" -> "gen -> hash(a.x)"
          )
        )

        // changing the type of struct field `a.y` when it's not
        // the field referenced by the generated column is allowed.
        sql("INSERT INTO t (a) VALUES (named_struct('x', CAST(2 AS byte), 'y', 200))")
        checkAnswer(sql("SELECT gen FROM t"), Seq(Row(1765031574), Row(1765031574)))
      }
    }
  }

  test("generated column on arrays and maps with type evolution") {
    withTable("t") {
      createTable(
        tableName = "t",
        path = None,
        schemaString = "a array<byte>, gen INT",
        generatedColumns = Map("gen" -> "hash(a[0])"),
        partitionColumns = Seq.empty
      )
      sql("INSERT INTO t (a) VALUES (array(2, 3))")
      checkAnswer(sql("SELECT gen FROM t"), Row(1765031574))

      withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
        // Insert by name is not supported by type evolution.
        checkError(
          intercept[DeltaAnalysisException] {
            spark.createDataFrame(Seq(Tuple1(Array(200000, 12345))))
              .toDF("a").withColumn("a", col("a").cast("array<int>"))
              .write.format("delta").mode("append").saveAsTable("t")
          },
          "DELTA_GENERATED_COLUMNS_DATA_TYPE_MISMATCH",
          parameters = Map(
            "columnName" -> "a.element",
            "columnType" -> "TINYINT",
            "dataType" -> "INT",
            "generatedColumns" -> "gen -> hash(a[0])"
          )
        )

        checkAnswer(sql("SELECT gen FROM t"), Row(1765031574))
      }
    }
  }

  test("generated column on nested field with complex type evolution") {
    withTable("t") {
      createTable(
        tableName = "t",
        path = None,
        schemaString = "a struct<x: struct<z: byte, h: byte>, y: byte>, gen int",
        generatedColumns = Map("gen" -> "hash(a.x.z)"),
        partitionColumns = Seq.empty
      )

      sql("INSERT INTO t (a) VALUES (named_struct('x', named_struct('z', 2, 'h', 3), 'y', 4))")
      checkAnswer(sql("SELECT gen FROM t"), Row(1765031574))

      withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
        checkError(
          intercept[DeltaAnalysisException] {
            sql(
              s"""
                 | INSERT INTO t (a) VALUES (
                 |   named_struct('x', named_struct('z', 200, 'h', 3), 'y', 4)
                 | )
                 |""".stripMargin
            )
          },
          "DELTA_GENERATED_COLUMNS_DATA_TYPE_MISMATCH",
          parameters = Map(
            "columnName" -> "a.x.z",
            "columnType" -> "TINYINT",
            "dataType" -> "INT",
            "generatedColumns" -> "gen -> hash(a.x.z)"
          )
        )

        // changing the type of struct field `a.y` when it's not
        // the field referenced by the generated column is allowed.
        sql(
          """
            | INSERT INTO t (a) VALUES (
            |   named_struct('x', named_struct('z', CAST(2 AS BYTE), 'h', 2002), 'y', 1030)
            | )
            |""".stripMargin
        )
        checkAnswer(sql("SELECT gen FROM t"), Seq(Row(1765031574), Row(1765031574)))
      }
    }
  }
}
