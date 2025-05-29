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
import org.apache.spark.sql.functions.col
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
        intercept[DeltaAnalysisException] {
          sql("ALTER TABLE t CHANGE COLUMN a TYPE SMALLINT")
        },
        "DELTA_CONSTRAINT_DEPENDENT_COLUMN_CHANGE",
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
        intercept[DeltaAnalysisException] {
          sql("ALTER TABLE t CHANGE COLUMN a.x TYPE SMALLINT")
        },
        "DELTA_CONSTRAINT_DEPENDENT_COLUMN_CHANGE",
        parameters = Map(
          "columnName" -> "a.x",
          "constraints" -> "delta.constraints.ck -> hash ( a . x ) > 0"
      ))

      // Changing the type of a.y is allowed since it's not referenced by the CHECK constraint.
      sql("ALTER TABLE t CHANGE COLUMN a.y TYPE SMALLINT")
      checkAnswer(sql("SELECT * FROM t"), Row(Row(2, 3)))
    }
  }

  test("check constraint on arrays and maps with type change") {
    withTable("t") {
      sql("CREATE TABLE t (m map<byte, byte>, a array<byte>) USING DELTA")
      sql("INSERT INTO t VALUES (map(1, 2, 7, -3), array(1, -2, 3))")

      sql("ALTER TABLE t CHANGE COLUMN a.element TYPE SMALLINT")
      sql("ALTER TABLE t ADD CONSTRAINT ch1 CHECK (hash(a[1]) = -1160545675)")
      checkError(
        intercept[DeltaAnalysisException] {
          sql("ALTER TABLE t CHANGE COLUMN a.element TYPE INTEGER")
        },
        "DELTA_CONSTRAINT_DEPENDENT_COLUMN_CHANGE",
        parameters = Map(
          "columnName" -> "a.element",
          "constraints" -> "delta.constraints.ch1 -> hash ( a [ 1 ] ) = - 1160545675"
        )
      )

      sql("ALTER TABLE t CHANGE COLUMN m.value TYPE SMALLINT")
      sql("ALTER TABLE t ADD CONSTRAINT ch2 CHECK (sign(m[7]) < 0)")
      checkError(
        intercept[DeltaAnalysisException] {
          sql("ALTER TABLE t CHANGE COLUMN m.value TYPE INTEGER")
        },
        "DELTA_CONSTRAINT_DEPENDENT_COLUMN_CHANGE",
        parameters = Map(
          "columnName" -> "m.value",
          "constraints" -> "delta.constraints.ch2 -> sign ( m [ 7 ] ) < 0"
        )
      )
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
          intercept[DeltaAnalysisException] {
            sql("INSERT INTO t VALUES (200)")
          },
          "DELTA_CONSTRAINT_DATA_TYPE_MISMATCH",
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
          intercept[DeltaAnalysisException] {
            sql("INSERT INTO t (a) VALUES (named_struct('x', 200, 'y', CAST(5 AS byte)))")
          },
          "DELTA_CONSTRAINT_DATA_TYPE_MISMATCH",
          parameters = Map(
            "columnName" -> "a.x",
            "columnType" -> "TINYINT",
            "dataType" -> "INT",
            "constraints" -> "delta.constraints.ck -> hash ( a . x ) > 0"
          )
        )

        // changing the type of struct field `a.y` when it's not
        // the field referenced by the CHECK constraint is allowed.
        sql("INSERT INTO t (a) VALUES (named_struct('x', CAST(2 AS byte), 'y', 500))")
        checkAnswer(sql("SELECT hash(a.x) FROM t"), Seq(Row(1765031574), Row(1765031574)))
      }
    }
  }

  test("check constraint on nested field with complex type evolution") {
    withTable("t") {
      sql("CREATE TABLE t (a struct<x: struct<z: byte, h: byte>, y: byte>) USING DELTA")
      sql("ALTER TABLE t ADD CONSTRAINT ck CHECK (hash(a.x.z) > 0)")
      sql("INSERT INTO t (a) VALUES (named_struct('x', named_struct('z', 2, 'h', 3), 'y', 4))")
      checkAnswer(sql("SELECT hash(a.x.z) FROM t"), Row(1765031574))

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
          "DELTA_CONSTRAINT_DATA_TYPE_MISMATCH",
          parameters = Map(
            "columnName" -> "a.x.z",
            "columnType" -> "TINYINT",
            "dataType" -> "INT",
            "constraints" -> "delta.constraints.ck -> hash ( a . x . z ) > 0"
          )
        )

        // changing the type of struct field `a.y` and `a.x.h` when it's not
        // the field referenced by the CHECK constraint is allowed.
        sql(
          """
            | INSERT INTO t (a) VALUES (
            |   named_struct('x', named_struct('z', CAST(2 AS BYTE), 'h', 2002), 'y', 1030)
            | )
            |""".stripMargin
        )
        checkAnswer(sql("SELECT hash(a.x.z) FROM t"), Seq(Row(1765031574), Row(1765031574)))
      }
    }
  }

  test("check constraint on arrays and maps with type evolution") {
    withTable("t") {
      sql("CREATE TABLE t (s struct<arr array<map<byte, byte>>>) USING DELTA")
      sql("ALTER TABLE t ADD CONSTRAINT ck CHECK (s.arr[0][3] = 3)")
      sql("INSERT INTO t(s) VALUES (struct(struct(array(map(1, 1, 3, 3)))))")
      checkAnswer(sql("SELECT s.arr[0][3] FROM t"), Row(3))

      withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
        // Insert by name is not supported by type evolution.
        checkError(
          intercept[DeltaAnalysisException] {
            // Migrate map's key to int type.
            spark.createDataFrame(Seq(Tuple1(Tuple1(Array(Map(999999 -> 1, 3 -> 3))))))
              .toDF("s").withColumn("s", col("s").cast("struct<arr:array<map<int,tinyint>>>"))
              .write.format("delta").mode("append").saveAsTable("t")
          },
          "DELTA_CONSTRAINT_DATA_TYPE_MISMATCH",
          parameters = Map(
            "columnName" -> "s.arr.element.key",
            "columnType" -> "TINYINT",
            "dataType" -> "INT",
            "constraints" -> "delta.constraints.ck -> s . arr [ 0 ] [ 3 ] = 3"
          )
        )
        checkError(
          intercept[DeltaAnalysisException] {
            // Migrate map's value to int type.
            spark.createDataFrame(Seq(Tuple1(Tuple1(Array(Map(1 -> 999999, 3 -> 3))))))
              .toDF("s").withColumn("s", col("s").cast("struct<arr:array<map<tinyint,int>>>"))
              .write.format("delta").mode("append").saveAsTable("t")
          },
          "DELTA_CONSTRAINT_DATA_TYPE_MISMATCH",
          parameters = Map(
            "columnName" -> "s.arr.element.value",
            "columnType" -> "TINYINT",
            "dataType" -> "INT",
            "constraints" -> "delta.constraints.ck -> s . arr [ 0 ] [ 3 ] = 3"
          )
        )
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
