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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.functions.{array, col, map, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}

trait DeltaAlterTableReplaceTests extends DeltaAlterTableTestBase {

  import testImplicits._

  ddlTest("REPLACE COLUMNS - add a comment") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("s", struct("v1", "v2"))
      .withColumn("a", array("s"))
      .withColumn("m", map(col("s"), col("s")))

    withDeltaTable(df) { tableName =>

      sql(s"""
             |ALTER TABLE $tableName REPLACE COLUMNS (
             |  v1 int COMMENT 'a comment for v1',
             |  v2 string COMMENT 'a comment for v2',
             |  s STRUCT<
             |    v1:int COMMENT 'a comment for s.v1',
             |    v2:string COMMENT 'a comment for s.v2'> COMMENT 'a comment for s',
             |  a ARRAY<STRUCT<
             |    v1:int COMMENT 'a comment for a.v1',
             |    v2:string COMMENT 'a comment for a.v2'>> COMMENT 'a comment for a',
             |  m MAP<STRUCT<
             |      v1:int COMMENT 'a comment for m.key.v1',
             |      v2:string COMMENT 'a comment for m.key.v2'>,
             |    STRUCT<
             |      v1:int COMMENT 'a comment for m.value.v1',
             |      v2:string COMMENT 'a comment for m.value.v2'>> COMMENT 'a comment for m'
             |)""".stripMargin)

      val deltaLog = getDeltaLog(tableName)
      val expectedSchema = new StructType()
        .add("v1", "integer", true, "a comment for v1")
        .add("v2", "string", true, "a comment for v2")
        .add("s", new StructType()
          .add("v1", "integer", true, "a comment for s.v1")
          .add("v2", "string", true, "a comment for s.v2"), true, "a comment for s")
        .add("a", ArrayType(new StructType()
          .add("v1", "integer", true, "a comment for a.v1")
          .add("v2", "string", true, "a comment for a.v2")), true, "a comment for a")
        .add("m", MapType(
          new StructType()
            .add("v1", "integer", true, "a comment for m.key.v1")
            .add("v2", "string", true, "a comment for m.key.v2"),
          new StructType()
            .add("v1", "integer", true, "a comment for m.value.v1")
            .add("v2", "string", true, "a comment for m.value.v2")), true, "a comment for m")
      assertEqual(deltaLog.snapshot.schema, expectedSchema)

      implicit val ordering = Ordering.by[
        (Int, String, (Int, String), Seq[(Int, String)], Map[(Int, String), (Int, String)]), Int] {
        case (v1, _, _, _, _) => v1
      }
      checkDatasetUnorderly(
        spark.table(tableName)
          .as[(Int, String, (Int, String), Seq[(Int, String)], Map[(Int, String), (Int, String)])],
        (1, "a", (1, "a"), Seq((1, "a")), Map((1, "a") -> ((1, "a")))),
        (2, "b", (2, "b"), Seq((2, "b")), Map((2, "b") -> ((2, "b")))))

      // REPLACE COLUMNS doesn't remove metadata.
      sql(s"""
             |ALTER TABLE $tableName REPLACE COLUMNS (
             |  v1 int,
             |  v2 string,
             |  s STRUCT<v1:int, v2:string>,
             |  a ARRAY<STRUCT<v1:int, v2:string>>,
             |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
             |)""".stripMargin)
      assertEqual(deltaLog.snapshot.schema, expectedSchema)
    }
  }

  ddlTest("REPLACE COLUMNS - reorder") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("s", struct("v1", "v2"))
      .withColumn("a", array("s"))
      .withColumn("m", map(col("s"), col("s")))
    withDeltaTable(df) { tableName =>

      sql(s"""
             |ALTER TABLE $tableName REPLACE COLUMNS (
             |  m MAP<STRUCT<v2:string, v1:int>, STRUCT<v2:string, v1:int>>,
             |  v2 string,
             |  a ARRAY<STRUCT<v2:string, v1:int>>,
             |  v1 int,
             |  s STRUCT<v2:string, v1:int>
             |)""".stripMargin)

      val deltaLog = getDeltaLog(tableName)
      assertEqual(deltaLog.snapshot.schema, new StructType()
        .add("m", MapType(
          new StructType().add("v2", "string").add("v1", "integer"),
          new StructType().add("v2", "string").add("v1", "integer")))
        .add("v2", "string")
        .add("a", ArrayType(new StructType().add("v2", "string").add("v1", "integer")))
        .add("v1", "integer")
        .add("s", new StructType().add("v2", "string").add("v1", "integer")))

      implicit val ordering = Ordering.by[
        (Map[(String, Int), (String, Int)], String, Seq[(String, Int)], Int, (String, Int)), Int] {
        case (_, _, _, v1, _) => v1
      }
      checkDatasetUnorderly(
        spark.table(tableName)
          .as[(Map[(String, Int), (String, Int)], String, Seq[(String, Int)], Int, (String, Int))],
        (Map(("a", 1) -> (("a", 1))), "a", Seq(("a", 1)), 1, ("a", 1)),
        (Map(("b", 2) -> (("b", 2))), "b", Seq(("b", 2)), 2, ("b", 2)))
    }
  }

  ddlTest("REPLACE COLUMNS - add columns") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("s", struct("v1", "v2"))
      .withColumn("a", array("s"))
      .withColumn("m", map(col("s"), col("s")))
    withDeltaTable(df) { tableName =>

      sql(s"""
             |ALTER TABLE $tableName REPLACE COLUMNS (
             |  v1 int,
             |  v2 string,
             |  v3 long,
             |  s STRUCT<v1:int, v2:string, v3:long>,
             |  a ARRAY<STRUCT<v1:int, v2:string, v3:long>>,
             |  m MAP<STRUCT<v1:int, v2:string, v3:long>, STRUCT<v1:int, v2:string, v3:long>>
             |)""".stripMargin)

      val deltaLog = getDeltaLog(tableName)
      assertEqual(deltaLog.snapshot.schema, new StructType()
        .add("v1", "integer")
        .add("v2", "string")
        .add("v3", "long")
        .add("s", new StructType()
          .add("v1", "integer").add("v2", "string").add("v3", "long"))
        .add("a", ArrayType(new StructType()
          .add("v1", "integer").add("v2", "string").add("v3", "long")))
        .add("m", MapType(
          new StructType().add("v1", "integer").add("v2", "string").add("v3", "long"),
          new StructType().add("v1", "integer").add("v2", "string").add("v3", "long"))))

      implicit val ordering = Ordering.by[
        (Int, String, Option[Long],
          (Int, String, Option[Long]),
          Seq[(Int, String, Option[Long])],
          Map[(Int, String, Option[Long]), (Int, String, Option[Long])]), Int] {
        case (v1, _, _, _, _, _) => v1
      }
      checkDatasetUnorderly(
        spark.table(tableName).as[
          (Int, String, Option[Long],
            (Int, String, Option[Long]),
            Seq[(Int, String, Option[Long])],
            Map[(Int, String, Option[Long]), (Int, String, Option[Long])])],
        (1, "a", None, (1, "a", None),
          Seq((1, "a", None)), Map((1, "a", Option.empty[Long]) -> ((1, "a", None)))),
        (2, "b", None, (2, "b", None),
          Seq((2, "b", None)), Map((2, "b", Option.empty[Long]) -> ((2, "b", None)))))
    }
  }

  ddlTest("REPLACE COLUMNS - special column names") {
    val df = Seq((1, "a"), (2, "b")).toDF("x.x", "y.y")
      .withColumn("s.s", struct("`x.x`", "`y.y`"))
      .withColumn("a.a", array("`s.s`"))
      .withColumn("m.m", map(col("`s.s`"), col("`s.s`")))
    withDeltaTable(df) { tableName =>

      sql(s"""
             |ALTER TABLE $tableName REPLACE COLUMNS (
             |  `m.m` MAP<STRUCT<`y.y`:string, `x.x`:int>, STRUCT<`y.y`:string, `x.x`:int>>,
             |  `y.y` string,
             |  `a.a` ARRAY<STRUCT<`y.y`:string, `x.x`:int>>,
             |  `x.x` int,
             |  `s.s` STRUCT<`y.y`:string, `x.x`:int>
             |)""".stripMargin)

      val deltaLog = getDeltaLog(tableName)
      assertEqual(deltaLog.snapshot.schema, new StructType()
        .add("m.m", MapType(
          new StructType().add("y.y", "string").add("x.x", "integer"),
          new StructType().add("y.y", "string").add("x.x", "integer")))
        .add("y.y", "string")
        .add("a.a", ArrayType(new StructType().add("y.y", "string").add("x.x", "integer")))
        .add("x.x", "integer")
        .add("s.s", new StructType().add("y.y", "string").add("x.x", "integer")))
    }
  }

  ddlTest("REPLACE COLUMNS - drop column") {
    // Column Mapping allows columns to be dropped
    def checkReplace(
                      text: String,
                      tableName: String,
                      columnDropped: Seq[String],
                      messages: String*): Unit = {
      if (!columnMappingEnabled) {
        assertNotSupported(text, messages: _*)
      } else {
        spark.sql(text)
        val deltaLog = getDeltaLog(tableName)
        val field = deltaLog.snapshot.schema
          .findNestedField(columnDropped, includeCollections = true)
        assert(field.isEmpty, "Column was not deleted")
      }
    }

    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("s", struct("v1", "v2"))
      .withColumn("a", array("s"))
      .withColumn("m", map(col("s"), col("s")))
    withDeltaTable(df) { tableName =>

      // trying to drop v1 of each struct, but it should fail because dropping column is
      // not supported unless column mapping is enabled
      checkReplace(
        s"""
           |ALTER TABLE $tableName REPLACE COLUMNS (
           |  v2 string,
           |  s STRUCT<v1:int, v2:string>,
           |  a ARRAY<STRUCT<v1:int, v2:string>>,
           |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
           |)""".stripMargin,
        tableName, Seq("v1"), "dropping column(s)", "v1")
      // s.v1
      checkReplace(
        s"""
           |ALTER TABLE $tableName REPLACE COLUMNS (
           |  v1 int,
           |  v2 string,
           |  s STRUCT<v2:string>,
           |  a ARRAY<STRUCT<v1:int, v2:string>>,
           |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
           |)""".stripMargin,
        tableName, Seq("s", "v1"), "dropping column(s)", "v1", "from s")
      // a.v1
      checkReplace(
        s"""
           |ALTER TABLE $tableName REPLACE COLUMNS (
           |  v1 int,
           |  v2 string,
           |  s STRUCT<v1:int, v2:string>,
           |  a ARRAY<STRUCT<v2:string>>,
           |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
           |)""".stripMargin,
        tableName, Seq("a", "element", "v1"), "dropping column(s)", "v1", "from a")
      // m.key.v1
      checkReplace(
        s"""
           |ALTER TABLE $tableName REPLACE COLUMNS (
           |  v1 int,
           |  v2 string,
           |  s STRUCT<v1:int, v2:string>,
           |  a ARRAY<STRUCT<v1:int, v2:string>>,
           |  m MAP<STRUCT<v2:string>, STRUCT<v1:int, v2:string>>
           |)""".stripMargin,
        tableName, Seq("m", "key", "v1"), "dropping column(s)", "v1", "from m.key")
      // m.value.v1
      checkReplace(
        s"""
           |ALTER TABLE $tableName REPLACE COLUMNS (
           |  v1 int,
           |  v2 string,
           |  s STRUCT<v1:int, v2:string>,
           |  a ARRAY<STRUCT<v1:int, v2:string>>,
           |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v2:string>>
           |)""".stripMargin,
        tableName, Seq("m", "value", "v1"), "dropping column(s)", "v1", "from m.value")
    }
  }

  ddlTest("REPLACE COLUMNS - incompatible data type") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("s", struct("v1", "v2"))
      .withColumn("a", array("s"))
      .withColumn("m", map(col("s"), col("s")))
    withDeltaTable(df) { tableName =>

      // trying to change the data type of v1 of each struct to long, but it should fail because
      // changing data type is not supported.
      assertNotSupported(s"""
                            |ALTER TABLE $tableName REPLACE COLUMNS (
                            |  v1 long,
                            |  v2 string,
                            |  s STRUCT<v1:int, v2:string>,
                            |  a ARRAY<STRUCT<v1:int, v2:string>>,
                            |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
                            |)""".stripMargin,
        "changing data type", "v1", "from IntegerType to LongType")
      // s.v1
      assertNotSupported(s"""
                            |ALTER TABLE $tableName REPLACE COLUMNS (
                            |  v1 int,
                            |  v2 string,
                            |  s STRUCT<v1:long, v2:string>,
                            |  a ARRAY<STRUCT<v1:int, v2:string>>,
                            |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
                            |)""".stripMargin,
        "changing data type", "s.v1", "from IntegerType to LongType")
      // a.element.v1
      assertNotSupported(s"""
                            |ALTER TABLE $tableName REPLACE COLUMNS (
                            |  v1 int,
                            |  v2 string,
                            |  s STRUCT<v1:int, v2:string>,
                            |  a ARRAY<STRUCT<v1:long, v2:string>>,
                            |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
                            |)""".stripMargin,
        "changing data type", "a.element.v1", "from IntegerType to LongType")
      // m.key.v1
      assertNotSupported(s"""
                            |ALTER TABLE $tableName REPLACE COLUMNS (
                            |  v1 int,
                            |  v2 string,
                            |  s STRUCT<v1:int, v2:string>,
                            |  a ARRAY<STRUCT<v1:int, v2:string>>,
                            |  m MAP<STRUCT<v1:long, v2:string>, STRUCT<v1:int, v2:string>>
                            |)""".stripMargin,
        "changing data type", "m.key.v1", "from IntegerType to LongType")
      // m.value.v1
      assertNotSupported(s"""
                            |ALTER TABLE $tableName REPLACE COLUMNS (
                            |  v1 int,
                            |  v2 string,
                            |  s STRUCT<v1:int, v2:string>,
                            |  a ARRAY<STRUCT<v1:int, v2:string>>,
                            |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:long, v2:string>>
                            |)""".stripMargin,
        "changing data type", "m.value.v1", "from IntegerType to LongType")
    }
  }

  ddlTest("REPLACE COLUMNS - case insensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("s", struct("v1", "v2"))
        .withColumn("a", array("s"))
        .withColumn("m", map(col("s"), col("s")))
      withDeltaTable(df) { tableName =>

        val deltaLog = getDeltaLog(tableName)
        def checkSchema(command: String): Unit = {
          sql(command)

          assertEqual(deltaLog.update().schema, new StructType()
            .add("v1", "integer")
            .add("v2", "string")
            .add("s", new StructType().add("v1", "integer").add("v2", "string"))
            .add("a", ArrayType(new StructType().add("v1", "integer").add("v2", "string")))
            .add("m", MapType(
              new StructType().add("v1", "integer").add("v2", "string"),
              new StructType().add("v1", "integer").add("v2", "string"))))
        }

        // trying to use V1 instead of v1 of each struct.
        checkSchema(s"""
                       |ALTER TABLE $tableName REPLACE COLUMNS (
                       |  V1 int,
                       |  v2 string,
                       |  s STRUCT<v1:int, v2:string>,
                       |  a ARRAY<STRUCT<v1:int, v2:string>>,
                       |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
                       |)""".stripMargin)
        // s.V1
        checkSchema(s"""
                       |ALTER TABLE $tableName REPLACE COLUMNS (
                       |  v1 int,
                       |  v2 string,
                       |  s STRUCT<V1:int, v2:string>,
                       |  a ARRAY<STRUCT<v1:int, v2:string>>,
                       |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
                       |)""".stripMargin)
        // a.V1
        checkSchema(s"""
                       |ALTER TABLE $tableName REPLACE COLUMNS (
                       |  v1 int,
                       |  v2 string,
                       |  s STRUCT<v1:int, v2:string>,
                       |  a ARRAY<STRUCT<V1:int, v2:string>>,
                       |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
                       |)""".stripMargin)
        // m.key.V1
        checkSchema(s"""
                       |ALTER TABLE $tableName REPLACE COLUMNS (
                       |  v1 int,
                       |  v2 string,
                       |  s STRUCT<v1:int, v2:string>,
                       |  a ARRAY<STRUCT<v1:int, v2:string>>,
                       |  m MAP<STRUCT<V1:int, v2:string>, STRUCT<v1:int, v2:string>>
                       |)""".stripMargin)
        // m.value.V1
        checkSchema(s"""
                       |ALTER TABLE $tableName REPLACE COLUMNS (
                       |  v1 int,
                       |  v2 string,
                       |  s STRUCT<v1:int, v2:string>,
                       |  a ARRAY<STRUCT<v1:int, v2:string>>,
                       |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<V1:int, v2:string>>
                       |)""".stripMargin)
      }
    }
  }

  ddlTest("REPLACE COLUMNS - case sensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("s", struct("v1", "v2"))
        .withColumn("a", array("s"))
        .withColumn("m", map(col("s"), col("s")))
      withDeltaTable(df) { tableName =>

        // trying to use V1 instead of v1 of each struct, but it should fail because case sensitive.
        assertNotSupported(s"""
                              |ALTER TABLE $tableName REPLACE COLUMNS (
                              |  V1 int,
                              |  v2 string,
                              |  s STRUCT<v1:int, v2:string>,
                              |  a ARRAY<STRUCT<v1:int, v2:string>>,
                              |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
                              |)""".stripMargin,
          "ambiguous", "v1")
        // s.V1
        assertNotSupported(s"""
                              |ALTER TABLE $tableName REPLACE COLUMNS (
                              |  v1 int,
                              |  v2 string,
                              |  s STRUCT<V1:int, v2:string>,
                              |  a ARRAY<STRUCT<v1:int, v2:string>>,
                              |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
                              |)""".stripMargin,
          "ambiguous", "data type of s")
        // a.V1
        assertNotSupported(s"""
                              |ALTER TABLE $tableName REPLACE COLUMNS (
                              | v1 int,
                              | v2 string,
                              | s STRUCT<v1:int, v2:string>,
                              | a ARRAY<STRUCT<V1:int, v2:string>>,
                              | m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
                              |)""".stripMargin,
          "ambiguous", "data type of a.element")
        // m.key.V1
        assertNotSupported(s"""
                              |ALTER TABLE $tableName REPLACE COLUMNS (
                              |  v1 int,
                              |  v2 string,
                              |  s STRUCT<v1:int, v2:string>,
                              |  a ARRAY<STRUCT<v1:int, v2:string>>,
                              |  m MAP<STRUCT<V1:int, v2:string>, STRUCT<v1:int, v2:string>>
                              |)""".stripMargin,
          "ambiguous", "data type of m.key")
        // m.value.V1
        assertNotSupported(s"""
                              |ALTER TABLE $tableName REPLACE COLUMNS (
                              |  v1 int,
                              |  v2 string,
                              |  s STRUCT<v1:int, v2:string>,
                              |  a ARRAY<STRUCT<v1:int, v2:string>>,
                              |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<V1:int, v2:string>>
                              |)""".stripMargin,
          "ambiguous", "data type of m.value")
      }
    }
  }

  ddlTest("REPLACE COLUMNS - duplicate") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("s", struct("v1", "v2"))
        .withColumn("a", array("s"))
        .withColumn("m", map(col("s"), col("s")))
      withDeltaTable(df) { tableName =>
        def assertDuplicate(command: String): Unit = {
          val ex = intercept[AnalysisException] {
            sql(command)
          }
          assert(ex.getMessage.contains("duplicate column(s)"))
        }

        // trying to add a V1 column, but it should fail because Delta doesn't allow columns
        // at the same level of nesting that differ only by case.
        assertDuplicate(s"""
                           |ALTER TABLE $tableName REPLACE COLUMNS (
                           |  v1 int,
                           |  V1 int,
                           |  v2 string,
                           |  s STRUCT<v1:int, v2:string>,
                           |  a ARRAY<STRUCT<v1:int, v2:string>>,
                           |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
                           |)""".stripMargin)
        // s.V1
        assertDuplicate(s"""
                           |ALTER TABLE $tableName REPLACE COLUMNS (
                           |  v1 int,
                           |  v2 string,
                           |  s STRUCT<v1:int, V1:int, v2:string>,
                           |  a ARRAY<STRUCT<v1:int, v2:string>>,
                           |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
                           |)""".stripMargin)
        // a.V1
        assertDuplicate(s"""
                           |ALTER TABLE $tableName REPLACE COLUMNS (
                           |  v1 int,
                           |  v2 string,
                           |  s STRUCT<v1:int, v2:string>,
                           |  a ARRAY<STRUCT<v1:int, V1:int, v2:string>>,
                           |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
                           |)""".stripMargin)
        // m.key.V1
        assertDuplicate(s"""
                           |ALTER TABLE $tableName REPLACE COLUMNS (
                           |   v1 int,
                           |   v2 string,
                           |   s STRUCT<v1:int, v2:string>,
                           |   a ARRAY<STRUCT<v1:int, v2:string>>,
                           |   m MAP<STRUCT<v1:int, V1:int, v2:string>, STRUCT<v1:int, v2:string>>
                           |)""".stripMargin)
        // m.value.V1
        assertDuplicate(s"""
                           |ALTER TABLE $tableName REPLACE COLUMNS (
                           |  v1 int,
                           |  v2 string,
                           |  s STRUCT<v1:int, v2:string>,
                           |  a ARRAY<STRUCT<v1:int, v2:string>>,
                           |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, V1:int, v2:string>>
                           |)""".stripMargin)
      }
    }
  }

  test("REPLACE COLUMNS - loosen nullability with unenforced allowed") {
    withSQLConf(("spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled", "true")) {
      val schema =
        """
          |  v1 int NOT NULL,
          |  v2 string,
          |  s STRUCT<v1:int NOT NULL, v2:string>,
          |  a ARRAY<STRUCT<v1:int NOT NULL, v2:string>>,
          |  m MAP<STRUCT<v1:int NOT NULL, v2:string>, STRUCT<v1:int NOT NULL, v2:string>>
        """.stripMargin
      withDeltaTable(schema) { tableName =>

        sql(
          s"""
             |ALTER TABLE $tableName REPLACE COLUMNS (
             |  v1 int,
             |  v2 string,
             |  s STRUCT<v1:int, v2:string>,
             |  a ARRAY<STRUCT<v1:int, v2:string>>,
             |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
             |)""".stripMargin)

        val deltaLog = getDeltaLog(tableName)
        assertEqual(deltaLog.snapshot.schema, new StructType()
          .add("v1", "integer")
          .add("v2", "string")
          .add("s", new StructType()
            .add("v1", "integer").add("v2", "string"))
          .add("a", ArrayType(new StructType()
            .add("v1", "integer").add("v2", "string")))
          .add("m", MapType(
            new StructType().add("v1", "integer").add("v2", "string"),
            new StructType().add("v1", "integer").add("v2", "string"))))
      }
    }
  }

  test("REPLACE COLUMNS - loosen nullability") {
    val schema =
      """
        |  v1 int NOT NULL,
        |  v2 string,
        |  s STRUCT<v1:int NOT NULL, v2:string>,
        |  a ARRAY<STRUCT<v1:int, v2:string>> NOT NULL,
        |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>> NOT NULL
      """.stripMargin
    withDeltaTable(schema) { tableName =>

      sql(s"""
             |ALTER TABLE $tableName REPLACE COLUMNS (
             |  v1 int,
             |  v2 string,
             |  s STRUCT<v1:int, v2:string>,
             |  a ARRAY<STRUCT<v1:int, v2:string>>,
             |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
             |)""".stripMargin)

      val deltaLog = getDeltaLog(tableName)
      assertEqual(deltaLog.snapshot.schema, new StructType()
        .add("v1", "integer")
        .add("v2", "string")
        .add("s", new StructType()
          .add("v1", "integer").add("v2", "string"))
        .add("a", ArrayType(new StructType()
          .add("v1", "integer").add("v2", "string")))
        .add("m", MapType(
          new StructType().add("v1", "integer").add("v2", "string"),
          new StructType().add("v1", "integer").add("v2", "string"))))
    }
  }

  test("REPLACE COLUMNS - add not-null column") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("s", struct("v1", "v2"))
      .withColumn("a", array("s"))
      .withColumn("m", map(col("s"), col("s")))
    withDeltaTable(df) { tableName =>
      // trying to add not-null column, but it should fail because adding not-null column is
      // not supported.
      assertNotSupported(s"""
                            |ALTER TABLE $tableName REPLACE COLUMNS (
                            |  v1 int,
                            |  v2 string,
                            |  v3 long NOT NULL,
                            |  s STRUCT<v1:int, v2:string>,
                            |  a ARRAY<STRUCT<v1:int, v2:string>>,
                            |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
                            |)""".stripMargin,
        "NOT NULL is not supported in Hive-style REPLACE COLUMNS")
      // s.v3
      assertNotSupported(s"""
                            |ALTER TABLE $tableName REPLACE COLUMNS (
                            |  v1 int,
                            |  v2 string,
                            |  s STRUCT<v1:int, v2:string, v3:long NOT NULL>,
                            |  a ARRAY<STRUCT<v1:int, v2:string>>,
                            |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
                            |)""".stripMargin,
        "adding non-nullable column", "s.v3")
      // a.element.v3
      assertNotSupported(s"""
                            |ALTER TABLE $tableName REPLACE COLUMNS (
                            |  v1 int,
                            |  v2 string,
                            |  s STRUCT<v1:int, v2:string>,
                            |  a ARRAY<STRUCT<v1:int, v2:string, v3:long NOT NULL>>,
                            |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
                            |)""".stripMargin,
        "adding non-nullable column", "a.element.v3")
      // m.key.v3
      assertNotSupported(s"""
                            |ALTER TABLE $tableName REPLACE COLUMNS (
                            |  v1 int,
                            |  v2 string,
                            |  s STRUCT<v1:int, v2:string>,
                            |  a ARRAY<STRUCT<v1:int, v2:string>>,
                            |  m MAP<STRUCT<v1:int, v2:string, v3:long NOT NULL>, STRUCT<v1:int, v2:string>>
                            |)""".stripMargin,
        "adding non-nullable column", "m.key.v3")
      // m.value.v3
      assertNotSupported(s"""
                            |ALTER TABLE $tableName REPLACE COLUMNS (
                            |  v1 int,
                            |  v2 string,
                            |  s STRUCT<v1:int, v2:string>,
                            |  a ARRAY<STRUCT<v1:int, v2:string>>,
                            |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string, v3:long NOT NULL>>
                            |)""".stripMargin,
        "adding non-nullable column", "m.value.v3")
    }
  }

  test("REPLACE COLUMNS - incompatible nullability") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("s", struct("v1", "v2"))
      .withColumn("a", array("s"))
      .withColumn("m", map(col("s"), col("s")))
    withDeltaTable(df) { tableName =>

      // trying to change the data type of v1 of each struct to not null, but it should fail because
      // tightening nullability is not supported.
      assertNotSupported(s"""
                            |ALTER TABLE $tableName REPLACE COLUMNS (
                            |  v1 int NOT NULL,
                            |  v2 string,
                            |  s STRUCT<v1:int, v2:string>,
                            |  a ARRAY<STRUCT<v1:int, v2:string>>,
                            |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
                            |)""".stripMargin,
        "NOT NULL is not supported in Hive-style REPLACE COLUMNS")
      // s.v1
      assertNotSupported(s"""
                            |ALTER TABLE $tableName REPLACE COLUMNS (
                            |  v1 int,
                            |  v2 string,
                            |  s STRUCT<v1:int NOT NULL, v2:string>,
                            |  a ARRAY<STRUCT<v1:int, v2:string>>,
                            |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
                            |)""".stripMargin,
        "tightening nullability", "s.v1")
      // a.element.v1
      assertNotSupported(s"""
                            |ALTER TABLE $tableName REPLACE COLUMNS (
                            |  v1 int,
                            |  v2 string,
                            |  s STRUCT<v1:int, v2:string>,
                            |  a ARRAY<STRUCT<v1:int NOT NULL, v2:string>>,
                            |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
                            |)""".stripMargin,
        "tightening nullability", "a.element.v1")
      // m.key.v1
      assertNotSupported(s"""
                            |ALTER TABLE $tableName REPLACE COLUMNS (
                            |  v1 int,
                            |  v2 string,
                            |  s STRUCT<v1:int, v2:string>,
                            |  a ARRAY<STRUCT<v1:int, v2:string>>,
                            |  m MAP<STRUCT<v1:int NOT NULL, v2:string>, STRUCT<v1:int, v2:string>>
                            |)""".stripMargin,
        "tightening nullability", "m.key.v1")
      // m.value.v1
      assertNotSupported(s"""
                            |ALTER TABLE $tableName REPLACE COLUMNS (
                            |  v1 int,
                            |  v2 string,
                            |  s STRUCT<v1:int, v2:string>,
                            |  a ARRAY<STRUCT<v1:int, v2:string>>,
                            |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int NOT NULL, v2:string>>
                            |)""".stripMargin,
        "tightening nullability", "m.value.v1")
    }
  }
}
