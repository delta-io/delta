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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Test suite covering behavior of INSERT operations with missing top-level columns or nested struct
 * fields.
 * This suite intends to exhaustively cover all the ways INSERT can be run on a Delta table. See
 * [[DeltaInsertIntoTest]] for a list of these INSERT operations covered.
 */
class DeltaInsertIntoMissingColumnSuite extends DeltaInsertIntoTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaSQLConf.DELTA_STREAMING_SINK_ALLOW_IMPLICIT_CASTS.key, "true")
    spark.conf.set(SQLConf.ANSI_ENABLED.key, "true")
  }

  test("all test cases are implemented") {
    checkAllTestCasesImplemented()
  }

  for (schemaEvolution <- BOOLEAN_DOMAIN) {
    // Missing top-level columns are allowed for all inserts by name (SQL+dataframe) but missing
    // nested fields are only allowed for dataframe inserts by name.
    testInserts(s"insert with missing top-level column, schemaEvolution=$schemaEvolution")(
      initialData = TestData("a int, b int, c int", Seq("""{ "a": 1, "b": 2, "c": 3 }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData("a int, b int", Seq("""{ "a": 1, "b": 4 }""")),
      expectedResult = ExpectedResult.Success(
        expected = new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType)
          .add("c", IntegerType)),
      includeInserts = insertsByName,
      confs = Seq(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolution.toString)
    )

    testInserts(s"insert with missing nested field, schemaEvolution=$schemaEvolution")(
      initialData =
        TestData("a int, s struct<x: int, y: int>", Seq("""{ "a": 1, "s": { "x": 2, "y": 3 } }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData =
        TestData("a int, s struct<y: int>", Seq("""{ "a": 1, "s": { "y": 5 } }""")),
      expectedResult = ExpectedResult.Success(
        expected = new StructType()
          .add("a", IntegerType)
          .add("s", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
          )),
      includeInserts = insertsByName.intersect(insertsDataframe),
      confs = Seq(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolution.toString)
    )

    // Missing columns for all inserts by name and missing nested fields for dataframe inserts by
    // name are also allowed when the insert includes type mismatches, with the difference that
    // dataframe inserts by name don't support implicit casting and will fail due to the type
    // mismatch (but not the missing column/field per se).
    testInserts(s"insert with implicit cast and missing top-level column," +
      s"schemaEvolution=$schemaEvolution")(
      initialData = TestData("a long, b int, c int", Seq("""{ "a": 1, "b": 2, "c": 3 }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData("a int, b long", Seq("""{ "a": 1, "b": 4 }""")),
      expectedResult = ExpectedResult.Success(
        expected = new StructType()
          .add("a", LongType)
          .add("b", IntegerType)
          .add("c", IntegerType)),
      includeInserts = insertsByName.intersect(insertsSQL) + StreamingInsert,
      confs = Seq(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolution.toString)
    )

    testInserts(s"insert with implicit cast and missing top-level column," +
      s"schemaEvolution=$schemaEvolution")(
      initialData = TestData("a long, b int, c int", Seq("""{ "a": 1, "b": 2, "c": 3 }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData("a int, b long", Seq("""{ "a": 1, "b": 4 }""")),
      expectedResult = ExpectedResult.Failure(ex => {
        // The missing column isn't an issue, but dataframe inserts by name (except streaming) don't
        // support implicit casting to reconcile the type mismatch.
        checkError(
          ex,
          "DELTA_FAILED_TO_MERGE_FIELDS",
          parameters = Map(
            "currentField" -> "a",
            "updateField" -> "a"
          ))
      }),
      includeInserts = insertsByName.intersect(insertsDataframe) - StreamingInsert,
      confs = Seq(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolution.toString)
    )

    testInserts(s"insert with implicit cast and missing nested field," +
      s"schemaEvolution=$schemaEvolution")(
      initialData =
        TestData("a int, s struct<x: int, y: int>", Seq("""{ "a": 1, "s": { "x": 2, "y": 3 } }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData =
        TestData("a int, s struct<y: long>", Seq("""{ "a": 1, "s": { "y": 5 } }""")),
      expectedResult = ExpectedResult.Failure(ex => {
        // The missing column isn't an issue, but dataframe inserts by name (except streaming) don't
        // support implicit casting to reconcile the type mismatch.
        checkError(
          ex,
          "DELTA_FAILED_TO_MERGE_FIELDS",
          parameters = Map(
            "currentField" -> "s",
            "updateField" -> "s"
          ))
      }),
      includeInserts = insertsByName.intersect(insertsDataframe) - StreamingInsert,
      confs = Seq(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolution.toString)
    )

  testInserts(s"insert with implicit cast and missing nested field," +
      s"schemaEvolution=$schemaEvolution")(
      initialData =
        TestData("a int, s struct<x: int, y: int>", Seq("""{ "a": 1, "s": { "x": 2, "y": 3 } }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData =
        TestData("a int, s struct<y: long>", Seq("""{ "a": 1, "s": { "y": 5 } }""")),
      // Missing nested fields are allowed when writing to a delta streaming sink when there's a
      // type mismatch, same as when there's no type mismatch.
      expectedResult = ExpectedResult.Success(
        expected = new StructType()
          .add("a", IntegerType)
          .add("s", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType))),
      includeInserts = Set(StreamingInsert),
      confs = Seq(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolution.toString)
    )

    // Missing columns for all inserts by position and missing nested fields for all inserts by
    // position or SQL inserts are rejected. Whether the insert also includes a type mismatch
    // doesn't play a role.
    testInserts(s"insert with missing top-level column, schemaEvolution=$schemaEvolution")(
      initialData = TestData("a int, b int, c int", Seq("""{ "a": 1, "b": 2, "c": 3 }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData("a int, b int", Seq("""{ "a": 1, "b": 4 }""")),
      expectedResult = ExpectedResult.Failure(ex => {
        checkError(
          ex,
          "DELTA_INSERT_COLUMN_ARITY_MISMATCH",
          parameters = Map(
            "tableName" -> "spark_catalog.default.target",
            "columnName" -> "not enough data columns",
            "numColumns" -> "3",
            "insertColumns" -> "2"
          ))
      }),
      includeInserts = insertsByPosition,
      confs = Seq(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolution.toString)
    )

    testInserts(s"insert with implicit cast and missing top-level column," +
      s"schemaEvolution=$schemaEvolution")(
      initialData = TestData("a long, b int, c int", Seq("""{ "a": 1, "b": 2, "c": 3 }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData("a int, b long", Seq("""{ "a": 1, "b": 4 }""")),
      expectedResult = ExpectedResult.Failure(ex => {
        checkError(
          ex,
          "DELTA_INSERT_COLUMN_ARITY_MISMATCH",
          parameters = Map(
            "tableName" -> "spark_catalog.default.target",
            "columnName" -> "not enough data columns",
            "numColumns" -> "3",
            "insertColumns" -> "2"
          ))
      }),
      includeInserts = insertsByPosition,
      confs = Seq(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolution.toString)
    )

    testInserts(s"insert with missing nested field, schemaEvolution=$schemaEvolution")(
      initialData =
        TestData("a int, s struct<x: int, y: int>", Seq("""{ "a": 1, "s": { "x": 2, "y": 3 } }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData =
        TestData("a int, s struct<y: int>", Seq("""{ "a": 1, "s": { "y": 5 } }""")),
      expectedResult = ExpectedResult.Failure(ex => {
        checkErrorMatchPVals(
          ex,
          "DELTA_INSERT_COLUMN_ARITY_MISMATCH",
          parameters = Map(
            "tableName" -> "spark_catalog\\.default\\.target",
            "columnName" -> "not enough nested fields in (spark_catalog\\.default\\.source\\.)?s",
            "numColumns" -> "2",
            "insertColumns" -> "1"
          ))
      }),
      includeInserts = insertsByPosition ++ insertsSQL,
      confs = Seq(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolution.toString)
    )

    testInserts(s"insert with implicit cast and missing nested field," +
      s"schemaEvolution=$schemaEvolution")(
      initialData =
        TestData("a int, s struct<x: int, y: int>", Seq("""{ "a": 1, "s": { "x": 2, "y": 3 } }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData =
        TestData("a int, s struct<y: long>", Seq("""{ "a": 1, "s": { "y": 5 } }""")),
      expectedResult = ExpectedResult.Failure(ex => {
        checkErrorMatchPVals(
          ex,
          "DELTA_INSERT_COLUMN_ARITY_MISMATCH",
          parameters = Map(
            "tableName" -> "spark_catalog\\.default\\.target",
            "columnName" -> "not enough nested fields in (spark_catalog\\.default\\.source\\.)?s",
            "numColumns" -> "2",
            "insertColumns" -> "1"
          ))
      }),
      includeInserts = insertsByPosition ++ insertsSQL,
      confs = Seq(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolution.toString)
    )
  }
}
