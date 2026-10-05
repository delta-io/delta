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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.internal.SQLConf

/**
 * Test suite covering INSERT operations with columns or struct fields ordered differently than in
 * the table schema.
 */
class DeltaInsertIntoColumnOrderSuite extends DeltaInsertIntoTest {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.ANSI_ENABLED.key, "true")
  }

  test("all test cases are implemented") {
    checkAllTestCasesImplemented()
  }

  // Inserting using a different ordering for top-level columns behaves as one would expect:
  // inserts by position resolve columns based on position, inserts by name resolve based on name.
  // Whether additional handling is required to add implicit casts doesn't impact this behavior.
  for { (inserts, expectedAnswer) <- Seq(
      insertsByPosition.intersect(insertsAppend) ->
        TestData("a int, b int, c int",
          Seq("""{ "a": 1, "b": 2, "c": 3 }""", """{ "a": 1, "b": 4, "c": 5 }""")),
      insertsByPosition.intersect(insertsOverwrite) ->
        TestData("a int, b int, c int", Seq("""{ "a": 1, "b": 4, "c": 5 }""")),
      insertsByName.intersect(insertsAppend) ->
        TestData("a int, b int, c int",
          Seq("""{ "a": 1, "b": 2, "c": 3 }""", """{ "a": 1, "b": 5, "c": 4 }""")),
      insertsByName.intersect(insertsOverwrite) ->
        TestData("a int, b int, c int", Seq("""{ "a": 1, "b": 5, "c": 4 }"""))
    )
  } {
    testInserts(s"insert with different top-level column ordering")(
      initialData = TestData("a int, b int, c int", Seq("""{ "a": 1, "b": 2, "c": 3 }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData("a int, c int, b int", Seq("""{ "a": 1, "c": 4, "b": 5 }""")),
      expectedResult = ExpectedResult.Success(expectedAnswer),
      includeInserts = inserts
    )

    testInserts(s"insert with implicit cast and different top-level column ordering")(
      initialData = TestData("a int, b int, c int", Seq("""{ "a": 1, "b": 2, "c": 3 }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData("a long, c int, b int", Seq("""{ "a": 1, "c": 4, "b": 5 }""")),
      expectedResult = ExpectedResult.Success(expectedAnswer),
      // Inserts that don't support implicit cast are failing, these are covered in the test below.
      includeInserts = inserts -- insertsWithoutImplicitCastSupport
    )
  }

  testInserts(s"insert with implicit cast and different top-level column ordering")(
    initialData = TestData("a int, b int, c int", Seq("""{ "a": 1, "b": 2, "c": 3 }""")),
    partitionBy = Seq("a"),
    overwriteWhere = "a" -> 1,
    insertData = TestData("a long, c int, b int", Seq("""{ "a": 1, "c": 4, "b": 4 }""")),
    expectedResult = ExpectedResult.Failure(ex => {
      checkError(
        ex,
        "DELTA_FAILED_TO_MERGE_FIELDS",
        parameters = Map(
          "currentField" -> "a",
          "updateField" -> "a"
        ))}),
    includeInserts = insertsWithoutImplicitCastSupport
  )

  // Inserting using a different ordering for struct fields is full of surprises...
  // `INSERT INTO/OVERWRITE (columns)` and `INSERT OVERWRITE PARTITION (partition) (columns)`
  // use position-based struct resolution even though they are by-name inserts.
  for { (inserts: Set[Insert], expectedAnswer) <- Seq(
    insertsAppend.intersect(insertsByName) - SQLInsertColList(SaveMode.Append) ->
      TestData("a int, s struct <x int, y: int>",
        Seq("""{ "a": 1, "s": { "x": 2, "y": 3 } }""", """{ "a": 1, "s": { "x": 4, "y": 5 } }""")),
    insertsOverwrite.intersect(insertsByName) -
        SQLInsertColList(SaveMode.Overwrite) - SQLInsertOverwritePartitionColList ->
      TestData("a int, s struct <x int, y: int>", Seq("""{ "a": 1, "s": { "x": 4, "y": 5 } }""")),
    insertsAppend.intersect(insertsByPosition) + SQLInsertColList(SaveMode.Append) ->
      TestData("a int, s struct <x int, y: int>",
        Seq("""{ "a": 1, "s": { "x": 2, "y": 3 } }""", """{ "a": 1, "s": { "x": 5, "y": 4 } }""")),
    insertsOverwrite.intersect(insertsByPosition) +
        SQLInsertColList(SaveMode.Overwrite) + SQLInsertOverwritePartitionColList ->
      TestData("a int, s struct <x int, y: int>", Seq("""{ "a": 1, "s": { "x": 5, "y": 4 } }"""))
    )
  } {
    testInserts(s"insert with different struct fields ordering")(
      initialData = TestData(
        "a int, s struct <x: int, y int>",
        Seq("""{ "a": 1, "s": { "x": 2, "y": 3 } }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData("a int, s struct <y int, x: int>",
        Seq("""{ "a": 1, "s": { "y": 5, "x": 4 } }""")),
      expectedResult = ExpectedResult.Success(expectedAnswer),
      includeInserts = inserts,
      confs = Seq(
        DeltaSQLConf.DELTA_INSERT_IMPLICIT_CAST_RESOLUTION_FIX_ENABLED.key -> "true")
    )
  }

  for { (inserts: Set[Insert], expectedAnswer) <- Seq(
    (insertsAppend.intersect(insertsByName) -- insertsWithoutImplicitCastSupport) -
        SQLInsertColList(SaveMode.Append) ->
      TestData("a int, s struct <x int, y: int>",
        Seq("""{ "a": 1, "s": { "x": 2, "y": 3 } }""", """{ "a": 1, "s": { "x": 4, "y": 5 } }""")),
    (insertsOverwrite.intersect(insertsByName) -- insertsWithoutImplicitCastSupport) -
        SQLInsertColList(SaveMode.Overwrite) - SQLInsertOverwritePartitionColList ->
      TestData("a int, s struct <x int, y: int>", Seq("""{ "a": 1, "s": { "x": 4, "y": 5 } }""")),
    insertsAppend.intersect(insertsByPosition) + SQLInsertColList(SaveMode.Append) ->
      TestData("a int, s struct <x int, y: int>",
        Seq("""{ "a": 1, "s": { "x": 2, "y": 3 } }""", """{ "a": 1, "s": { "x": 5, "y": 4 } }""")),
    insertsOverwrite.intersect(insertsByPosition) +
        SQLInsertColList(SaveMode.Overwrite) + SQLInsertOverwritePartitionColList ->
      TestData("a int, s struct <x int, y: int>", Seq("""{ "a": 1, "s": { "x": 5, "y": 4 } }"""))
    )
  } {
    testInserts(s"insert with implicit cast and different struct fields ordering")(
      initialData = TestData(
        "a int, s struct <x: int, y int>",
        Seq("""{ "a": 1, "s": { "x": 2, "y": 3 } }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData("a long, s struct <y int, x: int>",
        Seq("""{ "a": 1, "s": { "y": 5, "x": 4 } }""")),
      expectedResult = ExpectedResult.Success(expectedAnswer),
      includeInserts = inserts,
      confs = Seq(
        DeltaSQLConf.DELTA_INSERT_IMPLICIT_CAST_RESOLUTION_FIX_ENABLED.key -> "true"
      )
    )
  }

  testInserts(s"insert with implicit cast and different struct fields ordering")(
    initialData = TestData(
      "a int, s struct <x: int, y int>",
      Seq("""{ "a": 1, "s": { "x": 2, "y": 3 } }""")),
    partitionBy = Seq("a"),
    overwriteWhere = "a" -> 1,
    insertData = TestData("a long, s struct <y int, x: int>",
      Seq("""{ "a": 1, "s": { "y": 5, "x": 4 } }""")),
    expectedResult = ExpectedResult.Failure(ex => {
      checkError(
        ex,
        "DELTA_FAILED_TO_MERGE_FIELDS",
        parameters = Map(
          "currentField" -> "a",
          "updateField" -> "a"
        ))}),
    includeInserts = insertsWithoutImplicitCastSupport
  )

  for {
    preserveNullSourceStructs <- BOOLEAN_DOMAIN
    (inserts: Set[Insert], expectedAnswer) <- Seq(
      insertsAppend ->
        TestData("a int, s struct <x int, y: int>",
          Seq("""{ "a": 1, "s": { "x": 2, "y": 3 } }""", """{ "a": 1, "s": null }""")),
      insertsOverwrite ->
        TestData("a int, s struct <x int, y: int>", Seq("""{ "a": 1, "s": null }"""))
    )
  } {
    testInserts(s"null struct with different field order, " +
        s"preserveNullSourceStructs=$preserveNullSourceStructs")(
      initialData = TestData(
        "a int, s struct <x: int, y int>",
        Seq("""{ "a": 1, "s": { "x": 2, "y": 3 } }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData("a int, s struct <y int, x: int>", Seq("""{ "a": 1, "s": null }""")),
      expectedResult = ExpectedResult.Success(expectedAnswer),
      includeInserts = inserts,
      confs = Seq(
        // Implicit casts in streaming writes would cause the `null` struct to be incorrectly
        // expanded. This conf allows skipping adding casts since there are no actual data type
        // mismatch.
        DeltaSQLConf.DELTA_STREAMING_SINK_IMPLICIT_CAST_FOR_TYPE_MISMATCH_ONLY.key -> "true",
        DeltaSQLConf.DELTA_INSERT_PRESERVE_NULL_SOURCE_STRUCTS.key
          -> preserveNullSourceStructs.toString,
        // With null preservation disabled, both implicit-casting paths expand a null struct for a
        // field-order-only mismatch. This narrow case does not justify extra casting complexity.
        DeltaSQLConf.DELTA_INSERT_IMPLICIT_CAST_RESOLUTION_FIX_ENABLED.key
          -> preserveNullSourceStructs.toString,
        DeltaSQLConf.DELTA_DF_WRITE_ALLOW_IMPLICIT_CASTS.key
          -> preserveNullSourceStructs.toString
      )
    )
  }

  // Tests that DELTA_INSERT_IMPLICIT_CAST_RESOLUTION_FIX_ENABLED controls whether
  // DeltaImplicitCast handles non-DF-by-name inserts.

  test("DELTA_INSERT_IMPLICIT_CAST_RESOLUTION_FIX_ENABLED=false: SQL BY NAME uses old behavior") {
    withTable("target") {
      withSQLConf(DeltaSQLConf.DELTA_INSERT_IMPLICIT_CAST_RESOLUTION_FIX_ENABLED.key -> "false") {
        TestData("a int, b long", Seq("""{ "a": 1, "b": 2 }""")).toDF
          .write.format("delta").saveAsTable("target")
        // With the fix disabled, SQL BY NAME should still succeed via DeltaAnalysis.
        sql("INSERT INTO target BY NAME SELECT 1 as a, CAST(3 as int) as b")
        checkAnswer(spark.table("target"),
          TestData("a int, b long",
            Seq("""{ "a": 1, "b": 2 }""", """{ "a": 1, "b": 3 }""")).toDF)
      }
    }
  }

  test("DELTA_INSERT_IMPLICIT_CAST_RESOLUTION_FIX_ENABLED=false: " +
      "DF by-name still uses DeltaImplicitCast") {
    withTable("target") {
      withSQLConf(DeltaSQLConf.DELTA_INSERT_IMPLICIT_CAST_RESOLUTION_FIX_ENABLED.key -> "false") {
        TestData("a int, b long", Seq("""{ "a": 1, "b": 2 }""")).toDF
          .write.format("delta").saveAsTable("target")
        // DF by-name is controlled by DELTA_DF_WRITE_ALLOW_IMPLICIT_CASTS, not this flag.
        TestData("a int, b int", Seq("""{ "a": 1, "b": 3 }""")).toDF
          .writeTo("target").append()
        checkAnswer(spark.table("target"),
          TestData("a int, b long",
            Seq("""{ "a": 1, "b": 2 }""", """{ "a": 1, "b": 3 }""")).toDF)
      }
    }
  }

  test("DELTA_INSERT_IMPLICIT_CAST_RESOLUTION_FIX_ENABLED=false: by-position uses old behavior") {
    withTable("target") {
      withSQLConf(DeltaSQLConf.DELTA_INSERT_IMPLICIT_CAST_RESOLUTION_FIX_ENABLED.key -> "false") {
        TestData("a int, b long", Seq("""{ "a": 1, "b": 2 }""")).toDF
          .write.format("delta").saveAsTable("target")
        // With the fix disabled, insertInto (by-position) should still succeed via DeltaAnalysis.
        TestData("a int, b int", Seq("""{ "a": 1, "b": 3 }""")).toDF
          .write.mode(SaveMode.Append).format("delta").insertInto("target")
        checkAnswer(spark.table("target"),
          TestData("a int, b long",
            Seq("""{ "a": 1, "b": 2 }""", """{ "a": 1, "b": 3 }""")).toDF)
      }
    }
  }
}
