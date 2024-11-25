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

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaSQLConf.DELTA_STREAMING_SINK_ALLOW_IMPLICIT_CASTS.key, "false")
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
      // Dataframe insert by name don't support implicit cast, see negative test below.
      includeInserts = inserts -- insertsByName.intersect(insertsDataframe)
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
    includeInserts = insertsByName.intersect(insertsDataframe)
  )

  // Inserting using a different ordering for struct fields is full of surprises...
  for { (inserts: Set[Insert], expectedAnswer) <- Seq(
    // Most inserts use name based resolution for struct fields when there's no implicit cast
    // required due to mismatching data types, except for `INSERT INTO/OVERWRITE (columns)` and
    // `INSERT OVERWRITE PARTITION (partition) (columns)` which use position based resolution - even
    // though these are by name inserts.
    insertsAppend -
      SQLInsertColList(SaveMode.Append) ->
      TestData("a int, s struct <x int, y: int>",
        Seq("""{ "a": 1, "s": { "x": 2, "y": 3 } }""", """{ "a": 1, "s": { "x": 4, "y": 5 } }""")),
    insertsOverwrite -
      SQLInsertColList(SaveMode.Overwrite) - SQLInsertOverwritePartitionColList ->
      TestData("a int, s struct <x int, y: int>", Seq("""{ "a": 1, "s": { "x": 4, "y": 5 } }""")),
    Set(SQLInsertColList(SaveMode.Append)) ->
      TestData("a int, s struct <x int, y: int>",
        Seq("""{ "a": 1, "s": { "x": 2, "y": 3 } }""", """{ "a": 1, "s": { "x": 5, "y": 4 } }""")),
    Set(SQLInsertColList(SaveMode.Overwrite), SQLInsertOverwritePartitionColList) ->
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
      includeInserts = inserts
    )
  }

  for { (inserts: Set[Insert], expectedAnswer) <- Seq(
    // When there's a type mismatch and an implicit cast is required, then all inserts use position
    // based resolution for struct fields, except for `INSERT OVERWRITE PARTITION (partition)` which
    // uses name based resolution, and dataframe inserts by name which don't support implicit cast
    // and fail - see negative test below.
    insertsAppend - StreamingInsert ->
      TestData("a int, s struct <x int, y: int>",
        Seq("""{ "a": 1, "s": { "x": 2, "y": 3 } }""", """{ "a": 1, "s": { "x": 5, "y": 4 } }""")),
    insertsOverwrite - SQLInsertOverwritePartitionByPosition ->
      TestData("a int, s struct <x int, y: int>", Seq("""{ "a": 1, "s": { "x": 5, "y": 4 } }""")),
    Set(SQLInsertOverwritePartitionByPosition) ->
      TestData("a int, s struct <x int, y: int>", Seq("""{ "a": 1, "s": { "x": 4, "y": 5 } }"""))
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
      includeInserts = inserts -- insertsDataframe.intersect(insertsByName)
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
    includeInserts = insertsDataframe.intersect(insertsByName)
  )
}
