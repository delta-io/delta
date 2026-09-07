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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.internal.SQLConf

/**
 * Test suite covering INSERT operations with struct fields ordered differently
 * than in the table schema for complex nested types: structs containing arrays
 * of structs, structs containing maps with struct values, and arrays of maps
 * with struct values.
 *
 * Mirrors the structure of [[DeltaInsertIntoColumnOrderSuite]] but focuses on
 * deeply nested types where the struct field reordering is buried inside
 * container types (ArrayType, MapType).
 */
class DeltaInsertIntoColumnOrderAdvancedSuite extends DeltaInsertIntoTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.ANSI_ENABLED.key, "true")
  }

  test("all test cases are implemented") {
    checkAllTestCasesImplemented()
  }

  private val spark43ColumnListInserts: Set[Insert] = Set(
    SQLInsertColList(SaveMode.Append),
    SQLInsertColList(SaveMode.Overwrite),
    SQLInsertOverwritePartitionColList)

  private def expectedResultFor(
      insert: Insert,
      defaultExpectedAnswer: TestData,
      spark43InsertedRow: String): ExpectedResult[TestData] = {
    // Spark 4.3 matches struct fields inside arrays and maps by position for this INSERT syntax.
    // For example, inserting {y: 5, x: 4} into {x, y} produces {x: 5, y: 4}.
    // INSERT BY NAME still matches x and y by name.
    val expectedAnswer = if (org.apache.spark.SPARK_VERSION.startsWith("4.3") &&
        spark43ColumnListInserts.contains(insert)) {
      defaultExpectedAnswer.copy(
        data = defaultExpectedAnswer.data.dropRight(1) :+ spark43InsertedRow)
    } else {
      defaultExpectedAnswer
    }
    ExpectedResult.Success(expectedAnswer)
  }

  // --- struct<arr: array<struct<x, y>>>: no cast ---

  for {
    (inserts: Set[Insert], expectedAnswer) <- Seq(
      insertsAppend ->
        TestData("a int, s struct<arr: array<struct<x: int, y: int>>>",
          Seq("""{ "a": 1, "s": { "arr": [{ "x": 2, "y": 3 }] } }""",
              """{ "a": 1, "s": { "arr": [{ "x": 4, "y": 5 }] } }""")),
      insertsOverwrite ->
        TestData("a int, s struct<arr: array<struct<x: int, y: int>>>",
          Seq("""{ "a": 1, "s": { "arr": [{ "x": 4, "y": 5 }] } }"""))
    )
    insert <- inserts
  } {
    testInserts("struct with array of structs field reordering")(
      initialData = TestData(
        "a int, s struct<arr: array<struct<x: int, y: int>>>",
        Seq("""{ "a": 1, "s": { "arr": [{ "x": 2, "y": 3 }] } }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData(
        "a int, s struct<arr: array<struct<y: int, x: int>>>",
        Seq("""{ "a": 1, "s": { "arr": [{ "y": 5, "x": 4 }] } }""")),
      expectedResult = expectedResultFor(
        insert, expectedAnswer, """{ "a": 1, "s": { "arr": [{ "x": 5, "y": 4 }] } }"""),
      includeInserts = Set(insert)
    )
  }

  // --- struct<arr: array<struct<x, y>>>: with implicit cast ---

  for {
    (inserts: Set[Insert], expectedAnswer) <- Seq(
      insertsAppend - StreamingInsert
        -- insertsByName.intersect(insertsDataframe) ->
        TestData("a int, s struct<arr: array<struct<x: int, y: int>>>",
          Seq("""{ "a": 1, "s": { "arr": [{ "x": 2, "y": 3 }] } }""",
              """{ "a": 1, "s": { "arr": [{ "x": 5, "y": 4 }] } }""")),
      insertsOverwrite
        -- insertsByName.intersect(insertsDataframe) ->
        TestData("a int, s struct<arr: array<struct<x: int, y: int>>>",
          Seq("""{ "a": 1, "s": { "arr": [{ "x": 5, "y": 4 }] } }""")),
      (insertsAppend.intersect(insertsByName.intersect(insertsDataframe))
        -- insertsWithoutImplicitCastSupport - StreamingInsert) ->
        TestData("a int, s struct<arr: array<struct<x: int, y: int>>>",
          Seq("""{ "a": 1, "s": { "arr": [{ "x": 2, "y": 3 }] } }""",
              """{ "a": 1, "s": { "arr": [{ "x": 4, "y": 5 }] } }""")),
      (insertsOverwrite.intersect(insertsByName.intersect(insertsDataframe))
        -- insertsWithoutImplicitCastSupport) ->
        TestData("a int, s struct<arr: array<struct<x: int, y: int>>>",
          Seq("""{ "a": 1, "s": { "arr": [{ "x": 4, "y": 5 }] } }""")),
      Set(StreamingInsert) ->
        TestData("a int, s struct<arr: array<struct<x: int, y: int>>>",
          Seq("""{ "a": 1, "s": { "arr": [{ "x": 2, "y": 3 }] } }""",
              """{ "a": 1, "s": { "arr": [{ "x": 4, "y": 5 }] } }"""))
    )
  } {
    testInserts(
      "struct with array of structs field reordering with implicit cast")(
      initialData = TestData(
        "a int, s struct<arr: array<struct<x: int, y: int>>>",
        Seq("""{ "a": 1, "s": { "arr": [{ "x": 2, "y": 3 }] } }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData(
        "a int, s struct<arr: array<struct<y: long, x: int>>>",
        Seq("""{ "a": 1, "s": { "arr": [{ "y": 5, "x": 4 }] } }""")),
      expectedResult = ExpectedResult.Success(expectedAnswer),
      includeInserts = inserts
    )
  }

  testInserts(
    "struct with array of structs field reordering with implicit cast")(
    initialData = TestData(
      "a int, s struct<arr: array<struct<x: int, y: int>>>",
      Seq("""{ "a": 1, "s": { "arr": [{ "x": 2, "y": 3 }] } }""")),
    partitionBy = Seq("a"),
    overwriteWhere = "a" -> 1,
    insertData = TestData(
      "a int, s struct<arr: array<struct<y: long, x: int>>>",
      Seq("""{ "a": 1, "s": { "arr": [{ "y": 5, "x": 4 }] } }""")),
    expectedResult = ExpectedResult.Failure(ex => {
      checkError(ex, "DELTA_FAILED_TO_MERGE_FIELDS",
        parameters = Map(
          "currentField" -> "s", "updateField" -> "s"))
    }),
    includeInserts = insertsWithoutImplicitCastSupport
  )

  // --- struct<m: map<string, struct<x, y>>>: no cast ---

  for {
    (inserts: Set[Insert], expectedAnswer) <- Seq(
      insertsAppend ->
        TestData(
          "a int, s struct<m: map<string, struct<x: int, y: int>>>",
          Seq(
            """{ "a": 1, "s": { "m": { "k": { "x": 2, "y": 3 } } } }""",
            """{ "a": 1, "s": { "m": { "k": { "x": 4, "y": 5 } } } }""")),
      insertsOverwrite ->
        TestData(
          "a int, s struct<m: map<string, struct<x: int, y: int>>>",
          Seq(
            """{ "a": 1, "s": { "m": { "k": { "x": 4, "y": 5 } } } }"""))
    )
    insert <- inserts
  } {
    testInserts("struct with map of structs field reordering")(
      initialData = TestData(
        "a int, s struct<m: map<string, struct<x: int, y: int>>>",
        Seq(
          """{ "a": 1, "s": { "m": { "k": { "x": 2, "y": 3 } } } }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData(
        "a int, s struct<m: map<string, struct<y: int, x: int>>>",
        Seq(
          """{ "a": 1, "s": { "m": { "k": { "y": 5, "x": 4 } } } }""")),
      expectedResult = expectedResultFor(
        insert,
        expectedAnswer,
        """{ "a": 1, "s": { "m": { "k": { "x": 5, "y": 4 } } } }"""),
      includeInserts = Set(insert)
    )
  }

  // --- struct<m: map<string, struct<x, y>>>: with implicit cast ---

  for {
    (inserts: Set[Insert], expectedAnswer) <- Seq(
      insertsAppend - StreamingInsert
        -- insertsByName.intersect(insertsDataframe) ->
        TestData(
          "a int, s struct<m: map<string, struct<x: int, y: int>>>",
          Seq(
            """{ "a": 1, "s": { "m": { "k": { "x": 2, "y": 3 } } } }""",
            """{ "a": 1, "s": { "m": { "k": { "x": 5, "y": 4 } } } }""")),
      insertsOverwrite
        -- insertsByName.intersect(insertsDataframe) ->
        TestData(
          "a int, s struct<m: map<string, struct<x: int, y: int>>>",
          Seq(
            """{ "a": 1, "s": { "m": { "k": { "x": 5, "y": 4 } } } }""")),
      (insertsAppend.intersect(insertsByName.intersect(insertsDataframe))
        -- insertsWithoutImplicitCastSupport - StreamingInsert) ->
        TestData(
          "a int, s struct<m: map<string, struct<x: int, y: int>>>",
          Seq(
            """{ "a": 1, "s": { "m": { "k": { "x": 2, "y": 3 } } } }""",
            """{ "a": 1, "s": { "m": { "k": { "x": 4, "y": 5 } } } }""")),
      (insertsOverwrite.intersect(insertsByName.intersect(insertsDataframe))
        -- insertsWithoutImplicitCastSupport) ->
        TestData(
          "a int, s struct<m: map<string, struct<x: int, y: int>>>",
          Seq(
            """{ "a": 1, "s": { "m": { "k": { "x": 4, "y": 5 } } } }""")),
      Set(StreamingInsert) ->
        TestData(
          "a int, s struct<m: map<string, struct<x: int, y: int>>>",
          Seq(
            """{ "a": 1, "s": { "m": { "k": { "x": 2, "y": 3 } } } }""",
            """{ "a": 1, "s": { "m": { "k": { "x": 4, "y": 5 } } } }"""))
    )
  } {
    testInserts(
      "struct with map of structs field reordering with implicit cast")(
      initialData = TestData(
        "a int, s struct<m: map<string, struct<x: int, y: int>>>",
        Seq(
          """{ "a": 1, "s": { "m": { "k": { "x": 2, "y": 3 } } } }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData(
        "a int, s struct<m: map<string, struct<y: long, x: int>>>",
        Seq(
          """{ "a": 1, "s": { "m": { "k": { "y": 5, "x": 4 } } } }""")),
      expectedResult = ExpectedResult.Success(expectedAnswer),
      includeInserts = inserts
    )
  }

  testInserts(
    "struct with map of structs field reordering with implicit cast")(
    initialData = TestData(
      "a int, s struct<m: map<string, struct<x: int, y: int>>>",
      Seq(
        """{ "a": 1, "s": { "m": { "k": { "x": 2, "y": 3 } } } }""")),
    partitionBy = Seq("a"),
    overwriteWhere = "a" -> 1,
    insertData = TestData(
      "a int, s struct<m: map<string, struct<y: long, x: int>>>",
      Seq(
        """{ "a": 1, "s": { "m": { "k": { "y": 5, "x": 4 } } } }""")),
    expectedResult = ExpectedResult.Failure(ex => {
      checkError(ex, "DELTA_FAILED_TO_MERGE_FIELDS",
        parameters = Map(
          "currentField" -> "s", "updateField" -> "s"))
    }),
    includeInserts = insertsWithoutImplicitCastSupport
  )

  // --- array<map<string, struct<x, y>>>: no cast ---

  for {
    (inserts: Set[Insert], expectedAnswer) <- Seq(
      insertsAppend ->
        TestData(
          "a int, complex array<map<string, struct<x: int, y: int>>>",
          Seq(
            """{ "a": 1, "complex": [{ "k": { "x": 2, "y": 3 } }] }""",
            """{ "a": 1, "complex": [{ "k": { "x": 4, "y": 5 } }] }""")),
      insertsOverwrite ->
        TestData(
          "a int, complex array<map<string, struct<x: int, y: int>>>",
          Seq(
            """{ "a": 1, "complex": [{ "k": { "x": 4, "y": 5 } }] }"""))
    )
    insert <- inserts
  } {
    testInserts("array of maps with struct values field reordering")(
      initialData = TestData(
        "a int, complex array<map<string, struct<x: int, y: int>>>",
        Seq(
          """{ "a": 1, "complex": [{ "k": { "x": 2, "y": 3 } }] }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData(
        "a int, complex array<map<string, struct<y: int, x: int>>>",
        Seq(
          """{ "a": 1, "complex": [{ "k": { "y": 5, "x": 4 } }] }""")),
      expectedResult = expectedResultFor(
        insert,
        expectedAnswer,
        """{ "a": 1, "complex": [{ "k": { "x": 5, "y": 4 } }] }"""),
      includeInserts = Set(insert)
    )
  }

  // --- array<map<string, struct<x, y>>>: with implicit cast ---
  // Streaming uses by-position for top-level array<map<...>>.

  for {
    (inserts: Set[Insert], expectedAnswer) <- Seq(
      insertsAppend - StreamingInsert
        -- insertsByName.intersect(insertsDataframe) ->
        TestData(
          "a int, complex array<map<string, struct<x: int, y: int>>>",
          Seq(
            """{ "a": 1, "complex": [{ "k": { "x": 2, "y": 3 } }] }""",
            """{ "a": 1, "complex": [{ "k": { "x": 5, "y": 4 } }] }""")),
      insertsOverwrite
        -- insertsByName.intersect(insertsDataframe) ->
        TestData(
          "a int, complex array<map<string, struct<x: int, y: int>>>",
          Seq(
            """{ "a": 1, "complex": [{ "k": { "x": 5, "y": 4 } }] }""")),
      (insertsAppend.intersect(insertsByName.intersect(insertsDataframe))
        -- insertsWithoutImplicitCastSupport - StreamingInsert) ->
        TestData(
          "a int, complex array<map<string, struct<x: int, y: int>>>",
          Seq(
            """{ "a": 1, "complex": [{ "k": { "x": 2, "y": 3 } }] }""",
            """{ "a": 1, "complex": [{ "k": { "x": 4, "y": 5 } }] }""")),
      (insertsOverwrite.intersect(insertsByName.intersect(insertsDataframe))
        -- insertsWithoutImplicitCastSupport) ->
        TestData(
          "a int, complex array<map<string, struct<x: int, y: int>>>",
          Seq(
            """{ "a": 1, "complex": [{ "k": { "x": 4, "y": 5 } }] }""")),
      Set(StreamingInsert) ->
        TestData(
          "a int, complex array<map<string, struct<x: int, y: int>>>",
          Seq(
            """{ "a": 1, "complex": [{ "k": { "x": 2, "y": 3 } }] }""",
            """{ "a": 1, "complex": [{ "k": { "x": 5, "y": 4 } }] }"""))
    )
  } {
    testInserts(
      "array of maps with struct values field reordering with implicit cast"
    )(
      initialData = TestData(
        "a int, complex array<map<string, struct<x: int, y: int>>>",
        Seq(
          """{ "a": 1, "complex": [{ "k": { "x": 2, "y": 3 } }] }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData(
        "a int, complex array<map<string, struct<y: long, x: int>>>",
        Seq(
          """{ "a": 1, "complex": [{ "k": { "y": 5, "x": 4 } }] }""")),
      expectedResult = ExpectedResult.Success(expectedAnswer),
      includeInserts = inserts
    )
  }

  testInserts(
    "array of maps with struct values field reordering with implicit cast"
  )(
    initialData = TestData(
      "a int, complex array<map<string, struct<x: int, y: int>>>",
      Seq(
        """{ "a": 1, "complex": [{ "k": { "x": 2, "y": 3 } }] }""")),
    partitionBy = Seq("a"),
    overwriteWhere = "a" -> 1,
    insertData = TestData(
      "a int, complex array<map<string, struct<y: long, x: int>>>",
      Seq(
        """{ "a": 1, "complex": [{ "k": { "y": 5, "x": 4 } }] }""")),
    expectedResult = ExpectedResult.Failure(ex => {
      checkError(ex, "DELTA_FAILED_TO_MERGE_FIELDS",
        parameters = Map(
          "currentField" -> "complex",
          "updateField" -> "complex"))
    }),
    includeInserts = insertsWithoutImplicitCastSupport
  )
}
