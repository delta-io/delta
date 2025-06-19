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

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.types._

class TypeWideningInsertSchemaEvolutionExtendedSuite
  extends QueryTest
  with DeltaDMLTestUtils
  with TypeWideningTestMixin
  with TypeWideningInsertSchemaEvolutionExtendedTests {

  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key, "true")
  }
}

trait TypeWideningInsertSchemaEvolutionExtendedTests
  extends DeltaInsertIntoTest
  with TypeWideningTestCases {
  self: QueryTest with TypeWideningTestMixin with DeltaDMLTestUtils =>

  testInserts("top-level type evolution")(
    initialData = TestData("a int, b short", Seq("""{ "a": 1, "b": 2 }""")),
    partitionBy = Seq("a"),
    overwriteWhere = "a" -> 1,
    insertData = TestData("a int, b int", Seq("""{ "a": 1, "b": 4 }""")),
    expectedResult = ExpectedResult.Success(new StructType()
      .add("a", IntegerType)
      .add("b", IntegerType))
  )

  testInserts("top-level type evolution with column upcast")(
    initialData = TestData("a int, b short, c int", Seq("""{ "a": 1, "b": 2, "c": 3 }""")),
    partitionBy = Seq("a"),
    overwriteWhere = "a" -> 1,
    insertData = TestData("a int, b int, c short", Seq("""{ "a": 1, "b": 5, "c": 6 }""")),
    expectedResult = ExpectedResult.Success(new StructType()
      .add("a", IntegerType)
      .add("b", IntegerType)
      .add("c", IntegerType))
  )

  testInserts("top-level type evolution with schema evolution")(
    initialData = TestData("a int, b short", Seq("""{ "a": 1, "b": 2 }""")),
    partitionBy = Seq("a"),
    overwriteWhere = "a" -> 1,
    insertData = TestData("a int, b int, c int", Seq("""{ "a": 1, "b": 4, "c": 5 }""")),
    expectedResult = ExpectedResult.Success(new StructType()
      .add("a", IntegerType)
      .add("b", IntegerType)
      .add("c", IntegerType)),
    // SQL INSERT by name doesn't support schema evolution.
    excludeInserts = insertsSQL.intersect(insertsByName)
  )


  testInserts("nested type evolution by position")(
    initialData = TestData(
      "key int, s struct<x: short, y: short>, m map<string, short>, a array<short>",
      Seq("""{ "key": 1, "s": { "x": 1, "y": 2 }, "m": { "p": 3 }, "a": [4] }""")),
    partitionBy = Seq("key"),
    overwriteWhere = "key" -> 1,
    insertData = TestData(
      "key int, s struct<x: short, y: int>, m map<string, int>, a array<int>",
      Seq("""{ "key": 1, "s": { "x": 4, "y": 5 }, "m": { "p": 6 }, "a": [7] }""")),
    expectedResult = ExpectedResult.Success(new StructType()
      .add("key", IntegerType)
      .add("s", new StructType()
        .add("x", ShortType)
        .add("y", IntegerType))
      .add("m", MapType(StringType, IntegerType))
      .add("a", ArrayType(IntegerType)))
  )


  testInserts("nested type evolution with struct evolution by position")(
    initialData = TestData(
      "key int, s struct<x: short, y: short>, m map<string, short>, a array<short>",
      Seq("""{ "key": 1, "s": { "x": 1, "y": 2 }, "m": { "p": 3 }, "a": [4] }""")),
    partitionBy = Seq("key"),
    overwriteWhere = "key" -> 1,
    insertData = TestData(
      "key int, s struct<x: short, y: int, z: int>, m map<string, int>, a array<int>",
      Seq("""{ "key": 1, "s": { "x": 4, "y": 5, "z": 8 }, "m": { "p": 6 }, "a": [7] }""")),
    expectedResult = ExpectedResult.Success(new StructType()
      .add("key", IntegerType)
      .add("s", new StructType()
        .add("x", ShortType)
        .add("y", IntegerType)
        .add("z", IntegerType))
      .add("m", MapType(StringType, IntegerType))
      .add("a", ArrayType(IntegerType)))
  )


  testInserts("nested struct type evolution with field upcast")(
    initialData = TestData(
      "key int, s struct<x: int, y: short>",
      Seq("""{ "key": 1, "s": { "x": 1, "y": 2 } }""")),
    partitionBy = Seq("key"),
    overwriteWhere = "key" -> 1,
    insertData = TestData(
      "key int, s struct<x: short, y: int>",
      Seq("""{ "key": 1, "s": { "x": 4, "y": 5 } }""")),
    expectedResult = ExpectedResult.Success(new StructType()
      .add("key", IntegerType)
      .add("s", new StructType()
        .add("x", IntegerType)
        .add("y", IntegerType)))
  )

  // Interestingly, we introduced a special case to handle schema evolution / casting for structs
  // directly nested into an array. This doesn't always work with maps or with elements that
  // aren't a struct (see other tests).
  testInserts("nested struct type evolution with field upcast in array")(
    initialData = TestData(
      "key int, a array<struct<x: int, y: short>>",
      Seq("""{ "key": 1, "a": [ { "x": 1, "y": 2 } ] }""")),
    partitionBy = Seq("key"),
    overwriteWhere = "key" -> 1,
    insertData = TestData(
      "key int, a array<struct<x: short, y: int>>",
      Seq("""{ "key": 1, "a": [ { "x": 3, "y": 4 } ] }""")),
    expectedResult = ExpectedResult.Success(new StructType()
      .add("key", IntegerType)
      .add("a", ArrayType(new StructType()
        .add("x", IntegerType)
        .add("y", IntegerType))))
  )

  // maps now allow type evolution for INSERT by position and name in SQL and dataframe.
  testInserts("nested struct type evolution with field upcast in map")(
    initialData = TestData(
      "key int, m map<string, struct<x: int, y: short>>",
      Seq("""{ "key": 1, "m": { "a": { "x": 1, "y": 2 } } }""")),
    partitionBy = Seq("key"),
    overwriteWhere = "key" -> 1,
    insertData = TestData(
      "key int, m map<string, struct<x: short, y: int>>",
      Seq("""{ "key": 1, "m": { "a": { "x": 3, "y": 4 } } }""")),
    expectedResult = ExpectedResult.Success(new StructType()
      .add("key", IntegerType)
      // Type evolution was applied in the map.
      .add("m", MapType(StringType, new StructType()
        .add("x", IntegerType)
        .add("y", IntegerType))))
  )
}
