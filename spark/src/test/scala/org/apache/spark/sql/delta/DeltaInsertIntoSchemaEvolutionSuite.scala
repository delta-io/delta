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
import org.apache.spark.sql.types._

/**
 * Test suite covering behavior of INSERT operations with extra top-level columns or nested struct
 * fields in the input data.
 * This suite intends to exhaustively cover all the ways INSERT can be run on a Delta table. See
 * [[DeltaInsertIntoTest]] for a list of these INSERT operations covered.
 */
class DeltaInsertIntoSchemaEvolutionSuite extends DeltaInsertIntoTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaSQLConf.DELTA_STREAMING_SINK_ALLOW_IMPLICIT_CASTS.key, "false")
    spark.conf.set(SQLConf.ANSI_ENABLED.key, "true")
  }

  test("all test cases are implemented") {
    // we don't cover SQL INSERT with an explicit column list in this suite as it's not possible to
    // specify a column that doesn't exist in the target table that way.
    val ignoredTestCases = testCases.map { case (name, _) =>
      name -> Set(
        SQLInsertColList(SaveMode.Append),
        SQLInsertColList(SaveMode.Overwrite),
        SQLInsertOverwritePartitionColList)
    }.toMap
    checkAllTestCasesImplemented(ignoredTestCases)
  }

  for (schemaEvolution <- BOOLEAN_DOMAIN) {
    // We allow adding new top-level columns with schema evolution for all inserts.
    testInserts(s"insert with extra top-level column, schemaEvolution=$schemaEvolution")(
      initialData = TestData("a int, b int", Seq("""{ "a": 1, "b": 2 }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData("a int, b int, c int", Seq("""{ "a": 1, "b": 4, "c": 5  }""")),
      expectedResult = if (schemaEvolution) {
        ExpectedResult.Success(
          expected = new StructType()
            .add("a", IntegerType)
            .add("b", IntegerType)
            .add("c", IntegerType))
      } else {
        ExpectedResult.Failure(ex => {
          checkErrorMatchPVals(
            ex,
            "_LEGACY_ERROR_TEMP_DELTA_0007",
            parameters = Map(
              "message" -> "A schema mismatch detected when writing to the Delta table(.|\\n)*"
            ))
        })
      },
      excludeInserts = Set(
        SQLInsertColList(SaveMode.Append),
        SQLInsertColList(SaveMode.Overwrite),
        SQLInsertOverwritePartitionColList
      ),
      confs = Seq(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolution.toString)
    )


    // Adding new top-level columns with schema evolution is allowed for all inserts except SQL
    // inserts by name, but dataframe inserts by name don't support implicit casting and will fail
    // due to the type mismatch.
    testInserts(s"insert with extra top-level column and implicit cast," +
      s"schemaEvolution=$schemaEvolution")(
      initialData = TestData("a int, b int", Seq("""{ "a": 1, "b": 2 }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData("a int, b long, c int", Seq("""{ "a": 1, "b": 4, "c": 5  }""")),
      expectedResult = if (schemaEvolution) {
        ExpectedResult.Success(
          expected = new StructType()
            .add("a", IntegerType)
            .add("b", IntegerType)
            .add("c", IntegerType))
      } else {
        ExpectedResult.Failure(ex => {
          checkErrorMatchPVals(
            ex,
            "_LEGACY_ERROR_TEMP_DELTA_0007",
            parameters = Map(
              "message" -> "A schema mismatch detected when writing to the Delta table(.|\\n)*"
            ))
        })
      },
      includeInserts = insertsByPosition,
      confs = Seq(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolution.toString)
    )

    testInserts(s"insert with extra top-level column and implicit cast," +
      s"schemaEvolution=$schemaEvolution")(
      initialData = TestData("a int, b int", Seq("""{ "a": 1, "b": 2 }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData("a int, b long, c int", Seq("""{ "a": 1, "b": 4, "c": 5  }""")),
      expectedResult = ExpectedResult.Failure(ex => {
        checkError(
          ex,
          "DELTA_FAILED_TO_MERGE_FIELDS",
          parameters = Map(
            "currentField" -> "b",
            "updateField" -> "b"
          ))
      }),
      includeInserts = insertsDataframe.intersect(insertsByName),
      confs = Seq(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolution.toString)
    )

    testInserts(s"insert with extra top-level column and implicit cast," +
      s"schemaEvolution=$schemaEvolution")(
      initialData = TestData("a int, b int", Seq("""{ "a": 1, "b": 2 }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData("a int, b long, c int", Seq("""{ "a": 1, "b": 4, "c": 5  }""")),
      expectedResult = ExpectedResult.Failure(ex => {
        checkError(
          ex,
          "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
          parameters = Map(
            "tableName" -> "`spark_catalog`.`default`.`target`",
            "tableColumns" -> "`a`, `b`",
            "dataColumns" -> "`a`, `b`, `c`"
          ))
      }),
      includeInserts = insertsSQL.intersect(insertsByName) -- Set(
        // It's not possible to specify a column that doesn't exist in the target using SQL with an
        // explicit column list.
        SQLInsertColList(SaveMode.Append),
        SQLInsertColList(SaveMode.Overwrite),
        SQLInsertOverwritePartitionColList
      ),
      confs = Seq(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolution.toString)
    )

    // We allow adding new nested struct fields for all inserts, including SQL inserts by name.
    testInserts(s"insert with extra nested field, schemaEvolution=$schemaEvolution")(
      initialData = TestData("a int, s struct <x: int>", Seq("""{ "a": 1, "s": { "x": 2 } }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData("a int, s struct <x: int, y: int>",
        Seq("""{ "a": 1, "s": { "x": 4, "y": 5 } }""")),
      expectedResult = if (schemaEvolution) {
        ExpectedResult.Success(
          expected = new StructType()
            .add("a", IntegerType)
            .add("s", new StructType()
              .add("x", IntegerType)
              .add("y", IntegerType)
            ))
      } else {
        ExpectedResult.Failure(ex => {
          checkErrorMatchPVals(
            ex,
            "_LEGACY_ERROR_TEMP_DELTA_0007",
            parameters = Map(
              "message" -> "A schema mismatch detected when writing to the Delta table(.|\\n)*"
            ))
        })
      },
      confs = Seq(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolution.toString)
    )

    // Adding new nested struct fields with schema evolution is allowed for all inserts, but
    // dataframe inserts by name don't support implicit casting and will fail due to the type
    // mismatch.
    testInserts(s"insert with extra nested field and implicit cast," +
      s"schemaEvolution=$schemaEvolution")(
      initialData = TestData("a int, s struct <x: int>", Seq("""{ "a": 1, "s": { "x": 2 } }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData("a int, s struct <x: long, y: int>",
        Seq("""{ "a": 1, "s": { "x": 4, "y": 5 } }""")),
      expectedResult = if (schemaEvolution) {
        ExpectedResult.Success(
          expected = new StructType()
            .add("a", IntegerType)
            .add("s", new StructType()
              .add("x", IntegerType)
              .add("y", IntegerType)
            ))
      } else {
        ExpectedResult.Failure(ex => {
          checkErrorMatchPVals(
            ex,
            "_LEGACY_ERROR_TEMP_DELTA_0007",
            parameters = Map(
              "message" -> "A schema mismatch detected when writing to the Delta table(.|\\n)*"
            ))
        })
      },
      includeInserts = insertsSQL ++ insertsByPosition -- Seq(
        // It's not possible to specify a column that doesn't exist in the target using SQL with an
        // explicit column list.
        SQLInsertColList(SaveMode.Append),
        SQLInsertColList(SaveMode.Overwrite),
        SQLInsertOverwritePartitionColList
      ),
      confs = Seq(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolution.toString)
    )

    testInserts(s"insert with extra nested field and implicit cast," +
      s"schemaEvolution=$schemaEvolution")(
      initialData = TestData("a int, s struct <x: int>", Seq("""{ "a": 1, "s": { "x": 2 } }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData("a int, s struct <x: long, y: int>",
        Seq("""{ "a": 1, "s": { "x": 4, "y": 5 } }""")),
      expectedResult = ExpectedResult.Failure(ex => {
        checkError(
          ex,
          "DELTA_FAILED_TO_MERGE_FIELDS",
          parameters = Map(
            "currentField" -> "s",
            "updateField" -> "s"
          ))
      }),
      includeInserts = insertsDataframe.intersect(insertsByName),
      confs = Seq(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolution.toString)
    )
  }
}
