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

import java.util.regex.Pattern

import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types._

/**
 * Test suite covering behavior of INSERT operations with missing top-level columns or nested struct
 * fields.
 * This suite intends to exhaustively cover all the ways INSERT can be run on a Delta table. See
 * [[DeltaInsertIntoTest]] for a list of these INSERT operations covered.
 */
class DeltaInsertIntoMissingColumnSuite extends DeltaInsertIntoTest {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.ANSI_ENABLED.key, "true")
    spark.conf.set(DeltaSQLConf.DELTA_WRITE_MISSING_STRUCT_FIELDS_CHECK_MODE.key, "LOG_ONLY")
  }

  test("all test cases are implemented") {
    checkAllTestCasesImplemented()
  }

  private val missingFieldsMode = DeltaSQLConf.DELTA_WRITE_MISSING_STRUCT_FIELDS_CHECK_MODE.key

  private val missingFieldsErrorClass = "INCOMPATIBLE_DATA_FOR_TABLE.STRUCT_MISSING_FIELDS"

  private def targetIdentifier(byPath: Boolean = false): String = {
    if (byPath) {
      val path = DeltaLog.forTable(spark, TableIdentifier("target")).dataPath.toString
      TableIdentifier(path, Some("delta")).quotedString
    } else {
      spark.sessionState.catalog.getTableMetadata(TableIdentifier("target")).identifier.quotedString
    }
  }



  for (mode <- Seq("OFF", "LOG_ONLY", "ASSERT")) {
    test(s"missing nested fields with new table and schema replacement, mode=$mode") {
      withSQLConf(missingFieldsMode -> mode) {
        withTable("target") {
          val initial = TestData("s struct<x: int, y: int>",
            Seq("""{"s": {"x": 1, "y": 2}}"""))
          val replacement = TestData("s struct<y: int>", Seq("""{"s": {"y": 3}}"""))
            createTableFromTestData("target", initial)
            replacement.toDF.write.format(writeFormat).mode("overwrite")
              .option("overwriteSchema", "true").saveAsTable("target")
          assert(spark.table("target").schema === replacement.schema)
          checkExpectedRows(spark.table("target"), replacement)
        }
      }
    }
  }

  test("missing top-level struct is allowed in ASSERT mode") {
    withSQLConf(missingFieldsMode -> "ASSERT") {
      withTable("target") {
        val initial = TestData("a int, s struct<x: int, y: int>",
          Seq("""{"a": 1, "s": {"x": 2, "y": 3}}"""))
        createTableFromTestData("target", initial)
        val data = TestData("a int", Seq("""{"a": 4}"""))
          data.toDF.write.format(writeFormat).mode("append").saveAsTable("target")
        assert(spark.table("target").schema === initial.schema)
        checkExpectedRows(spark.table("target"),
          TestData(initial.schemaDDL, initial.data :+ """{"a": 4, "s": null}"""))
      }
    }
  }

  for (operation <- Seq("replace", "createOrReplace", "SQL REPLACE TABLE")) {
    test(s"ASSERT permits schema replacement with $operation") {
      withSQLConf(missingFieldsMode -> "ASSERT") {
        withTable("target") {
          val initial = TestData("s struct<x: int, y: int>",
            Seq("""{"s": {"x": 1, "y": 2}}"""))
          val replacement = TestData("s struct<y: int>", Seq("""{"s": {"y": 3}}"""))
          createTableFromTestData("target", initial)
            operation match {
              case "replace" => replacement.toDF.writeTo("target").using(writeFormat).replace()
              case "createOrReplace" =>
                replacement.toDF.writeTo("target").using(writeFormat).createOrReplace()
              case "SQL REPLACE TABLE" =>
                sql(s"REPLACE TABLE target USING $tableProvider " +
                  "AS SELECT named_struct('y', 3) s")
            }
          assert(spark.table("target").schema === replacement.schema)
          checkExpectedRows(spark.table("target"), replacement)
        }
      }
    }
  }

  test("ASSERT permits adding nested fields with schema evolution") {
    withSQLConf(missingFieldsMode -> "ASSERT") {
      withTable("target") {
        val initial = TestData("s struct<x: int, y: int>",
          Seq("""{"s": {"x": 1, "y": 2}}"""))
        createTableFromTestData("target", initial)
        val data = TestData("s struct<x: int, y: int, z: int>",
          Seq("""{"s": {"x": 3, "y": 4, "z": 5}}"""))
          data.toDF.write.format(writeFormat).mode("append")
            .option("mergeSchema", "true").saveAsTable("target")
        assert(spark.table("target").schema === data.schema)
        checkExpectedRows(spark.table("target"), TestData(data.schemaDDL,
          Seq("""{"s": {"x": 1, "y": 2, "z": null}}""", data.data.head)))
      }
    }
  }

  test("overwriteSchema in append mode does not bypass missing nested field validation") {
    withSQLConf(missingFieldsMode -> "ASSERT") {
      withTable("target") {
        val initial = TestData("s struct<x: int, y: int>",
          Seq("""{"s": {"x": 1, "y": 2}}"""))
        createTableFromTestData("target", initial)
        val data = TestData("s struct<y: int>", Seq("""{"s": {"y": 3}}"""))
        checkError(
          intercept[AnalysisException] {
            data.toDF.write.format(writeFormat).mode("append")
              .option("overwriteSchema", "true").saveAsTable("target")
          },
          missingFieldsErrorClass,
          sqlState = "KD000",
          parameters = Map("tableName" -> targetIdentifier(),
            "colName" -> QueryCompilationErrors.toSQLId(Seq("s")),
            "missingFields" -> QueryCompilationErrors.toSQLId(Seq("x"))))
        assert(DeltaLog.forTable(spark, TableIdentifier("target")).update().version === 0L)
        assert(spark.table("target").schema === initial.schema)
        checkExpectedRows(spark.table("target"), initial)
      }
    }
  }

  for {
    mode <- Seq("OFF", "LOG_ONLY", "ASSERT")
    overwrite <- BOOLEAN_DOMAIN
    schemaEvolution <- BOOLEAN_DOMAIN
  } {
    test(s"InsertableRelation missing struct field, mode=$mode, overwrite=$overwrite, " +
        s"evolution=$schemaEvolution") {
      withSQLConf(
          missingFieldsMode -> mode,
          DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolution.toString) {
        withTable("target") {
          val initial = TestData("s struct<x: int, y: int>",
            Seq("""{"s": {"x": 1, "y": 2}}"""))
          createTableFromTestData("target", initial)
          val deltaLog = DeltaLog.forTable(spark, TableIdentifier("target"))
          val relation = deltaLog.createRelation().asInstanceOf[InsertableRelation]
          val data = TestData("s struct<y: int>", Seq("""{"s": {"y": 3}}"""))
            if (mode == "ASSERT") {
              checkError(
                intercept[AnalysisException] { relation.insert(data.toDF, overwrite) },
                missingFieldsErrorClass,
                sqlState = "KD000",
                parameters = Map("tableName" -> targetIdentifier(byPath = true),
                  "colName" -> QueryCompilationErrors.toSQLId(Seq("s")),
                  "missingFields" -> QueryCompilationErrors.toSQLId(Seq("x"))))
              assert(deltaLog.update().version === 0L)
              checkExpectedRows(spark.table("target"), initial)
            } else {
              relation.insert(data.toDF, overwrite)
              val existing = if (overwrite) Seq.empty else initial.data
              checkExpectedRows(spark.table("target"),
                TestData(initial.schemaDDL, existing :+ """{"s": {"x": null, "y": 3}}"""))
            }
          assert(spark.table("target").schema === initial.schema)
        }
      }
    }
  }

  test("ASSERT rejects equal-count nested field replacement with schema evolution") {
    withSQLConf(missingFieldsMode -> "ASSERT") {
      withTable("target") {
        val initial = TestData("s struct<x: int, y: int>",
          Seq("""{"s": {"x": 1, "y": 2}}"""))
        createTableFromTestData("target", initial)
        val data = TestData("s struct<y: int, z: int>",
          Seq("""{"s": {"y": 3, "z": 4}}"""))
        checkError(
          intercept[AnalysisException] {
            data.toDF.write.format(writeFormat).mode("append")
              .option("mergeSchema", "true").saveAsTable("target")
          },
          missingFieldsErrorClass,
          sqlState = "KD000",
          parameters = Map("tableName" -> targetIdentifier(),
            "colName" -> QueryCompilationErrors.toSQLId(Seq("s")),
            "missingFields" -> QueryCompilationErrors.toSQLId(Seq("x"))))
        assert(DeltaLog.forTable(spark, TableIdentifier("target")).update().version === 0L)
        assert(spark.table("target").schema === initial.schema)
        checkExpectedRows(spark.table("target"), initial)
      }
    }
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
      withSchemaEvolution = schemaEvolution
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
      withSchemaEvolution = schemaEvolution
    )

    // Use explicit expected rows because unionByName does not reconcile missing map-value fields.
    for {
      validationMode <- Seq("OFF", "LOG_ONLY", "ASSERT")
      (container, initialData, insertData, expectedInsert, parentPath) <- Seq(
        (
          "struct",
          TestData("a int, s struct<x: int, y: int>",
            Seq("""{"a": 1, "s": {"x": 2, "y": 3}}""")),
          TestData("a int, s struct<y: int>",
            Seq("""{"a": 1, "s": {"y": 5}}""")),
          """{"a": 1, "s": {"x": null, "y": 5}}""",
          Seq("s")
        ),
        (
          "nested struct",
          TestData("a int, s struct<inner: struct<x: int, y: int>>",
            Seq("""{"a": 1, "s": {"inner": {"x": 2, "y": 3}}}""")),
          TestData("a int, s struct<inner: struct<y: int>>",
            Seq("""{"a": 1, "s": {"inner": {"y": 5}}}""")),
          """{"a": 1, "s": {"inner": {"x": null, "y": 5}}}""",
          Seq("s", "inner")
        ),
        (
          "array element",
          TestData("a int, s array<struct<x: int, y: int>>",
            Seq("""{"a": 1, "s": [{"x": 2, "y": 3}]}""")),
          TestData("a int, s array<struct<y: int>>",
            Seq("""{"a": 1, "s": [{"y": 5}]}""")),
          """{"a": 1, "s": [{"x": null, "y": 5}]}""",
          Seq("s", "element")
        ),
        (
          "map value",
          TestData("a int, s map<string, struct<x: int, y: int>>",
            Seq("""{"a": 1, "s": {"k": {"x": 2, "y": 3}}}""")),
          TestData("a int, s map<string, struct<y: int>>",
            Seq("""{"a": 1, "s": {"k": {"y": 5}}}""")),
          """{"a": 1, "s": {"k": {"x": null, "y": 5}}}""",
          Seq("s", "value")
        ))
    } {
      val name = s"missing nested field in $container, mode=$validationMode, " +
        s"schemaEvolution=$schemaEvolution"
      val byNameDataframeInserts = insertsByName.intersect(insertsDataframe)

      val partialOverwriteOptions = Seq(
        "replaceWhere" -> Map("replaceWhere" -> "a = 1"),
        "dynamic partitions" -> Map("partitionOverwriteMode" -> "dynamic"),
        "replaceUsing" -> Map("replaceUsing" -> "a"))
      for {
        saveAsTable <- BOOLEAN_DOMAIN
        (operation, options) <- partialOverwriteOptions ++ (
          if (saveAsTable) Seq("replaceOn" -> Map("replaceOn" -> "t.a = 1", "targetAlias" -> "t"))
          else Seq.empty)
      } {
        val api = if (saveAsTable) "saveAsTable" else "save"
        test(s"$api $operation - $name") {
          withSQLConf(
              missingFieldsMode -> validationMode,
              DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolution.toString,
              DeltaSQLConf.REPLACE_USING_OPTION_IN_DATAFRAME_WRITER_ENABLED.key -> "true",
              DeltaSQLConf.REPLACE_ON_OPTION_IN_DATAFRAME_WRITER_ENABLED.key -> "true",
              DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "true") {
            withTable("source", "target") {
              val untouched = initialData.data.map(_.replace("\"a\": 1", "\"a\": 2"))
              val original = initialData.copy(data = initialData.data ++ untouched)
              createTableFromTestData("target", original, Seq("a"))
              createTableFromTestData("source", insertData)
              val deltaLog = DeltaLog.forTable(spark, TableIdentifier("target"))
              def write(): Unit = {
                val writer = spark.table("source").write.format(writeFormat).mode("overwrite")
                  .option("mergeSchema", schemaEvolution.toString).options(options)
                if (saveAsTable) writer.saveAsTable("target")
                else writer.save(deltaLog.dataPath.toString)
              }
                if (validationMode == "ASSERT") {
                  checkError(
                    intercept[AnalysisException] { write() },
                    missingFieldsErrorClass,
                    sqlState = "KD000",
                    parameters = Map("tableName" -> targetIdentifier(byPath = !saveAsTable),
                      "colName" -> QueryCompilationErrors.toSQLId(parentPath),
                      "missingFields" -> QueryCompilationErrors.toSQLId(Seq("x"))))
                  assert(deltaLog.update().version === 0L)
                  checkExpectedRows(spark.table("target"), original)
                } else {
                  write()
                  checkExpectedRows(spark.table("target"),
                    TestData(initialData.schemaDDL, untouched :+ expectedInsert))
                }
              assert(spark.table("target").schema === initialData.schema)
            }
          }
        }
      }

      for {
        (mode, inserts) <- Seq(
          SaveMode.Append -> insertsAppend,
          SaveMode.Overwrite -> insertsOverwrite)
        insert <- byNameDataframeInserts.intersect(inserts) - StreamingInsert
      } {
        val expectedRows = if (mode == SaveMode.Append) {
          initialData.data :+ expectedInsert
        } else {
          Seq(expectedInsert)
        }
        testInserts(name)(
          initialData = initialData,
          partitionBy = Seq("a"),
          overwriteWhere = "a" -> 1,
          insertData = insertData,
          expectedResult = if (validationMode == "ASSERT") {
            ExpectedResult.Failure(ex => {
              val byPath = insert.isInstanceOf[DFv1Save] || insert == DFv1SaveReplaceOn
              checkError(ex, missingFieldsErrorClass,
                sqlState = "KD000",
                parameters = Map("tableName" -> targetIdentifier(byPath),
                  "colName" -> QueryCompilationErrors.toSQLId(parentPath),
                  "missingFields" -> QueryCompilationErrors.toSQLId(Seq("x"))))
              assert(DeltaLog.forTable(spark, TableIdentifier("target")).update().version === 0L)
              assert(spark.table("target").schema === initialData.schema)
              checkExpectedRows(spark.table("target"), initialData)
            })
          } else {
            ExpectedResult.Success(TestData(initialData.schemaDDL, expectedRows))
          },
          includeInserts = Set(insert),
          confs = Seq(missingFieldsMode -> validationMode),
          withSchemaEvolution = schemaEvolution
        )
      }

      testInserts(name)(
        initialData = initialData,
        partitionBy = Seq("a"),
        overwriteWhere = "a" -> 1,
        insertData = insertData,
        expectedResult = ExpectedResult.Success(
          TestData(initialData.schemaDDL, initialData.data :+ expectedInsert)),
        includeInserts = Set(StreamingInsert),
        confs = Seq(missingFieldsMode -> validationMode),
        withSchemaEvolution = schemaEvolution
      )

      testInserts(name)(
        initialData = initialData,
        partitionBy = Seq("a"),
        overwriteWhere = "a" -> 1,
        insertData = insertData,
        expectedResult = ExpectedResult.Failure(ex => {
          val tableName = spark.sessionState.catalog.getTableMetadata(TableIdentifier("target"))
            .identifier.unquotedString
          val fieldPattern = container match {
            case "struct" =>
              val sourceName = spark.sessionState.catalog
                .getTableMetadata(TableIdentifier("source")).identifier.unquotedString
              s"(${Pattern.quote(sourceName + ".")})?s"
            case "nested struct" => "inner"
            case "array element" => QueryCompilationErrors.toSQLId(Seq("lambda indexVar#[0-9]+"))
            case "map value" => "value"
          }
          checkError(
            ex,
            "DELTA_INSERT_COLUMN_ARITY_MISMATCH",
            sqlState = Some("42802"),
            parameters = Map(
              "tableName" -> Pattern.quote(tableName),
              "columnName" -> s"not enough nested fields in $fieldPattern",
              "numColumns" -> "2",
              "insertColumns" -> "1"),
            matchPVals = true)
          assert(spark.table("target").schema === initialData.schema)
          checkExpectedRows(spark.table("target"), initialData)
        }),
        includeInserts = insertsByPosition ++ insertsSQL,
        confs = Seq(missingFieldsMode -> validationMode),
        withSchemaEvolution = schemaEvolution
      )
    }

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
      withSchemaEvolution = schemaEvolution
    )

    testInserts(s"insert with implicit cast and missing top-level column," +
      s"schemaEvolution=$schemaEvolution")(
      initialData = TestData("a long, b int, c int", Seq("""{ "a": 1, "b": 2, "c": 3 }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData = TestData("a int, b long", Seq("""{ "a": 1, "b": 4 }""")),
      expectedResult = ExpectedResult.Failure(ex => {
        // The missing column isn't an issue, but save() and saveAsTable() overwrite don't
        // support implicit casting to reconcile the type mismatch.
        checkError(
          ex,
          "DELTA_FAILED_TO_MERGE_FIELDS",
          parameters = Map(
            "currentField" -> "a",
            "updateField" -> "a"
          ))
      }),
      includeInserts = insertsWithoutImplicitCastSupport,
      withSchemaEvolution = schemaEvolution
    )

    // Other df-by-name inserts (DFv2, saveAsTable append) support implicit casting.
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
      includeInserts = insertsByName.intersect(insertsDataframe) - StreamingInsert --
        insertsWithoutImplicitCastSupport,
      withSchemaEvolution = schemaEvolution
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
        // The missing column isn't an issue, but save() and saveAsTable() overwrite don't
        // support implicit casting to reconcile the type mismatch.
        checkError(
          ex,
          "DELTA_FAILED_TO_MERGE_FIELDS",
          parameters = Map(
            "currentField" -> "s",
            "updateField" -> "s"
          ))
      }),
      includeInserts = insertsWithoutImplicitCastSupport,
      withSchemaEvolution = schemaEvolution
    )

    testInserts(s"insert with implicit cast and missing nested field," +
      s"schemaEvolution=$schemaEvolution")(
      initialData =
        TestData("a int, s struct<x: int, y: int>", Seq("""{ "a": 1, "s": { "x": 2, "y": 3 } }""")),
      partitionBy = Seq("a"),
      overwriteWhere = "a" -> 1,
      insertData =
        TestData("a int, s struct<y: long>", Seq("""{ "a": 1, "s": { "y": 5 } }""")),
      // Missing nested fields are allowed when writing to a delta streaming sink or using DFv2 and
      // saveAsTable(Append) when there's a type mismatch, same as when there's no type mismatch.
      expectedResult = ExpectedResult.Success(
        expected = new StructType()
          .add("a", IntegerType)
          .add("s", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType))),
      includeInserts = insertsByName.intersect(insertsDataframe) --
        insertsWithoutImplicitCastSupport,
      withSchemaEvolution = schemaEvolution
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
            "tableName" -> s"$catalogName.default.target",
            "columnName" -> "not enough data columns",
            "numColumns" -> "3",
            "insertColumns" -> "2"
          ))
      }),
      includeInserts = insertsByPosition,
      withSchemaEvolution = schemaEvolution
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
            "tableName" -> s"$catalogName.default.target",
            "columnName" -> "not enough data columns",
            "numColumns" -> "3",
            "insertColumns" -> "2"
          ))
      }),
      includeInserts = insertsByPosition,
      withSchemaEvolution = schemaEvolution
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
            "tableName" -> s"$catalogName\\.default\\.target",
            "columnName" -> s"not enough nested fields in ($catalogName\\.default\\.source\\.)?s",
            "numColumns" -> "2",
            "insertColumns" -> "1"
          ))
      }),
      includeInserts = insertsByPosition ++ insertsSQL,
      withSchemaEvolution = schemaEvolution
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
            "tableName" -> s"$catalogName\\.default\\.target",
            "columnName" -> s"not enough nested fields in ($catalogName\\.default\\.source\\.)?s",
            "numColumns" -> "2",
            "insertColumns" -> "1"
          ))
      }),
      includeInserts = insertsByPosition ++ insertsSQL,
      withSchemaEvolution = schemaEvolution
    )
  }
}
