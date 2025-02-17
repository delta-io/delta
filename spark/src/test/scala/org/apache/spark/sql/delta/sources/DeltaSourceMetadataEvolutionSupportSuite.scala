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

package org.apache.spark.sql.delta.sources

import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaOptions, DeltaTestUtilsBase, DeltaThrowable}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

/**
 * Unit tests covering `DeltaSourceMetadataEvolutionSupport`, which detects non-additive schema
 * changes when reading from a Delta source and checks user provided confs to decide whether to
 * allow resuming streaming processing.
 */
class DeltaSourceMetadataEvolutionSupportSuite
  extends SparkFunSuite
    with SharedSparkSession
    with DeltaTestUtilsBase {

  protected override def sparkConf: SparkConf =
    super.sparkConf
      .set(DeltaSQLConf.DELTA_ALLOW_TYPE_WIDENING_STREAMING_SOURCE.key, "true")
      .set(DeltaSQLConf.DELTA_TYPE_WIDENING_BYPASS_STREAMING_TYPE_CHANGE_CHECK.key, "false")

  private def persistedMetadata(
      schemaDDL: String,
      physicalNames: Map[Seq[String], String]): PersistedMetadata = {
    var schemaWithPhysicalNames = StructType.fromDDL(schemaDDL)
    schemaWithPhysicalNames = DeltaColumnMapping.setPhysicalNames(
      schema = schemaWithPhysicalNames,
      physicalNames
    )
    schemaWithPhysicalNames = DeltaColumnMapping.assignPhysicalNames(
      schemaWithPhysicalNames,
      reuseLogicalName = true)

    PersistedMetadata(
      tableId = "tableId",
      deltaCommitVersion = 0,
      dataSchemaJson = schemaWithPhysicalNames.json,
      partitionSchemaJson = "",
      sourceMetadataPath = "sourceMetadataPath"
    )
  }

  private def expectColumnMappingChangeBlocked(opType: String): ExpectedResult[Nothing] =
    ExpectedResult.Failure(ex => {
      assert(
        ex.getErrorClass === "DELTA_STREAMING_CANNOT_CONTINUE_PROCESSING_POST_SCHEMA_EVOLUTION")
      assert(ex.getMessageParameters.get("opType") === opType)
    })

  private def expectTypeWideningBlocked(wideningTypeChanges: Seq[String]): ExpectedResult[Nothing] =
    ExpectedResult.Failure(ex => {
      assert(
        ex.getErrorClass === "DELTA_STREAMING_CANNOT_CONTINUE_PROCESSING_TYPE_WIDENING")
      assert(
        ex.getMessageParameters.get("wideningTypeChanges") === wideningTypeChanges.mkString("\n"))
    })

  private def expectNonWideningTypeChangeError: ExpectedResult[Nothing] =
    ExpectedResult.Failure(ex => {
      assert(
        ex.getErrorClass === "DELTA_SCHEMA_CHANGED_WITH_VERSION")
    })

  private def withSQLConfUnblockedChanges(unblock: Seq[String])(f: => Unit): Unit = {
    val confs = unblock.map( conf => s"spark.databricks.delta.streaming.$conf" -> "always")
    withSQLConf(confs: _*) {
      f
    }
  }

  /**
   * Unit test runner covering `validateIfSchemaChangeCanBeUnblocked()`. Takes as input
   * an initial schema (from) and an updated schema (to) and checks that:
   *   1. Non-additive schema changes are correctly detected: matches `expectedResult`
   *   2. Setting SQL confs to unblock the changes allows the check to succeeds.
   * @param name              Name of the test.
   * @param fromDDL           Initial schema, as a DDL string: 'a INT'
   * @param fromPhysicalNames Physical column/field names for the initial schema: assigning
   *                          physical names that are different than the logical names in
   *                          `fromDDL` allows simulating column mapping operations: DROP, RENAME.
   * @param toDDL             Updated schema, as a DDL string.
   * @param toPhysicalNames   Physical column/field names for the updated schema
   * @param expectedResult    Expected result, either failure or success. In case of failure, a
   *                          check to apply on the returned expression can be passed.
   * @param unblock           SQL confs to unblock the schema change. Each entry is a set of SQL
   *                          confs which together allow the schema change to be unblocked. There
   *                          can be multiple such sets, e.g.
   *                          [[allowSourceColumnDrop], [allowSourceColumnRenameAndDrop]] as both
   *                          allow dropping columns independently.
   * @param confs             Additional SQL confs to set when running the test.
   */
  private def testSchemaChange(
      name: String,
      fromDDL: String,
      fromPhysicalNames: Map[Seq[String], String] = Map.empty,
      toDDL: String,
      toPhysicalNames: Map[Seq[String], String] = Map.empty,
      expectedResult: ExpectedResult[Nothing],
      unblock: Seq[Seq[String]] = Seq.empty,
      confs: Seq[(String, String)] = Seq.empty): Unit =
    test(s"$name") {
      def validate(parameters: Map[String, String]): Unit =
        DeltaSourceMetadataEvolutionSupport.validateIfSchemaChangeCanBeUnblocked(
          spark,
          parameters,
          metadataPath = "sourceMetadataPath",
          currentSchema = persistedMetadata(toDDL, toPhysicalNames),
          previousSchema = persistedMetadata(fromDDL, fromPhysicalNames)
        )
      withSQLConf(confs: _*) {
        expectedResult match {
          case ExpectedResult.Success(_) => validate(parameters = Map.empty)
          case ExpectedResult.Failure(checkError) =>
            // Run first without setting any configuration to unblock and check that the validation
            // fails => column dropped, renamed or with changed type.
            val ex = intercept[DeltaThrowable] {
              validate(parameters = Map.empty)
            }
            checkError(ex)

            // Verify that we can unblock using SQL confs
            for (u <- unblock) {
              withSQLConfUnblockedChanges(u) {
                validate(parameters = Map.empty)
              }
            }
            // Verify that we can unblock using dataframe reader options.
            for (u <- unblock) {
              val parameters = u.flatMap {
                case "allowSourceColumnRenameAndDrop" =>
                  Seq(DeltaOptions.ALLOW_SOURCE_COLUMN_RENAME -> "always",
                    DeltaOptions.ALLOW_SOURCE_COLUMN_DROP -> "always")
                case option => Seq(option -> "always")
              }
              validate(parameters.toMap)
            }
        }
      }
    }

  testSchemaChange(
    name = "no schema change, use logical names",
    fromDDL = "a int",
    toDDL = "a int",
    expectedResult = ExpectedResult.Success()
  )

  testSchemaChange(
    name = "no schema change, use physical names",
    fromDDL = "a int",
    fromPhysicalNames = Map(Seq("a") -> "x"),
    toDDL = "a int",
    toPhysicalNames = Map(Seq("a") -> "x"),
    expectedResult = ExpectedResult.Success()
  )

  testSchemaChange(
    name = "schema overwrite, different column name",
    fromDDL = "a int",
    toDDL = "b string",
    expectedResult = expectColumnMappingChangeBlocked("DROP COLUMN"),
    unblock = Seq(
      Seq("allowSourceColumnDrop"),
      Seq("allowSourceColumnRenameAndDrop")
    )
  )

  testSchemaChange(
    name = "schema overwrite, same column name, non-widening type change",
    fromDDL = "a int",
    toDDL = "a string",
    toPhysicalNames = Map(Seq("a") -> "b"),
    expectedResult = expectColumnMappingChangeBlocked("DROP COLUMN"),
    unblock = Seq(
      Seq("allowSourceColumnDrop"),
      Seq("allowSourceColumnRenameAndDrop")
    )
  )

  testSchemaChange(
    name = "schema overwrite, same column name, widening type change",
    fromDDL = "a byte",
    toDDL = "a int",
    toPhysicalNames = Map(Seq("a") -> "b"),
    expectedResult = expectColumnMappingChangeBlocked("DROP COLUMN"),
    unblock = Seq(
      Seq("allowSourceColumnDrop"),
      Seq("allowSourceColumnRenameAndDrop")
    )
  )

  /////////////////
  // Rename column
  /////////////////
  testSchemaChange(
    name = "column rename, use logical names",
    fromDDL = "a int",
    toDDL = "b int",
    toPhysicalNames = Map(Seq("b") -> "a"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME COLUMN"),
    unblock = Seq(
      Seq("allowSourceColumnRename"),
      Seq("allowSourceColumnRenameAndDrop")
    )
  )

  testSchemaChange(
    name = "column rename, use physical names",
    fromDDL = "a int",
    fromPhysicalNames = Map(Seq("a") -> "x"),
    toDDL = "b int",
    toPhysicalNames = Map(Seq("b") -> "x"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME COLUMN"),
    unblock = Seq(
      Seq("allowSourceColumnRename"),
      Seq("allowSourceColumnRenameAndDrop")
    )
  )

  testSchemaChange(
    name = "column rename with widening type change",
    fromDDL = "a byte",
    fromPhysicalNames = Map(Seq("a") -> "x"),
    toDDL = "b int",
    toPhysicalNames = Map(Seq("b") -> "x"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME AND TYPE WIDENING"),
    unblock = Seq(
      Seq("allowSourceColumnRename", "allowSourceColumnTypeChange"),
      Seq("allowSourceColumnRenameAndDrop", "allowSourceColumnTypeChange")
    )
  )

  testSchemaChange(
    name = "column rename with non-widening type change",
    fromDDL = "a int",
    fromPhysicalNames = Map(Seq("a") -> "x"),
    toDDL = "b string",
    toPhysicalNames = Map(Seq("b") -> "x"),
    expectedResult = expectNonWideningTypeChangeError
  )

  testSchemaChange(
    name = "swap columns",
    fromDDL = "a int, b int",
    toDDL = "b int, a int",
    toPhysicalNames = Map(Seq("b") -> "a", Seq("a") -> "b"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME COLUMN"),
    unblock = Seq(
      Seq("allowSourceColumnRename"),
      Seq("allowSourceColumnRenameAndDrop")
    )
  )

  testSchemaChange(
    name = "swap columns with widening type change",
    fromDDL = "a byte, b byte",
    toDDL = "b byte, a int",
    toPhysicalNames = Map(Seq("b") -> "a", Seq("a") -> "b"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME AND TYPE WIDENING"),
    unblock = Seq(
      Seq("allowSourceColumnRename", "allowSourceColumnTypeChange"),
      Seq("allowSourceColumnRenameAndDrop", "allowSourceColumnTypeChange")
    )
  )

  testSchemaChange(
    name = "swap columns with non-widening type change",
    fromDDL = "a int, b int",
    toDDL = "b int, a string",
    toPhysicalNames = Map(Seq("b") -> "a", Seq("a") -> "b"),
    expectedResult = expectNonWideningTypeChangeError
  )

  testSchemaChange(
    name = "swap columns with widening and non-widening type change",
    fromDDL = "a byte, b int",
    toDDL = "b int, a string",
    toPhysicalNames = Map(Seq("b") -> "a", Seq("a") -> "b"),
    expectedResult = expectNonWideningTypeChangeError
  )

  /////////////////
  // Drop column
  /////////////////
  testSchemaChange(
    name = "drop column, use logical names",
    fromDDL = "a int, b int",
    toDDL = "b int",
    expectedResult = expectColumnMappingChangeBlocked("DROP COLUMN"),
    unblock = Seq(
      Seq("allowSourceColumnDrop", "allowSourceColumnTypeChange"),
      Seq("allowSourceColumnRenameAndDrop", "allowSourceColumnTypeChange")
    )
  )

  testSchemaChange(
    name = "drop column, use physical names",
    fromDDL = "a int, b int",
    fromPhysicalNames = Map(Seq("a") -> "x", Seq("b") -> "y"),
    toDDL = "b int",
    toPhysicalNames = Map(Seq("b") -> "y"),
    expectedResult = expectColumnMappingChangeBlocked("DROP COLUMN"),
    unblock = Seq(
      Seq("allowSourceColumnDrop", "allowSourceColumnTypeChange"),
      Seq("allowSourceColumnRenameAndDrop", "allowSourceColumnTypeChange")
    )
  )

  testSchemaChange(
    name = "drop column with widening type change",
    fromDDL = "a byte, b byte",
    fromPhysicalNames = Map(Seq("a") -> "x", Seq("b") -> "y"),
    toDDL = "b int",
    toPhysicalNames = Map(Seq("b") -> "y"),
    expectedResult = expectColumnMappingChangeBlocked("DROP AND TYPE WIDENING"),
    unblock = Seq(
      Seq("allowSourceColumnDrop", "allowSourceColumnTypeChange"),
      Seq("allowSourceColumnRenameAndDrop", "allowSourceColumnTypeChange")
    )
  )

  testSchemaChange(
    name = "drop column with non-widening type change",
    fromDDL = "a int, b int",
    fromPhysicalNames = Map(Seq("a") -> "x", Seq("b") -> "y"),
    toDDL = "b string",
    toPhysicalNames = Map(Seq("b") -> "y"),
    expectedResult = expectNonWideningTypeChangeError
  )

  testSchemaChange(
    name = "drop column, swapped physical names",
    fromDDL = "a int, b int",
    fromPhysicalNames = Map(Seq("a") -> "b", Seq("b") -> "a"),
    toDDL = "b int",
    toPhysicalNames = Map(Seq("b") -> "a"),
    expectedResult = expectColumnMappingChangeBlocked("DROP COLUMN"),
    unblock = Seq(
      Seq("allowSourceColumnDrop", "allowSourceColumnTypeChange"),
      Seq("allowSourceColumnRenameAndDrop", "allowSourceColumnTypeChange")
    )
  )

  testSchemaChange(
    name = "drop column, swapped physical names with widening type change",
    fromDDL = "a byte, b byte",
    fromPhysicalNames = Map(Seq("a") -> "b", Seq("b") -> "a"),
    toDDL = "b int",
    toPhysicalNames = Map(Seq("b") -> "a"),
    expectedResult = expectColumnMappingChangeBlocked("DROP AND TYPE WIDENING"),
    unblock = Seq(
      Seq("allowSourceColumnDrop", "allowSourceColumnTypeChange"),
      Seq("allowSourceColumnRenameAndDrop", "allowSourceColumnTypeChange")
    )
  )

  testSchemaChange(
    name = "drop column, swapped physical names with non-widening type change",
    fromDDL = "a int, b int",
    fromPhysicalNames = Map(Seq("a") -> "b", Seq("b") -> "a"),
    toDDL = "b float",
    toPhysicalNames = Map(Seq("b") -> "a"),
    expectedResult = expectNonWideningTypeChangeError
  )

  //////////////////////////
  // Drop and rename column
  //////////////////////////
  testSchemaChange(
    name = "drop column, rename to other column name",
    fromDDL = "a int, b int",
    toDDL = "c int",
    toPhysicalNames = Map(Seq("c") -> "b"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME AND DROP COLUMN"),
    unblock = Seq(
      Seq("allowSourceColumnRename", "allowSourceColumnDrop"),
      Seq("allowSourceColumnRenameAndDrop")
    )
  )

  testSchemaChange(
    name = "drop column, rename to other column name with widening type change",
    fromDDL = "a byte, b byte",
    toDDL = "c int",
    toPhysicalNames = Map(Seq("c") -> "b"),
    // We don't block the type change itself here as the column is also renamed
    expectedResult = expectColumnMappingChangeBlocked("RENAME, DROP AND TYPE WIDENING"),
    unblock = Seq(
      Seq("allowSourceColumnRename", "allowSourceColumnDrop", "allowSourceColumnTypeChange"),
      Seq("allowSourceColumnRenameAndDrop", "allowSourceColumnTypeChange")
    )
  )

  testSchemaChange(
    name = "drop column, rename to other column name with non-widening type change",
    fromDDL = "a int, b int",
    toDDL = "c string",
    toPhysicalNames = Map(Seq("c") -> "b"),
    expectedResult = expectNonWideningTypeChangeError
  )

  testSchemaChange(
    name = "drop column, rename to same column name",
    fromDDL = "a int, b int",
    toDDL = "a int",
    toPhysicalNames = Map(Seq("a") -> "b"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME AND DROP COLUMN"),
    unblock = Seq(
      Seq("allowSourceColumnRename", "allowSourceColumnDrop"),
      Seq("allowSourceColumnRenameAndDrop")
    )
  )

  testSchemaChange(
    name = "drop column, rename to same column name with widening type change",
    fromDDL = "a byte, b byte",
    toDDL = "a int",
    toPhysicalNames = Map(Seq("a") -> "b"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME, DROP AND TYPE WIDENING"),
    unblock = Seq(
      Seq("allowSourceColumnRename", "allowSourceColumnDrop", "allowSourceColumnTypeChange"),
      Seq("allowSourceColumnRenameAndDrop", "allowSourceColumnTypeChange")
    )
  )

  testSchemaChange(
    name = "drop column, rename to same column name with non-widening type change",
    fromDDL = "a int, b int",
    toDDL = "a string",
    toPhysicalNames = Map(Seq("a") -> "b"),
    expectedResult = expectNonWideningTypeChangeError
  )

  ////////////////
  // Type changes
  ////////////////
  testSchemaChange(
    name = "widen single column",
    fromDDL = "a byte",
    toDDL = "a int",
    expectedResult = expectTypeWideningBlocked(Seq("  a: TINYINT -> INT")),
    unblock = Seq(
      Seq("allowSourceColumnTypeChange")
    )
  )

  testSchemaChange(
    name = "widening and non-widening type changes",
    fromDDL = "a byte, b int",
    toDDL = "a int, b byte",
    expectedResult = expectNonWideningTypeChangeError
  )

  testSchemaChange(
    name = "widen single column with type widening disabled in Delta source",
    fromDDL = "a byte",
    toDDL = "a int",
    expectedResult = expectNonWideningTypeChangeError,
    confs = Seq(DeltaSQLConf.DELTA_ALLOW_TYPE_WIDENING_STREAMING_SOURCE.key -> "false")
  )

  testSchemaChange(
    name = "widen single column with type change check disabled",
    fromDDL = "a byte",
    toDDL = "a int",
    expectedResult = ExpectedResult.Success(),
    confs = Seq(DeltaSQLConf.DELTA_TYPE_WIDENING_BYPASS_STREAMING_TYPE_CHANGE_CHECK.key -> "true")
  )

  testSchemaChange(
    name = "widen single column with both type widening and type change check disabled",
    fromDDL = "a byte",
    toDDL = "a int",
    expectedResult = ExpectedResult.Success(),
    confs = Seq(
      DeltaSQLConf.DELTA_ALLOW_TYPE_WIDENING_STREAMING_SOURCE.key -> "false",
      DeltaSQLConf.DELTA_TYPE_WIDENING_BYPASS_STREAMING_TYPE_CHANGE_CHECK.key -> "true"
    )
  )

  testSchemaChange(
    name = "narrow single column",
    fromDDL = "a long",
    toDDL = "a int",
    expectedResult = expectNonWideningTypeChangeError
  )

  testSchemaChange(
    name = "narrow single column with type change check disabled",
    fromDDL = "a long",
    toDDL = "a int",
    expectedResult = ExpectedResult.Success(),
    confs = Seq(DeltaSQLConf.DELTA_TYPE_WIDENING_BYPASS_STREAMING_TYPE_CHANGE_CHECK.key -> "true")
  )

  testSchemaChange(
    name = "change to nullable",
    fromDDL = "a int not null",
    toDDL = "a int",
    expectedResult = ExpectedResult.Success()
  )
  testSchemaChange(
    name = "change to non-nullable",
    fromDDL = "a int",
    toDDL = "a int not null",
    expectedResult = expectNonWideningTypeChangeError
  )

  testSchemaChange(
    name = "widen and change to nullable",
    fromDDL = "a byte not null",
    toDDL = "a int",
    expectedResult = expectTypeWideningBlocked(Seq("  a: TINYINT -> INT")),
    unblock = Seq(
      Seq("allowSourceColumnTypeChange")
    )
  )

  testSchemaChange(
    name = "widen change to non-nullable",
    fromDDL = "a byte",
    toDDL = "a int not null",
    expectedResult = expectNonWideningTypeChangeError
  )

  testSchemaChange(
    name = "widen map",
    fromDDL = "a map<byte, short>",
    toDDL = "a map<short, int>",
    expectedResult = expectTypeWideningBlocked(Seq(
      "  a.key: TINYINT -> SMALLINT",
      "  a.value: SMALLINT -> INT"
    )),
    unblock = Seq(
      Seq("allowSourceColumnTypeChange")
    )
  )

  testSchemaChange(
    name = "widen array",
    fromDDL = "a array<byte>",
    toDDL = "a array<short>",
    expectedResult = expectTypeWideningBlocked(Seq("  a.element: TINYINT -> SMALLINT")),
    unblock = Seq(
      Seq("allowSourceColumnTypeChange")
    )
  )

  testSchemaChange(
    name = "widen struct",
    fromDDL = "a struct<x: byte>",
    toDDL = "a struct<x: short>",
    expectedResult = expectTypeWideningBlocked(Seq("  a.x: TINYINT -> SMALLINT")),
    unblock = Seq(
      Seq("allowSourceColumnTypeChange")
    )
  )

  testSchemaChange(
    name = "widen struct and struct field rename",
    fromDDL = "a struct<x: byte, y: int>",
    toDDL = "a struct<x: short, z: int>",
    toPhysicalNames = Map(Seq("a", "z") -> "y"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME AND TYPE WIDENING"),
    unblock = Seq(
      Seq("allowSourceColumnRename", "allowSourceColumnTypeChange"),
      Seq("allowSourceColumnRenameAndDrop", "allowSourceColumnTypeChange")
    )
  )

  testSchemaChange(
    name = "widen struct and struct field drop",
    fromDDL = "a struct<x: byte, y: int>",
    toDDL = "a struct<x: short>",
    expectedResult = expectColumnMappingChangeBlocked("DROP AND TYPE WIDENING"),
    unblock = Seq(
      Seq("allowSourceColumnDrop", "allowSourceColumnTypeChange"),
      Seq("allowSourceColumnRenameAndDrop", "allowSourceColumnTypeChange")
    )
  )

  testSchemaChange(
    name = "widen struct and struct field rename and drop",
    fromDDL = "a struct<x: byte, y: int, z: int>",
    toDDL = "a struct<x: short, w: int>",
    toPhysicalNames = Map(Seq("a", "w") -> "y"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME, DROP AND TYPE WIDENING"),
    unblock = Seq(
      Seq("allowSourceColumnRename", "allowSourceColumnDrop", "allowSourceColumnTypeChange"),
      Seq("allowSourceColumnRenameAndDrop", "allowSourceColumnTypeChange")
    )
  )

  test("combining individual SQL confs to unblock is supported") {
    withSQLConfUnblockedChanges(Seq("allowSourceColumnRename", "allowSourceColumnDrop")) {
      DeltaSourceMetadataEvolutionSupport.validateIfSchemaChangeCanBeUnblocked(
        spark,
        parameters = Map.empty,
        metadataPath = "sourceMetadataPath",
        currentSchema = persistedMetadata("a int", Map(Seq("a") -> "b")),
        previousSchema = persistedMetadata("a int, b int", Map.empty)
      )
    }
  }

  test("combining SQL confs and reader options to unblock is supported") {
    withSQLConfUnblockedChanges(Seq("allowSourceColumnRename")) {
      DeltaSourceMetadataEvolutionSupport.validateIfSchemaChangeCanBeUnblocked(
        spark,
        parameters = Map("allowSourceColumnDrop" -> "always"),
        metadataPath = "sourceMetadataPath",
        currentSchema = persistedMetadata("a int", Map(Seq("a") -> "b")),
        previousSchema = persistedMetadata("a int, b int", Map.empty)
      )
    }
  }

  test("unblocking column drop for specific version with reader option is supported") {
    DeltaSourceMetadataEvolutionSupport.validateIfSchemaChangeCanBeUnblocked(
      spark,
      parameters = Map("allowSourceColumnDrop" -> "0"),
      metadataPath = "sourceMetadataPath",
      currentSchema = persistedMetadata("a int", Map.empty),
      previousSchema = persistedMetadata("a int, b int", Map.empty)
    )
  }

  test("unblocking column rename for specific version with reader option is supported") {
    DeltaSourceMetadataEvolutionSupport.validateIfSchemaChangeCanBeUnblocked(
      spark,
      parameters = Map("allowSourceColumnRename" -> "0"),
      metadataPath = "sourceMetadataPath",
      currentSchema = persistedMetadata("b int", Map(Seq("b") -> "a")),
      previousSchema = persistedMetadata("a int", Map.empty)
    )
  }

  test("unblocking column type change for specific version with reader option is supported") {
    DeltaSourceMetadataEvolutionSupport.validateIfSchemaChangeCanBeUnblocked(
      spark,
      parameters = Map("allowSourceColumnTypeChange" -> "0"),
      metadataPath = "sourceMetadataPath",
      currentSchema = persistedMetadata("a long", Map.empty),
      previousSchema = persistedMetadata("a int", Map.empty)
    )
  }
}
