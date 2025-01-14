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

import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaTestUtilsBase, DeltaThrowable}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

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

  private val allUnblockConfs: Seq[String] = Seq(
    "allowSourceColumnRename",
    "allowSourceColumnDrop",
    "allowSourceTypeWidening",
    "allowSourceColumnRenameAndDrop",
    "allowSourceColumnRenameAndTypeWidening",
    "allowSourceColumnDropAndTypeWidening",
    "allowSourceColumnRenameAndDropAndTypeWidening"
  )

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

  private def withUnblockedChanges(unblock: Seq[String])(f: => Unit): Unit = {
    val confs = unblock.map( conf => s"spark.databricks.delta.streaming.$conf" -> "always")
    withSQLConf(confs: _*) {
      f
    }
  }

  private def testSchemaChange(
      name: String,
      fromDDL: String,
      fromPhysicalNames: Map[Seq[String], String] = Map.empty,
      toDDL: String,
      toPhysicalNames: Map[Seq[String], String] = Map.empty,
      expectedResult: ExpectedResult[Nothing],
      unblock: Seq[String] = Seq.empty,
      confs: Seq[(String, String)] = Seq.empty): Unit =
  test(s"$name") {
    def validate(): Unit =
      DeltaSourceMetadataEvolutionSupport.validateIfSchemaChangeCanBeUnblockedWithSQLConf(
        spark,
        metadataPath = "sourceMetadataPath",
        currentSchema = persistedMetadata(toDDL, toPhysicalNames),
        previousSchema = persistedMetadata(fromDDL, fromPhysicalNames)
      )
    withSQLConf(confs: _*) {
      expectedResult match {
        case ExpectedResult.Success(_) => validate()
        case ExpectedResult.Failure(checkError) =>
          withUnblockedChanges(allUnblockConfs.filterNot(unblock.contains)) {
            val ex = intercept[DeltaThrowable] {
              validate()
            }
            checkError(ex)
          }

          // Verify that any of the unblock conf will allow the schema change to go through.
          for (u <- unblock) {
            withUnblockedChanges(Seq(u)) {
              validate()
            }
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


  ///////////////
  // Rename column
  ///////////////
  testSchemaChange(
    name = "column rename, use logical names",
    fromDDL = "a int",
    toDDL = "b int",
    toPhysicalNames = Map(Seq("b") -> "a"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME COLUMN"),
    unblock = Seq(
      "allowSourceColumnRename",
      "allowSourceColumnRenameAndDrop",
      "allowSourceColumnRenameAndTypeWidening",
      "allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "column rename, use physical names",
    fromDDL = "a int",
    fromPhysicalNames = Map(Seq("a") -> "x"),
    toDDL = "b int",
    toPhysicalNames = Map(Seq("b") -> "x"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME COLUMN"),
    unblock = Seq(
      "allowSourceColumnRename",
      "allowSourceColumnRenameAndDrop",
      "allowSourceColumnRenameAndTypeWidening",
      "allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "column rename with widening type change",
    fromDDL = "a byte",
    fromPhysicalNames = Map(Seq("a") -> "x"),
    toDDL = "b int",
    toPhysicalNames = Map(Seq("b") -> "x"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME COLUMN"),
    // We don't block the type change itself: either the user treats 'b' as a nwe column and the
    // type change doesn't matter downstream, or 'b' is already in use in which case its type can
    // already be arbitrary.
    unblock = Seq(
      "allowSourceColumnRename",
      "allowSourceColumnRenameAndDrop",
      "allowSourceColumnRenameAndTypeWidening",
      "allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "column rename with non-widening type change",
    fromDDL = "a int",
    fromPhysicalNames = Map(Seq("a") -> "x"),
    toDDL = "b string",
    toPhysicalNames = Map(Seq("b") -> "x"),
    // We don't block the type change itself: either the user treats 'b' as a nwe column and the
    // type change doesn't matter downstream, or 'b' is already in use in which case its type can
    // already be arbitrary.
    expectedResult = expectColumnMappingChangeBlocked("RENAME COLUMN"),
    unblock = Seq(
      "allowSourceColumnRename",
      "allowSourceColumnRenameAndDrop",
      "allowSourceColumnRenameAndTypeWidening",
      "allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "swap columns",
    fromDDL = "a int, b int",
    toDDL = "b int, a int",
    toPhysicalNames = Map(Seq("b") -> "a", Seq("a") -> "b"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME COLUMN"),
    unblock = Seq(
      "allowSourceColumnRename",
      "allowSourceColumnRenameAndDrop",
      "allowSourceColumnRenameAndTypeWidening",
      "allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "swap columns with widening type change",
    fromDDL = "a byte, b byte",
    toDDL = "b byte, a int",
    toPhysicalNames = Map(Seq("b") -> "a", Seq("a") -> "b"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME AND TYPE WIDENING"),
    unblock = Seq(
      "allowSourceColumnRenameAndTypeWidening",
      "allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "swap columns with non-widening type change",
    fromDDL = "a int, b int",
    toDDL = "b int, a string",
    toPhysicalNames = Map(Seq("b") -> "a", Seq("a") -> "b"),
    expectedResult = expectNonWideningTypeChangeError
  )

  ///////////////
  // Drop column
  ///////////////
  testSchemaChange(
    name = "drop column, use logical names",
    fromDDL = "a int, b int",
    toDDL = "b int",
    expectedResult = expectColumnMappingChangeBlocked("DROP COLUMN"),
    unblock = Seq(
      "allowSourceColumnDrop",
      "allowSourceColumnRenameAndDrop",
      "allowSourceColumnDropAndTypeWidening",
      "allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "drop column, use physical names",
    fromDDL = "a int, b int",
    fromPhysicalNames = Map(Seq("a") -> "x", Seq("b") -> "y"),
    toDDL = "b int",
    toPhysicalNames = Map(Seq("b") -> "y"),
    expectedResult = expectColumnMappingChangeBlocked("DROP COLUMN"),
    unblock = Seq(
      "allowSourceColumnDrop",
      "allowSourceColumnRenameAndDrop",
      "allowSourceColumnDropAndTypeWidening",
      "allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "drop column with widening type change",
    fromDDL = "a byte, b byte",
    fromPhysicalNames = Map(Seq("a") -> "x", Seq("b") -> "y"),
    toDDL = "b int",
    toPhysicalNames = Map(Seq("b") -> "y"),
    expectedResult = expectColumnMappingChangeBlocked("DROP AND TYPE WIDENING"),
    unblock = Seq(
      "allowSourceColumnDropAndTypeWidening",
      "allowSourceColumnRenameAndDropAndTypeWidening")
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
      "allowSourceColumnDrop",
      "allowSourceColumnRenameAndDrop",
      "allowSourceColumnDropAndTypeWidening",
      "allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "drop column, swapped physical names with widening type change",
    fromDDL = "a byte, b byte",
    fromPhysicalNames = Map(Seq("a") -> "b", Seq("b") -> "a"),
    toDDL = "b int",
    toPhysicalNames = Map(Seq("b") -> "a"),
    expectedResult = expectColumnMappingChangeBlocked("DROP AND TYPE WIDENING"),
    unblock = Seq(
      "allowSourceColumnDropAndTypeWidening",
      "allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "drop column, swapped physical names with non-widening type change",
    fromDDL = "a int, b int",
    fromPhysicalNames = Map(Seq("a") -> "b", Seq("b") -> "a"),
    toDDL = "b float",
    toPhysicalNames = Map(Seq("b") -> "a"),
    expectedResult = expectNonWideningTypeChangeError
  )

  ///////////////
  // Drop and rename column
  ///////////////
  testSchemaChange(
    name = "drop column, rename to other column name",
    fromDDL = "a int, b int",
    toDDL = "c int",
    toPhysicalNames = Map(Seq("c") -> "b"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME AND DROP COLUMN"),
    unblock = Seq(
      "allowSourceColumnRenameAndDrop",
      "allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "drop column, rename to other column name with widening type change",
    fromDDL = "a byte, b byte",
    toDDL = "c int",
    toPhysicalNames = Map(Seq("c") -> "b"),
    // We don't block the type change itself here as the column is also renamed
    expectedResult = expectColumnMappingChangeBlocked("RENAME AND DROP COLUMN"),
    unblock = Seq(
      "allowSourceColumnRenameAndDrop",
      "allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "drop column, rename to other column name with non-widening type change",
    fromDDL = "a int, b int",
    toDDL = "c string",
    toPhysicalNames = Map(Seq("c") -> "b"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME AND DROP COLUMN"),
    unblock = Seq(
      "allowSourceColumnRenameAndDrop",
      "allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "drop column, rename to same column name",
    fromDDL = "a int, b int",
    toDDL = "a int",
    toPhysicalNames = Map(Seq("a") -> "b"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME AND DROP COLUMN"),
    unblock = Seq(
      "allowSourceColumnRenameAndDrop",
      "allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "drop column, rename to same column name with widening type change",
    fromDDL = "a byte, b byte",
    toDDL = "a int",
    toPhysicalNames = Map(Seq("a") -> "b"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME, DROP AND TYPE WIDENING"),
    unblock = Seq("allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "drop column, rename to same column name with non-widening type change",
    fromDDL = "a int, b int",
    toDDL = "a string",
    toPhysicalNames = Map(Seq("a") -> "b"),
    expectedResult = expectNonWideningTypeChangeError
  )

  ///////////////
  // Type changes
  ///////////////
  testSchemaChange(
    name = "widen single column",
    fromDDL = "a byte",
    toDDL = "a int",
    expectedResult = expectTypeWideningBlocked(Seq("  a: TINYINT -> INT")),
    unblock = Seq(
      "allowSourceTypeWidening",
      "allowSourceColumnRenameAndTypeWidening",
      "allowSourceColumnDropAndTypeWidening",
      "allowSourceColumnRenameAndDropAndTypeWidening")
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
      "allowSourceTypeWidening",
      "allowSourceColumnRenameAndTypeWidening",
      "allowSourceColumnDropAndTypeWidening",
      "allowSourceColumnRenameAndDropAndTypeWidening")
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
      "allowSourceTypeWidening",
      "allowSourceColumnRenameAndTypeWidening",
      "allowSourceColumnDropAndTypeWidening",
      "allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "widen array",
    fromDDL = "a array<byte>",
    toDDL = "a array<short>",
    expectedResult = expectTypeWideningBlocked(Seq("  a.element: TINYINT -> SMALLINT")),
    unblock = Seq(
      "allowSourceTypeWidening",
      "allowSourceColumnRenameAndTypeWidening",
      "allowSourceColumnDropAndTypeWidening",
      "allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "widen struct",
    fromDDL = "a struct<x: byte>",
    toDDL = "a struct<x: short>",
    expectedResult = expectTypeWideningBlocked(Seq("  a.x: TINYINT -> SMALLINT")),
    unblock = Seq(
      "allowSourceTypeWidening",
      "allowSourceColumnRenameAndTypeWidening",
      "allowSourceColumnDropAndTypeWidening",
      "allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "widen struct and struct field rename",
    fromDDL = "a struct<x: byte, y: int>",
    toDDL = "a struct<x: short, z: int>",
    toPhysicalNames = Map(Seq("a", "z") -> "y"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME AND TYPE WIDENING"),
    unblock = Seq(
      "allowSourceColumnRenameAndTypeWidening",
      "allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "widen struct and struct field drop",
    fromDDL = "a struct<x: byte, y: int>",
    toDDL = "a struct<x: short>",
    expectedResult = expectColumnMappingChangeBlocked("DROP AND TYPE WIDENING"),
    unblock = Seq(
      "allowSourceColumnDropAndTypeWidening",
      "allowSourceColumnRenameAndDropAndTypeWidening")
  )

  testSchemaChange(
    name = "widen struct and struct field rename and drop",
    fromDDL = "a struct<x: byte, y: int, z: int>",
    toDDL = "a struct<x: short, w: int>",
    toPhysicalNames = Map(Seq("a", "w") -> "y"),
    expectedResult = expectColumnMappingChangeBlocked("RENAME, DROP AND TYPE WIDENING"),
    unblock = Seq("allowSourceColumnRenameAndDropAndTypeWidening")
  )

  test("combining individual SQL confs to unblock isn't supported") {
    val ex = intercept[DeltaThrowable] {
      // We're dropping a column and renaming another one. Unblocking the DROP and RENAME separately
      // isn't supported, the user must use `allowSourceColumnRenameAndDrop` instead.
      withUnblockedChanges(Seq("allowSourceColumnRename", "allowSourceColumnDrop")) {
        DeltaSourceMetadataEvolutionSupport.validateIfSchemaChangeCanBeUnblockedWithSQLConf(
          spark,
          metadataPath = "sourceMetadataPath",
          currentSchema = persistedMetadata("a int", Map(Seq("a") -> "b")),
          previousSchema = persistedMetadata("a int, b int", Map.empty)
        )
      }
    }
    assert(ex.getErrorClass === "DELTA_STREAMING_CANNOT_CONTINUE_PROCESSING_POST_SCHEMA_EVOLUTION")
  }
}
