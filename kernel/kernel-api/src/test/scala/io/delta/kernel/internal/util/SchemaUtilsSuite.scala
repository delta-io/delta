/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.util

import java.util
import java.util.{Locale, Optional}
import java.util.Collections.emptySet

import scala.collection.JavaConverters._
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.reflect.ClassTag

import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.actions.{Format, Metadata}
import io.delta.kernel.internal.types.DataTypeJsonSerDe
import io.delta.kernel.internal.util.ColumnMapping.{COLUMN_MAPPING_ID_KEY, COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_NESTED_IDS_KEY, COLUMN_MAPPING_PHYSICAL_NAME_KEY, ColumnMappingMode}
import io.delta.kernel.internal.util.SchemaUtils.{filterRecursively, validateSchema, validateUpdatedSchema}
import io.delta.kernel.internal.util.VectorUtils.stringStringMapValue
import io.delta.kernel.types.{ArrayType, DataType, FieldMetadata, IntegerType, LongType, MapType, StringType, StructField, StructType}
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.LongType.LONG
import io.delta.kernel.types.TimestampType.TIMESTAMP

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.TableFor2
import org.scalatest.prop.Tables.Table

class SchemaUtilsSuite extends AnyFunSuite {
  private def expectFailure(shouldContain: String*)(f: => Unit): Unit = {
    val e = intercept[KernelException] {
      f
    }
    val msg = e.getMessage.toLowerCase(Locale.ROOT)
    assert(
      shouldContain.map(_.toLowerCase(Locale.ROOT)).forall(msg.contains),
      s"Error message '$msg' didn't contain: $shouldContain")
  }

  ///////////////////////////////////////////////////////////////////////////
  // Duplicate Column Checks
  ///////////////////////////////////////////////////////////////////////////

  test("duplicate column name in top level") {
    val schema = new StructType()
      .add("dupColName", INTEGER)
      .add("b", INTEGER)
      .add("dupColName", StringType.STRING)
    expectFailure("dupColName") {
      validateSchema(schema, false /* isColumnMappingEnabled */ )
    }
  }

  test("duplicate column name in top level - case sensitivity") {
    val schema = new StructType()
      .add("dupColName", INTEGER)
      .add("b", INTEGER)
      .add("dupCOLNAME", StringType.STRING)
    expectFailure("dupColName") {
      validateSchema(schema, false /* isColumnMappingEnabled */ )
    }
  }

  test("duplicate column name for nested column + non-nested column") {
    val schema = new StructType()
      .add(
        "dupColName",
        new StructType()
          .add("a", INTEGER)
          .add("b", INTEGER))
      .add("dupColName", INTEGER)
    expectFailure("dupColName") {
      validateSchema(schema, false /* isColumnMappingEnabled */ )
    }
  }

  test("duplicate column name for nested column + non-nested column - case sensitivity") {
    val schema = new StructType()
      .add(
        "dupColName",
        new StructType()
          .add("a", INTEGER)
          .add("b", INTEGER))
      .add("dupCOLNAME", INTEGER)
    expectFailure("dupCOLNAME") {
      validateSchema(schema, false /* isColumnMappingEnabled */ )
    }
  }

  test("duplicate column name in nested level") {
    val schema = new StructType()
      .add(
        "top",
        new StructType()
          .add("dupColName", INTEGER)
          .add("b", INTEGER)
          .add("dupColName", StringType.STRING))
    expectFailure("top.dupColName") {
      validateSchema(schema, false /* isColumnMappingEnabled */ )
    }
  }

  test("duplicate column name in nested level - case sensitivity") {
    val schema = new StructType()
      .add(
        "top",
        new StructType()
          .add("dupColName", INTEGER)
          .add("b", INTEGER)
          .add("dupCOLNAME", StringType.STRING))
    expectFailure("top.dupColName") {
      validateSchema(schema, false /* isColumnMappingEnabled */ )
    }
  }

  test("duplicate column name in double nested level") {
    val schema = new StructType()
      .add(
        "top",
        new StructType()
          .add(
            "b",
            new StructType()
              .add("dupColName", StringType.STRING)
              .add("c", INTEGER)
              .add("dupColName", StringType.STRING))
          .add("d", INTEGER))
    expectFailure("top.b.dupColName") {
      validateSchema(schema, false /* isColumnMappingEnabled */ )
    }
  }

  test("duplicate column name in double nested array") {
    val schema = new StructType()
      .add(
        "top",
        new StructType()
          .add(
            "b",
            new ArrayType(
              new ArrayType(
                new StructType()
                  .add("dupColName", StringType.STRING)
                  .add("c", INTEGER)
                  .add("dupColName", StringType.STRING),
                true),
              true))
          .add("d", INTEGER))
    expectFailure("top.b.element.element.dupColName") {
      validateSchema(schema, false /* isColumnMappingEnabled */ )
    }
  }

  test("only duplicate columns are listed in the error message") {
    val schema = new StructType()
      .add("top", new StructType().add("a", INTEGER).add("b", INTEGER).add("c", INTEGER)).add(
        "top",
        new StructType().add("b", INTEGER).add("c", INTEGER).add("d", INTEGER)).add(
        "bottom",
        new StructType().add("b", INTEGER).add("c", INTEGER).add("d", INTEGER))

    val e = intercept[KernelException] {
      validateSchema(schema, false /* isColumnMappingEnabled */ )
    }
    assert(e.getMessage.contains("Schema contains duplicate columns: top, top.b, top.c"))
  }

  test("duplicate column name in double nested map") {
    val keyType = new StructType()
      .add("dupColName", INTEGER)
      .add("d", StringType.STRING)
    expectFailure("top.b.key.dupColName") {
      val schema = new StructType()
        .add(
          "top",
          new StructType()
            .add("b", new MapType(keyType.add("dupColName", StringType.STRING), keyType, true)))
      validateSchema(schema, false /* isColumnMappingEnabled */ )
    }
    expectFailure("top.b.value.dupColName") {
      val schema = new StructType()
        .add(
          "top",
          new StructType()
            .add("b", new MapType(keyType, keyType.add("dupColName", StringType.STRING), true)))
      validateSchema(schema, false /* isColumnMappingEnabled */ )
    }
    // This is okay
    val schema = new StructType()
      .add(
        "top",
        new StructType()
          .add("b", new MapType(keyType, keyType, true)))
    validateSchema(schema, false /* isColumnMappingEnabled */ )
  }

  test("duplicate column name in nested array") {
    val schema = new StructType()
      .add(
        "top",
        new ArrayType(
          new StructType()
            .add("dupColName", INTEGER)
            .add("b", INTEGER)
            .add("dupColName", StringType.STRING),
          true))
    expectFailure("top.element.dupColName") {
      validateSchema(schema, false /* isColumnMappingEnabled */ )
    }
  }

  test("duplicate column name in nested array - case sensitivity") {
    val schema = new StructType()
      .add(
        "top",
        new ArrayType(
          new StructType()
            .add("dupColName", INTEGER)
            .add("b", INTEGER)
            .add("dupCOLNAME", StringType.STRING),
          true))
    expectFailure("top.element.dupColName") {
      validateSchema(schema, false /* isColumnMappingEnabled */ )
    }
  }

  test("non duplicate column because of back tick") {
    val schema = new StructType()
      .add(
        "top",
        new StructType()
          .add("a", INTEGER)
          .add("b", INTEGER))
      .add("top.a", INTEGER)
    validateSchema(schema, false /* isColumnMappingEnabled */ )
  }

  test("non duplicate column because of back tick - nested") {
    val schema = new StructType()
      .add(
        "first",
        new StructType()
          .add(
            "top",
            new StructType()
              .add("a", INTEGER)
              .add("b", INTEGER))
          .add("top.a", INTEGER))
    validateSchema(schema, false /* isColumnMappingEnabled */ )
  }

  test("duplicate column with back ticks - nested") {
    val schema = new StructType()
      .add(
        "first",
        new StructType()
          .add("top.a", StringType.STRING)
          .add("b", INTEGER)
          .add("top.a", INTEGER))
    expectFailure("first.`top.a`") {
      validateSchema(schema, false /* isColumnMappingEnabled */ )
    }
  }

  test("duplicate column with back ticks - nested and case sensitivity") {
    val schema = new StructType()
      .add(
        "first",
        new StructType()
          .add("TOP.a", StringType.STRING)
          .add("b", INTEGER)
          .add("top.a", INTEGER))
    expectFailure("first.`top.a`") {
      validateSchema(schema, false /* isColumnMappingEnabled */ )
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // checkFieldNames
  ///////////////////////////////////////////////////////////////////////////
  test("check non alphanumeric column characters") {
    val badCharacters = " ,;{}()\n\t="
    val goodCharacters = "#.`!@$%^&*~_<>?/:"

    badCharacters.foreach { char =>
      Seq(s"a${char}b", s"${char}ab", s"ab${char}", char.toString).foreach { name =>
        val schema = new StructType().add(name, INTEGER)
        val e = intercept[KernelException] {
          validateSchema(schema, false /* isColumnMappingEnabled */ )
        }

        if (char != '\n') {
          // with column mapping disabled this should be a valid name
          validateSchema(schema, true /* isColumnMappingEnabled */ )
        }

        assert(e.getMessage.contains("contains one of the unsupported"))
      }
    }

    goodCharacters.foreach { char =>
      // no issues here
      Seq(s"a${char}b", s"${char}ab", s"ab${char}", char.toString).foreach { name =>
        val schema = new StructType().add(name, INTEGER);
        validateSchema(schema, false /* isColumnMappingEnabled */ )
        validateSchema(schema, true /* isColumnMappingEnabled */ )
      }
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // computeSchemaChangesById
  ///////////////////////////////////////////////////////////////////////////
  test("Compute schema changes with added columns") {
    val fieldMappingBefore = Map(
      1 -> new StructField("id", IntegerType.INTEGER, true))

    val fieldMappingAfter = Map(
      1 -> new StructField("id", IntegerType.INTEGER, true),
      2 -> new StructField("data", StringType.STRING, true))

    val schemaChanges = schemaChangesHelper(fieldMappingBefore, fieldMappingAfter)

    assert(schemaChanges.removedFields().isEmpty)
    assert(schemaChanges.updatedFields().isEmpty)
    assert(schemaChanges.addedFields().size() == 1)
    assert(schemaChanges.addedFields().get(0) == fieldMappingAfter(2))
  }

  test("Compute schema changes with renamed fields") {
    val fieldMappingBefore = Map(
      1 -> new StructField("id", IntegerType.INTEGER, true))

    val fieldMappingAfter = Map(
      1 -> new StructField("renamed_id", IntegerType.INTEGER, true))

    val schemaChanges = schemaChangesHelper(fieldMappingBefore, fieldMappingAfter)

    assert(schemaChanges.addedFields().isEmpty)
    assert(schemaChanges.removedFields().isEmpty)
    assert(schemaChanges.updatedFields().size() == 1)
    assert(schemaChanges.updatedFields().get(0) ==
      new Tuple2(fieldMappingBefore(1), fieldMappingAfter(1)))
  }

  test("Compute schema changes with type changed columns") {
    val fieldMappingBefore = Map(
      1 -> new StructField("id", IntegerType.INTEGER, true))

    val fieldMappingAfter = Map(
      1 -> new StructField("promoted_to_long", LongType.LONG, true))

    val schemaChanges = schemaChangesHelper(fieldMappingBefore, fieldMappingAfter)

    assert(schemaChanges.addedFields().isEmpty)
    assert(schemaChanges.removedFields().isEmpty)
    assert(schemaChanges.updatedFields().size() == 1)
    assert(schemaChanges.updatedFields().get(0) ==
      new Tuple2(fieldMappingBefore(1), fieldMappingAfter(1)))
  }

  test("Compute schema changes with dropped fields") {
    val fieldMappingBefore = Map(
      1 -> new StructField("id", IntegerType.INTEGER, true),
      2 -> new StructField("data", StringType.STRING, true))

    val fieldMappingAfter = Map(
      2 -> new StructField("data", StringType.STRING, true))

    val schemaChanges = schemaChangesHelper(fieldMappingBefore, fieldMappingAfter)

    assert(schemaChanges.addedFields().isEmpty)
    assert(schemaChanges.updatedFields().isEmpty)
    assert(schemaChanges.removedFields().size() == 1)
    assert(schemaChanges.removedFields().get(0) == fieldMappingBefore(1))
  }

  test("Compute schema changes with nullability change") {
    val fieldMappingBefore = Map(
      1 -> new StructField("id", IntegerType.INTEGER, true),
      2 -> new StructField("data", StringType.STRING, true))

    val fieldMappingAfter = Map(
      1 -> new StructField("id", IntegerType.INTEGER, true),
      2 -> new StructField("required_data", StringType.STRING, false))

    val schemaChanges = schemaChangesHelper(fieldMappingBefore, fieldMappingAfter)

    assert(schemaChanges.addedFields().isEmpty)
    assert(schemaChanges.removedFields().isEmpty)
    assert(schemaChanges.updatedFields().size() == 1)
    assert(schemaChanges.updatedFields().get(0) == new Tuple2(
      fieldMappingBefore(2),
      fieldMappingAfter(2)))
  }

  test("Compute schema changes with moved fields") {
    val fieldMappingBefore = Map(
      1 -> new StructField(
        "struct",
        new StructType()
          .add(new StructField("id", IntegerType.INTEGER, true))
          .add(new StructField("data", StringType.STRING, true)),
        true),
      2 -> new StructField("id", IntegerType.INTEGER, true),
      3 -> new StructField("data", StringType.STRING, true))

    val fieldMappingAfter = Map(
      1 -> new StructField(
        "struct",
        new StructType()
          .add(new StructField("data", StringType.STRING, true))
          .add(new StructField("id", IntegerType.INTEGER, true)),
        true),
      2 -> new StructField("id", IntegerType.INTEGER, true),
      3 -> new StructField("data", StringType.STRING, true))

    val schemaChanges = schemaChangesHelper(fieldMappingBefore, fieldMappingAfter)

    assert(schemaChanges.addedFields().isEmpty)
    assert(schemaChanges.removedFields().isEmpty)
    assert(schemaChanges.updatedFields().size() == 1)
    assert(schemaChanges.updatedFields().get(0) == new Tuple2(
      fieldMappingBefore(1),
      fieldMappingAfter(1)))
  }

  test("Compute schema changes with field metadata changes") {
    val fieldMappingBefore = Map(
      1 -> new StructField(
        "id",
        IntegerType.INTEGER,
        true,
        FieldMetadata.builder().putString(
          "metadata_col",
          "metadata_val").build()))

    val fieldMappingAfter = Map(
      1 -> new StructField(
        "id",
        IntegerType.INTEGER,
        true,
        FieldMetadata.builder().putString(
          "metadata_col",
          "updated_metadata_val").build()))

    val schemaChanges = schemaChangesHelper(fieldMappingBefore, fieldMappingAfter)

    assert(schemaChanges.addedFields().isEmpty)
    assert(schemaChanges.removedFields().isEmpty)
    assert(schemaChanges.updatedFields().size() == 1)
    assert(schemaChanges.updatedFields().get(0) == new Tuple2(
      fieldMappingBefore(1),
      fieldMappingAfter(1)))
  }

  private def schemaChangesHelper(
      before: Map[Int, StructField],
      after: Map[Int, StructField]): SchemaChanges = {
    SchemaUtils.computeSchemaChangesById(
      before.map {
        case (k, v) => java.lang.Integer.valueOf(k) -> v
      }.asJava,
      after.map {
        case (k, v) => java.lang.Integer.valueOf(k) -> v
      }.asJava)
  }

  ///////////////////////////////////////////////////////////////////////////
  // validateUpdatedSchema
  ///////////////////////////////////////////////////////////////////////////

  test("validateUpdatedSchema fails when column mapping is disabled") {
    val current = new StructType().add(new StructField("id", IntegerType.INTEGER, true))
    val updated = current.add(new StructField("data", StringType.STRING, true))

    val e = intercept[IllegalArgumentException] {
      val tblProperties = Map(COLUMN_MAPPING_MODE_KEY -> "none")
      validateUpdatedSchema(
        metadata(current, properties = tblProperties),
        metadata(updated, properties = tblProperties),
        emptySet(),
        false // allowNewRequiredFields
      )
    }

    assert(e.getMessage == "Cannot validate updated schema when column mapping is disabled")
  }

  private val primitiveSchema = new StructType()
    .add(
      "id",
      IntegerType.INTEGER,
      true,
      fieldMetadata(id = 1, physicalName = "id"))

  private val mapWithStructKey = new StructType()
    .add(
      "map",
      new MapType(
        new StructType()
          .add("id", IntegerType.INTEGER, true, fieldMetadata(id = 2, physicalName = "id")),
        IntegerType.INTEGER,
        false),
      true,
      fieldMetadata(id = 1, physicalName = "map"))

  private val structWithArrayOfStructs = new StructType()
    .add(
      "top_level_struct",
      new StructType().add(
        "array",
        new ArrayType(
          new StructType().add("id", IntegerType.INTEGER, true, fieldMetadata(4, "id")),
          false),
        false,
        fieldMetadata(2, "array_field")),
      fieldMetadata(1, "top_level_struct"))

  private val updatedSchemasWithInconsistentPhysicalNames = Table(
    ("schemaBefore", "updatedSchemaWithInconsistentPhysicalNames"),
    // Top level primitive has inconsistent physical name
    (
      primitiveSchema,
      new StructType()
        .add(
          "renamed_id",
          IntegerType.INTEGER,
          true,
          fieldMetadata(id = 1, physicalName = "inconsistent_name"))),
    // Map with struct key has inconsistent physical name
    (
      mapWithStructKey,
      new StructType()
        .add(
          "map",
          new MapType(
            new StructType()
              .add(
                "renamed_id",
                IntegerType.INTEGER,
                false,
                fieldMetadata(id = 2, physicalName = "inconsistent_name")),
            IntegerType.INTEGER,
            false),
          true,
          fieldMetadata(id = 1, physicalName = "map"))),
    // Struct with array of struct field where inner struct field has inconsistent physical name
    (
      structWithArrayOfStructs,
      structWithArrayOfStructs(arrayType = new ArrayType(
        new StructType().add(
          "renamed_id",
          IntegerType.INTEGER,
          fieldMetadata(4, "inconsistent_name")),
        false))))

  test("validateUpdatedSchema fails when physical names are not consistent across ids") {
    assertSchemaEvolutionFailure[IllegalArgumentException](
      updatedSchemasWithInconsistentPhysicalNames,
      "Existing field with id .* in current schema" +
        " has physical name id which is different from inconsistent_name")
  }

  private val updatedSchemasMissingId = Table(
    ("schemaBefore", "updatedSchemaWithMissingId"),
    // Top level primitive missing field ID
    (
      primitiveSchema,
      new StructType()
        .add(
          "renamed_id",
          IntegerType.INTEGER,
          true,
          FieldMetadata.builder().putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, "id").build())),
    // Map with struct key missing field ID
    (
      mapWithStructKey,
      mapWithStructKey(mapType = new MapType(
        new StructType()
          .add(
            "renamed_id",
            IntegerType.INTEGER,
            false,
            FieldMetadata.builder().putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, "id").build()),
        IntegerType.INTEGER,
        false))),
    // Struct with array of struct field where inner struct is missing ID
    (
      structWithArrayOfStructs,
      structWithArrayOfStructs(new ArrayType(
        new StructType().add(
          "renamed_id",
          IntegerType.INTEGER,
          FieldMetadata.builder()
            .putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, "id").build()),
        false))))

  test("validateUpdatedSchema fails when field is missing ID") {
    assertSchemaEvolutionFailure[IllegalArgumentException](
      updatedSchemasMissingId,
      "Field renamed_id is missing column id")
  }

  private val updatedSchemasMissingPhysicalName = Table(
    ("schemaBefore", "updatedSchemaWithMissingPhysicalName"),
    // Top level primitive missing physical name
    (
      primitiveSchema,
      new StructType()
        .add(
          "renamed_id",
          IntegerType.INTEGER,
          true,
          FieldMetadata.builder().putLong(COLUMN_MAPPING_ID_KEY, 1).build())),
    // Map with struct key missing physical name
    (
      mapWithStructKey,
      mapWithStructKey(mapType = new MapType(
        new StructType()
          .add(
            "renamed_id",
            IntegerType.INTEGER,
            false,
            FieldMetadata.builder().putLong(COLUMN_MAPPING_ID_KEY, 1).build()),
        IntegerType.INTEGER,
        false))),
    // Struct with array of struct field where inner struct is missing physical name
    (
      structWithArrayOfStructs,
      structWithArrayOfStructs(arrayType = new ArrayType(
        new StructType().add(
          "renamed_id",
          IntegerType.INTEGER,
          FieldMetadata.builder()
            .putLong(COLUMN_MAPPING_ID_KEY, 4L).build()),
        false))))

  test("validateUpdatedSchema fails when field is missing physical name") {
    assertSchemaEvolutionFailure[IllegalArgumentException](
      updatedSchemasMissingPhysicalName,
      "Field renamed_id is missing physical name")
  }

  private val updatedSchemaHasDuplicateColumnId = Table(
    ("schemaBefore", "updatedSchemaWithMissingPhysicalName"),
    // Top level primitive has duplicate id
    (
      primitiveSchema,
      primitiveSchema
        .add(
          "duplicate_id",
          IntegerType.INTEGER,
          true,
          fieldMetadata(id = 1, physicalName = "duplicate_id"))),
    // Map with struct key adds duplicate field
    (
      mapWithStructKey,
      mapWithStructKey
        .add(
          "duplicate_id",
          IntegerType.INTEGER,
          fieldMetadata(id = 2, physicalName = "duplicate_id"))),
    // Struct with array of struct field where field with duplicate ID is added
    (
      structWithArrayOfStructs,
      structWithArrayOfStructs
        .add(
          "duplicate_id",
          IntegerType.INTEGER,
          fieldMetadata(4, "duplicate_id"))))

  test("validateUpdatedSchema fails with schema with duplicate column ID") {
    forAll(updatedSchemaHasDuplicateColumnId) { (schemaBefore, schemaAfter) =>
      val e = intercept[IllegalArgumentException] {
        validateUpdatedSchema(
          metadata(schemaBefore),
          metadata(schemaAfter),
          emptySet(),
          false /* allowNewRequiredFields */ )
      }

      assert(e.getMessage.matches("Field duplicate_id with id .* already exists"))
    }
  }

  private val validUpdatedSchemas = Table(
    ("schemaBefore", "updatedSchemaWithRenamedColumns"),
    // Top level primitive missing physical name
    (
      primitiveSchema,
      new StructType()
        .add(
          "renamed_id",
          IntegerType.INTEGER,
          true,
          fieldMetadata(id = 1, physicalName = "id"))),
    // Map with struct key renamed
    (
      mapWithStructKey,
      mapWithStructKey(
        new MapType(
          new StructType()
            .add(
              "renamed_id",
              IntegerType.INTEGER,
              true,
              fieldMetadata(id = 2, physicalName = "id")),
          IntegerType.INTEGER,
          false),
        fieldMetadata(id = 1, physicalName = "map"))),
    // Struct with array of struct field where inner struct field is renamed
    (
      structWithArrayOfStructs,
      new StructType()
        .add(
          "top_level_struct",
          new StructType().add(
            "array",
            new ArrayType(
              new StructType().add("renamed_id", IntegerType.INTEGER, fieldMetadata(4, "id")),
              false),
            false,
            fieldMetadata(2, "array_field")),
          fieldMetadata(1, "top_level_struct"))))

  test("validateUpdatedSchema succeeds with valid ID and physical name") {
    forAll(validUpdatedSchemas) { (schemaBefore, schemaAfter) =>
      validateUpdatedSchema(
        metadata(schemaBefore),
        metadata(schemaAfter),
        emptySet(),
        false /* allowNewRequiredFields */ )
    }
  }

  private val nonNullableFieldAdded = Table(
    ("schemaBefore", "schemaWithNonNullableFieldAdded"),
    (
      primitiveSchema,
      primitiveSchema.add(
        "required_field",
        IntegerType.INTEGER,
        false,
        fieldMetadata(3, "required_field"))),
    // Map with struct key where non-nullable field is added to struct
    (
      mapWithStructKey,
      mapWithStructKey(
        new MapType(
          new StructType()
            .add(
              "renamed_id",
              IntegerType.INTEGER,
              true,
              fieldMetadata(id = 2, physicalName = "id"))
            .add(
              "required_field",
              IntegerType.INTEGER,
              false,
              fieldMetadata(id = 3, physicalName = "required_field")),
          IntegerType.INTEGER,
          false),
        fieldMetadata(id = 1, physicalName = "map"))),
    // Struct of array of structs where non-nullable field is added to struct
    (
      structWithArrayOfStructs,
      structWithArrayOfStructs(
        arrayType = new ArrayType(
          new StructType().add(
            "renamed_id",
            IntegerType.INTEGER,
            fieldMetadata(4, "id"))
            .add("required_field", IntegerType.INTEGER, false, fieldMetadata(5, "required_field")),
          false),
        arrayName = "renamed_array")))

  test("validateUpdatedSchema fails when non-nullable field is added with " +
    "allowNewRequiredFields=false") {
    assertSchemaEvolutionFailure[KernelException](
      nonNullableFieldAdded,
      "Cannot add non-nullable field required_field",
      allowNewRequiredFields = false)
  }

  test("validateUpdatedSchema succeeds when non-nullable field is added with " +
    "allowNewRequiredFields=true") {
    forAll(nonNullableFieldAdded) { (schemaBefore, schemaAfter) =>
      validateUpdatedSchema(
        metadata(schemaBefore),
        metadata(schemaAfter),
        emptySet(),
        true /* allowNewRequiredFields */ )
    }
  }

  private val existingFieldNullabilityTightened = Table(
    ("schemaBefore", "schemaWithFieldNullabilityTightened"),
    (
      primitiveSchema,
      new StructType()
        .add(
          "id",
          IntegerType.INTEGER,
          false,
          fieldMetadata(id = 1, physicalName = "id"))),
    // Map with struct key where existing id field has nullability tightened
    (
      mapWithStructKey,
      mapWithStructKey(
        new MapType(
          new StructType()
            .add(
              "renamed_id",
              IntegerType.INTEGER,
              false,
              fieldMetadata(id = 2, physicalName = "id")),
          IntegerType.INTEGER,
          false),
        fieldMetadata(id = 1, physicalName = "map"))),
    // Struct of array of structs where id field in inner struct has nullability tightened
    (
      structWithArrayOfStructs,
      structWithArrayOfStructs(
        arrayType = new ArrayType(
          new StructType().add(
            "renamed_id",
            IntegerType.INTEGER,
            false,
            fieldMetadata(4, "id")),
          false),
        arrayName = "renamed_array")))

  test("validateUpdatedSchema fails when existing nullability is tightened with " +
    "allowNewRequiredFields=false") {
    assertSchemaEvolutionFailure[KernelException](
      existingFieldNullabilityTightened,
      "Cannot tighten the nullability of existing field id")
  }

  test("validateUpdatedSchema fails when existing nullability is tightened with " +
    "allowNewRequiredFields=true") {
    assertSchemaEvolutionFailure[KernelException](
      existingFieldNullabilityTightened,
      "Cannot tighten the nullability of existing field id",
      allowNewRequiredFields = true)
  }

  private val invalidTypeChange = Table(
    ("schemaBefore", "schemaWithInvalidTypeChange"),
    (
      primitiveSchema,
      new StructType()
        .add(
          "id",
          StringType.STRING,
          true,
          fieldMetadata(id = 1, physicalName = "id"))),
    // Map with struct key where id is changed to string
    (
      mapWithStructKey,
      mapWithStructKey(
        new MapType(
          new StructType()
            .add(
              "renamed_id_as_string",
              StringType.STRING,
              true,
              fieldMetadata(id = 2, physicalName = "id")),
          IntegerType.INTEGER,
          false),
        fieldMetadata(id = 1, physicalName = "map"))),
    // Struct of array of struct where inner id field is changed to string
    (
      structWithArrayOfStructs,
      structWithArrayOfStructs(
        arrayType = new ArrayType(
          new StructType().add(
            "renamed_id_as_string",
            StringType.STRING,
            true,
            fieldMetadata(4, "id")),
          false),
        arrayName = "renamed_array")))

  test("validateUpdatedSchema fails when invalid type change is performed") {
    assertSchemaEvolutionFailure[KernelException](
      invalidTypeChange,
      "Cannot change the type of existing field id from integer to string")
  }

  private val validateAddedFields = Table(
    ("schemaBefore", "schemaWithAddedField"),
    (
      primitiveSchema,
      primitiveSchema
        .add(
          "data",
          StringType.STRING,
          true,
          fieldMetadata(id = 2, physicalName = "data"))),
    // Map with struct key where data field is added
    (
      mapWithStructKey,
      mapWithStructKey(
        new MapType(
          new StructType()
            .add(
              "renamed_id",
              IntegerType.INTEGER,
              true,
              fieldMetadata(id = 2, physicalName = "id"))
            .add(
              "data",
              StringType.STRING,
              true,
              fieldMetadata(id = 3, physicalName = "data")),
          IntegerType.INTEGER,
          false),
        fieldMetadata(id = 1, physicalName = "map"))),
    // Struct of array of struct where inner struct has added string data field
    (
      structWithArrayOfStructs,
      structWithArrayOfStructs(
        arrayType = new ArrayType(
          new StructType().add(
            "renamed_id",
            IntegerType.INTEGER,
            true,
            fieldMetadata(4, "id"))
            .add(
              "data",
              StringType.STRING,
              true,
              fieldMetadata(5, "data")),
          false),
        arrayName = "renamed_array")))

  test("validateUpdatedSchema succeeds when adding field") {
    forAll(validateAddedFields) { (schemaBefore, schemaAfter) =>
      validateUpdatedSchema(
        metadata(schemaBefore),
        metadata(schemaAfter),
        emptySet(),
        false /* allowNewRequiredFields */ )
    }
  }

  private val validateMetadataChange = Table(
    ("schemaBefore", "schemaWithAddedField"),
    // Adding column comment to id column
    (
      primitiveSchema,
      new StructType()
        .add(
          "id",
          IntegerType.INTEGER,
          true,
          FieldMetadata.builder().fromMetadata(fieldMetadata(
            id = 1,
            physicalName = "id")).putString("comment", "id comment").build())),
    // Struct of array of struct where inner struct has added column comment to metadata
    (
      structWithArrayOfStructs,
      structWithArrayOfStructs(
        arrayType = new ArrayType(
          new StructType().add(
            "id",
            IntegerType.INTEGER,
            true,
            FieldMetadata.builder().fromMetadata(fieldMetadata(4, "id")).putString(
              "comment",
              "id comment").build()),
          false),
        arrayName = "renamed_array")))

  test("validateUpdatedSchema succeeds when updating field metadata") {
    forAll(validateMetadataChange) { (schemaBefore, schemaAfter) =>
      validateUpdatedSchema(
        metadata(schemaBefore),
        metadata(schemaAfter),
        emptySet(),
        false /* allowNewRequiredFields */ )
    }
  }

  private val underMaxColIdFieldAdded = Table(
    ("schemaBefore", "schemaWithUnderMaxFieldIdAdded"),
    (
      primitiveSchema,
      primitiveSchema.add(
        "too_low_field_id_field",
        IntegerType.INTEGER,
        true,
        fieldMetadata(0, "too_low_field_id_field"))),
    // Map with struct key where under max col-id field is added
    (
      mapWithStructKey,
      mapWithStructKey(
        new MapType(
          new StructType()
            .add(
              "renamed_id",
              IntegerType.INTEGER,
              true,
              fieldMetadata(id = 2, physicalName = "id"))
            .add(
              "too_low_field_id_field",
              IntegerType.INTEGER,
              true,
              fieldMetadata(id = 0, physicalName = "too_low_field_id_field")),
          IntegerType.INTEGER,
          true),
        fieldMetadata(id = 1, physicalName = "map"))),
    // Struct of array of structs where under max col-id field is added
    (
      structWithArrayOfStructs,
      structWithArrayOfStructs(
        arrayType = new ArrayType(
          new StructType().add(
            "renamed_id",
            IntegerType.INTEGER,
            fieldMetadata(4, "id"))
            .add(
              "too_low_field_id_field",
              IntegerType.INTEGER,
              true,
              fieldMetadata(0, "too_low_field_id_field")),
          true),
        arrayName = "renamed_array")))

  test("validateUpdatedSchema fails when a new field with a fieldId < maxColId is added") {
    assertSchemaEvolutionFailure[IllegalArgumentException](
      underMaxColIdFieldAdded,
      "Cannot add a new column with a fieldId <= maxFieldId. Found field: .* with"
        + "fieldId=0. Current maxFieldId in the table is: .*")
  }

  private def mapWithStructKey(
      mapType: DataType =
        mapWithStructKey.get("map").getDataType,
      mapFieldMetadata: FieldMetadata =
        mapWithStructKey.get("map").getMetadata): StructType = {
    new StructType()
      .add(
        "map",
        mapType,
        true,
        mapFieldMetadata)
  }

  private def structWithArrayOfStructs(
      arrayType: ArrayType =
        structWithArrayOfStructs
          .get("top_level_struct")
          .getDataType.asInstanceOf[StructType]
          .get("array").getDataType.asInstanceOf[ArrayType],
      arrayMetadata: FieldMetadata =
        structWithArrayOfStructs
          .get("top_level_struct")
          .getDataType.asInstanceOf[StructType]
          .get("array").getMetadata,
      arrayName: String = "array") = {
    new StructType()
      .add(
        "top_level_struct",
        new StructType().add(
          arrayName,
          arrayType,
          false,
          arrayMetadata),
        fieldMetadata(1, "top_level_struct"))
  }

  private def assertSchemaEvolutionFailure[T <: Throwable](
      evolutionCases: TableFor2[StructType, StructType],
      expectedMessage: String,
      tableProperties: Map[String, String] =
        Map(ColumnMapping.COLUMN_MAPPING_MODE_KEY -> "id"),
      allowNewRequiredFields: Boolean = false)(implicit classTag: ClassTag[T]) {
    forAll(evolutionCases) { (schemaBefore, schemaAfter) =>
      val e = intercept[T] {
        validateUpdatedSchema(
          metadata(schemaBefore, tableProperties),
          metadata(schemaAfter, tableProperties),
          emptySet(),
          allowNewRequiredFields)
      }

      assert(e.getMessage.matches(expectedMessage))
    }
  }

  private def metadata(
      schema: StructType,
      properties: Map[String, String] =
        Map(ColumnMapping.COLUMN_MAPPING_MODE_KEY -> "id")): Metadata = {
    val tblProperties = properties ++
      Map(ColumnMapping.COLUMN_MAPPING_MAX_COLUMN_ID_KEY ->
        ColumnMapping.findMaxColumnId(schema).toString)
    new Metadata(
      "id",
      Optional.empty(), /* name */
      Optional.empty(), /* description */
      new Format(),
      DataTypeJsonSerDe.serializeDataType(schema),
      schema,
      VectorUtils.buildArrayValue(util.Arrays.asList(), StringType.STRING), // partitionColumns
      Optional.empty(), // createdTime
      stringStringMapValue(tblProperties.asJava) // configurationMap
    )
  }

  private def fieldMetadata(id: Integer, physicalName: String): FieldMetadata = {
    FieldMetadata.builder()
      .putLong(COLUMN_MAPPING_ID_KEY, id.longValue())
      .putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, physicalName)
      .build()
  }

  ///////////////////////////////////////////////////////////////////////////
  // filterRecursively
  ///////////////////////////////////////////////////////////////////////////
  val testSchema = new StructType()
    .add("a", INTEGER)
    .add("b", INTEGER)
    .add("c", LONG)
    .add(
      "s",
      new StructType()
        .add("a", TIMESTAMP)
        .add("e", INTEGER)
        .add("f", LONG)
        .add(
          "g",
          new StructType()
            .add("a", INTEGER)
            .add("b", TIMESTAMP)
            .add("c", LONG)).add(
          "h",
          new MapType(
            new StructType().add("a", TIMESTAMP),
            new StructType().add("b", INTEGER),
            true)).add(
          "i",
          new ArrayType(
            new StructType().add("b", TIMESTAMP),
            true))).add(
      "d",
      new MapType(
        new StructType().add("b", TIMESTAMP),
        new StructType().add("a", INTEGER),
        true)).add(
      "e",
      new ArrayType(
        new StructType()
          .add("f", TIMESTAMP)
          .add("b", INTEGER),
        true))
  val flattenedTestSchema = {
    SchemaUtils.filterRecursively(
      testSchema,
      /* visitListMapTypes = */ true,
      /* stopOnFirstMatch = */ false,
      (v1: StructField) => true).asScala.map(f => f._1.asScala.mkString(".") -> f._2).toMap
  }
  Seq(
    // Format: (testPrefix, visitListMapTypes, stopOnFirstMatch, filter, expectedColumns)
    (
      "Filter by name 'b', stop on first match",
      true,
      true,
      (field: StructField) => field.getName == "b",
      Seq("b")),
    (
      "Filter by name 'b', visit all matches",
      false,
      false,
      (field: StructField) => field.getName == "b",
      Seq("b", "s.g.b")),
    (
      "Filter by name 'b', visit all matches including nested structures",
      true,
      false,
      (field: StructField) => field.getName == "b",
      Seq(
        "b",
        "s.g.b",
        "s.h.value.b",
        "s.i.element.b",
        "d.key.b",
        "e.element.b")),
    (
      "Filter by TIMESTAMP type, stop on first match",
      false,
      true,
      (field: StructField) => field.getDataType == TIMESTAMP,
      Seq("s.a")),
    (
      "Filter by TIMESTAMP type, visit all matches including nested structures",
      true,
      false,
      (field: StructField) => field.getDataType == TIMESTAMP,
      Seq(
        "s.a",
        "s.g.b",
        "s.h.key.a",
        "s.i.element.b",
        "d.key.b",
        "e.element.f")),
    (
      "Filter by TIMESTAMP type and name 'f', visit all matches",
      true,
      false,
      (field: StructField) => field.getDataType == TIMESTAMP && field.getName == "f",
      Seq("e.element.f")),
    (
      "Filter by non-existent field name 'z'",
      true,
      false,
      (field: StructField) => field.getName == "z",
      Seq())).foreach {
    case (testDescription, visitListMapTypes, stopOnFirstMatch, filter, expectedColumns) =>
      test(s"filterRecursively - $testDescription | " +
        s"visitListMapTypes=$visitListMapTypes, stopOnFirstMatch=$stopOnFirstMatch") {

        val results =
          filterRecursively(
            testSchema,
            visitListMapTypes,
            stopOnFirstMatch,
            (v1: StructField) => filter(v1))
            // convert to map of column path concatenated with '.' and the StructField
            .asScala.map(f => (f._1.asScala.mkString("."), f._2)).toMap

        // Assert that the number of results matches the expected columns
        assert(results.size === expectedColumns.size)
        assert(results === flattenedTestSchema.filterKeys(expectedColumns.contains).toMap)
      }
  }

  ///////////////////////////////////////////////////////////////////////////
  // validateNoMapStructKeyChanges
  ///////////////////////////////////////////////////////////////////////////

  private val updatedSchemasWithChangedMaps = Table(
    ("schemaBefore", "updatedSchemaWithChangedMapKey"),
    // add a col
    (
      mapWithStructKey,
      new StructType()
        .add(
          "map",
          new MapType(
            new StructType()
              .add("id", IntegerType.INTEGER, true, fieldMetadata(id = 2, physicalName = "id"))
              .add("id2", IntegerType.INTEGER, true, fieldMetadata(id = 3, physicalName = "id2")),
            IntegerType.INTEGER,
            false),
          true,
          fieldMetadata(id = 1, physicalName = "map"))),
    (
      new StructType()
        .add(
          "map",
          new MapType(
            new StructType()
              .add("id", IntegerType.INTEGER, true, fieldMetadata(id = 2, physicalName = "id"))
              .add("id2", IntegerType.INTEGER, true, fieldMetadata(id = 3, physicalName = "id2")),
            IntegerType.INTEGER,
            false),
          true,
          fieldMetadata(id = 1, physicalName = "map")),
      mapWithStructKey),
    (
      new StructType()
        .add(
          "top_level_struct",
          new StructType().add(
            "map",
            new MapType(
              new StructType()
                .add("id", IntegerType.INTEGER, true, fieldMetadata(id = 3, physicalName = "id")),
              IntegerType.INTEGER,
              false),
            true,
            fieldMetadata(2, "map")),
          fieldMetadata(1, "top_level_struct")),
      new StructType()
        .add(
          "top_level_struct",
          new StructType().add(
            "map",
            new MapType(
              new StructType()
                .add("id", IntegerType.INTEGER, true, fieldMetadata(id = 3, physicalName = "id"))
                .add("id2", IntegerType.INTEGER, true, fieldMetadata(id = 4, physicalName = "id")),
              IntegerType.INTEGER,
              false),
            true,
            fieldMetadata(2, "map")),
          fieldMetadata(1, "top_level_struct"))))

  test("validateNoMapStructKeyChanges fails when map struct changes") {
    val tblProperties = Map(
      TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.getKey -> "true",
      ColumnMapping.COLUMN_MAPPING_MODE_KEY -> "id")
    assertSchemaEvolutionFailure[KernelException](
      updatedSchemasWithChangedMaps,
      "Cannot change the type key of Map field map from .*",
      tableProperties = tblProperties)
  }
}
