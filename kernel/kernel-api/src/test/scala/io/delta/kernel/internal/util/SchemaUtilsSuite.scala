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
import java.util.{Collections, Locale, Optional}

import scala.collection.JavaConverters._
import scala.collection.JavaConverters.mapAsJavaMapConverter

import io.delta.kernel.data.{ArrayValue, MapValue}
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.actions.{Format, Metadata}
import io.delta.kernel.internal.types.DataTypeJsonSerDe
import io.delta.kernel.internal.util.ColumnMapping.{COLUMN_MAPPING_ID_KEY, COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_PHYSICAL_NAME_KEY, ColumnMappingMode}
import io.delta.kernel.internal.util.SchemaUtils.{filterRecursively, validateSchema}
import io.delta.kernel.internal.util.VectorUtils.stringStringMapValue
import io.delta.kernel.types.{ArrayType, FieldMetadata, IntegerType, LongType, MapType, StringType, StructField, StructType}
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.LongType.LONG
import io.delta.kernel.types.TimestampType.TIMESTAMP

import org.scalatest.funsuite.AnyFunSuite

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
      1L -> new StructField("id", IntegerType.INTEGER, true))

    val fieldMappingAfter = Map(
      1L -> new StructField("id", IntegerType.INTEGER, true),
      2L -> new StructField("data", StringType.STRING, true))

    val schemaChanges = schemaChangesHelper(fieldMappingBefore, fieldMappingAfter)

    assert(schemaChanges.removedFields().isEmpty)
    assert(schemaChanges.updatedFields().isEmpty)
    assert(schemaChanges.addedFields().size() == 1)
    assert(schemaChanges.addedFields().get(0) == fieldMappingAfter(2))
  }

  test("Compute schema changes with renamed fields") {
    val fieldMappingBefore = Map(
      1L -> new StructField("id", IntegerType.INTEGER, true))

    val fieldMappingAfter = Map(
      1L -> new StructField("renamed_id", IntegerType.INTEGER, true))

    val schemaChanges = schemaChangesHelper(fieldMappingBefore, fieldMappingAfter)

    assert(schemaChanges.addedFields().isEmpty)
    assert(schemaChanges.removedFields().isEmpty)
    assert(schemaChanges.updatedFields().size() == 1)
    assert(schemaChanges.updatedFields().get(0) ==
      new Tuple2(fieldMappingBefore(1), fieldMappingAfter(1)))
  }

  test("Compute schema changes with type changed columns") {
    val fieldMappingBefore = Map(
      1L -> new StructField("id", IntegerType.INTEGER, true))

    val fieldMappingAfter = Map(
      1L -> new StructField("promoted_to_long", LongType.LONG, true))

    val schemaChanges = schemaChangesHelper(fieldMappingBefore, fieldMappingAfter)

    assert(schemaChanges.addedFields().isEmpty)
    assert(schemaChanges.removedFields().isEmpty)
    assert(schemaChanges.updatedFields().size() == 1)
    assert(schemaChanges.updatedFields().get(0) ==
      new Tuple2(fieldMappingBefore(1), fieldMappingAfter(1)))
  }

  test("Compute schema changes with dropped fields") {
    val fieldMappingBefore = Map(
      1L -> new StructField("id", IntegerType.INTEGER, true),
      2L -> new StructField("data", StringType.STRING, true))

    val fieldMappingAfter = Map(
      2L -> new StructField("data", StringType.STRING, true))

    val schemaChanges = schemaChangesHelper(fieldMappingBefore, fieldMappingAfter)

    assert(schemaChanges.addedFields().isEmpty)
    assert(schemaChanges.updatedFields().isEmpty)
    assert(schemaChanges.removedFields().size() == 1)
    assert(schemaChanges.removedFields().get(0) == fieldMappingBefore(1))
  }

  test("Compute schema changes with nullability change") {
    val fieldMappingBefore = Map(
      1L -> new StructField("id", IntegerType.INTEGER, true),
      2L -> new StructField("data", StringType.STRING, true))

    val fieldMappingAfter = Map(
      1L -> new StructField("id", IntegerType.INTEGER, true),
      2L -> new StructField("required_data", StringType.STRING, false))

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
      1L -> new StructField(
        "struct",
        new StructType()
          .add(new StructField("id", IntegerType.INTEGER, true))
          .add(new StructField("data", StringType.STRING, true)),
        true),
      2L -> new StructField("id", IntegerType.INTEGER, true),
      3L -> new StructField("data", StringType.STRING, true))

    val fieldMappingAfter = Map(
      1L -> new StructField(
        "struct",
        new StructType()
          .add(new StructField("data", StringType.STRING, true))
          .add(new StructField("id", IntegerType.INTEGER, true)),
        true),
      2L -> new StructField("id", IntegerType.INTEGER, true),
      3L -> new StructField("data", StringType.STRING, true))

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
      1L -> new StructField(
        "id",
        IntegerType.INTEGER,
        true,
        FieldMetadata.builder().putString(
          "metadata_col",
          "metadata_val").build()))

    val fieldMappingAfter = Map(
      1L -> new StructField(
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
      before: Map[Long, StructField],
      after: Map[Long, StructField]): SchemaChanges = {
    SchemaUtils.computeSchemaChangesById(
      before.map {
        case (k, v) => java.lang.Long.valueOf(k) -> v
      }.asJava,
      after.map {
        case (k, v) => java.lang.Long.valueOf(k) -> v
      }.asJava)
  }

  ///////////////////////////////////////////////////////////////////////////
  // validateUpdatedSchema
  ///////////////////////////////////////////////////////////////////////////
  test("validateUpdatedSchema fails when column mapping is disabled") {
    val current = new StructType().add(new StructField("id", IntegerType.INTEGER, true))
    val updated = current.add(new StructField("data", StringType.STRING, true))

    val e = intercept[IllegalArgumentException] {
      SchemaUtils.validateUpdatedSchema(
        current,
        updated,
        metadata(current, properties = Map(COLUMN_MAPPING_MODE_KEY -> "none")))
    }

    assert(e.getMessage == "Cannot validate updated schema when column mapping is disabled")
  }

  test("validateUpdatedSchema fails when physical names are not consistent across ids") {
    val current = new StructType()
      .add(
        "id",
        IntegerType.INTEGER,
        true,
        FieldMetadata.builder().putLong(COLUMN_MAPPING_ID_KEY, 1L)
          .putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, "id").build())

    val updated = new StructType()
      .add(
        "renamed_id",
        IntegerType.INTEGER,
        true,
        FieldMetadata.builder().putLong(COLUMN_MAPPING_ID_KEY, 1L)
          .putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, "inconsistent_id").build())

    val e = intercept[IllegalArgumentException] {
      SchemaUtils.validateUpdatedSchema(current, updated, metadata(current))
    }

    assert(e.getMessage == "Existing field with id 1 in current schema has " +
      "physical name id which is different from inconsistent_id")
  }

  test("validateUpdatedSchema fails when field is missing ID") {
    val current = new StructType()
      .add(
        "id",
        IntegerType.INTEGER,
        true,
        FieldMetadata.builder().putLong(COLUMN_MAPPING_ID_KEY, 1L)
          .putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, "id").build())

    val updated = new StructType()
      .add(
        "renamed_id",
        IntegerType.INTEGER,
        true,
        FieldMetadata.builder().putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, "id").build())

    val e = intercept[IllegalArgumentException] {
      SchemaUtils.validateUpdatedSchema(current, updated, metadata(current))
    }

    assert(e.getMessage == "Field renamed_id is missing column id")
  }

  test("validateUpdatedSchema fails when field is missing physical name") {
    val current = new StructType()
      .add(
        "id",
        IntegerType.INTEGER,
        true,
        FieldMetadata.builder().putLong(COLUMN_MAPPING_ID_KEY, 1L)
          .putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, "id").build())

    val updated = new StructType()
      .add(
        "renamed_id",
        IntegerType.INTEGER,
        true,
        FieldMetadata.builder().putLong(COLUMN_MAPPING_ID_KEY, 1L).build())

    val e = intercept[IllegalArgumentException] {
      SchemaUtils.validateUpdatedSchema(current, updated, metadata(current))
    }

    assert(e.getMessage == "Field renamed_id is missing physical name")
  }

  test("validateUpdatedSchema fails with schema with duplicate column ID") {
    val current = new StructType()
      .add(
        "id",
        IntegerType.INTEGER,
        true,
        FieldMetadata.builder().putLong(COLUMN_MAPPING_ID_KEY, 1L)
          .putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, "id").build())

    val updated = current.add(
      "duplicate_field",
      StringType.STRING,
      true,
      FieldMetadata.builder().putLong(COLUMN_MAPPING_ID_KEY, 1L)
        .putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, "id").build())

    val e = intercept[IllegalArgumentException] {
      SchemaUtils.validateUpdatedSchema(current, updated, metadata(current))
    }

    assert(e.getMessage == "Field duplicate_field with id 1 already exists")
  }

  test("validateUpdatedSchema succeeds with valid ID and physical name") {
    val current = new StructType()
      .add(
        "id",
        IntegerType.INTEGER,
        true,
        FieldMetadata.builder().putLong(COLUMN_MAPPING_ID_KEY, 1L)
          .putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, "id").build())

    val updated = new StructType()
      .add(
        "renamed_id",
        IntegerType.INTEGER,
        true,
        FieldMetadata.builder().putLong(COLUMN_MAPPING_ID_KEY, 1L)
          .putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, "id").build())

    SchemaUtils.validateUpdatedSchema(current, updated, metadata(current))
  }

  private def metadata(
      schema: StructType,
      properties: Map[String, String] =
        Map(ColumnMapping.COLUMN_MAPPING_MODE_KEY -> "id")): Metadata = {
    new Metadata(
      "id",
      Optional.empty(), /* name */
      Optional.empty(), /* description */
      new Format(),
      DataTypeJsonSerDe.serializeDataType(schema),
      schema,
      VectorUtils.buildArrayValue(util.Arrays.asList(), StringType.STRING), // partitionColumns
      Optional.empty(), // createdTime
      stringStringMapValue(properties.asJava) // configurationMap
    )
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
        assert(results === flattenedTestSchema.filterKeys(expectedColumns.contains))
      }
  }
}
