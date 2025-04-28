/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults

import java.util.Collections.emptySet

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel.{Operation, Table}
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.expressions.Column
import io.delta.kernel.internal.{SnapshotImpl, TableConfig}
import io.delta.kernel.internal.util.{ColumnMapping, ColumnMappingSuiteBase}
import io.delta.kernel.types.{ArrayType, FieldMetadata, IntegerType, LongType, MapType, StringType, StructType}
import io.delta.kernel.utils.CloseableIterable.emptyIterable

import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables

/**
 * ToDo: Clean this up by moving some common schemas to fixtures and abstracting
 * the setup/run schema evolution/assert loop
 */
class DeltaTableSchemaEvolutionSuite extends DeltaTableWriteSuiteBase with ColumnMappingSuiteBase {

  test("Add nullable column succeeds and correctly updates maxFieldId") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema()
      val newSchema = new StructType()
        .add("a", StringType.STRING, true, currentSchema.get("a").getMetadata)
        .add(
          "b",
          new StructType()
            .add("d", IntegerType.INTEGER, true, fieldMetadataForColumn(4, "d"))
            .add("e", IntegerType.INTEGER, true, fieldMetadataForColumn(5, "e")),
          true,
          fieldMetadataForColumn(3, "b"))
        .add("c", IntegerType.INTEGER, true, currentSchema.get("c").getMetadata)

      table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
        .withSchema(engine, newSchema)
        .build(engine).commit(engine, emptyIterable())

      val structType = table.getLatestSnapshot(engine).getSchema
      assertColumnMapping(structType.get("a"), 1)

      val innerStruct = structType.get("b").getDataType.asInstanceOf[StructType]
      assertColumnMapping(innerStruct.get("d"), 4, "d")
      assertColumnMapping(innerStruct.get("e"), 5, "e")
      assertColumnMapping(structType.get("c"), 2)
      assert(getMaxFieldId(engine, tablePath) == 5)
    }
  }

  test("Drop column succeeds") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))
      assertColumnMapping(table.getLatestSnapshot(engine).getSchema.get("c"), 2)

      val currentSchema = table.getLatestSnapshot(engine).getSchema()
      val newSchema = new StructType()
        .add("a", StringType.STRING, true, currentSchema.get("a").getMetadata)

      table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
        .withSchema(engine, newSchema)
        .build(engine).commit(engine, emptyIterable())

      val structType = table.getLatestSnapshot(engine).getSchema
      assertColumnMapping(structType.get("a"), 1)
      assert(getMaxFieldId(engine, tablePath) == 2)
    }
  }

  test("Rename fields") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", StringType.STRING, true)
        .add(
          "b",
          new StructType()
            .add("d", IntegerType.INTEGER, true)
            .add("e", IntegerType.INTEGER, true),
          true)
        .add("c", IntegerType.INTEGER, true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema()

      val innerStruct = currentSchema.get("b").getDataType.asInstanceOf[StructType]
      val newSchema = new StructType()
        .add("a", StringType.STRING, true, currentSchema.get("a").getMetadata)
        .add(
          "b",
          new StructType()
            .add("renamed-d", IntegerType.INTEGER, true, innerStruct.get("d").getMetadata)
            .add("e", IntegerType.INTEGER, true, innerStruct.get("e").getMetadata),
          true,
          currentSchema.get("b").getMetadata)
        .add("renamed-c", IntegerType.INTEGER, true, currentSchema.get("c").getMetadata)

      table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
        .withSchema(engine, newSchema)
        .build(engine).commit(engine, emptyIterable())

      val updatedSchema = table.getLatestSnapshot(engine).getSchema
      assertColumnMapping(updatedSchema.get("a"), 1)

      val updatedInnerStruct = updatedSchema.get("b").getDataType.asInstanceOf[StructType]
      assertColumnMapping(updatedInnerStruct.get("renamed-d"), 3)
      assertColumnMapping(updatedInnerStruct.get("e"), 4)
      assertColumnMapping(updatedSchema.get("renamed-c"), 5)
    }
  }

  test("Move fields") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", StringType.STRING, true)
        .add(
          "b",
          new StructType()
            .add("d", IntegerType.INTEGER, true)
            .add("e", IntegerType.INTEGER, true),
          true)
        .add("c", IntegerType.INTEGER, true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema()

      val innerStruct = currentSchema.get("b").getDataType.asInstanceOf[StructType]
      val newSchema = new StructType()
        .add("a", StringType.STRING, true, currentSchema.get("a").getMetadata)
        .add("c", IntegerType.INTEGER, true, currentSchema.get("c").getMetadata)
        .add(
          "b",
          new StructType()
            .add("e", IntegerType.INTEGER, true, innerStruct.get("e").getMetadata)
            .add("d", IntegerType.INTEGER, true, innerStruct.get("d").getMetadata),
          true,
          currentSchema.get("b").getMetadata)

      table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
        .withSchema(engine, newSchema)
        .build(engine).commit(engine, emptyIterable())

      val updatedSchema = table.getLatestSnapshot(engine).getSchema
      assertColumnMapping(updatedSchema.get("a"), 1)

      val updatedInnerStruct = updatedSchema.get("b").getDataType.asInstanceOf[StructType]
      assertColumnMapping(updatedInnerStruct.get("d"), 3)
      assertColumnMapping(updatedInnerStruct.get("e"), 4)
      assertColumnMapping(updatedSchema.get("c"), 5)

      // Verify the top level and nested field reordering is maintained
      val topLevelFields = updatedSchema.fieldNames().asScala
      assert(topLevelFields == Array("a", "c", "b").toSeq)
      val innerFields = updatedInnerStruct.fieldNames().asScala
      assert(innerFields == Array("e", "d").toSeq)
    }
  }

  test("Updating schema with adding an array and map type") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", IntegerType.INTEGER, true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add("a", IntegerType.INTEGER, true, currentSchema.get("a").getMetadata)
        .add(
          "arr",
          new ArrayType(StringType.STRING, false),
          true,
          fieldMetadataForArrayColumn(2, "arr", "arr", 3))
        .add(
          "map",
          new MapType(StringType.STRING, StringType.STRING, false),
          true,
          fieldMetadataForMapColumn(4, "map", "map", 5, 6))

      table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
        .withSchema(engine, newSchema)
        .build(engine).commit(engine, emptyIterable())

      val structType = table.getLatestSnapshot(engine).getSchema
      assertColumnMapping(structType.get("a"), 1)
      assertColumnMapping(structType.get("arr"), 2, "arr")
      assertColumnMapping(structType.get("map"), 4, "map")
      assert(structType.get("arr").getMetadata.get(ColumnMapping.COLUMN_MAPPING_NESTED_IDS_KEY)
        == FieldMetadata.builder().putLong("arr.element", 3).build())
      assert(structType.get("map").getMetadata.get(ColumnMapping.COLUMN_MAPPING_NESTED_IDS_KEY)
        == FieldMetadata.builder().putLong("map.key", 5).putLong("map.value", 6).build())
    }
  }

  test("Add map whose values are array of struct") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", IntegerType.INTEGER, true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add("a", IntegerType.INTEGER, true, currentSchema.get("a").getMetadata)
        .add(
          "map",
          new MapType(
            StringType.STRING,
            new ArrayType(
              new StructType().add(
                "nested_map_value",
                IntegerType.INTEGER,
                fieldMetadataForColumn(3, "some-physical-column")),
              true),
            false),
          true,
          fieldMetadataForMapColumn(4, "map", "map", 5, 6))

      table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
        .withSchema(engine, newSchema)
        .build(engine).commit(engine, emptyIterable())

      val structType = table.getLatestSnapshot(engine).getSchema
      assertColumnMapping(structType.get("a"), 1)
      assertColumnMapping(structType.get("map"), 4, "map")
      assert(structType.get("map").getMetadata.get(ColumnMapping.COLUMN_MAPPING_NESTED_IDS_KEY)
        == FieldMetadata.builder().putLong("map.key", 5).putLong("map.value", 6).build())
    }
  }

  test("Drop nested struct field in map<int, array<struct>>") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", IntegerType.INTEGER, true)
        .add(
          "map",
          new MapType(
            StringType.STRING,
            new ArrayType(
              new StructType().add("field", IntegerType.INTEGER)
                .add("field_to_drop", IntegerType.INTEGER),
              true),
            false),
          true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val mapSchema = currentSchema.get("map").getDataType.asInstanceOf[MapType]
      val arrayValue = mapSchema.getValueType.asInstanceOf[ArrayType]
      val innerStruct = arrayValue.getElementType.asInstanceOf[StructType]

      val newSchema = new StructType()
        .add("a", IntegerType.INTEGER, true, currentSchema.get("a").getMetadata)
        .add(
          "map",
          new MapType(
            StringType.STRING,
            new ArrayType(
              new StructType()
                .add("field", IntegerType.INTEGER, innerStruct.get("field").getMetadata),
              true),
            false),
          true,
          currentSchema.get("map").getMetadata)

      table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
        .withSchema(engine, newSchema)
        .build(engine).commit(engine, emptyIterable())

      val structType = table.getLatestSnapshot(engine).getSchema
      assertColumnMapping(structType.get("a"), 1)
      assertColumnMapping(structType.get("map"), 2)
    }
  }

  test("Add nested struct field to map<int, array<struct>>") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", IntegerType.INTEGER, true)
        .add(
          "map",
          new MapType(
            StringType.STRING,
            new ArrayType(
              new StructType().add("field", IntegerType.INTEGER),
              true),
            false),
          true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val mapSchema = currentSchema.get("map").getDataType.asInstanceOf[MapType]
      val arrayValue = mapSchema.getValueType.asInstanceOf[ArrayType]
      val innerStruct = arrayValue.getElementType.asInstanceOf[StructType]

      val newSchema = new StructType()
        .add("a", IntegerType.INTEGER, true, currentSchema.get("a").getMetadata)
        .add(
          "map",
          new MapType(
            StringType.STRING,
            new ArrayType(
              new StructType()
                .add("field", IntegerType.INTEGER, innerStruct.get("field").getMetadata)
                .add(
                  "field_to_add",
                  IntegerType.INTEGER,
                  fieldMetadataForColumn(6, "field_to_add")),
              true),
            false),
          true,
          currentSchema.get("map").getMetadata)

      table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
        .withSchema(engine, newSchema)
        .build(engine).commit(engine, emptyIterable())

      val structType = table.getLatestSnapshot(engine).getSchema
      assertColumnMapping(structType.get("a"), 1)
      assertColumnMapping(structType.get("map"), 2)

      val mapType = structType.get("map").getDataType.asInstanceOf[MapType]
      val updatedArrayValue = mapType.getValueField.getDataType.asInstanceOf[ArrayType]
      val updatedInnerStruct = updatedArrayValue.getElementType.asInstanceOf[StructType]

      assertColumnMapping(updatedInnerStruct.get("field_to_add"), 6, "field_to_add")

    }
  }

  test("Renaming clustering columns") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("clustering-col", StringType.STRING, true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        clusteringColsOpt = Some(List(new Column("clustering-col"))),
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val expectedSchema = new StructType()
        .add(
          "renamed-clustering-col",
          StringType.STRING,
          true,
          currentSchema.get("clustering-col").getMetadata)

      table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
        .withSchema(engine, expectedSchema)
        .build(engine)
        .commit(engine, emptyIterable())

      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      val actualSchema = snapshot.getSchema

      assert(expectedSchema == actualSchema)
    }
  }

  test("Add nested array field to map<int, struct> with already assigned IDs") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", IntegerType.INTEGER, true)
        .add(
          "map",
          new MapType(
            StringType.STRING,
            new StructType().add("field", IntegerType.INTEGER),
            false),
          true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val mapSchema = currentSchema.get("map").getDataType.asInstanceOf[MapType]
      val innerStruct = mapSchema.getValueType.asInstanceOf[StructType]

      val newSchema = new StructType()
        .add("a", IntegerType.INTEGER, true, currentSchema.get("a").getMetadata)
        .add(
          "map",
          new MapType(
            StringType.STRING,
            new StructType()
              .add("field", IntegerType.INTEGER, innerStruct.get("field").getMetadata)
              .add(
                "array_field_to_add",
                new ArrayType(IntegerType.INTEGER, true),
                FieldMetadata.builder()
                  .putFieldMetadata(
                    ColumnMapping.COLUMN_MAPPING_NESTED_IDS_KEY,
                    FieldMetadata.builder().putLong("array_field_to_add", 7).build())
                  .putLong(ColumnMapping.COLUMN_MAPPING_ID_KEY, 6)
                  .putString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, "array_field_to_add")
                  .build()),
            false),
          true,
          currentSchema.get("map").getMetadata)

      table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
        .withSchema(engine, newSchema)
        .build(engine).commit(engine, emptyIterable())

      val structType = table.getLatestSnapshot(engine).getSchema
      assertColumnMapping(structType.get("a"), 1)
      assertColumnMapping(structType.get("map"), 2)
      val mapType = structType.get("map").getDataType.asInstanceOf[MapType]
      val updatedInnerStruct = mapType.getValueType.asInstanceOf[StructType]

      assertColumnMapping(updatedInnerStruct.get("array_field_to_add"), 6, "array_field_to_add")
    }
  }

  test("Add nested array field to map<int, struct> with fresh IDs") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", IntegerType.INTEGER, true)
        .add(
          "map",
          new MapType(
            StringType.STRING,
            new StructType().add("field", IntegerType.INTEGER),
            false),
          true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val mapSchema = currentSchema.get("map").getDataType.asInstanceOf[MapType]
      val innerStruct = mapSchema.getValueType.asInstanceOf[StructType]

      val newSchema = new StructType()
        .add(
          "map",
          new MapType(
            StringType.STRING,
            new StructType()
              .add(
                "array_field_to_add",
                new ArrayType(IntegerType.INTEGER, true))
              .add("field", IntegerType.INTEGER, innerStruct.get("field").getMetadata),
            false),
          true,
          currentSchema.get("map").getMetadata)
        .add("a", IntegerType.INTEGER, true, currentSchema.get("a").getMetadata)

      table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
        .withSchema(engine, newSchema)
        .build(engine).commit(engine, emptyIterable())

      val structType = table.getLatestSnapshot(engine).getSchema
      assertColumnMapping(structType.get("a"), 1)
      assertColumnMapping(structType.get("map"), 2)
      val mapType = structType.get("map").getDataType.asInstanceOf[MapType]
      val updatedInnerStruct = mapType.getValueType.asInstanceOf[StructType]

      assertColumnMapping(updatedInnerStruct.get("array_field_to_add"), 6, "array_field_to_add")
    }
  }

  test("Drop nested array field in map<int, struct>") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", IntegerType.INTEGER, true)
        .add(
          "map",
          new MapType(
            StringType.STRING,
            new StructType().add("field", IntegerType.INTEGER)
              .add("array_field_to_drop", new ArrayType(IntegerType.INTEGER, true)),
            false),
          true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val mapSchema = currentSchema.get("map").getDataType.asInstanceOf[MapType]
      val innerStruct = mapSchema.getValueType.asInstanceOf[StructType]

      val newSchema = new StructType()
        .add("a", IntegerType.INTEGER, true, currentSchema.get("a").getMetadata)
        .add(
          "map",
          new MapType(
            StringType.STRING,
            new StructType()
              .add("field", IntegerType.INTEGER, innerStruct.get("field").getMetadata),
            false),
          true,
          currentSchema.get("map").getMetadata)

      table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
        .withSchema(engine, newSchema)
        .build(engine).commit(engine, emptyIterable())

      val structType = table.getLatestSnapshot(engine).getSchema
      assertColumnMapping(structType.get("a"), 1)
      assertColumnMapping(structType.get("map"), 2)
      val mapType = structType.get("map").getDataType.asInstanceOf[MapType]
      val updatedInnerStruct = mapType.getValueType.asInstanceOf[StructType]

      assert(updatedInnerStruct == innerStruct.withoutField("array_field_to_drop"))
    }
  }

  test("Rename nested array field in map<int, struct>") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", IntegerType.INTEGER, true)
        .add(
          "map",
          new MapType(
            StringType.STRING,
            new StructType().add("field", IntegerType.INTEGER)
              .add("array_field_to_rename", new ArrayType(IntegerType.INTEGER, true)),
            false),
          true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val mapSchema = currentSchema.get("map").getDataType.asInstanceOf[MapType]
      val innerStruct = mapSchema.getValueType.asInstanceOf[StructType]

      val newSchema = new StructType()
        .add("a", IntegerType.INTEGER, true, currentSchema.get("a").getMetadata)
        .add(
          "map",
          new MapType(
            StringType.STRING,
            new StructType()
              .add("field", IntegerType.INTEGER, innerStruct.get("field").getMetadata)
              .add(
                "renamed_array_field",
                new ArrayType(IntegerType.INTEGER, true),
                innerStruct.get("array_field_to_rename").getMetadata),
            false),
          true,
          currentSchema.get("map").getMetadata)

      table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
        .withSchema(engine, newSchema)
        .build(engine).commit(engine, emptyIterable())

      val structType = table.getLatestSnapshot(engine).getSchema
      assertColumnMapping(structType.get("a"), 1)
      assertColumnMapping(structType.get("map"), 2)
      val mapType = structType.get("map").getDataType.asInstanceOf[MapType]
      val updatedInnerStruct = mapType.getValueType.asInstanceOf[StructType]

      assert(updatedInnerStruct.get("renamed_array_field").getDataType
        == innerStruct.get("array_field_to_rename").getDataType)
      assert(updatedInnerStruct.get("renamed_array_field").getMetadata
        == innerStruct.get("array_field_to_rename").getMetadata)
    }
  }

  test("Adding struct of structs") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "name",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema()
      val newSchema = new StructType()
        .add("a", StringType.STRING, true, currentSchema.get("a").getMetadata)
        .add(
          "b",
          new StructType()
            .add(
              "d",
              new StructType().add("e", IntegerType.INTEGER, fieldMetadataForColumn(5, "e")),
              true,
              fieldMetadataForColumn(4, "d")),
          true,
          fieldMetadataForColumn(3, "b"))
        .add("c", IntegerType.INTEGER, true, currentSchema.get("c").getMetadata)

      table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
        .withSchema(engine, newSchema)
        .build(engine).commit(engine, emptyIterable())

      val structType = table.getLatestSnapshot(engine).getSchema
      assertColumnMapping(structType.get("a"), 1)

      val firstInnerStruct = structType.get("b").getDataType.asInstanceOf[StructType]
      assertColumnMapping(firstInnerStruct.get("d"), 4, "d")

      val secondInnerStruct = firstInnerStruct.get("d").getDataType.asInstanceOf[StructType]
      assertColumnMapping(secondInnerStruct.get("e"), 5, "e")
    }
  }

  test("Add array of arrays") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", StringType.STRING, true)
      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema()

      val newSchema = new StructType()
        .add("a", StringType.STRING, true, currentSchema.get("a").getMetadata)
        .add(
          "array_of_arrays",
          new ArrayType(new ArrayType(IntegerType.INTEGER, true), true),
          true,
          FieldMetadata.builder()
            .putString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, "array_of_arrays")
            .putLong(ColumnMapping.COLUMN_MAPPING_ID_KEY, 4L)
            .putFieldMetadata(
              ColumnMapping.COLUMN_MAPPING_NESTED_IDS_KEY,
              FieldMetadata.builder().putLong("array_of_arrays.element", 2L)
                .putLong("array_of_arrays.element.element", 3L).build()).build())

      table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
        .withSchema(engine, newSchema)
        .build(engine).commit(engine, emptyIterable())

      val updatedSchema = table.getLatestSnapshot(engine).getSchema()

      assertColumnMapping(updatedSchema.get("a"), 1)
      assertColumnMapping(updatedSchema.get("array_of_arrays"), 4L, "array_of_arrays")

      val arrayMetadata = updatedSchema.get("array_of_arrays").getMetadata
      assert(arrayMetadata.getMetadata(ColumnMapping.COLUMN_MAPPING_NESTED_IDS_KEY)
        .getLong("array_of_arrays.element") == 2L)
      assert(arrayMetadata.getMetadata(ColumnMapping.COLUMN_MAPPING_NESTED_IDS_KEY)
        .getLong("array_of_arrays.element.element") == 3L)
    }
  }

  test("Changing column mapping on table and evolve schema at same time fails") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      // Create a table initially without column mapping
      createEmptyTable(engine, tablePath, initialSchema)

      val currentSchema = table.getLatestSnapshot(engine).getSchema()
      val newSchema = new StructType()
        .add("a", StringType.STRING, true, currentSchema.get("a").getMetadata)
        .add(
          "b",
          new StructType()
            .add("d", IntegerType.INTEGER, true, fieldMetadataForColumn(4, "d"))
            .add("e", IntegerType.INTEGER, true, fieldMetadataForColumn(5, "e")),
          true,
          fieldMetadataForColumn(3, "b"))
        .add("c", IntegerType.INTEGER, true, currentSchema.get("c").getMetadata)

      assertSchemaEvolutionFails[IllegalArgumentException](
        table,
        engine,
        newSchema,
        "Cannot update mapping mode and perform schema evolution",
        Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "name",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))
    }
  }

  test("Updating schema on table when column mapping disabled fails") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      createEmptyTable(engine, tablePath, initialSchema, tableProperties = Map.empty)

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add("a", StringType.STRING, true, currentSchema.get("a").getMetadata)
        .add(
          "b",
          new StructType()
            .add("d", IntegerType.INTEGER, true, fieldMetadataForColumn(4, "d")),
          true,
          fieldMetadataForColumn(3, "b"))
        .add("c", IntegerType.INTEGER, true, currentSchema.get("c").getMetadata)

      assertSchemaEvolutionFails[KernelException](
        table,
        engine,
        newSchema,
        "Cannot update schema for table when column mapping is disabled")
    }
  }

  test("Move partition columns") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("partition1", StringType.STRING, true)
        .add("partition2", IntegerType.INTEGER, true)
        .add("data", StringType.STRING, true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        partCols = Seq("partition1", "partition2"),
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "name",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add("partition2", IntegerType.INTEGER, true, currentSchema.get("partition2").getMetadata)
        .add("partition1", StringType.STRING, true, currentSchema.get("partition1").getMetadata)
        .add("data", StringType.STRING, true, currentSchema.get("data").getMetadata)

      table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
        .withSchema(engine, newSchema)
        .build(engine).commit(engine, emptyIterable())
      val updatedSchema = table.getLatestSnapshot(engine).getSchema

      // Verify the ordering is expected
      val topLevelFields = updatedSchema.fieldNames().asScala
      assert(topLevelFields == Array("partition2", "partition1", "data").toSeq)
    }
  }

  test("Updating schema with duplicate field IDs fails") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add("a", StringType.STRING, true, currentSchema.get("a").getMetadata)
        .add(
          "b",
          new StructType()
            .add("duplicate_field_id", IntegerType.INTEGER, true, fieldMetadataForColumn(1, "d"))
            .add("e", IntegerType.INTEGER, true, fieldMetadataForColumn(5, "e")),
          true,
          fieldMetadataForColumn(3, "b"))
        .add("c", IntegerType.INTEGER, true, currentSchema.get("c").getMetadata)

      assertSchemaEvolutionFails[IllegalArgumentException](
        table,
        engine,
        newSchema,
        "Field duplicate_field_id with id 1 already exists")
    }
  }

  test("Adding non-nullable field fails") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add("a", StringType.STRING, true, currentSchema.get("a").getMetadata)
        .add(
          "b",
          new StructType()
            .add("non_nullable_field", IntegerType.INTEGER, false, fieldMetadataForColumn(4, "d"))
            .add("e", IntegerType.INTEGER, true, fieldMetadataForColumn(5, "e")),
          true,
          fieldMetadataForColumn(3, "b"))
        .add("c", IntegerType.INTEGER, true, currentSchema.get("c").getMetadata)

      assertSchemaEvolutionFails[KernelException](
        table,
        engine,
        newSchema,
        "Cannot add non-nullable field non_nullable_field")
    }
  }

  test("Adding non-nullable field to map value which is a struct fails") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", StringType.STRING, true)
        .add(
          "map",
          new MapType(
            StringType.STRING,
            new ArrayType(
              new StructType().add("nested_map_value", IntegerType.INTEGER),
              true),
            false))

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "name",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val mapSchema = currentSchema.get("map").getDataType.asInstanceOf[MapType]
      val arrayValue = mapSchema.getValueType.asInstanceOf[ArrayType]
      val innerStruct = arrayValue.getElementType.asInstanceOf[StructType]
      val newSchema = new StructType()
        .add("a", StringType.STRING, true, currentSchema.get("a").getMetadata)
        .add(
          "map",
          new MapType(
            StringType.STRING,
            new ArrayType(
              new StructType().add(
                "nested_map_value",
                IntegerType.INTEGER,
                innerStruct.get("nested_map_value").getMetadata)
                .add(
                  "new_required_field",
                  IntegerType.INTEGER,
                  false,
                  fieldMetadataForColumn(7, "7")),
              true),
            false),
          true,
          fieldMetadataForMapColumn(4, "map", "map", 5, 6))

      assertSchemaEvolutionFails[KernelException](
        table,
        engine,
        newSchema,
        "Cannot add non-nullable field new_required_field")
    }
  }

  test("Cannot drop a partition column") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        partCols = Seq("c"),
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add("a", StringType.STRING, true, currentSchema.get("a").getMetadata)
        .add(
          "b",
          new StructType()
            .add("d", IntegerType.INTEGER, true, fieldMetadataForColumn(4, "d"))
            .add("e", IntegerType.INTEGER, true, fieldMetadataForColumn(5, "e")),
          true,
          fieldMetadataForColumn(3, "b"))

      assertSchemaEvolutionFails[IllegalArgumentException](
        table,
        engine,
        newSchema,
        "Partition column c not found in the schema")
    }
  }

  test("Cannot rename a partition column") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        partCols = Seq("c"),
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add("a", StringType.STRING, true, currentSchema.get("a").getMetadata)
        .add("e", IntegerType.INTEGER, true, currentSchema.get("c").getMetadata)

      assertSchemaEvolutionFails[IllegalArgumentException](
        table,
        engine,
        newSchema,
        "Partition column c not found in the schema")
    }
  }

  test("Cannot change types") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add("a", StringType.STRING, true, currentSchema.get("a").getMetadata)
        .add("c", LongType.LONG, true, currentSchema.get("c").getMetadata)

      assertSchemaEvolutionFails[IllegalArgumentException](
        table,
        engine,
        newSchema,
        "Cannot change the type of existing field c from integer to long")
    }
  }

  test("Cannot change clustering column type") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("clustering_col", StringType.STRING, true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        clusteringColsOpt = Some(List(new Column("clustering_col"))),
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add("clustering_col", LongType.LONG, true, currentSchema.get("clustering_col").getMetadata)

      assertSchemaEvolutionFails[IllegalArgumentException](
        table,
        engine,
        newSchema,
        "Cannot change the type of existing field clustering_col from string to long")
    }
  }

  test("Updating schema if physical columns are not preserved fails") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add(
          "a",
          StringType.STRING,
          true,
          fieldMetadataForColumn(1, "not-preserving-physical-column"))
        .add("c", LongType.LONG, true, currentSchema.get("c").getMetadata)

      assertSchemaEvolutionFails[IllegalArgumentException](
        table,
        engine,
        newSchema,
        "Existing field with id 1 in current schema has physical name")
    }
  }

  test("Updating schema and tightening nullability on existing field fails") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", IntegerType.INTEGER, true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add("renamed_a", IntegerType.INTEGER, false, currentSchema.get("a").getMetadata)

      assertSchemaEvolutionFails[IllegalArgumentException](
        table,
        engine,
        newSchema,
        "Cannot tighten the nullability of existing field a")
    }
  }

  test("Cannot tighten nullability on renamed array element") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", IntegerType.INTEGER, true)
        .add(
          "arr",
          new ArrayType(StringType.STRING, true))

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add("a", IntegerType.INTEGER, true, currentSchema.get("a").getMetadata)
        .add(
          "some_renamed_array",
          new ArrayType(StringType.STRING, false),
          currentSchema.get("arr").getMetadata)

      assertSchemaEvolutionFails[IllegalArgumentException](
        table,
        engine,
        newSchema,
        "Cannot tighten the nullability of existing field")
    }
  }

  test("Cannot change a partition column type") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val initialSchema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      createEmptyTable(
        engine,
        tablePath,
        initialSchema,
        partCols = Seq("c"),
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add("c", StringType.STRING, true, currentSchema.get("c").getMetadata)
        .add("a", StringType.STRING, true, currentSchema.get("a").getMetadata)

      assertSchemaEvolutionFails[IllegalArgumentException](
        table,
        engine,
        newSchema,
        "Cannot change the type of existing field c from integer to string")
    }
  }

  val primitiveSchemaWithClusteringColumn = new StructType()
    .add(
      "clustering_col",
      IntegerType.INTEGER,
      fieldMetadataForColumn(1, "clustering_col_physical"))
    .add("data", IntegerType.INTEGER, fieldMetadataForColumn(2, "data_physical"))

  val nestedSchemaWithClusteringColumn = new StructType()
    .add(
      "struct",
      new StructType()
        .add(
          "clustering_col",
          IntegerType.INTEGER,
          fieldMetadataForColumn(1, "clustering_col_physical"))
        .add("data", IntegerType.INTEGER, fieldMetadataForColumn(2, "data_physical")),
      true,
      fieldMetadataForColumn(3, "struct_physical"))

  private val updatedSchemaWithDroppedClusteringColumn = Tables.Table(
    ("schemaBefore", "updatedSchemaWithDroppedClusteringColumn", "clusteringColumn"),
    (
      primitiveSchemaWithClusteringColumn,
      new StructType()
        .add(
          "data",
          IntegerType.INTEGER,
          true,
          primitiveSchemaWithClusteringColumn.get("data").getMetadata),
      new Column("clustering_col")),
    (
      nestedSchemaWithClusteringColumn,
      new StructType()
        .add(
          "struct",
          new StructType()
            .add(
              "data",
              IntegerType.INTEGER,
              nestedSchemaWithClusteringColumn.get("struct").getDataType
                .asInstanceOf[StructType].get("data").getMetadata),
          true,
          nestedSchemaWithClusteringColumn.get("struct").getMetadata),
      new Column(Array("struct", "clustering_col"))),
    (
      nestedSchemaWithClusteringColumn,
      new StructType().add("id", IntegerType.INTEGER, fieldMetadataForColumn(4, "id")),
      new Column(Array("struct", "clustering_col"))))

  test("Cannot drop clustering column") {
    forAll(updatedSchemaWithDroppedClusteringColumn) {
      (schemaBefore, schemaAfter, clusteringColumn) =>
        withTempDirAndEngine { (tablePath, engine) =>
          val table = Table.forPath(engine, tablePath)
          createEmptyTable(
            engine,
            tablePath,
            schemaBefore,
            clusteringColsOpt = Some(List(clusteringColumn)),
            tableProperties = Map(
              TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
              TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))
          assertSchemaEvolutionFails[KernelException](
            table,
            engine,
            schemaAfter,
            "Cannot drop clustering column clustering_col")
        }
    }
  }

  def fieldMetadataForColumn(
      columnId: Long,
      physicalColumnId: String): FieldMetadata = {
    FieldMetadata.builder()
      .putLong(ColumnMapping.COLUMN_MAPPING_ID_KEY, columnId)
      .putString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, physicalColumnId)
      .build()
  }

  def fieldMetadataForArrayColumn(
      columnId: Long,
      physicalColumnId: String,
      arrayFieldName: String,
      nestedElementId: Long): FieldMetadata = {
    FieldMetadata.builder()
      .fromMetadata(fieldMetadataForColumn(columnId, physicalColumnId))
      .putFieldMetadata(
        ColumnMapping.COLUMN_MAPPING_NESTED_IDS_KEY,
        FieldMetadata.builder().putLong(s"$arrayFieldName.element", nestedElementId).build())
      .build()
  }

  def fieldMetadataForMapColumn(
      columnId: Long,
      physicalColumnId: String,
      mapFieldName: String,
      keyId: Long,
      valueId: Long): FieldMetadata = {
    FieldMetadata.builder()
      .fromMetadata(fieldMetadataForColumn(columnId, physicalColumnId))
      .putFieldMetadata(
        ColumnMapping.COLUMN_MAPPING_NESTED_IDS_KEY,
        FieldMetadata.builder().putLong(s"$mapFieldName.key", keyId)
          .putLong(s"$mapFieldName.value", valueId).build())
      .build()
  }

  private def assertSchemaEvolutionFails[T <: Throwable](
      table: Table,
      engine: Engine,
      newSchema: StructType,
      expectedMessageContained: String,
      tableProperties: Map[String, String] = Map.empty): Unit = {
    val e = intercept[Exception] {
      table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
        .withSchema(engine, newSchema)
        .withTableProperties(engine, tableProperties.asJava)
        .build(engine).commit(engine, emptyIterable())
    }

    assert(e.isInstanceOf[T])
    assert(e.getMessage.contains(expectedMessageContained))
  }

  private def getMaxFieldId(engine: Engine, tablePath: String): Long = {
    TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID
      .fromMetadata(getMetadata(engine, tablePath))
  }
}
