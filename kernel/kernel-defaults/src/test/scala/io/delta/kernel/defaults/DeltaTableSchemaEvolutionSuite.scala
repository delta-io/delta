package io.delta.kernel.defaults

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel.{Operation, Table}
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.util.ColumnMapping
import io.delta.kernel.types.{ArrayType, FieldMetadata, IntegerType, LongType, MapType, StringType, StructType}
import io.delta.kernel.utils.CloseableIterable.emptyIterable

class DeltaTableSchemaEvolutionSuite extends DeltaTableWriteSuiteBase {

  test("Add column") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      createTxn(
        engine,
        tablePath,
        isNewTable = true,
        schema,
        partCols = Seq.empty,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")).commit(engine, emptyIterable())

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
    }
  }

  test("Rename fields") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", StringType.STRING, true)
        .add(
          "b",
          new StructType()
            .add("d", IntegerType.INTEGER, true)
            .add("e", IntegerType.INTEGER, true),
          true)
        .add("c", IntegerType.INTEGER, true)

      createTxn(
        engine,
        tablePath,
        isNewTable = true,
        schema,
        partCols = Seq.empty,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")).commit(engine, emptyIterable())

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
      val schema = new StructType()
        .add("a", StringType.STRING, true)
        .add(
          "b",
          new StructType()
            .add("d", IntegerType.INTEGER, true)
            .add("e", IntegerType.INTEGER, true),
          true)
        .add("c", IntegerType.INTEGER, true)

      createTxn(
        engine,
        tablePath,
        isNewTable = true,
        schema,
        partCols = Seq.empty,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")).commit(engine, emptyIterable())

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
      val schema = new StructType()
        .add("a", IntegerType.INTEGER, true)

      createTxn(
        engine,
        tablePath,
        isNewTable = true,
        schema,
        partCols = Seq.empty,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")).commit(engine, emptyIterable())

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
      val schema = new StructType()
        .add("a", IntegerType.INTEGER, true)

      createTxn(
        engine,
        tablePath,
        isNewTable = true,
        schema,
        partCols = Seq.empty,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")).commit(engine, emptyIterable())

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add("a", IntegerType.INTEGER, true, currentSchema.get("a").getMetadata)
        .add(
          "map",
          new MapType(
            StringType.STRING,
            new ArrayType(
              new StructType().add("nested_map_value", IntegerType.INTEGER),
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

  test("Adding struct of structs") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      createTxn(
        engine,
        tablePath,
        isNewTable = true,
        schema,
        partCols = Seq.empty,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")).commit(engine, emptyIterable())

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

  test("Updating schema on table when column mapping disabled fails") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      createTxn(
        engine,
        tablePath,
        isNewTable = true,
        schema,
        partCols = Seq.empty,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "none")).commit(engine, emptyIterable())

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

      val e = intercept[KernelException] {
        table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
          .withSchema(engine, newSchema)
          .build(engine).commit(engine, emptyIterable())
      }

      assert(e.getMessage.contains(
        "Cannot update schema for table when column mapping is disabled"))
    }
  }

  test("Updating schema with duplicate field IDs fails") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      createTxn(
        engine,
        tablePath,
        isNewTable = true,
        schema,
        partCols = Seq.empty,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")).commit(engine, emptyIterable())

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

      val e = intercept[IllegalArgumentException] {
        table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
          .withSchema(engine, newSchema)
          .build(engine).commit(engine, emptyIterable())
      }
      assert(e.getMessage.contains(
        "Field duplicate_field_id with id 1 already exists"))
    }
  }

  test("Adding non-nullable field fails") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      createTxn(
        engine,
        tablePath,
        isNewTable = true,
        schema,
        partCols = Seq.empty,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")).commit(engine, emptyIterable())

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

      val e = intercept[KernelException] {
        table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
          .withSchema(engine, newSchema)
          .build(engine).commit(engine, emptyIterable())
      }
      assert(e.getMessage.contains(
        "Cannot add a non-nullable field non_nullable_field"))
    }
  }

  test("Cannot drop a partition column") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      createTxn(
        engine,
        tablePath,
        isNewTable = true,
        schema,
        partCols = Seq("c"),
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")).commit(engine, emptyIterable())

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

      val e = intercept[IllegalArgumentException] {
        table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
          .withSchema(engine, newSchema)
          .build(engine).commit(engine, emptyIterable())
      }
      assert(e.getMessage.contains(
        "Partition column c not found in the schema"))
    }
  }

  test("Cannot promote types") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      createTxn(
        engine,
        tablePath,
        isNewTable = true,
        schema,
        partCols = Seq.empty,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")).commit(engine, emptyIterable())

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add("a", StringType.STRING, true, currentSchema.get("a").getMetadata)
        .add("c", LongType.LONG, true, currentSchema.get("c").getMetadata)

      val e = intercept[KernelException] {
        table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
          .withSchema(engine, newSchema)
          .build(engine).commit(engine, emptyIterable())
      }
      assert(e.getMessage.contains(
        "Cannot change the type of existing field c from integer to long"))
    }
  }

  test("Updating schema if physical columns are not preserved fails") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", StringType.STRING, true)
        .add("c", IntegerType.INTEGER, true)

      createTxn(
        engine,
        tablePath,
        isNewTable = true,
        schema,
        partCols = Seq.empty,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")).commit(engine, emptyIterable())

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add(
          "a",
          StringType.STRING,
          true,
          fieldMetadataForColumn(1, "not-preserving-physical-column"))
        .add("c", LongType.LONG, true, currentSchema.get("c").getMetadata)

      val e = intercept[IllegalArgumentException] {
        table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
          .withSchema(engine, newSchema)
          .build(engine).commit(engine, emptyIterable())
      }
      assert(e.getMessage.contains(
        "Existing field with id 1 in current schema has physical name"))
    }
  }

  test("Updating schema if map columns are missing nested ids for key fails") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", IntegerType.INTEGER, true)

      createTxn(
        engine,
        tablePath,
        isNewTable = true,
        schema,
        partCols = Seq.empty,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")).commit(engine, emptyIterable())

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add("a", StringType.STRING, true, currentSchema.get("a").getMetadata)
        .add(
          "c",
          new MapType(
            StringType.STRING,
            StringType.STRING,
            false),
          true,
          fieldMetadataForColumn(2, "c"))

      val e = intercept[IllegalArgumentException] {
        table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
          .withSchema(engine, newSchema)
          .build(engine).commit(engine, emptyIterable())
      }

      assert(e.getMessage.contains(
        "Map field c must have exactly 2 nested IDs"))
    }
  }

  test("Updating schema if map column nested ids are not preserved fails") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", IntegerType.INTEGER, true)
        .add(
          "c",
          new MapType(
            StringType.STRING,
            StringType.STRING,
            false),
          true)

      createTxn(
        engine,
        tablePath,
        isNewTable = true,
        schema,
        partCols = Seq.empty,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")).commit(engine, emptyIterable())

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val mapPhysicalColumnName =
        currentSchema.get("c").getMetadata.getString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY)
      val newSchema = new StructType()
        .add("a", IntegerType.INTEGER, true, currentSchema.get("a").getMetadata)
        .add(
          "c",
          new MapType(
            StringType.STRING,
            StringType.STRING,
            false),
          true,
          fieldMetadataForMapColumn(2, mapPhysicalColumnName, "c", 5, 6))

      val e = intercept[IllegalArgumentException] {
        table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
          .withSchema(engine, newSchema)
          .build(engine).commit(engine, emptyIterable())
      }

      assert(e.getMessage.contains(
        "Existing field with id 2 in " +
          "current schema has nested field IDs [3, 4] which is different from [5, 6]"))
    }
  }

  test("Updating schema if map columns are missing nested ids fails") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", IntegerType.INTEGER, true)

      createTxn(
        engine,
        tablePath,
        isNewTable = true,
        schema,
        partCols = Seq.empty,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")).commit(engine, emptyIterable())

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add("a", StringType.STRING, true, currentSchema.get("a").getMetadata)
        .add(
          "c",
          new MapType(
            StringType.STRING,
            StringType.STRING,
            false),
          true,
          fieldMetadataForColumn(2, "c"))

      val e = intercept[IllegalArgumentException] {
        table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
          .withSchema(engine, newSchema)
          .build(engine).commit(engine, emptyIterable())
      }

      assert(e.getMessage.contains(
        "Map field c must have exactly 2 nested IDs"))
    }
  }

  test("Updating schema if array columns is missing nested id fails") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", IntegerType.INTEGER, true)

      createTxn(
        engine,
        tablePath,
        isNewTable = true,
        schema,
        partCols = Seq.empty,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")).commit(engine, emptyIterable())

      val currentSchema = table.getLatestSnapshot(engine).getSchema
      val newSchema = new StructType()
        .add("a", StringType.STRING, true, currentSchema.get("a").getMetadata)
        .add("c", new ArrayType(StringType.STRING, false), true, fieldMetadataForColumn(2, "c"))

      val e = intercept[IllegalArgumentException] {
        table.createTransactionBuilder(engine, testEngineInfo, Operation.MANUAL_UPDATE)
          .withSchema(engine, newSchema)
          .build(engine).commit(engine, emptyIterable())
      }

      assert(e.getMessage.contains(
        "Array field c must have exactly 1 nested ID"))
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
}
