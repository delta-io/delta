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

package org.apache.spark.sql.delta.commands.convert

import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaConfigs}
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.types.{
  StringType => SparkStringType,
  StructField => SparkStructField,
  StructType => SparkStructType
}
import shadedForDelta.org.apache.iceberg.{PartitionData, PartitionSpec, Schema}
import shadedForDelta.org.apache.iceberg.transforms._
import shadedForDelta.org.apache.iceberg.types.Types._

class IcebergPartitionConverterSuite extends SparkFunSuite {
  private def assignColumnIdAndPhysicalName(fields: Seq[SparkStructField]): Metadata = {
    val schemaWithPhysicalNames =
      DeltaColumnMapping.assignPhysicalNames(
        SparkStructType(fields.toArray),
        reuseLogicalName = true)
    val maxFromFields = DeltaColumnMapping.findMaxColumnId(schemaWithPhysicalNames)
    val provisionalMetadata = Metadata(
      schemaString = schemaWithPhysicalNames.json,
      configuration = Map(
        DeltaConfigs.COLUMN_MAPPING_MODE.key -> "id",
        DeltaConfigs.COLUMN_MAPPING_MAX_ID.key -> maxFromFields.toString))
    DeltaColumnMapping.assignColumnIdAndPhysicalName(
      provisionalMetadata,
      Metadata(),
      isChangingModeOnExistingTable = false,
      isOverwritingSchema = false)
  }

  test("convert partition simple case, including empty and null") {
    val icebergSchema = new Schema(10, Seq[NestedField](
      NestedField.required(1, "col_int", IntegerType.get),
      NestedField.required(2, "col_long", LongType.get),
      NestedField.required(3, "col_st", StringType.get)
    ).asJava)

    val icebergPartSpec = PartitionSpec
      .builderFor(icebergSchema)
      .identity("col_int")
      .truncate("col_st", 3)
      .identity("col_long")
      .build

    val physicalNameToField = Map(
      "pname1" -> icebergPartSpec.fields().get(0),
      "pname2" -> icebergPartSpec.fields().get(1),
      "pname3" -> icebergPartSpec.fields().get(2)
    )

    val partitionConverter = IcebergPartitionConverter(icebergSchema, physicalNameToField)

    val partData = new PartitionData(
      StructType.of(
        NestedField.required(1000, "col_int", IntegerType.get),
        NestedField.required(1001, "col_st", StringType.get)
      )
    )
    partData.put(0, 100)
    partData.put(1, "alo")
    assertResult("Map(pname1 -> 100, pname2 -> alo, pname3 -> null)")(
      partitionConverter.toDelta(partData).toString)

    val partData2 = new PartitionData(
      StructType.of(
        NestedField.required(1000, "col_int", IntegerType.get),
        NestedField.required(1001, "col_long", LongType.get),
        NestedField.required(1002, "col_st", StringType.get)
      )
    )
    partData2.put(2, 100000000000000L)
    partData2.put(1, null)
    assertResult("Map(pname1 -> null, pname2 -> null, pname3 -> 100000000000000)")(
      partitionConverter.toDelta(partData2).toString)
  }

  test("convert partition with complex types") {
    val icebergSchema = new Schema(10, Seq[NestedField](
      NestedField.required(4, "col_date", DateType.get),
      NestedField.required(5, "col_ts", TimestampType.withZone),
      NestedField.required(6, "col_tsnz", TimestampType.withoutZone)
    ).asJava)

    val icebergPartSpec = PartitionSpec
      .builderFor(icebergSchema)
      .identity("col_date")
      .identity("col_ts")
      .identity("col_tsnz")
      .build

    val physicalNameToField = Map(
      "pname1" -> icebergPartSpec.fields().get(0),
      "pname2" -> icebergPartSpec.fields().get(1),
      "pname3" -> icebergPartSpec.fields().get(2)
    )

    val partitionConverter = IcebergPartitionConverter(icebergSchema, physicalNameToField)

    val partData = new PartitionData(
      StructType.of(
        NestedField.required(1000, "col_date", DateType.get),
        NestedField.required(1001, "col_ts", TimestampType.withZone),
        NestedField.required(1002, "col_tsnz", TimestampType.withoutZone)
      )
    )
    partData.put(0, 12800)
    partData.put(1, 1790040414914000L)
    partData.put(2, 1790040414914000L)
    assertResult("Map(pname1 -> 2005-01-17, " +
      "pname2 -> 2026-09-21 18:26:54.9, pname3 -> 2026-09-21 18:26:54.9)")(
      partitionConverter.toDelta(partData).toString)
  }

  test("identity partition with custom spec field: merged schema has unique column ids") {
    val icebergSchema = new Schema(
      1,
      Seq(
        NestedField.required(1, "id", LongType.get),
        NestedField.required(4, "org_id", StringType.get),
        NestedField.required(5, "other_id", StringType.get)
      ).asJava)

    val partSpec = PartitionSpec
      .builderFor(icebergSchema)
      .identity("org_id", "org_id_identity")
      .build()

    val partitionField = partSpec.fields().get(0)
    assert(partitionField.name() == "org_id_identity")
    assert(partitionField.sourceId() == 4)
    assert(partitionField.fieldId() == 1000)

    val partitionFields =
      IcebergPartitionUtil.getPartitionFields(partSpec, icebergSchema, castTimeType = false)
    assert(partitionFields.length == 1)
    assert(partitionFields.head.name == "org_id_identity")
    assert(!DeltaColumnMapping.hasColumnId(partitionFields.head))
    val customPhysicalNameToField =
      Map(DeltaColumnMapping.getPhysicalName(partitionFields.head) -> partitionField)
    val customConverter = IcebergPartitionConverter(icebergSchema, customPhysicalNameToField)
    val customPartData = new PartitionData(partSpec.partitionType())
    customPartData.put(0, "tenant-a")
    assertResult("Map(org_id_identity -> tenant-a)") {
      customConverter.toDelta(customPartData).toString
    }

    val sourceSchema = IcebergSchemaUtils.convertIcebergSchemaToSpark(icebergSchema)
    val mergedSchema = PartitioningUtils.mergeDataAndPartitionSchema(
      sourceSchema,
      SparkStructType(partitionFields),
      caseSensitive = true)._1
    val metadata = assignColumnIdAndPhysicalName(mergedSchema.fields.toSeq)
    DeltaColumnMapping.checkColumnIdAndPhysicalNameAssignments(metadata)

    val fields = metadata.schema.fields
    assert(fields.length == 4)
    assert(fields(0).name == "id")
    assert(DeltaColumnMapping.getColumnId(fields(0)) == 1)
    assert(fields(1).name == "org_id")
    assert(DeltaColumnMapping.getColumnId(fields(1)) == 4)
    assert(fields(2).name == "other_id")
    assert(DeltaColumnMapping.getColumnId(fields(2)) == 5)
    assert(fields(3).name == "org_id_identity")
    assert(DeltaColumnMapping.getColumnId(fields(3)) == 6)
  }

  test("identity partition field name matches source column: validate merged schema") {
    val icebergSchema = new Schema(
      1,
      Seq(
        NestedField.required(1, "id", LongType.get),
        NestedField.required(4, "org_id", StringType.get),
        NestedField.required(5, "other_id", StringType.get)
      ).asJava)

    val partSpec = PartitionSpec.builderFor(icebergSchema).identity("org_id").build()
    val icebergPartField = partSpec.fields().get(0)
    val partitionFields =
      IcebergPartitionUtil.getPartitionFields(partSpec, icebergSchema, castTimeType = false)
    assert(partitionFields.length == 1)
    assert(partitionFields.head.name == "org_id")
    assert(DeltaColumnMapping.hasColumnId(partitionFields.head))
    assert(DeltaColumnMapping.getColumnId(partitionFields.head) == 4)
    val sameNamePhysicalNameToField =
      Map(DeltaColumnMapping.getPhysicalName(partitionFields.head) -> icebergPartField)
    val sameNameConverter = IcebergPartitionConverter(icebergSchema, sameNamePhysicalNameToField)
    val sameNamePartData = new PartitionData(partSpec.partitionType())
    sameNamePartData.put(0, "tenant-a")
    assertResult("Map(org_id -> tenant-a)") {
      sameNameConverter.toDelta(sameNamePartData).toString
    }

    val sourceSchema = IcebergSchemaUtils.convertIcebergSchemaToSpark(icebergSchema)
    val mergedSchema = PartitioningUtils.mergeDataAndPartitionSchema(
      sourceSchema,
      SparkStructType(partitionFields),
      caseSensitive = true)._1
    val metadata = assignColumnIdAndPhysicalName(mergedSchema.fields.toSeq)
    DeltaColumnMapping.checkColumnIdAndPhysicalNameAssignments(metadata)

    val fields = metadata.schema.fields
    assert(fields.length == 3)
    assert(fields(0).name == "id")
    assert(DeltaColumnMapping.getColumnId(fields(0)) == 1)
    assert(fields(1).name == "org_id")
    assert(DeltaColumnMapping.getColumnId(fields(1)) == 4)
    assert(fields(2).name == "other_id")
    assert(DeltaColumnMapping.getColumnId(fields(2)) == 5)
  }

  test("nested schema: identity on nested source omits partition column id") {
    val addressStruct = StructType.of(
      NestedField.required(3, "zip", StringType.get()),
      NestedField.required(4, "city", StringType.get()))
    val icebergSchema = new Schema(
      1,
      Seq(
        NestedField.required(1, "id", LongType.get()),
        NestedField.required(2, "address", addressStruct)
      ).asJava)

    val partSpec = PartitionSpec.builderFor(icebergSchema).identity("address.zip").build()
    val icebergPartField = partSpec.fields().get(0)
    assert(icebergPartField.sourceId() == 3)
    assert(icebergSchema.findColumnName(3) == "address.zip")

    val partitionFields =
      IcebergPartitionUtil.getPartitionFields(partSpec, icebergSchema, castTimeType = false)
    assert(partitionFields.length == 1)
    assert(partitionFields.head.dataType == SparkStringType)
    assert(partitionFields.head.name == icebergPartField.name())
    // Nested dotted paths: partition column is extra after Spark merge vs data schema; do not
    // reuse nested field id (see IcebergPartitionUtil identity branch).
    assert(!DeltaColumnMapping.hasColumnId(partitionFields.head))
    val nestedPhysicalNameToField =
      Map(DeltaColumnMapping.getPhysicalName(partitionFields.head) -> icebergPartField)
    val nestedConverter = IcebergPartitionConverter(icebergSchema, nestedPhysicalNameToField)
    val nestedPartData = new PartitionData(partSpec.partitionType())
    nestedPartData.put(0, "94103")
    assertResult("Map(address.zip -> 94103)") {
      nestedConverter.toDelta(nestedPartData).toString
    }

    val sourceSchema = IcebergSchemaUtils.convertIcebergSchemaToSpark(icebergSchema)
    val mergedSchema = PartitioningUtils.mergeDataAndPartitionSchema(
      sourceSchema,
      SparkStructType(partitionFields),
      caseSensitive = true)._1
    val metadata = assignColumnIdAndPhysicalName(mergedSchema.fields.toSeq)
    DeltaColumnMapping.checkColumnIdAndPhysicalNameAssignments(metadata)

    val fields = metadata.schema.fields
    assert(fields.length == 3)
    assert(fields(0).name == "id")
    assert(DeltaColumnMapping.getColumnId(fields(0)) == 1)
    assert(fields(1).name == "address")
    assert(DeltaColumnMapping.getColumnId(fields(1)) == 2)
    val zipInStruct = fields(1).dataType.asInstanceOf[SparkStructType]("zip")
    assert(DeltaColumnMapping.getColumnId(zipInStruct) == 3)
    assert(fields(2).name == icebergPartField.name())
    assert(DeltaColumnMapping.getColumnId(fields(2)) == 5)
  }

  test("renamed identity partition with source field id 1000: no id collision after Delta assign") {
    val icebergSchema = new Schema(
      1,
      Seq(
        NestedField.required(1, "id", LongType.get),
        NestedField.required(1000, "org_id", StringType.get)
      ).asJava)

    val partSpec = PartitionSpec
      .builderFor(icebergSchema)
      .identity("org_id", "org_id_identity")
      .build()

    val partitionField = partSpec.fields().get(0)
    assert(icebergSchema.findField("org_id").fieldId() == 1000)
    assert(partitionField.name() == "org_id_identity")
    assert(partitionField.sourceId() == 1000)
    assert(partitionField.fieldId() == 1000)

    val partitionFields =
      IcebergPartitionUtil.getPartitionFields(partSpec, icebergSchema, castTimeType = false)
    assert(!DeltaColumnMapping.hasColumnId(partitionFields.head))
    val physicalNameToField =
      Map(DeltaColumnMapping.getPhysicalName(partitionFields.head) -> partitionField)
    val converter = IcebergPartitionConverter(icebergSchema, physicalNameToField)
    val partData = new PartitionData(partSpec.partitionType())
    partData.put(0, "tenant-a")
    assertResult("Map(org_id_identity -> tenant-a)") {
      converter.toDelta(partData).toString
    }

    val sourceSchema = IcebergSchemaUtils.convertIcebergSchemaToSpark(icebergSchema)
    val sourceFields = sourceSchema.fields.toSeq
    assert(DeltaColumnMapping.getColumnId(sourceFields.find(_.name == "org_id").get) == 1000)

    val mergedSchema = PartitioningUtils.mergeDataAndPartitionSchema(
      sourceSchema,
      SparkStructType(partitionFields),
      caseSensitive = true)._1
    val metadata = assignColumnIdAndPhysicalName(mergedSchema.fields.toSeq)
    DeltaColumnMapping.checkColumnIdAndPhysicalNameAssignments(metadata)

    val fields = metadata.schema.fields
    assert(fields.length == 3)
    assert(fields(0).name == "id")
    assert(DeltaColumnMapping.getColumnId(fields(0)) == 1)
    assert(fields(1).name == "org_id")
    assert(DeltaColumnMapping.getColumnId(fields(1)) == 1000)
    assert(fields(2).name == "org_id_identity")
    assert(DeltaColumnMapping.getColumnId(fields(2)) == 1001)
  }
}
