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
import java.util.{Collections, Optional, UUID}

import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.actions.{Format, Metadata}
import io.delta.kernel.internal.util.ColumnMapping._
import io.delta.kernel.internal.util.ColumnMapping.ColumnMappingMode._
import io.delta.kernel.types.{ArrayType, FieldMetadata, IntegerType, MapType, StringType, StructField, StructType}

import org.assertj.core.api.Assertions.{assertThat, assertThatNoException, assertThatThrownBy}
import org.assertj.core.util.Maps
import org.scalatest.funsuite.AnyFunSuite

class ColumnMappingSuite extends AnyFunSuite {
  test("column mapping is only enabled on known mapping modes") {
    assertThat(ColumnMapping.isColumnMappingModeEnabled(null)).isFalse
    assertThat(ColumnMapping.isColumnMappingModeEnabled(NONE)).isFalse
    assertThat(ColumnMapping.isColumnMappingModeEnabled(NAME)).isTrue
    assertThat(ColumnMapping.isColumnMappingModeEnabled(ID)).isTrue
  }

  test("column mapping change with empty config") {
    assertThatNoException.isThrownBy(() =>
      ColumnMapping.verifyColumnMappingChange(
        new util.HashMap(),
        new util.HashMap(),
        true /* isNewTable */ ))
  }

  test("column mapping mode change is allowed") {
    val isNewTable = true
    assertThatNoException.isThrownBy(() =>
      ColumnMapping.verifyColumnMappingChange(
        new util.HashMap(),
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, NAME.toString),
        isNewTable))

    assertThatNoException.isThrownBy(() =>
      ColumnMapping.verifyColumnMappingChange(
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, NONE.toString),
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, NAME.toString),
        isNewTable))

    assertThatNoException.isThrownBy(() =>
      ColumnMapping.verifyColumnMappingChange(
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, ID.toString),
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, ID.toString),
        isNewTable))

    assertThatNoException.isThrownBy(() =>
      ColumnMapping.verifyColumnMappingChange(
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, NAME.toString),
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, NAME.toString),
        isNewTable))
  }

  test("column mapping mode change not allowed on existing table") {
    val isNewTable = false
    assertThatThrownBy(() =>
      ColumnMapping.verifyColumnMappingChange(
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, NAME.toString),
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, ID.toString),
        isNewTable))
      .isInstanceOf(classOf[IllegalArgumentException])
      .hasMessage("Changing column mapping mode from 'name' to 'id' is not supported")

    assertThatThrownBy(() =>
      ColumnMapping.verifyColumnMappingChange(
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, ID.toString),
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, NAME.toString),
        isNewTable))
      .isInstanceOf(classOf[IllegalArgumentException])
      .hasMessage("Changing column mapping mode from 'id' to 'name' is not supported")

    assertThatThrownBy(() =>
      ColumnMapping.verifyColumnMappingChange(
        new util.HashMap(),
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, ID.toString),
        isNewTable))
      .isInstanceOf(classOf[IllegalArgumentException])
      .hasMessage("Changing column mapping mode from 'none' to 'id' is not supported")
  }

  test("finding max column id with different schemas") {
    assertThat(ColumnMapping.findMaxColumnId(new StructType)).isEqualTo(0)

    assertThat(ColumnMapping.findMaxColumnId(
      new StructType()
        .add("a", StringType.STRING, true)
        .add("b", IntegerType.INTEGER, true)))
      .isEqualTo(0)

    assertThat(ColumnMapping.findMaxColumnId(
      new StructType()
        .add("a", StringType.STRING, createMetadataWithFieldId(14))
        .add("b", IntegerType.INTEGER, createMetadataWithFieldId(17))
        .add("c", IntegerType.INTEGER, createMetadataWithFieldId(3))))
      .isEqualTo(17)

    // nested columns are currently not supported
    assertThat(ColumnMapping.findMaxColumnId(
      new StructType().add("a", StringType.STRING, createMetadataWithFieldId(14))
        .add(
          "b",
          new StructType()
            .add("d", IntegerType.INTEGER, true)
            .add("e", IntegerType.INTEGER, true),
          createMetadataWithFieldId(15))
        .add("c", IntegerType.INTEGER, createMetadataWithFieldId(7))))
      .isEqualTo(15)
  }

  test("finding max column id with nested struct type") {
    val nestedStruct = new StructType()
      .add("d", IntegerType.INTEGER, createMetadataWithFieldId(3))
      .add("e", IntegerType.INTEGER, createMetadataWithFieldId(4))

    val schema = new StructType()
      .add("a", StringType.STRING, createMetadataWithFieldId(1))
      .add("b", nestedStruct, createMetadataWithFieldId(2))
      .add("c", IntegerType.INTEGER, createMetadataWithFieldId(5))

    assertThat(ColumnMapping.findMaxColumnId(schema)).isEqualTo(5)
  }

  test("finding max column id with nested struct type and random ids") {
    val nestedStruct = new StructType()
      .add("d", IntegerType.INTEGER, createMetadataWithFieldId(2))
      .add("e", IntegerType.INTEGER, createMetadataWithFieldId(1))

    val schema = new StructType()
      .add("a", StringType.STRING, createMetadataWithFieldId(3))
      .add("b", nestedStruct, createMetadataWithFieldId(4))
      .add("c", IntegerType.INTEGER, createMetadataWithFieldId(5))

    assertThat(ColumnMapping.findMaxColumnId(schema)).isEqualTo(5)
  }

  test("finding max column id with nested array type") {
    val nestedStruct = new StructType()
      .add("e", IntegerType.INTEGER, createMetadataWithFieldId(4))
      .add("f", IntegerType.INTEGER, createMetadataWithFieldId(5))

    val nestedMeta = FieldMetadata.builder()
      .putLong(COLUMN_MAPPING_ID_KEY, 2)
      .putFieldMetadata(
        COLUMN_MAPPING_NESTED_IDS_KEY,
        FieldMetadata.builder().putLong("b.element", 6).build())
      .build()

    val schema = new StructType()
      .add("a", StringType.STRING, createMetadataWithFieldId(1))
      .add("b", new ArrayType(new StructField("d", nestedStruct, false)), nestedMeta)
      .add("c", IntegerType.INTEGER, createMetadataWithFieldId(3))

    assertThat(ColumnMapping.findMaxColumnId(schema)).isEqualTo(6)
  }

  test("finding max column id with nested map type") {
    val nestedStruct = new StructType()
      .add("e", IntegerType.INTEGER, createMetadataWithFieldId(4))
      .add("f", IntegerType.INTEGER, createMetadataWithFieldId(5))

    val nestedMeta = FieldMetadata.builder()
      .putLong(COLUMN_MAPPING_ID_KEY, 2)
      .putFieldMetadata(
        COLUMN_MAPPING_NESTED_IDS_KEY,
        FieldMetadata.builder()
          .putLong("b.key", 11)
          .putLong("b.value", 12).build())
      .build()

    val schema = new StructType()
      .add("a", StringType.STRING, createMetadataWithFieldId(1))
      .add(
        "b",
        new MapType(
          IntegerType.INTEGER,
          new StructField("d", nestedStruct, false).getDataType,
          false),
        nestedMeta)
      .add("c", IntegerType.INTEGER, createMetadataWithFieldId(3))

    assertThat(ColumnMapping.findMaxColumnId(schema)).isEqualTo(12)
  }

  test("assign id and physical name to new table") {
    val schema: StructType = new StructType()
      .add("a", StringType.STRING, true)
      .add("b", StringType.STRING, true)

    val metadata: Metadata =
      ColumnMapping.updateColumnMappingMetadata(createMetadata(schema), ID, true)

    assertColumnMapping(metadata.getSchema.get("a"), 1L)
    assertColumnMapping(metadata.getSchema.get("b"), 2L)

    assertThat(metadata.getConfiguration)
      .containsEntry(ColumnMapping.COLUMN_MAPPING_MAX_COLUMN_ID_KEY, "2")
  }

  test("assign id and physical name to updated table") {
    val schema = new StructType()
      .add("a", StringType.STRING, true)
      .add("b", StringType.STRING, true)

    val metadata = ColumnMapping.updateColumnMappingMetadata(createMetadata(schema), ID, false)

    assertColumnMapping(metadata.getSchema.get("a"), 1L, "a")
    assertColumnMapping(metadata.getSchema.get("b"), 2L, "b")

    assertThat(metadata.getConfiguration)
      .containsEntry(ColumnMapping.COLUMN_MAPPING_MAX_COLUMN_ID_KEY, "2")
  }

  test("none mapping mode returns original schema") {
    val schema = new StructType().add("a", StringType.STRING, true)

    val metadata =
      ColumnMapping.updateColumnMappingMetadata(createMetadata(schema), NONE, true)
    assertThat(metadata.getSchema).isEqualTo(schema)
  }

  test("assigning id and physical name preserves field metadata") {
    val schema = new StructType()
      .add(
        "a",
        StringType.STRING,
        FieldMetadata.builder.putString("key1", "val1").putString("key2", "val2").build)

    val metadata = ColumnMapping.updateColumnMappingMetadata(createMetadata(schema), ID, true)
    val fieldMetadata = metadata.getSchema.get("a").getMetadata.getEntries

    assertThat(fieldMetadata)
      .containsEntry("key1", "val1")
      .containsEntry("key2", "val2")
      .containsEntry(ColumnMapping.COLUMN_MAPPING_ID_KEY, (1L).asInstanceOf[AnyRef])
      .hasEntrySatisfying(
        ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY,
        (k: AnyRef) => assertThat(k).asString.startsWith("col-"))
  }

  test("assign id and physical name to new table with nested struct type" +
    " and enableIcebergCompatV2=true") {
    val schema: StructType =
      new StructType()
        .add("a", StringType.STRING)
        .add(
          "b",
          new StructType()
            .add("d", IntegerType.INTEGER)
            .add("e", IntegerType.INTEGER))
        .add("c", IntegerType.INTEGER)

    val metadata: Metadata =
      ColumnMapping.updateColumnMappingMetadata(
        withIcebergCompatV2Enabled(createMetadata(schema)),
        ID,
        true
        /** isNewTable */
      )

    assertColumnMapping(metadata.getSchema.get("a"), 1L)
    assertColumnMapping(metadata.getSchema.get("b"), 2L)
    val innerStruct = metadata.getSchema.get("b").getDataType.asInstanceOf[StructType]
    assertColumnMapping(innerStruct.get("d"), 3L)
    assertColumnMapping(innerStruct.get("e"), 4L)
    assertColumnMapping(metadata.getSchema.get("c"), 5L)

    assertThat(metadata.getConfiguration)
      .containsEntry(ColumnMapping.COLUMN_MAPPING_MAX_COLUMN_ID_KEY, "5")
  }

  test("assign id and physical name to new table with array type and enableIcebergCompatV2=true") {
    val schema: StructType =
      new StructType()
        .add("a", StringType.STRING)
        .add("b", new ArrayType(IntegerType.INTEGER, false))
        .add("c", IntegerType.INTEGER)

    val metadata: Metadata =
      ColumnMapping.updateColumnMappingMetadata(
        withIcebergCompatV2Enabled(createMetadata(schema)),
        ID,
        true
        /** isNewTable */
      )

    assertColumnMapping(metadata.getSchema.get("a"), 1L)
    assertColumnMapping(metadata.getSchema.get("b"), 2L)
    assertColumnMapping(metadata.getSchema.get("c"), 3L)

    // verify nested ids
    assertThat(metadata.getSchema.get("b").getMetadata.getEntries
      .get(COLUMN_MAPPING_NESTED_IDS_KEY).asInstanceOf[FieldMetadata].getEntries)
      .hasSize(1)
      .anySatisfy((k: AnyRef, v: AnyRef) => {
        assertThat(k).asString.startsWith("col-")
        assertThat(k).asString.endsWith(".element")
        assertThat(v).isEqualTo(4L)
      })

    assertThat(metadata.getConfiguration)
      .containsEntry(ColumnMapping.COLUMN_MAPPING_MAX_COLUMN_ID_KEY, "4")
  }

  test("assign id and physical name to existing table with array type " +
    "and enableIcebergCompatV2=true") {
    val schema: StructType =
      new StructType()
        .add("a", StringType.STRING)
        .add("b", new ArrayType(IntegerType.INTEGER, false))
        .add("c", IntegerType.INTEGER)

    val metadata: Metadata =
      ColumnMapping.updateColumnMappingMetadata(
        withIcebergCompatV2Enabled(createMetadata(schema)),
        ID,
        false
        /** isNewTable */
      )

    assertColumnMapping(metadata.getSchema.get("a"), 1L, "a")
    assertColumnMapping(metadata.getSchema.get("b"), 2L, "b")
    assertColumnMapping(metadata.getSchema.get("c"), 3L, "c")

    // verify nested ids
    assertThat(metadata.getSchema.get("b").getMetadata.getEntries
      .get(COLUMN_MAPPING_NESTED_IDS_KEY).asInstanceOf[FieldMetadata].getEntries)
      .hasSize(1)
      .containsEntry("b.element", 4L.asInstanceOf[AnyRef])

    assertThat(metadata.getConfiguration)
      .containsEntry(ColumnMapping.COLUMN_MAPPING_MAX_COLUMN_ID_KEY, "4")
  }

  test("assign id and physical name to new table with map type and enableIcebergCompatV2=true") {
    val schema: StructType =
      new StructType()
        .add("a", StringType.STRING)
        .add("b", new MapType(IntegerType.INTEGER, StringType.STRING, false))
        .add("c", IntegerType.INTEGER)

    val metadata: Metadata =
      ColumnMapping.updateColumnMappingMetadata(
        withIcebergCompatV2Enabled(createMetadata(schema)),
        ID,
        true
        /** isNewTable */
      )

    assertColumnMapping(metadata.getSchema.get("a"), 1L)
    assertColumnMapping(metadata.getSchema.get("b"), 2L)
    assertColumnMapping(metadata.getSchema.get("c"), 3L)

    // verify nested ids
    assertThat(metadata.getSchema.get("b").getMetadata.getEntries
      .get(COLUMN_MAPPING_NESTED_IDS_KEY).asInstanceOf[FieldMetadata].getEntries)
      .hasSize(2)
      .anySatisfy((k: AnyRef, v: AnyRef) => {
        assertThat(k).asString.startsWith("col-")
        assertThat(k).asString.endsWith(".key")
        assertThat(v).isEqualTo(4L)
      })
      .anySatisfy((k: AnyRef, v: AnyRef) => {
        assertThat(k).asString.startsWith("col-")
        assertThat(k).asString.endsWith(".value")
        assertThat(v).isEqualTo(5L)
      })

    assertThat(metadata.getConfiguration)
      .containsEntry(ColumnMapping.COLUMN_MAPPING_MAX_COLUMN_ID_KEY, "5")
  }

  test("assign id and physical name to existing table with map type " +
    "and enableIcebergCompatV2=true") {
    val schema: StructType =
      new StructType()
        .add("a", StringType.STRING)
        .add("b", new MapType(IntegerType.INTEGER, StringType.STRING, false))
        .add("c", IntegerType.INTEGER)

    val metadata: Metadata =
      ColumnMapping.updateColumnMappingMetadata(
        withIcebergCompatV2Enabled(createMetadata(schema)),
        ID,
        false
        /** isNewTable */
      )

    assertColumnMapping(metadata.getSchema.get("a"), 1L, "a")
    assertColumnMapping(metadata.getSchema.get("b"), 2L, "b")
    assertColumnMapping(metadata.getSchema.get("c"), 3L, "c")

    // verify nested ids
    assertThat(metadata.getSchema.get("b").getMetadata.getEntries
      .get(COLUMN_MAPPING_NESTED_IDS_KEY).asInstanceOf[FieldMetadata].getEntries)
      .hasSize(2)
      .containsEntry("b.key", 4L.asInstanceOf[AnyRef])
      .containsEntry("b.value", 5L.asInstanceOf[AnyRef])

    assertThat(metadata.getConfiguration)
      .containsEntry(ColumnMapping.COLUMN_MAPPING_MAX_COLUMN_ID_KEY, "5")
  }

  test("assign id and physical name to new table with nested schema " +
    "and enableIcebergCompatV2=true") {
    val schema: StructType =
      new StructType()
        .add("a", StringType.STRING)
        .add(
          "b",
          new MapType(
            IntegerType.INTEGER,
            new StructType()
              .add("d", IntegerType.INTEGER)
              .add("e", IntegerType.INTEGER)
              .add(
                "f",
                new ArrayType(
                  new StructType()
                    .add("g", IntegerType.INTEGER)
                    .add("h", IntegerType.INTEGER),
                  false),
                false),
            false))
        .add("c", IntegerType.INTEGER)

    val metadata: Metadata =
      ColumnMapping.updateColumnMappingMetadata(
        withIcebergCompatV2Enabled(createMetadata(schema)),
        ID,
        true
        /** isNewTable */
      )

    assertColumnMapping(metadata.getSchema.get("a"), 1L)
    assertColumnMapping(metadata.getSchema.get("b"), 2L)
    val mapType = metadata.getSchema.get("b").getDataType.asInstanceOf[MapType]
    val innerStruct = mapType.getValueField.getDataType.asInstanceOf[StructType]
    assertColumnMapping(innerStruct.get("d"), 3L)
    assertColumnMapping(innerStruct.get("e"), 4L)
    assertColumnMapping(innerStruct.get("f"), 5L)
    val innerArray = innerStruct.get("f").getDataType.asInstanceOf[ArrayType]
    val structInArray = innerArray.getElementField.getDataType.asInstanceOf[StructType]
    assertColumnMapping(structInArray.get("g"), 6L)
    assertColumnMapping(structInArray.get("h"), 7L)
    assertColumnMapping(metadata.getSchema.get("c"), 8L)

    // verify nested ids
    assertThat(metadata.getSchema.get("b").getMetadata.getEntries
      .get(COLUMN_MAPPING_NESTED_IDS_KEY).asInstanceOf[FieldMetadata].getEntries)
      .hasSize(2)
      .anySatisfy((k: AnyRef, v: AnyRef) => {
        assertThat(k).asString.startsWith("col-")
        assertThat(k).asString.endsWith(".key")
        assertThat(v).isEqualTo(9L)
      })
      .anySatisfy((k: AnyRef, v: AnyRef) => {
        assertThat(k).asString.startsWith("col-")
        assertThat(k).asString.endsWith(".value")
        assertThat(v).isEqualTo(10L)
      })

    assertThat(mapType.getKeyField.getMetadata.getEntries)
      .doesNotContainKey(ColumnMapping.COLUMN_MAPPING_ID_KEY)
      .doesNotContainKey(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY)

    assertThat(mapType.getValueField.getMetadata.getEntries)
      .doesNotContainKey(ColumnMapping.COLUMN_MAPPING_ID_KEY)
      .doesNotContainKey(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY)

    // verify nested ids
    assertThat(innerStruct.get("f").getMetadata.getEntries
      .get(COLUMN_MAPPING_NESTED_IDS_KEY).asInstanceOf[FieldMetadata].getEntries)
      .hasSize(1)
      .anySatisfy((k: AnyRef, v: AnyRef) => {
        assertThat(k).asString.startsWith("col-")
        assertThat(k).asString.endsWith(".element")
        assertThat(v).isEqualTo(11L)
      })

    assertThat(metadata.getConfiguration)
      .containsEntry(ColumnMapping.COLUMN_MAPPING_MAX_COLUMN_ID_KEY, "11")
  }

  private def createMetadata(schema: StructType) = new Metadata(
    UUID.randomUUID.toString,
    Optional.empty(),
    Optional.empty(),
    new Format,
    schema.toJson,
    schema,
    VectorUtils.buildArrayValue(Collections.emptyList(), StringType.STRING),
    Optional.empty(),
    VectorUtils.stringStringMapValue(Collections.emptyMap()))

  private def assertColumnMapping(
      field: StructField,
      expId: Long,
      expPhysicalName: String = "UUID"): Unit = {
    assertThat(field.getMetadata.getEntries)
      .containsEntry(ColumnMapping.COLUMN_MAPPING_ID_KEY, expId.asInstanceOf[AnyRef])
      .hasEntrySatisfying(
        ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY,
        (k: AnyRef) => {
          if (expPhysicalName == "UUID") {
            assertThat(k).asString.startsWith("col-")
          } else {
            assertThat(k).asString.isEqualTo(expPhysicalName)
          }
        })
  }

  private def withIcebergCompatV2Enabled(metadata: Metadata): Metadata = {
    metadata.withNewConfiguration(
      Maps.newHashMap(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey, "true"))
  }

  private def createMetadataWithFieldId(fieldId: Int) = {
    FieldMetadata.builder.putLong(COLUMN_MAPPING_ID_KEY, fieldId).build()
  }
}
