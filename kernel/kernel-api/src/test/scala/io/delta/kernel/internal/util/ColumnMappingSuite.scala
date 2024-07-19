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

import io.delta.kernel.internal.actions.{Format, Metadata}
import io.delta.kernel.internal.util.ColumnMapping._
import io.delta.kernel.types.{FieldMetadata, IntegerType, StringType, StructType}
import org.assertj.core.api.Assertions.{assertThat, assertThatNoException, assertThatThrownBy}
import org.assertj.core.util.Maps
import org.scalatest.funsuite.AnyFunSuite

import java.util
import java.util.{Collections, Optional, UUID}

class ColumnMappingSuite extends AnyFunSuite {
  test("column mapping is only enabled on known mapping modes") {
    assertThat(ColumnMapping.isColumnMappingModeEnabled(null)).isFalse
    assertThat(ColumnMapping.isColumnMappingModeEnabled("null")).isFalse
    assertThat(ColumnMapping.isColumnMappingModeEnabled("none")).isFalse
    assertThat(ColumnMapping.isColumnMappingModeEnabled("NaMe")).isFalse
    assertThat(ColumnMapping.isColumnMappingModeEnabled("name")).isTrue
    assertThat(ColumnMapping.isColumnMappingModeEnabled("id")).isTrue
  }

  test("column mapping change with empty config") {
    assertThatNoException.isThrownBy(() =>
      ColumnMapping.verifyColumnMappingChange(
        new util.HashMap(), new util.HashMap(), true /* isNewTable */))
  }

  test("column mapping mode change is allowed") {
    val isNewTable = true
    assertThatNoException.isThrownBy(() =>
      ColumnMapping.verifyColumnMappingChange(
        new util.HashMap(),
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_NAME),
        isNewTable))

    assertThatNoException.isThrownBy(() =>
      ColumnMapping.verifyColumnMappingChange(
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_NONE),
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_NAME),
        isNewTable))

    assertThatNoException.isThrownBy(() =>
      ColumnMapping.verifyColumnMappingChange(
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_ID),
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_ID),
        isNewTable))

    assertThatNoException.isThrownBy(() =>
      ColumnMapping.verifyColumnMappingChange(
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_NAME),
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_NAME),
        isNewTable))
  }

  test("column mapping mode change not allowed on existing table") {
    val isNewTable = false
    assertThatThrownBy(() =>
      ColumnMapping.verifyColumnMappingChange(
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_NAME),
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_ID), isNewTable))
      .isInstanceOf(classOf[IllegalArgumentException])
      .hasMessage("Changing column mapping mode from 'name' to 'id' is not supported")

    assertThatThrownBy(() =>
      ColumnMapping.verifyColumnMappingChange(
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_ID),
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_NAME), isNewTable))
      .isInstanceOf(classOf[IllegalArgumentException])
      .hasMessage("Changing column mapping mode from 'id' to 'name' is not supported")

    assertThatThrownBy(() =>
      ColumnMapping.verifyColumnMappingChange(
        new util.HashMap(),
        Maps.newHashMap(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_ID), isNewTable))
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
        .add("a", StringType.STRING,
          FieldMetadata.builder.putLong(COLUMN_MAPPING_ID_KEY, 14).build)
        .add("b", IntegerType.INTEGER,
          FieldMetadata.builder.putLong(COLUMN_MAPPING_ID_KEY, 17).build)
        .add("c", IntegerType.INTEGER,
          FieldMetadata.builder.putLong(COLUMN_MAPPING_ID_KEY, 3).build)))
      .isEqualTo(17)

    // nested columns are currently not supported
    assertThat(ColumnMapping.findMaxColumnId(
      new StructType().add("a", StringType.STRING,
          FieldMetadata.builder.putLong(COLUMN_MAPPING_ID_KEY, 14).build)
        .add("b",
          new StructType()
            .add("d", IntegerType.INTEGER, true)
            .add("e", IntegerType.INTEGER, true),
          FieldMetadata.builder.putLong(COLUMN_MAPPING_ID_KEY, 15).build)
        .add("c", IntegerType.INTEGER,
          FieldMetadata.builder.putLong(COLUMN_MAPPING_ID_KEY, 7).build)))
      .isEqualTo(15)
  }

  test("assign id and physical name to new table") {
    val schema: StructType = new StructType()
      .add("a", StringType.STRING, true)
      .add("b", StringType.STRING, true)

    val metadata: Metadata =
      ColumnMapping.updateColumnMappingMetadata(createMetadata(schema), "id", true)

    assertThat(metadata.getSchema.get("a").getMetadata.getEntries)
      .containsEntry(ColumnMapping.COLUMN_MAPPING_ID_KEY, (1L).asInstanceOf[AnyRef])
      .hasEntrySatisfying(
        ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY,
        (k: AnyRef) => assertThat(k).asString.startsWith("col-"))

    assertThat(metadata.getSchema.get("b").getMetadata.getEntries)
      .containsEntry(ColumnMapping.COLUMN_MAPPING_ID_KEY, (2L).asInstanceOf[AnyRef])
      .hasEntrySatisfying(
        ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY,
        (k: AnyRef) => assertThat(k).asString.startsWith("col-"))

    assertThat(metadata.getConfiguration)
      .containsEntry(ColumnMapping.COLUMN_MAPPING_MAX_COLUMN_ID_KEY, "2")
  }

  test("assign id and physical name to updated table") {
    val schema = new StructType()
      .add("a", StringType.STRING, true)
      .add("b", StringType.STRING, true)

    val metadata = ColumnMapping.updateColumnMappingMetadata(createMetadata(schema), "id", false)

    assertThat(metadata.getSchema.get("a").getMetadata.getEntries)
      .containsEntry(ColumnMapping.COLUMN_MAPPING_ID_KEY, (1L).asInstanceOf[AnyRef])
      .hasEntrySatisfying(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY,
        (k: AnyRef) => assertThat(k).asString.isEqualTo("a"))

    assertThat(metadata.getSchema.get("b").getMetadata.getEntries)
      .containsEntry(ColumnMapping.COLUMN_MAPPING_ID_KEY, (2L).asInstanceOf[AnyRef])
      .hasEntrySatisfying(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY,
        (k: AnyRef) => assertThat(k).asString.isEqualTo("b"))

    assertThat(metadata.getConfiguration)
      .containsEntry(ColumnMapping.COLUMN_MAPPING_MAX_COLUMN_ID_KEY, "2")
  }

  test("none mapping mode returns original schema") {
    val schema = new StructType().add("a", StringType.STRING, true)

    val metadata =
      ColumnMapping.updateColumnMappingMetadata(createMetadata(schema), "none", true)
    assertThat(metadata.getSchema).isEqualTo(schema)
  }

  test("cannot use invalid mapping mode") {
    assertThatThrownBy(() =>
      ColumnMapping.updateColumnMappingMetadata(createMetadata(new StructType), "invalid", true))
      .isInstanceOf(classOf[UnsupportedOperationException])
      .hasMessage("Unsupported column mapping mode: invalid")
  }

  test("assigning id and physical name preserves field metadata") {
    val schema = new StructType()
      .add("a", StringType.STRING,
        FieldMetadata.builder.putString("key1", "val1").putString("key2", "val2").build)

    val metadata = ColumnMapping.updateColumnMappingMetadata(createMetadata(schema), "id", true)
    val fieldMetadata = metadata.getSchema.get("a").getMetadata.getEntries

    assertThat(fieldMetadata)
      .containsEntry("key1", "val1")
      .containsEntry("key2", "val2")
      .containsEntry(ColumnMapping.COLUMN_MAPPING_ID_KEY, (1L).asInstanceOf[AnyRef])
      .hasEntrySatisfying(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY,
        (k: AnyRef) => assertThat(k).asString.startsWith("col-"))
  }

  private def createMetadata(schema: StructType) = new Metadata(
    UUID.randomUUID.toString,
    Optional.empty(),
    Optional.empty(),
    new Format,
    schema.toJson,
    schema,
    VectorUtils.stringArrayValue(Collections.emptyList()),
    Optional.empty(),
    VectorUtils.stringStringMapValue(Collections.emptyMap()))
}
