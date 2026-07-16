/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.flink.kernel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import org.junit.jupiter.api.Test;

class ColumnMappingUtilsTest {

  private static FieldMetadata physicalName(String name) {
    return FieldMetadata.builder()
        .putString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, name)
        .build();
  }

  @Test
  void noColumnMapping_returnsSchemaUnchanged() {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);

    StructType physical = ColumnMappingUtils.toPhysicalSchema(schema);

    // No physical-name metadata: the original schema instance is returned untouched, no rebuild.
    assertSame(schema, physical);
  }

  @Test
  void emptySchema_returnsSchemaUnchanged() {
    StructType schema = new StructType();

    StructType physical = ColumnMappingUtils.toPhysicalSchema(schema);

    assertSame(schema, physical);
  }

  @Test
  void columnMapping_buildsNewSchema() {
    StructType schema = new StructType().add("id", IntegerType.INTEGER, physicalName("col-aaaa"));

    StructType physical = ColumnMappingUtils.toPhysicalSchema(schema);

    // Column mapping present: a new physical schema is built rather than returning the input.
    assertNotSame(schema, physical);
    assertEquals("col-aaaa", physical.fields().get(0).getName());
  }

  @Test
  void renamesTopLevelFieldsToPhysicalName() {
    StructType schema =
        new StructType()
            .add("id", IntegerType.INTEGER, physicalName("col-aaaa"))
            .add("name", StringType.STRING, physicalName("col-bbbb"));

    StructType physical = ColumnMappingUtils.toPhysicalSchema(schema);

    assertEquals("col-aaaa", physical.fields().get(0).getName());
    assertEquals("col-bbbb", physical.fields().get(1).getName());
  }

  @Test
  void recursesIntoNestedStructArrayAndMap() {
    StructType nested =
        new StructType().add("inner", IntegerType.INTEGER, physicalName("col-inner"));
    StructType elementStruct =
        new StructType().add("elem", IntegerType.INTEGER, physicalName("col-elem"));
    StructType valueStruct =
        new StructType().add("val", IntegerType.INTEGER, physicalName("col-val"));

    StructType schema =
        new StructType()
            .add("s", nested, physicalName("col-s"))
            .add("arr", new ArrayType(elementStruct, true), physicalName("col-arr"))
            .add("m", new MapType(StringType.STRING, valueStruct, true), physicalName("col-m"));

    StructType physical = ColumnMappingUtils.toPhysicalSchema(schema);

    // Top-level fields renamed.
    assertEquals("col-s", physical.fields().get(0).getName());
    assertEquals("col-arr", physical.fields().get(1).getName());
    assertEquals("col-m", physical.fields().get(2).getName());

    // Nested struct field renamed.
    StructType physNested = (StructType) physical.fields().get(0).getDataType();
    assertEquals("col-inner", physNested.fields().get(0).getName());

    // Array element struct field renamed.
    ArrayType physArr = (ArrayType) physical.fields().get(1).getDataType();
    StructType physElem = (StructType) physArr.getElementType();
    assertEquals("col-elem", physElem.fields().get(0).getName());

    // Map value struct field renamed.
    MapType physMap = (MapType) physical.fields().get(2).getDataType();
    StructType physVal = (StructType) physMap.getValueType();
    assertEquals("col-val", physVal.fields().get(0).getName());
  }
}
