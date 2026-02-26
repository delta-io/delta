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
package io.delta.spark.internal.v2.utils;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.*;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Test;

public class SparkRowToKernelRowTest {

  @Test
  public void testPrimitiveTypes() {
    StructType kernelSchema =
        new StructType()
            .add("boolField", BooleanType.BOOLEAN)
            .add("intField", IntegerType.INTEGER)
            .add("longField", LongType.LONG)
            .add("stringField", StringType.STRING)
            .add("doubleField", DoubleType.DOUBLE);

    org.apache.spark.sql.Row sparkRow = RowFactory.create(true, 42, 9876543210L, "hello", 2.71828);

    Row kernelRow = new SparkRowToKernelRow(sparkRow, kernelSchema);

    assertEquals(true, kernelRow.getBoolean(0));
    assertEquals(42, kernelRow.getInt(1));
    assertEquals(9876543210L, kernelRow.getLong(2));
    assertEquals("hello", kernelRow.getString(3));
    assertEquals(2.71828, kernelRow.getDouble(4));
  }

  @Test
  public void testNullableFields() {
    StructType kernelSchema =
        new StructType().add("a", StringType.STRING, true).add("b", LongType.LONG, true);

    org.apache.spark.sql.Row sparkRow = RowFactory.create("present", null);

    Row kernelRow = new SparkRowToKernelRow(sparkRow, kernelSchema);

    assertFalse(kernelRow.isNullAt(0));
    assertEquals("present", kernelRow.getString(0));
    assertTrue(kernelRow.isNullAt(1));
  }

  @Test
  public void testMapField() {
    StructType kernelSchema =
        new StructType().add("tags", new MapType(StringType.STRING, StringType.STRING, true));

    Map<String, String> tags = new HashMap<>();
    tags.put("key1", "val1");
    tags.put("key2", "val2");
    scala.collection.Map<String, String> scalaMap =
        scala.jdk.javaapi.CollectionConverters.asScala(tags);

    org.apache.spark.sql.Row sparkRow = RowFactory.create(scalaMap);

    Row kernelRow = new SparkRowToKernelRow(sparkRow, kernelSchema);

    MapValue mv = kernelRow.getMap(0);
    assertNotNull(mv);
    assertEquals(2, mv.getSize());
    Map<?, ?> roundTripped = VectorUtils.toJavaMap(mv);
    assertEquals("val1", roundTripped.get("key1"));
    assertEquals("val2", roundTripped.get("key2"));
  }

  @Test
  public void testNestedStruct() {
    StructType innerKernelSchema =
        new StructType().add("x", IntegerType.INTEGER).add("y", StringType.STRING);

    StructType outerKernelSchema = new StructType().add("nested", innerKernelSchema);

    org.apache.spark.sql.Row innerSparkRow = RowFactory.create(99, "inner");
    org.apache.spark.sql.Row outerSparkRow = RowFactory.create(innerSparkRow);

    Row kernelRow = new SparkRowToKernelRow(outerSparkRow, outerKernelSchema);

    Row nested = kernelRow.getStruct(0);
    assertNotNull(nested);
    assertEquals(99, nested.getInt(0));
    assertEquals("inner", nested.getString(1));
  }
}
