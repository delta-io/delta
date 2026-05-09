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

import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.util.*;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class KernelRowToSparkRowTest {

  @Test
  public void testPrimitiveTypes() {
    StructType schema =
        new StructType()
            .add("boolField", BooleanType.BOOLEAN)
            .add("byteField", ByteType.BYTE)
            .add("shortField", ShortType.SHORT)
            .add("intField", IntegerType.INTEGER)
            .add("longField", LongType.LONG)
            .add("floatField", FloatType.FLOAT)
            .add("doubleField", DoubleType.DOUBLE)
            .add("stringField", StringType.STRING)
            .add("decimalField", new DecimalType(10, 2));

    Map<Integer, Object> fieldMap = new HashMap<>();
    fieldMap.put(0, true);
    fieldMap.put(1, (byte) 42);
    fieldMap.put(2, (short) 1000);
    fieldMap.put(3, 123456);
    fieldMap.put(4, 9876543210L);
    fieldMap.put(5, 3.14f);
    fieldMap.put(6, 2.71828);
    fieldMap.put(7, "hello");
    fieldMap.put(8, new BigDecimal("12345.67"));

    io.delta.kernel.data.Row kernelRow = new GenericRow(schema, fieldMap);
    Row sparkRow = new KernelRowToSparkRow(kernelRow);

    assertEquals(9, sparkRow.length());
    assertEquals(true, sparkRow.get(0));
    assertEquals((byte) 42, sparkRow.get(1));
    assertEquals((short) 1000, sparkRow.get(2));
    assertEquals(123456, sparkRow.get(3));
    assertEquals(9876543210L, sparkRow.get(4));
    assertEquals(3.14f, sparkRow.get(5));
    assertEquals(2.71828, sparkRow.get(6));
    assertEquals("hello", sparkRow.get(7));
    assertEquals(new BigDecimal("12345.67"), sparkRow.get(8));
  }

  @Test
  public void testNullableFields() {
    StructType schema =
        new StructType()
            .add("a", StringType.STRING, true)
            .add("b", LongType.LONG, true)
            .add("c", StringType.STRING, true);

    Map<Integer, Object> fieldMap = new HashMap<>();
    fieldMap.put(0, "present");
    // b and c are null (not in fieldMap)

    io.delta.kernel.data.Row kernelRow = new GenericRow(schema, fieldMap);
    Row sparkRow = new KernelRowToSparkRow(kernelRow);

    assertFalse(sparkRow.isNullAt(0));
    assertEquals("present", sparkRow.get(0));
    assertTrue(sparkRow.isNullAt(1));
    assertNull(sparkRow.get(1));
    assertTrue(sparkRow.isNullAt(2));
    assertNull(sparkRow.get(2));
  }

  @Test
  public void testMapField() {
    StructType schema =
        new StructType()
            .add("tags", new MapType(StringType.STRING, StringType.STRING, true), false);

    Map<String, String> tags = new HashMap<>();
    tags.put("key1", "val1");
    tags.put("key2", "val2");

    Map<Integer, Object> fieldMap = new HashMap<>();
    fieldMap.put(0, VectorUtils.stringStringMapValue(tags));

    io.delta.kernel.data.Row kernelRow = new GenericRow(schema, fieldMap);
    Row sparkRow = new KernelRowToSparkRow(kernelRow);

    Object mapObj = sparkRow.get(0);
    assertTrue(mapObj instanceof scala.collection.Map);
    @SuppressWarnings("unchecked")
    scala.collection.Map<String, String> scalaMap = (scala.collection.Map<String, String>) mapObj;
    assertEquals("val1", scalaMap.apply("key1"));
    assertEquals("val2", scalaMap.apply("key2"));
  }

  @Test
  public void testNestedStruct() {
    StructType innerSchema =
        new StructType().add("x", IntegerType.INTEGER).add("y", StringType.STRING);

    StructType outerSchema = new StructType().add("nested", innerSchema);

    Map<Integer, Object> innerFieldMap = new HashMap<>();
    innerFieldMap.put(0, 99);
    innerFieldMap.put(1, "inner");
    io.delta.kernel.data.Row innerRow = new GenericRow(innerSchema, innerFieldMap);

    Map<Integer, Object> outerFieldMap = new HashMap<>();
    outerFieldMap.put(0, innerRow);
    io.delta.kernel.data.Row outerRow = new GenericRow(outerSchema, outerFieldMap);

    Row sparkRow = new KernelRowToSparkRow(outerRow);
    Row nested = sparkRow.getStruct(0);
    assertNotNull(nested);
    assertEquals(99, nested.get(0));
    assertEquals("inner", nested.get(1));
  }

  @Test
  public void testArrayField() {
    StructType schema = new StructType().add("arr", new ArrayType(StringType.STRING, true));

    List<String> elements = Arrays.asList("a", "b", "c");

    Map<Integer, Object> fieldMap = new HashMap<>();
    fieldMap.put(0, VectorUtils.buildArrayValue(elements, StringType.STRING));

    io.delta.kernel.data.Row kernelRow = new GenericRow(schema, fieldMap);
    Row sparkRow = new KernelRowToSparkRow(kernelRow);

    Object seqObj = sparkRow.get(0);
    assertTrue(seqObj instanceof scala.collection.Seq);
    @SuppressWarnings("unchecked")
    scala.collection.Seq<String> seq = (scala.collection.Seq<String>) seqObj;
    assertEquals(3, seq.length());
    assertEquals("a", seq.apply(0));
    assertEquals("b", seq.apply(1));
    assertEquals("c", seq.apply(2));
  }

  /** Integration test: verifies AddFile survives Kernel Row -> Spark Row -> Kernel Row. */
  @Tag("integration")
  @Test
  public void testAddFileRoundTrip() {
    Map<String, String> partVals = Collections.singletonMap("date", "2025-01-01");
    Map<String, String> tags = new HashMap<>();
    tags.put("ZCUBE_ID", "abc123");

    Map<Integer, Object> fieldMap = new HashMap<>();
    fieldMap.put(AddFile.SCHEMA_WITHOUT_STATS.indexOf("path"), "file.parquet");
    fieldMap.put(
        AddFile.SCHEMA_WITHOUT_STATS.indexOf("partitionValues"),
        VectorUtils.stringStringMapValue(partVals));
    fieldMap.put(AddFile.SCHEMA_WITHOUT_STATS.indexOf("size"), 2048L);
    fieldMap.put(AddFile.SCHEMA_WITHOUT_STATS.indexOf("modificationTime"), 1000L);
    fieldMap.put(AddFile.SCHEMA_WITHOUT_STATS.indexOf("dataChange"), true);
    fieldMap.put(
        AddFile.SCHEMA_WITHOUT_STATS.indexOf("deletionVector"),
        new DeletionVectorDescriptor("u", "ab^-aqEH.-t@S}", Optional.of(4), 40, 3).toRow());
    fieldMap.put(
        AddFile.SCHEMA_WITHOUT_STATS.indexOf("tags"), VectorUtils.stringStringMapValue(tags));
    fieldMap.put(AddFile.SCHEMA_WITHOUT_STATS.indexOf("baseRowId"), 42L);
    fieldMap.put(AddFile.SCHEMA_WITHOUT_STATS.indexOf("defaultRowCommitVersion"), 7L);

    AddFile original = new AddFile(new GenericRow(AddFile.SCHEMA_WITHOUT_STATS, fieldMap));

    Row sparkRow = new KernelRowToSparkRow(original.toRow());
    io.delta.kernel.data.Row kernelRow =
        new SparkRowToKernelRow(sparkRow, AddFile.SCHEMA_WITHOUT_STATS);
    AddFile roundTripped = new AddFile(kernelRow);

    assertEquals(original.getPath(), roundTripped.getPath());
    assertEquals(original.getSize(), roundTripped.getSize());
    assertEquals(original.getModificationTime(), roundTripped.getModificationTime());
    assertEquals(original.getDataChange(), roundTripped.getDataChange());

    assertTrue(roundTripped.getDeletionVector().isPresent());
    DeletionVectorDescriptor dv = roundTripped.getDeletionVector().get();
    assertEquals("u", dv.getStorageType());
    assertEquals("ab^-aqEH.-t@S}", dv.getPathOrInlineDv());
    assertEquals(Optional.of(4), dv.getOffset());
    assertEquals(40, dv.getSizeInBytes());
    assertEquals(3, dv.getCardinality());

    assertTrue(roundTripped.getTags().isPresent());
    Map<?, ?> roundTrippedTags = VectorUtils.toJavaMap(roundTripped.getTags().get());
    assertEquals("abc123", roundTrippedTags.get("ZCUBE_ID"));

    assertEquals(Optional.of(42L), roundTripped.getBaseRowId());
    assertEquals(Optional.of(7L), roundTripped.getDefaultRowCommitVersion());

    Map<?, ?> roundTrippedPartVals = VectorUtils.toJavaMap(roundTripped.getPartitionValues());
    assertEquals("2025-01-01", roundTrippedPartVals.get("date"));
  }
}
