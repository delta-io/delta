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
package io.delta.spark.internal.v2.utils;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.util.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
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

  @Test
  public void testEqualsAndHashCode() {
    StructType schema =
        new StructType()
            .add("name", StringType.STRING)
            .add("value", IntegerType.INTEGER)
            .add("flag", BooleanType.BOOLEAN);

    Map<Integer, Object> fieldMap1 = new HashMap<>();
    fieldMap1.put(0, "alpha");
    fieldMap1.put(1, 42);
    fieldMap1.put(2, true);

    Map<Integer, Object> fieldMap2 = new HashMap<>();
    fieldMap2.put(0, "alpha");
    fieldMap2.put(1, 42);
    fieldMap2.put(2, true);

    Map<Integer, Object> fieldMap3 = new HashMap<>();
    fieldMap3.put(0, "beta");
    fieldMap3.put(1, 42);
    fieldMap3.put(2, true);

    Row row1 = new KernelRowToSparkRow(new GenericRow(schema, fieldMap1));
    Row row2 = new KernelRowToSparkRow(new GenericRow(schema, fieldMap2));
    Row row3 = new KernelRowToSparkRow(new GenericRow(schema, fieldMap3));

    assertEquals(row1, row2);
    assertEquals(row1.hashCode(), row2.hashCode());
    assertNotEquals(row1, row3);
  }

  /**
   * Uses vector-backed rows (via DefaultColumnarBatch) instead of GenericRow because GenericRow's
   * throwIfUnsafeAccess rejects getInt() for DateType and getLong() for
   * TimestampType/TimestampNTZType. In production, KernelRowToSparkRow wraps ColumnarBatchRow or
   * StructRow (both extend ChildVectorBasedRow), where getInt()/getLong() delegate to
   * ColumnVector.getInt()/getLong() which correctly handle date/timestamp types.
   *
   * <p>This test exercises the exact production code path: ColumnarBatchRow ->
   * ChildVectorBasedRow.getInt()/getLong() -> GenericColumnVector.getInt()/getLong() ->
   * KernelRowToSparkRow.toSparkValue() -> DateTimeUtils conversion
   */
  @Test
  public void testDateAndTimestampConversion() {
    StructType schema =
        new StructType()
            .add("dateField", DateType.DATE)
            .add("tsField", TimestampType.TIMESTAMP)
            .add("tsNtzField", TimestampNTZType.TIMESTAMP_NTZ);

    int epochDays = 20254; // ~2025-06-15
    long tsMicros = 1750000000000000L;
    long ntzMicros = 1750000000000000L;

    ColumnVector dateVector = VectorUtils.buildColumnVector(List.of(epochDays), DateType.DATE);
    ColumnVector tsVector =
        VectorUtils.buildColumnVector(List.of(tsMicros), TimestampType.TIMESTAMP);
    ColumnVector ntzVector =
        VectorUtils.buildColumnVector(List.of(ntzMicros), TimestampNTZType.TIMESTAMP_NTZ);

    DefaultColumnarBatch batch =
        new DefaultColumnarBatch(1, schema, new ColumnVector[] {dateVector, tsVector, ntzVector});
    io.delta.kernel.data.Row kernelRow = batch.getRows().next();

    Row sparkRow = new KernelRowToSparkRow(kernelRow);

    Object dateVal = sparkRow.get(0);
    assertTrue(
        dateVal instanceof java.sql.Date, "Expected java.sql.Date, got " + dateVal.getClass());
    assertEquals(java.sql.Date.valueOf(java.time.LocalDate.ofEpochDay(epochDays)), dateVal);

    Object tsVal = sparkRow.get(1);
    assertTrue(
        tsVal instanceof java.sql.Timestamp,
        "Expected java.sql.Timestamp, got " + tsVal.getClass());
    assertEquals(DateTimeUtils.toJavaTimestamp(tsMicros), tsVal);

    Object ntzVal = sparkRow.get(2);
    assertTrue(
        ntzVal instanceof java.time.LocalDateTime,
        "Expected java.time.LocalDateTime, got " + ntzVal.getClass());
    assertEquals(DateTimeUtils.microsToLocalDateTime(ntzMicros), ntzVal);
  }

  @Test
  public void testDateEpochBoundary() {
    StructType schema = new StructType().add("dateField", DateType.DATE);

    ColumnVector dateVector = VectorUtils.buildColumnVector(List.of(0), DateType.DATE);
    DefaultColumnarBatch batch =
        new DefaultColumnarBatch(1, schema, new ColumnVector[] {dateVector});
    io.delta.kernel.data.Row kernelRow = batch.getRows().next();

    Row sparkRow = new KernelRowToSparkRow(kernelRow);
    assertEquals(java.sql.Date.valueOf(java.time.LocalDate.ofEpochDay(0)), sparkRow.get(0));
  }

  @Test
  public void testPreEpochDate() {
    StructType schema = new StructType().add("dateField", DateType.DATE);

    ColumnVector dateVector = VectorUtils.buildColumnVector(List.of(-1), DateType.DATE);
    DefaultColumnarBatch batch =
        new DefaultColumnarBatch(1, schema, new ColumnVector[] {dateVector});
    io.delta.kernel.data.Row kernelRow = batch.getRows().next();

    Row sparkRow = new KernelRowToSparkRow(kernelRow);
    assertEquals(java.sql.Date.valueOf(java.time.LocalDate.ofEpochDay(-1)), sparkRow.get(0));
  }

  @Test
  public void testTimestampZeroMicros() {
    StructType schema = new StructType().add("tsField", TimestampType.TIMESTAMP);

    ColumnVector tsVector = VectorUtils.buildColumnVector(List.of(0L), TimestampType.TIMESTAMP);
    DefaultColumnarBatch batch = new DefaultColumnarBatch(1, schema, new ColumnVector[] {tsVector});
    io.delta.kernel.data.Row kernelRow = batch.getRows().next();

    Row sparkRow = new KernelRowToSparkRow(kernelRow);
    assertEquals(DateTimeUtils.toJavaTimestamp(0L), sparkRow.get(0));
  }

  @Test
  public void testNullDateTimestampFields() {
    StructType schema =
        new StructType()
            .add("dateField", DateType.DATE, true)
            .add("tsField", TimestampType.TIMESTAMP, true)
            .add("ntzField", TimestampNTZType.TIMESTAMP_NTZ, true);

    ColumnVector dateVector =
        VectorUtils.buildColumnVector(Collections.singletonList(null), DateType.DATE);
    ColumnVector tsVector =
        VectorUtils.buildColumnVector(Collections.singletonList(null), TimestampType.TIMESTAMP);
    ColumnVector ntzVector =
        VectorUtils.buildColumnVector(
            Collections.singletonList(null), TimestampNTZType.TIMESTAMP_NTZ);

    DefaultColumnarBatch batch =
        new DefaultColumnarBatch(1, schema, new ColumnVector[] {dateVector, tsVector, ntzVector});
    io.delta.kernel.data.Row kernelRow = batch.getRows().next();

    Row sparkRow = new KernelRowToSparkRow(kernelRow);
    assertTrue(sparkRow.isNullAt(0));
    assertTrue(sparkRow.isNullAt(1));
    assertTrue(sparkRow.isNullAt(2));
    assertNull(sparkRow.get(0));
    assertNull(sparkRow.get(1));
    assertNull(sparkRow.get(2));
  }

  @Test
  public void testDateTimestampRoundTrip() {
    StructType schema =
        new StructType()
            .add("dateField", DateType.DATE)
            .add("tsField", TimestampType.TIMESTAMP)
            .add("ntzField", TimestampNTZType.TIMESTAMP_NTZ);

    int epochDays = 20254;
    long tsMicros = 1750000000000000L;
    long ntzMicros = 1750000000000000L;

    ColumnVector dateVector = VectorUtils.buildColumnVector(List.of(epochDays), DateType.DATE);
    ColumnVector tsVector =
        VectorUtils.buildColumnVector(List.of(tsMicros), TimestampType.TIMESTAMP);
    ColumnVector ntzVector =
        VectorUtils.buildColumnVector(List.of(ntzMicros), TimestampNTZType.TIMESTAMP_NTZ);

    DefaultColumnarBatch batch =
        new DefaultColumnarBatch(1, schema, new ColumnVector[] {dateVector, tsVector, ntzVector});
    io.delta.kernel.data.Row kernelRow = batch.getRows().next();

    Row sparkRow = new KernelRowToSparkRow(kernelRow);
    io.delta.kernel.data.Row backToKernel = new SparkRowToKernelRow(sparkRow, schema);

    assertEquals(epochDays, backToKernel.getInt(0));
    assertEquals(tsMicros, backToKernel.getLong(1));
    assertEquals(ntzMicros, backToKernel.getLong(2));
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

  /**
   * Verifies that struct values nested inside a map are materialized (deep-copied) when copy() is
   * called on the outer KernelRowToSparkRow. Without the fix in mapValueToScalaMap(), these struct
   * values would remain as live KernelRowToSparkRow wrappers holding references to the underlying
   * kernel ColumnVector.
   */
  @Test
  public void testCopyMaterializesStructsInsideMap() {
    StructType innerSchema =
        new StructType().add("innerInt", IntegerType.INTEGER).add("innerStr", StringType.STRING);
    MapType mapType = new MapType(StringType.STRING, innerSchema, true);
    StructType outerSchema = new StructType().add("mapField", mapType, false);

    Map<Integer, Object> innerFieldMap1 = new HashMap<>();
    innerFieldMap1.put(0, 10);
    innerFieldMap1.put(1, "hello");
    GenericRow innerRow1 = new GenericRow(innerSchema, innerFieldMap1);

    Map<Integer, Object> innerFieldMap2 = new HashMap<>();
    innerFieldMap2.put(0, 20);
    innerFieldMap2.put(1, "world");
    GenericRow innerRow2 = new GenericRow(innerSchema, innerFieldMap2);

    MapValue mv =
        new MapValue() {
          @Override
          public int getSize() {
            return 2;
          }

          @Override
          public ColumnVector getKeys() {
            return VectorUtils.buildColumnVector(List.of("key1", "key2"), StringType.STRING);
          }

          @Override
          public ColumnVector getValues() {
            return VectorUtils.buildColumnVector(List.of(innerRow1, innerRow2), innerSchema);
          }
        };

    ColumnVector mapVector = VectorUtils.buildColumnVector(List.of(mv), mapType);
    DefaultColumnarBatch batch =
        new DefaultColumnarBatch(1, outerSchema, new ColumnVector[] {mapVector});
    io.delta.kernel.data.Row kernelRow = batch.getRows().next();

    KernelRowToSparkRow sparkRow = new KernelRowToSparkRow(kernelRow);

    // Struct values are materialized at collection-build time (in mapValueToScalaMap),
    // so even before copy(), the map values are already GenericRow, not KernelRowToSparkRow.
    @SuppressWarnings("unchecked")
    scala.collection.Map<Object, Object> preCopyMap =
        (scala.collection.Map<Object, Object>) sparkRow.get(0);
    Row preCopyVal1 = (Row) preCopyMap.apply("key1");
    assertFalse(
        preCopyVal1 instanceof KernelRowToSparkRow,
        "Struct inside map should be materialized at collection-build time");

    Row copied = sparkRow.copy();

    @SuppressWarnings("unchecked")
    scala.collection.Map<Object, Object> scalaMap =
        (scala.collection.Map<Object, Object>) copied.get(0);

    Row val1 = (Row) scalaMap.apply("key1");
    assertFalse(
        val1 instanceof KernelRowToSparkRow,
        "Struct inside map should be materialized, not a live KernelRowToSparkRow");
    assertNotSame(preCopyVal1, val1, "Copied struct should be a different reference");
    assertEquals(preCopyVal1, val1, "Copied struct should have the same content");
    assertEquals(10, val1.get(0));
    assertEquals("hello", val1.get(1));

    Row val2 = (Row) scalaMap.apply("key2");
    assertFalse(
        val2 instanceof KernelRowToSparkRow,
        "Struct inside map should be materialized, not a live KernelRowToSparkRow");
    assertEquals(20, val2.get(0));
    assertEquals("world", val2.get(1));
  }

  /**
   * Verifies that struct elements nested inside an array are materialized (deep-copied) when copy()
   * is called on the outer KernelRowToSparkRow. Without the fix in arrayValueToScalaSeq(), these
   * struct elements would remain as live KernelRowToSparkRow wrappers.
   */
  @Test
  public void testCopyMaterializesStructsInsideArray() {
    StructType innerSchema =
        new StructType().add("innerInt", IntegerType.INTEGER).add("innerStr", StringType.STRING);
    ArrayType arrayType = new ArrayType(innerSchema, true);
    StructType outerSchema = new StructType().add("arrayField", arrayType, false);

    Map<Integer, Object> innerFieldMap1 = new HashMap<>();
    innerFieldMap1.put(0, 100);
    innerFieldMap1.put(1, "alpha");
    GenericRow innerRow1 = new GenericRow(innerSchema, innerFieldMap1);

    Map<Integer, Object> innerFieldMap2 = new HashMap<>();
    innerFieldMap2.put(0, 200);
    innerFieldMap2.put(1, "beta");
    GenericRow innerRow2 = new GenericRow(innerSchema, innerFieldMap2);

    ArrayValue av = VectorUtils.buildArrayValue(List.of(innerRow1, innerRow2), innerSchema);

    ColumnVector arrayVector = VectorUtils.buildColumnVector(List.of(av), arrayType);
    DefaultColumnarBatch batch =
        new DefaultColumnarBatch(1, outerSchema, new ColumnVector[] {arrayVector});
    io.delta.kernel.data.Row kernelRow = batch.getRows().next();

    KernelRowToSparkRow sparkRow = new KernelRowToSparkRow(kernelRow);

    // Struct elements are materialized at collection-build time (in arrayValueToScalaSeq),
    // so even before copy(), the array elements are already GenericRow.
    @SuppressWarnings("unchecked")
    scala.collection.Seq<Object> preCopySeq = (scala.collection.Seq<Object>) sparkRow.get(0);
    Row preCopyElem0 = (Row) preCopySeq.apply(0);
    assertFalse(
        preCopyElem0 instanceof KernelRowToSparkRow,
        "Struct inside array should be materialized at collection-build time");

    Row copied = sparkRow.copy();

    @SuppressWarnings("unchecked")
    scala.collection.Seq<Object> seq = (scala.collection.Seq<Object>) copied.get(0);
    assertEquals(2, seq.length());

    Row elem0 = (Row) seq.apply(0);
    assertFalse(
        elem0 instanceof KernelRowToSparkRow,
        "Struct inside array should be materialized, not a live KernelRowToSparkRow");
    assertNotSame(preCopyElem0, elem0, "Copied struct should be a different reference");
    assertEquals(preCopyElem0, elem0, "Copied struct should have the same content");
    assertEquals(100, elem0.get(0));
    assertEquals("alpha", elem0.get(1));

    Row elem1 = (Row) seq.apply(1);
    assertFalse(
        elem1 instanceof KernelRowToSparkRow,
        "Struct inside array should be materialized, not a live KernelRowToSparkRow");
    assertEquals(200, elem1.get(0));
    assertEquals("beta", elem1.get(1));
  }

  /**
   * Verifies that the basic read path (without copy()) returns correct values for struct fields
   * nested inside both maps and arrays in a single row.
   */
  @Test
  public void testMapAndArrayStructValuesAreCorrect() {
    StructType innerSchema =
        new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);
    MapType mapType = new MapType(StringType.STRING, innerSchema, true);
    ArrayType arrayType = new ArrayType(innerSchema, true);
    StructType outerSchema =
        new StructType().add("mapField", mapType, false).add("arrayField", arrayType, false);

    Map<Integer, Object> mapStructFields = new HashMap<>();
    mapStructFields.put(0, 1);
    mapStructFields.put(1, "one");
    GenericRow mapStructRow = new GenericRow(innerSchema, mapStructFields);

    Map<Integer, Object> arrayStructFields = new HashMap<>();
    arrayStructFields.put(0, 2);
    arrayStructFields.put(1, "two");
    GenericRow arrayStructRow = new GenericRow(innerSchema, arrayStructFields);

    MapValue mv =
        new MapValue() {
          @Override
          public int getSize() {
            return 1;
          }

          @Override
          public ColumnVector getKeys() {
            return VectorUtils.buildColumnVector(List.of("entry"), StringType.STRING);
          }

          @Override
          public ColumnVector getValues() {
            return VectorUtils.buildColumnVector(List.of(mapStructRow), innerSchema);
          }
        };

    ArrayValue av = VectorUtils.buildArrayValue(List.of(arrayStructRow), innerSchema);

    ColumnVector mapVector = VectorUtils.buildColumnVector(List.of(mv), mapType);
    ColumnVector arrayVector = VectorUtils.buildColumnVector(List.of(av), arrayType);
    DefaultColumnarBatch batch =
        new DefaultColumnarBatch(1, outerSchema, new ColumnVector[] {mapVector, arrayVector});
    io.delta.kernel.data.Row kernelRow = batch.getRows().next();

    Row sparkRow = new KernelRowToSparkRow(kernelRow);

    @SuppressWarnings("unchecked")
    scala.collection.Map<Object, Object> map =
        (scala.collection.Map<Object, Object>) sparkRow.get(0);
    Row mapStruct = (Row) map.apply("entry");
    assertEquals(1, mapStruct.get(0));
    assertEquals("one", mapStruct.get(1));

    @SuppressWarnings("unchecked")
    scala.collection.Seq<Object> seq = (scala.collection.Seq<Object>) sparkRow.get(1);
    assertEquals(1, seq.length());
    Row arrayStruct = (Row) seq.apply(0);
    assertEquals(2, arrayStruct.get(0));
    assertEquals("two", arrayStruct.get(1));
  }

  /**
   * Verifies that null struct values inside maps and arrays are handled correctly -- they should
   * come through as null rather than throwing.
   *
   * <p>Uses {@link DefaultGenericVector} instead of {@link VectorUtils#buildColumnVector} for
   * struct-typed vectors because GenericColumnVector.getChild() eagerly iterates all entries via
   * extractChildValues(), which crashes on null elements. DefaultGenericVector.getChild() creates a
   * DefaultSubFieldVector that lazily accesses rows per-index and properly handles nulls.
   */
  @Test
  public void testNullStructValuesInsideMapAndArray() {
    StructType innerSchema =
        new StructType().add("innerInt", IntegerType.INTEGER).add("innerStr", StringType.STRING);
    MapType mapType = new MapType(StringType.STRING, innerSchema, true);
    ArrayType arrayType = new ArrayType(innerSchema, true);
    StructType outerSchema =
        new StructType().add("mapField", mapType, false).add("arrayField", arrayType, false);

    Map<Integer, Object> innerFieldMap = new HashMap<>();
    innerFieldMap.put(0, 10);
    innerFieldMap.put(1, "hello");
    GenericRow innerRow1 = new GenericRow(innerSchema, innerFieldMap);

    List<Object> mapStructValues = new ArrayList<>();
    mapStructValues.add(innerRow1);
    mapStructValues.add(null);

    MapValue mv =
        new MapValue() {
          @Override
          public int getSize() {
            return 2;
          }

          @Override
          public ColumnVector getKeys() {
            return VectorUtils.buildColumnVector(List.of("key1", "key2"), StringType.STRING);
          }

          @Override
          public ColumnVector getValues() {
            return DefaultGenericVector.fromList(innerSchema, mapStructValues);
          }
        };

    List<Object> arrayStructValues = new ArrayList<>();
    arrayStructValues.add(innerRow1);
    arrayStructValues.add(null);
    ArrayValue av =
        new ArrayValue() {
          @Override
          public int getSize() {
            return 2;
          }

          @Override
          public ColumnVector getElements() {
            return DefaultGenericVector.fromList(innerSchema, arrayStructValues);
          }
        };

    ColumnVector mapVector = VectorUtils.buildColumnVector(List.of(mv), mapType);
    ColumnVector arrayVector = VectorUtils.buildColumnVector(List.of(av), arrayType);
    DefaultColumnarBatch batch =
        new DefaultColumnarBatch(1, outerSchema, new ColumnVector[] {mapVector, arrayVector});
    io.delta.kernel.data.Row kernelRow = batch.getRows().next();

    KernelRowToSparkRow sparkRow = new KernelRowToSparkRow(kernelRow);
    Row copied = sparkRow.copy();

    @SuppressWarnings("unchecked")
    scala.collection.Map<Object, Object> map = (scala.collection.Map<Object, Object>) copied.get(0);
    Row mapVal1 = (Row) map.apply("key1");
    assertNotNull(mapVal1);
    assertEquals(10, mapVal1.get(0));
    assertEquals("hello", mapVal1.get(1));
    assertNull(map.apply("key2"));

    @SuppressWarnings("unchecked")
    scala.collection.Seq<Object> seq = (scala.collection.Seq<Object>) copied.get(1);
    assertEquals(2, seq.length());
    Row arrayElem0 = (Row) seq.apply(0);
    assertNotNull(arrayElem0);
    assertEquals(10, arrayElem0.get(0));
    assertEquals("hello", arrayElem0.get(1));
    assertNull(seq.apply(1));
  }
}
