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
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.*;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
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

  @Test
  public void testDateField() {
    StructType schema = new StructType().add("dateField", DateType.DATE);

    Date sqlDate = Date.valueOf(LocalDate.of(2025, 6, 15));
    org.apache.spark.sql.Row sparkRow = RowFactory.create(sqlDate);
    Row kernelRow = new SparkRowToKernelRow(sparkRow, schema);

    int epochDays = kernelRow.getInt(0);
    assertEquals(DateTimeUtils.fromJavaDate(sqlDate), epochDays);
  }

  @Test
  public void testTimestampField() {
    StructType schema = new StructType().add("tsField", TimestampType.TIMESTAMP);

    Timestamp sqlTs = Timestamp.valueOf("2025-06-15 10:30:00");
    org.apache.spark.sql.Row sparkRow = RowFactory.create(sqlTs);
    Row kernelRow = new SparkRowToKernelRow(sparkRow, schema);

    long micros = kernelRow.getLong(0);
    assertEquals(DateTimeUtils.fromJavaTimestamp(sqlTs), micros);
  }

  @Test
  public void testTimestampNTZField() {
    StructType schema = new StructType().add("tsNtzField", TimestampNTZType.TIMESTAMP_NTZ);

    LocalDateTime ldt = LocalDateTime.of(2025, 6, 15, 10, 30, 0);
    org.apache.spark.sql.Row sparkRow = RowFactory.create(ldt);
    Row kernelRow = new SparkRowToKernelRow(sparkRow, schema);

    long micros = kernelRow.getLong(0);
    assertEquals(DateTimeUtils.localDateTimeToMicros(ldt), micros);
  }

  @Test
  public void testNullGuardsOnPrimitiveGetters() {
    StructType schema =
        new StructType()
            .add("boolField", BooleanType.BOOLEAN, true)
            .add("intField", IntegerType.INTEGER, true)
            .add("longField", LongType.LONG, true)
            .add("floatField", FloatType.FLOAT, true)
            .add("doubleField", DoubleType.DOUBLE, true);

    org.apache.spark.sql.Row sparkRow = RowFactory.create(null, null, null, null, null);
    Row kernelRow = new SparkRowToKernelRow(sparkRow, schema);

    assertTrue(kernelRow.isNullAt(0));
    assertThrows(IllegalStateException.class, () -> kernelRow.getBoolean(0));
    assertThrows(IllegalStateException.class, () -> kernelRow.getInt(1));
    assertThrows(IllegalStateException.class, () -> kernelRow.getLong(2));
    assertThrows(IllegalStateException.class, () -> kernelRow.getFloat(3));
    assertThrows(IllegalStateException.class, () -> kernelRow.getDouble(4));
  }

  @Test
  public void testJavaUtilMapInput() {
    StructType schema =
        new StructType().add("tags", new MapType(StringType.STRING, StringType.STRING, true));

    Map<String, String> javaMap = new HashMap<>();
    javaMap.put("engine", "spark");
    javaMap.put("version", "4.0");

    org.apache.spark.sql.Row sparkRow = RowFactory.create(javaMap);
    Row kernelRow = new SparkRowToKernelRow(sparkRow, schema);

    MapValue mv = kernelRow.getMap(0);
    assertNotNull(mv);
    assertEquals(2, mv.getSize());
    Map<?, ?> roundTripped = VectorUtils.toJavaMap(mv);
    assertEquals("spark", roundTripped.get("engine"));
    assertEquals("4.0", roundTripped.get("version"));
  }

  @Test
  public void testDateFieldWithLocalDate() {
    StructType schema = new StructType().add("dateField", DateType.DATE);

    LocalDate ld = LocalDate.of(1970, 1, 1);
    org.apache.spark.sql.Row sparkRow = RowFactory.create(Date.valueOf(ld));
    Row kernelRow = new SparkRowToKernelRow(sparkRow, schema);

    assertEquals(0, kernelRow.getInt(0));
  }

  @Test
  public void testDateFieldEpochBoundary() {
    StructType schema = new StructType().add("dateField", DateType.DATE);

    Date preEpoch = Date.valueOf(LocalDate.of(1969, 12, 31));
    org.apache.spark.sql.Row sparkRow = RowFactory.create(preEpoch);
    Row kernelRow = new SparkRowToKernelRow(sparkRow, schema);

    int epochDays = kernelRow.getInt(0);
    assertEquals(-1, epochDays);
  }

  @Test
  public void testTimestampFieldWithInstant() {
    StructType schema = new StructType().add("tsField", TimestampType.TIMESTAMP);

    java.time.Instant instant = java.time.Instant.parse("2025-06-15T10:30:00Z");
    Timestamp sqlTs = Timestamp.from(instant);
    org.apache.spark.sql.Row sparkRow = RowFactory.create(sqlTs);
    Row kernelRow = new SparkRowToKernelRow(sparkRow, schema);

    long micros = kernelRow.getLong(0);
    assertEquals(DateTimeUtils.fromJavaTimestamp(sqlTs), micros);
  }

  @Test
  public void testTimestampNTZFieldRoundTrip() {
    StructType schema = new StructType().add("ntzField", TimestampNTZType.TIMESTAMP_NTZ);

    LocalDateTime ldt = LocalDateTime.of(2025, 1, 1, 0, 0, 0);
    org.apache.spark.sql.Row sparkRow = RowFactory.create(ldt);
    Row kernelRow = new SparkRowToKernelRow(sparkRow, schema);

    long micros = kernelRow.getLong(0);
    assertEquals(ldt, DateTimeUtils.microsToLocalDateTime(micros));
  }

  @Test
  public void testGetLongPassesThroughRawLongForTimestampNTZ() {
    StructType ntzStruct = new StructType().add("ntz", TimestampNTZType.TIMESTAMP_NTZ);
    StructType schema = new StructType().add("nested", ntzStruct);

    // Inner Row holds a raw Long (simulates InternalRow-backed path).
    // getLong() on the Kernel Row interface should still work — it fulfills the
    // Row.getLong() contract directly without going through sparkValueToKernel().
    org.apache.spark.sql.Row inner = RowFactory.create(1750000000000000L);
    org.apache.spark.sql.Row sparkRow = RowFactory.create(inner);
    Row kernelRow = new SparkRowToKernelRow(sparkRow, schema);

    Row nested = kernelRow.getStruct(0);
    assertEquals(1750000000000000L, nested.getLong(0));
  }

  @Test
  public void testSparkValueToKernelRejectsRawLongForTimestampNTZ() {
    // sparkValueToKernel (used in map/array/struct recursion) should reject Long for
    // TimestampNTZType because a raw Long is ambiguous — it could be UTC micros from a
    // TimestampType context silently misinterpreted as wall-clock micros.
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            SparkRowToKernelRow.sparkValueToKernel(
                1750000000000000L, TimestampNTZType.TIMESTAMP_NTZ));
  }

  @Test
  public void testSparkValueToKernelAcceptsLocalDateTimeForTimestampNTZ() {
    LocalDateTime ldt = LocalDateTime.of(2025, 6, 15, 10, 30, 0);
    Object result = SparkRowToKernelRow.sparkValueToKernel(ldt, TimestampNTZType.TIMESTAMP_NTZ);
    assertEquals(DateTimeUtils.localDateTimeToMicros(ldt), result);
  }

  @Test
  public void testSparkValueToKernelAcceptsLongForTimestampType() {
    // For TimestampType (UTC), raw Long IS accepted — UTC micros is unambiguous.
    long utcMicros = 1750000000000000L;
    Object result = SparkRowToKernelRow.sparkValueToKernel(utcMicros, TimestampType.TIMESTAMP);
    assertEquals(utcMicros, result);
  }

  @Test
  public void testSparkValueToKernelAcceptsTimestampForTimestampType() {
    Timestamp ts = Timestamp.valueOf("2025-06-15 10:30:00");
    Object result = SparkRowToKernelRow.sparkValueToKernel(ts, TimestampType.TIMESTAMP);
    assertEquals(DateTimeUtils.fromJavaTimestamp(ts), result);
  }

  @Test
  public void testSparkValueToKernelAcceptsInstantForTimestampType() {
    java.time.Instant instant = java.time.Instant.parse("2025-06-15T10:30:00Z");
    Object result = SparkRowToKernelRow.sparkValueToKernel(instant, TimestampType.TIMESTAMP);
    assertEquals(DateTimeUtils.instantToMicros(instant), result);
  }

  @Test
  public void testSparkValueToKernelDateConversions() {
    LocalDate ld = LocalDate.of(2025, 6, 15);
    Date sqlDate = Date.valueOf(ld);

    Object fromSqlDate = SparkRowToKernelRow.sparkValueToKernel(sqlDate, DateType.DATE);
    Object fromLocalDate = SparkRowToKernelRow.sparkValueToKernel(ld, DateType.DATE);
    Object fromInt = SparkRowToKernelRow.sparkValueToKernel(20254, DateType.DATE);

    assertEquals(fromSqlDate, fromLocalDate);
    assertEquals(DateTimeUtils.fromJavaDate(sqlDate), fromInt);
  }

  @Test
  public void testNullDateAndTimestampFields() {
    StructType schema =
        new StructType()
            .add("dateField", DateType.DATE, true)
            .add("tsField", TimestampType.TIMESTAMP, true)
            .add("ntzField", TimestampNTZType.TIMESTAMP_NTZ, true);

    org.apache.spark.sql.Row sparkRow = RowFactory.create(null, null, null);
    Row kernelRow = new SparkRowToKernelRow(sparkRow, schema);

    assertTrue(kernelRow.isNullAt(0));
    assertTrue(kernelRow.isNullAt(1));
    assertTrue(kernelRow.isNullAt(2));
    assertThrows(IllegalStateException.class, () -> kernelRow.getInt(0));
    assertThrows(IllegalStateException.class, () -> kernelRow.getLong(1));
    assertThrows(IllegalStateException.class, () -> kernelRow.getLong(2));
  }

  @Test
  public void testJavaUtilListArrayInput() {
    StructType schema = new StructType().add("items", new ArrayType(StringType.STRING, true));

    List<String> javaList = List.of("alpha", "beta", "gamma");

    org.apache.spark.sql.Row sparkRow = RowFactory.create(javaList);
    Row kernelRow = new SparkRowToKernelRow(sparkRow, schema);

    ArrayValue av = kernelRow.getArray(0);
    assertNotNull(av);
    // Verify round-trip
    List<?> roundTripped = VectorUtils.toJavaList(av);
    assertEquals(3, roundTripped.size());
    assertEquals("alpha", roundTripped.get(0));
    assertEquals("beta", roundTripped.get(1));
    assertEquals("gamma", roundTripped.get(2));
  }
}
