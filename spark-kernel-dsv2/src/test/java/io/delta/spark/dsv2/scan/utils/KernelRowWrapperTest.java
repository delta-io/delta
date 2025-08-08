package io.delta.spark.dsv2.scan.utils;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

/** Tests for {@link KernelRowWrapper}. */
public class KernelRowWrapperTest {

  @Test
  public void testPrimitiveGetters() {
    StructType schema =
        new StructType(
            Arrays.asList(
                new StructField("b", BooleanType.BOOLEAN, true /*nullable*/),
                new StructField("y", ByteType.BYTE, true /*nullable*/),
                new StructField("s", ShortType.SHORT, true /*nullable*/),
                new StructField("i", IntegerType.INTEGER, true /*nullable*/),
                new StructField("l", LongType.LONG, true /*nullable*/),
                new StructField("f", FloatType.FLOAT, true /*nullable*/),
                new StructField("d", DoubleType.DOUBLE, true /*nullable*/),
                new StructField(
                    "dec", new DecimalType(10 /*precision*/, 2 /*scale*/), true /*nullable*/),
                new StructField("bin", BinaryType.BINARY, true /*nullable*/),
                new StructField("str", StringType.STRING, true /*nullable*/),
                new StructField(
                    "decNull", new DecimalType(10 /*precision*/, 2 /*scale*/), true /*nullable*/),
                new StructField("strNull", StringType.STRING, true /*nullable*/)));

    byte[] sampleBytes = new byte[] {1, 2, 3};
    BigDecimal sampleDecimal = new BigDecimal("1234.56");

    List<Object> values =
        Arrays.asList(
            true,
            (byte) 7,
            (short) 300,
            42,
            1234567890123L,
            3.14f,
            6.28d,
            sampleDecimal,
            sampleBytes,
            "hello",
            null,
            null);

    Row kernelRow = makePrimitiveRow(schema, values);
    InternalRow row = new KernelRowWrapper(kernelRow);

    assertEquals(12, row.numFields(), "field count");
    assertAll(
        "primitive getters",
        () -> assertTrue(row.getBoolean(0 /*ordinal*/), "boolean"),
        () -> assertEquals((byte) 7, row.getByte(1 /*ordinal*/), "byte"),
        () -> assertEquals((short) 300, row.getShort(2 /*ordinal*/), "short"),
        () -> assertEquals(42, row.getInt(3 /*ordinal*/), "int"),
        () -> assertEquals(1234567890123L, row.getLong(4 /*ordinal*/), "long"),
        () -> assertEquals(3.14f, row.getFloat(5 /*ordinal*/), "float"),
        () -> assertEquals(6.28d, row.getDouble(6 /*ordinal*/), "double"),
        () ->
            assertEquals(
                sampleDecimal, row.getDecimal(7 /*ordinal*/, 10, 2).toJavaBigDecimal(), "decimal"),
        () -> assertArrayEquals(sampleBytes, row.getBinary(8 /*ordinal*/), "binary"),
        () ->
            assertEquals(
                UTF8String.fromString("hello"), row.getUTF8String(9 /*ordinal*/), "string"));
    assertAll(
        "null value",
        () -> assertTrue(row.isNullAt(10 /*ordinal*/), "decNull isNull"),
        () -> assertNull(row.getDecimal(10 /*ordinal*/, 10, 2), "decNull value"),
        () -> assertTrue(row.isNullAt(11 /*ordinal*/), "strNull isNull"),
        () -> assertNull(row.getUTF8String(11 /*ordinal*/), "strNull value"));
  }

  @Test
  public void testNestedType_unsupported() {
    StructType schema =
        new StructType(Arrays.asList(new StructField("i", IntegerType.INTEGER, true /*nullable*/)));

    Row kernelRow = makePrimitiveRow(schema, Arrays.asList(1));
    InternalRow row = new KernelRowWrapper(kernelRow);

    assertThrows(
        UnsupportedOperationException.class,
        () -> row.getArray(0 /*ordinal*/),
        "array unsupported");
    assertThrows(
        UnsupportedOperationException.class, () -> row.getMap(0 /*ordinal*/), "map unsupported");
    assertThrows(
        UnsupportedOperationException.class,
        () -> row.getStruct(0 /*ordinal*/, 0),
        "struct unsupported");
  }

  //////////////////////
  // Private helpers //
  /////////////////////
  private static Row makePrimitiveRow(StructType schema, List<Object> values) {
    return new Row() {
      @Override
      public StructType getSchema() {
        return schema;
      }

      @Override
      public boolean isNullAt(int ordinal) {
        return values.get(ordinal) == null;
      }

      @Override
      public boolean getBoolean(int ordinal) {
        return (Boolean) values.get(ordinal);
      }

      @Override
      public byte getByte(int ordinal) {
        return (Byte) values.get(ordinal);
      }

      @Override
      public short getShort(int ordinal) {
        return (Short) values.get(ordinal);
      }

      @Override
      public int getInt(int ordinal) {
        return (Integer) values.get(ordinal);
      }

      @Override
      public long getLong(int ordinal) {
        return (Long) values.get(ordinal);
      }

      @Override
      public float getFloat(int ordinal) {
        return (Float) values.get(ordinal);
      }

      @Override
      public double getDouble(int ordinal) {
        return (Double) values.get(ordinal);
      }

      @Override
      public BigDecimal getDecimal(int ordinal) {
        return (BigDecimal) values.get(ordinal);
      }

      @Override
      public String getString(int ordinal) {
        return (String) values.get(ordinal);
      }

      @Override
      public byte[] getBinary(int ordinal) {
        return (byte[]) values.get(ordinal);
      }

      @Override
      public Row getStruct(int ordinal) {
        throw new UnsupportedOperationException();
      }

      @Override
      public ArrayValue getArray(int ordinal) {
        throw new UnsupportedOperationException();
      }

      @Override
      public MapValue getMap(int ordinal) {
        throw new UnsupportedOperationException();
      }
    };
  }
}
