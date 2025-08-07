package io.delta.spark.dsv2.scan.internal;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.GenericColumnVector;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.util.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

public class KernelWrapperSuite {

  private Row makeStructRow(StructType schema, Object... values) {
    return new Row() {
      @Override
      public StructType getSchema() {
        return schema;
      }

      @Override
      public boolean isNullAt(int ordinal) {
        return values[ordinal] == null;
      }

      @Override
      public boolean getBoolean(int ordinal) {
        return (Boolean) values[ordinal];
      }

      @Override
      public byte getByte(int ordinal) {
        return (Byte) values[ordinal];
      }

      @Override
      public short getShort(int ordinal) {
        return (Short) values[ordinal];
      }

      @Override
      public int getInt(int ordinal) {
        return (Integer) values[ordinal];
      }

      @Override
      public long getLong(int ordinal) {
        return (Long) values[ordinal];
      }

      @Override
      public float getFloat(int ordinal) {
        return (Float) values[ordinal];
      }

      @Override
      public double getDouble(int ordinal) {
        return (Double) values[ordinal];
      }

      @Override
      public BigDecimal getDecimal(int ordinal) {
        return (BigDecimal) values[ordinal];
      }

      @Override
      public String getString(int ordinal) {
        return (String) values[ordinal];
      }

      @Override
      public byte[] getBinary(int ordinal) {
        return (byte[]) values[ordinal];
      }

      @Override
      public Row getStruct(int ordinal) {
        return (Row) values[ordinal];
      }

      @Override
      public io.delta.kernel.data.ArrayValue getArray(int ordinal) {
        return (ArrayValue) values[ordinal];
      }

      @Override
      public io.delta.kernel.data.MapValue getMap(int ordinal) {
        return (MapValue) values[ordinal];
      }
    };
  }

  @Test
  public void testKernelRowToSparkRowWrapper_primitivesAndComplex() {
    // Schema: (s string, i int, d decimal(10,2), a array<int>, m map<string,int>, st
    // struct<b:boolean>)
    StructType inner =
        new StructType(Arrays.asList(new StructField("b", BooleanType.BOOLEAN, true)));
    StructType schema =
        new StructType(
            Arrays.asList(
                new StructField("s", StringType.STRING, true),
                new StructField("i", IntegerType.INTEGER, true),
                new StructField("d", new DecimalType(10, 2), true),
                new StructField("a", new ArrayType(IntegerType.INTEGER, true), true),
                new StructField(
                    "m", new MapType(StringType.STRING, IntegerType.INTEGER, true), true),
                new StructField("st", inner, true)));

    // ArrayValue: [1,2,3]
    GenericColumnVector arrayElems =
        new GenericColumnVector(Arrays.asList(1, 2, 3), IntegerType.INTEGER);
    ArrayValue arrayVal =
        new ArrayValue() {
          @Override
          public int getSize() {
            return arrayElems.getSize();
          }

          @Override
          public io.delta.kernel.data.ColumnVector getElements() {
            return arrayElems;
          }
        };

    // MapValue: {"k1":10, "k2":20}
    GenericColumnVector mapKeys =
        new GenericColumnVector(Arrays.asList("k1", "k2"), StringType.STRING);
    GenericColumnVector mapVals =
        new GenericColumnVector(Arrays.asList(10, 20), IntegerType.INTEGER);
    MapValue mapVal =
        new MapValue() {
          @Override
          public int getSize() {
            return 2;
          }

          @Override
          public io.delta.kernel.data.ColumnVector getKeys() {
            return mapKeys;
          }

          @Override
          public io.delta.kernel.data.ColumnVector getValues() {
            return mapVals;
          }
        };

    Row innerRow = makeStructRow(inner, true);
    Row row =
        makeStructRow(schema, "hello", 42, new BigDecimal("12.34"), arrayVal, mapVal, innerRow);

    InternalRow wrapper = new KernelRowToSparkRowWrapper(row);
    assertEquals(6, wrapper.numFields());
    assertEquals(UTF8String.fromString("hello"), wrapper.getUTF8String(0));
    assertEquals(42, wrapper.getInt(1));
    assertEquals(new BigDecimal("12.34"), wrapper.getDecimal(2, 10, 2).toJavaBigDecimal());

    ArrayData arrayData = wrapper.getArray(3);
    assertEquals(3, arrayData.numElements());
    assertEquals(1, arrayData.getInt(0));
    assertEquals(2, arrayData.getInt(1));
    assertEquals(3, arrayData.getInt(2));

    MapData mapData = wrapper.getMap(4);
    assertEquals(2, mapData.numElements());
    assertEquals(UTF8String.fromString("k1"), mapData.keyArray().getUTF8String(0));
    assertEquals(10, mapData.valueArray().getInt(0));

    InternalRow innerWrapped = wrapper.getStruct(5, 1);
    assertTrue(innerWrapped.getBoolean(0));
  }

  @Test
  public void testKernelColumnVectorToSparkInternalRowWrapper_structRow() {
    // Build a struct vector with two fields: (x int, y string)
    StructType structSchema =
        new StructType(
            Arrays.asList(
                new StructField("x", IntegerType.INTEGER, true),
                new StructField("y", StringType.STRING, true)));

    // Two rows: [{x=1,y=a}, {x=2,y=b}]
    Row r1 = makeStructRow(structSchema, 1, "a");
    Row r2 = makeStructRow(structSchema, 2, "b");
    GenericColumnVector structVec = new GenericColumnVector(Arrays.asList(r1, r2), structSchema);

    // Wrap second row (index 1)
    KernelColumnVectorToSparkInternalRowWrapper rowView =
        new KernelColumnVectorToSparkInternalRowWrapper(structVec, 1);
    assertEquals(2, rowView.numFields());
    assertEquals(2, rowView.getInt(0));
    assertEquals(UTF8String.fromString("b"), rowView.getUTF8String(1));
  }
}
