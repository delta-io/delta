package io.delta.spark.dsv2.scan.internal;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.types.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/** Present one row from a struct ColumnVector as a Spark InternalRow. Useful for nested structs. */
public class KernelColumnVectorToSparkInternalRowWrapper extends InternalRow {
  private final ColumnVector structVector;
  private final int rowIndex;
  private final io.delta.kernel.types.StructType structType;

  public KernelColumnVectorToSparkInternalRowWrapper(ColumnVector structVector, int rowIndex) {
    this.structVector = structVector;
    this.rowIndex = rowIndex;
    this.structType = (io.delta.kernel.types.StructType) structVector.getDataType();
  }

  @Override
  public int numFields() {
    return structType.length();
  }

  @Override
  public boolean isNullAt(int ordinal) {
    return structVector.getChild(ordinal).isNullAt(rowIndex);
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    throw new UnsupportedOperationException("get should not be called");
  }

  @Override
  public InternalRow copy() {
    return this;
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return structVector.getChild(ordinal).getBoolean(rowIndex);
  }

  @Override
  public byte getByte(int ordinal) {
    return structVector.getChild(ordinal).getByte(rowIndex);
  }

  @Override
  public short getShort(int ordinal) {
    return structVector.getChild(ordinal).getShort(rowIndex);
  }

  @Override
  public int getInt(int ordinal) {
    return structVector.getChild(ordinal).getInt(rowIndex);
  }

  @Override
  public long getLong(int ordinal) {
    return structVector.getChild(ordinal).getLong(rowIndex);
  }

  @Override
  public float getFloat(int ordinal) {
    return structVector.getChild(ordinal).getFloat(rowIndex);
  }

  @Override
  public double getDouble(int ordinal) {
    return structVector.getChild(ordinal).getDouble(rowIndex);
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    java.math.BigDecimal dec = structVector.getChild(ordinal).getDecimal(rowIndex);
    return dec == null ? null : Decimal.apply(dec, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    String s = structVector.getChild(ordinal).getString(rowIndex);
    return s == null ? null : UTF8String.fromString(s);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return structVector.getChild(ordinal).getBinary(rowIndex);
  }

  @Override
  public InternalRow getStruct(int ordinal, int numFields) {
    ColumnVector child = structVector.getChild(ordinal);
    if (child.isNullAt(rowIndex)) return null;
    return new KernelColumnVectorToSparkInternalRowWrapper(child, rowIndex);
  }

  @Override
  public ArrayData getArray(int ordinal) {
    ArrayValue array = structVector.getChild(ordinal).getArray(rowIndex);
    if (array == null) return null;
    ColumnVector elementsVector = array.getElements();
    int size = array.getSize();
    List<Object> out = new ArrayList<>(size);
    for (int i = 0; i < size; i++) out.add(convertNested(elementsVector, i));
    return new GenericArrayData(out.toArray());
  }

  @Override
  public MapData getMap(int ordinal) {
    MapValue map = structVector.getChild(ordinal).getMap(rowIndex);
    if (map == null) return null;
    ColumnVector keysVector = map.getKeys();
    ColumnVector valuesVector = map.getValues();
    int size = map.getSize();
    List<Object> keys = new ArrayList<>(size);
    List<Object> values = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      keys.add(convertNested(keysVector, i));
      values.add(convertNested(valuesVector, i));
    }
    return new ArrayBasedMapData(
        new GenericArrayData(keys.toArray()), new GenericArrayData(values.toArray()));
  }

  private Object convertNested(ColumnVector v, int i) {
    if (v.isNullAt(i)) return null;
    io.delta.kernel.types.DataType t = v.getDataType();
    if (t instanceof StringType) return UTF8String.fromString(v.getString(i));
    if (t instanceof BooleanType) return v.getBoolean(i);
    if (t instanceof ByteType) return v.getByte(i);
    if (t instanceof ShortType) return v.getShort(i);
    if (t instanceof IntegerType) return v.getInt(i);
    if (t instanceof LongType) return v.getLong(i);
    if (t instanceof FloatType) return v.getFloat(i);
    if (t instanceof DoubleType) return v.getDouble(i);
    if (t instanceof DateType) return v.getInt(i);
    if (t instanceof TimestampType) return v.getLong(i);
    if (t instanceof TimestampNTZType) return v.getLong(i);
    if (t instanceof DecimalType) {
      DecimalType dt = (DecimalType) t;
      java.math.BigDecimal dec = v.getDecimal(i);
      return dec == null ? null : Decimal.apply(dec, dt.getPrecision(), dt.getScale());
    }
    if (t instanceof BinaryType) return v.getBinary(i);
    if (t instanceof ArrayType) {
      ArrayValue a = v.getArray(i);
      ColumnVector ev = a.getElements();
      int size = a.getSize();
      List<Object> out = new ArrayList<>(size);
      for (int x = 0; x < size; x++) out.add(convertNested(ev, x));
      return new GenericArrayData(out.toArray());
    }
    if (t instanceof MapType) {
      MapValue m = v.getMap(i);
      ColumnVector kv = m.getKeys();
      ColumnVector vv = m.getValues();
      int size = m.getSize();
      List<Object> k = new ArrayList<>(size);
      List<Object> val = new ArrayList<>(size);
      for (int x = 0; x < size; x++) {
        k.add(convertNested(kv, x));
        val.add(convertNested(vv, x));
      }
      return new ArrayBasedMapData(
          new GenericArrayData(k.toArray()), new GenericArrayData(val.toArray()));
    }
    if (t instanceof StructType) {
      return new KernelColumnVectorToSparkInternalRowWrapper(v, i);
    }
    throw new UnsupportedOperationException("Unsupported kernel data type: " + t);
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    throw new UnsupportedOperationException(
        "getInterval is not supported in read-only rows (struct view)");
  }

  @Override
  public void setNullAt(int i) {
    throw new UnsupportedOperationException(
        "setNullAt is not supported in read-only rows (struct view)");
  }

  @Override
  public void update(int i, Object value) {
    throw new UnsupportedOperationException(
        "update is not supported in read-only rows (struct view)");
  }
}
