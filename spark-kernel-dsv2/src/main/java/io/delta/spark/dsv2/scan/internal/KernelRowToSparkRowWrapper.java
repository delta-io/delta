package io.delta.spark.dsv2.scan.internal;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
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

/** Wrapper for a Delta Kernel Row to be used as a Spark InternalRow with full type support. */
public class KernelRowToSparkRowWrapper extends InternalRow {
  private final Row row;

  public KernelRowToSparkRowWrapper(Row row) {
    this.row = row;
  }

  @Override
  public boolean anyNull() {
    for (int i = 0; i < numFields(); i++) {
      if (isNullAt(i)) return true;
    }
    return false;
  }

  @Override
  public InternalRow copy() {
    return new KernelRowToSparkRowWrapper(row);
  }

  @Override
  public int numFields() {
    return row.getSchema().length();
  }

  @Override
  public boolean isNullAt(int ordinal) {
    return row.isNullAt(ordinal);
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return row.getBoolean(ordinal);
  }

  @Override
  public byte getByte(int ordinal) {
    return row.getByte(ordinal);
  }

  @Override
  public short getShort(int ordinal) {
    return row.getShort(ordinal);
  }

  @Override
  public int getInt(int ordinal) {
    return row.getInt(ordinal);
  }

  @Override
  public long getLong(int ordinal) {
    return row.getLong(ordinal);
  }

  @Override
  public float getFloat(int ordinal) {
    return row.getFloat(ordinal);
  }

  @Override
  public double getDouble(int ordinal) {
    return row.getDouble(ordinal);
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    java.math.BigDecimal dec = row.getDecimal(ordinal);
    return dec == null ? null : Decimal.apply(dec, precision, scale);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return row.getBinary(ordinal);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    String s = row.getString(ordinal);
    return s == null ? null : UTF8String.fromString(s);
  }

  @Override
  public InternalRow getStruct(int ordinal, int numFields) {
    Row child = row.getStruct(ordinal);
    return child == null ? null : new KernelRowToSparkRowWrapper(child);
  }

  @Override
  public ArrayData getArray(int ordinal) {
    ArrayValue array = row.getArray(ordinal);
    if (array == null) return null;
    ColumnVector elementsVector = array.getElements();
    int size = array.getSize();
    List<Object> out = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      out.add(convertKernelValueToSparkValue(elementsVector, i));
    }
    return new GenericArrayData(out.toArray());
  }

  @Override
  public MapData getMap(int ordinal) {
    MapValue map = row.getMap(ordinal);
    if (map == null) return null;
    ColumnVector keysVector = map.getKeys();
    ColumnVector valuesVector = map.getValues();
    int size = map.getSize();

    List<Object> keys = new ArrayList<>(size);
    List<Object> values = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      keys.add(convertKernelValueToSparkValue(keysVector, i));
      values.add(convertKernelValueToSparkValue(valuesVector, i));
    }
    ArrayData keysArray = new GenericArrayData(keys.toArray());
    ArrayData valuesArray = new GenericArrayData(values.toArray());
    return new ArrayBasedMapData(keysArray, valuesArray);
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    throw new UnsupportedOperationException("get(ordinal, dataType) should not be called");
  }

  private Object convertKernelValueToSparkValue(ColumnVector vector, int index) {
    if (vector.isNullAt(index)) return null;

    io.delta.kernel.types.DataType t = vector.getDataType();
    if (t instanceof StringType) return UTF8String.fromString(vector.getString(index));
    if (t instanceof BooleanType) return vector.getBoolean(index);
    if (t instanceof ByteType) return vector.getByte(index);
    if (t instanceof ShortType) return vector.getShort(index);
    if (t instanceof IntegerType) return vector.getInt(index);
    if (t instanceof LongType) return vector.getLong(index);
    if (t instanceof FloatType) return vector.getFloat(index);
    if (t instanceof DoubleType) return vector.getDouble(index);
    if (t instanceof DateType) return vector.getInt(index);
    if (t instanceof TimestampType) return vector.getLong(index);
    if (t instanceof TimestampNTZType) return vector.getLong(index);
    if (t instanceof DecimalType) {
      DecimalType dt = (DecimalType) t;
      java.math.BigDecimal dec = vector.getDecimal(index);
      return dec == null ? null : Decimal.apply(dec, dt.getPrecision(), dt.getScale());
    }
    if (t instanceof BinaryType) return vector.getBinary(index);
    if (t instanceof ArrayType) {
      ArrayValue a = vector.getArray(index);
      ColumnVector ev = a.getElements();
      int size = a.getSize();
      List<Object> out = new ArrayList<>(size);
      for (int i = 0; i < size; i++) out.add(convertKernelValueToSparkValue(ev, i));
      return new GenericArrayData(out.toArray());
    }
    if (t instanceof MapType) {
      MapValue m = vector.getMap(index);
      ColumnVector kv = m.getKeys();
      ColumnVector vv = m.getValues();
      int size = m.getSize();
      List<Object> k = new ArrayList<>(size);
      List<Object> v = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        k.add(convertKernelValueToSparkValue(kv, i));
        v.add(convertKernelValueToSparkValue(vv, i));
      }
      return new ArrayBasedMapData(
          new GenericArrayData(k.toArray()), new GenericArrayData(v.toArray()));
    }
    if (t instanceof StructType) {
      // Flatten struct row view using a child-row wrapper
      return new KernelColumnVectorToSparkInternalRowWrapper(vector, index);
    }
    throw new UnsupportedOperationException("Unsupported kernel data type: " + t);
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    throw new UnsupportedOperationException(
        "getInterval is not supported for KernelRowToSparkRowWrapper");
  }

  @Override
  public void setNullAt(int i) {
    throw new UnsupportedOperationException("setNullAt is not supported in read-only rows");
  }

  @Override
  public void update(int i, Object value) {
    throw new UnsupportedOperationException("update is not supported in read-only rows");
  }
}
