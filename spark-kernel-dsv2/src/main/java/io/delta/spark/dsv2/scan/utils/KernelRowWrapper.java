package io.delta.spark.dsv2.scan.utils;

import io.delta.kernel.data.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/** Wrapper for Delta Kernel Row to Spark InternalRow. */
public class KernelRowWrapper extends InternalRow {
  private final Row row;

  public KernelRowWrapper(Row row) {
    this.row = row;
  }

  @Override
  public boolean anyNull() {
    for (int i = 0; i < numFields(); i++) {
      if (isNullAt(i)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public InternalRow copy() {
    return new KernelRowWrapper(row);
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

  // Nested types unsupported for now (TODO support arrays, maps, structs)
  @Override
  public InternalRow getStruct(int ordinal, int numFields) {
    throw new UnsupportedOperationException("Struct type is not supported");
  }

  @Override
  public ArrayData getArray(int ordinal) {
    throw new UnsupportedOperationException("Array type is not supported");
  }

  @Override
  public MapData getMap(int ordinal) {
    throw new UnsupportedOperationException("Map type is not supported");
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    if (dataType instanceof BooleanType) {
      return getBoolean(ordinal);
    } else if (dataType instanceof ByteType) {
      return getByte(ordinal);
    } else if (dataType instanceof ShortType) {
      return getShort(ordinal);
    } else if (dataType instanceof IntegerType) {
      return getInt(ordinal);
    } else if (dataType instanceof LongType) {
      return getLong(ordinal);
    } else if (dataType instanceof FloatType) {
      return getFloat(ordinal);
    } else if (dataType instanceof DoubleType) {
      return getDouble(ordinal);
    } else if (dataType instanceof DateType) {
      return getInt(ordinal);
    } else if (dataType instanceof TimestampType) {
      return getLong(ordinal);
    } else if (dataType instanceof TimestampNTZType) {
      return getLong(ordinal);
    } else if (dataType instanceof DecimalType) {
      DecimalType dt = (DecimalType) dataType;
      return getDecimal(ordinal, dt.precision(), dt.scale());
    } else if (dataType instanceof BinaryType) {
      return getBinary(ordinal);
    } else if (dataType instanceof StringType) {
      return getUTF8String(ordinal);
    }
    throw new UnsupportedOperationException("get(ordinal, dataType) not supported for " + dataType);
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    throw new UnsupportedOperationException("interval type is not supported");
  }

  @Override
  public void setNullAt(int i) {
    throw new UnsupportedOperationException("KernelRowWrapper is read-only");
  }

  @Override
  public void update(int i, Object value) {
    throw new UnsupportedOperationException("KernelRowWrapper is read-only");
  }
}
