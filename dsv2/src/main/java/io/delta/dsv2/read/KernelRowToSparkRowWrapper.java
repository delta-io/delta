package io.delta.dsv2.read;

import io.delta.kernel.data.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/** Wrapper for a Delta Kernel Row to be used as a Spark InternalRow. */
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
  public int getInt(int ordinal) {
    return row.getInt(ordinal);
  }

  @Override
  public long getLong(int ordinal) {
    return row.getLong(ordinal);
  }

  // This method isn't in the InternalRow interface but let's keep it for compatibility
  public String getString(int ordinal) {
    return row.getString(ordinal);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    return UTF8String.fromString(row.getString(ordinal));
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    if (dataType instanceof org.apache.spark.sql.types.BooleanType) {
      return Boolean.valueOf(getBoolean(ordinal));
    } else if (dataType instanceof org.apache.spark.sql.types.IntegerType) {
      return Integer.valueOf(getInt(ordinal));
    } else if (dataType instanceof org.apache.spark.sql.types.LongType) {
      return Long.valueOf(getLong(ordinal));
    } else if (dataType instanceof org.apache.spark.sql.types.StringType) {
      return getUTF8String(ordinal);
    } else {
      throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  /////////////////////////
  // Unsupported methods //
  /////////////////////////

  @Override
  public CalendarInterval getInterval(int ordinal) {
    throw new UnsupportedOperationException(
        "getInterval is not supported for KernelRowToSparkRowWrapper");
  }

  @Override
  public byte getByte(int ordinal) {
    throw new UnsupportedOperationException(
        "getByte is not supported for KernelRowToSparkRowWrapper");
  }

  @Override
  public short getShort(int ordinal) {
    throw new UnsupportedOperationException(
        "getShort is not supported for KernelRowToSparkRowWrapper");
  }

  @Override
  public float getFloat(int ordinal) {
    throw new UnsupportedOperationException(
        "getFloat is not supported for KernelRowToSparkRowWrapper");
  }

  @Override
  public double getDouble(int ordinal) {
    throw new UnsupportedOperationException(
        "getDouble is not supported for KernelRowToSparkRowWrapper");
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    throw new UnsupportedOperationException(
        "getDecimal is not supported for KernelRowToSparkRowWrapper");
  }

  @Override
  public byte[] getBinary(int ordinal) {
    throw new UnsupportedOperationException(
        "getBinary is not supported for KernelRowToSparkRowWrapper");
  }

  @Override
  public InternalRow getStruct(int ordinal, int numFields) {
    throw new UnsupportedOperationException(
        "getStruct is not supported for KernelRowToSparkRowWrapper");
  }

  @Override
  public ArrayData getArray(int ordinal) {
    throw new UnsupportedOperationException(
        "getArray is not supported for KernelRowToSparkRowWrapper");
  }

  @Override
  public MapData getMap(int ordinal) {
    throw new UnsupportedOperationException(
        "getMap is not supported for KernelRowToSparkRowWrapper");
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
