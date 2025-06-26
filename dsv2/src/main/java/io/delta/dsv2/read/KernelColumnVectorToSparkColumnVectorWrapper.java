package io.delta.dsv2.read;

import io.delta.dsv2.utils.SchemaUtils;
import io.delta.kernel.data.ColumnVector;
import java.util.Optional;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KernelColumnVectorToSparkColumnVectorWrapper
    extends org.apache.spark.sql.vectorized.ColumnVector {

  private static final Logger logger =
      LoggerFactory.getLogger(KernelColumnVectorToSparkColumnVectorWrapper.class);

  private final ColumnVector kernelColumnVector;
  private final Optional<int[]> desiredToActualRowIdMapping;

  public KernelColumnVectorToSparkColumnVectorWrapper(
      ColumnVector kernelColumnVector, Optional<int[]> desiredToActualRowIdMapping) {
    super(SchemaUtils.convertKernelDataTypeToSparkDataType(kernelColumnVector.getDataType()));
    this.kernelColumnVector = kernelColumnVector;
    this.desiredToActualRowIdMapping = desiredToActualRowIdMapping;
  }

  private int getActualRowId(int desiredRowId) {
    if (desiredToActualRowIdMapping.isPresent()) {
      int actualRowId = desiredToActualRowIdMapping.get()[desiredRowId];
      logger.info("desiredRowId: " + desiredRowId + " --> actualRowId " + actualRowId);
      return actualRowId;
    } else {
      return desiredRowId;
    }
  }

  @Override
  public void close() {
    kernelColumnVector.close();
  }

  @Override
  public boolean hasNull() {
    return true;
  }

  @Override
  public int numNulls() {
    return 0;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return kernelColumnVector.isNullAt(getActualRowId(rowId));
  }

  @Override
  public boolean getBoolean(int rowId) {
    return kernelColumnVector.getBoolean(getActualRowId(rowId));
  }

  @Override
  public byte getByte(int rowId) {
    return kernelColumnVector.getByte(getActualRowId(rowId));
  }

  @Override
  public short getShort(int rowId) {
    return kernelColumnVector.getShort(getActualRowId(rowId));
  }

  @Override
  public int getInt(int rowId) {
    return kernelColumnVector.getInt(getActualRowId(rowId));
  }

  @Override
  public long getLong(int rowId) {
    return kernelColumnVector.getLong(getActualRowId(rowId));
  }

  @Override
  public float getFloat(int rowId) {
    return kernelColumnVector.getFloat(getActualRowId(rowId));
  }

  @Override
  public double getDouble(int rowId) {
    return kernelColumnVector.getDouble(getActualRowId(rowId));
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return UTF8String.fromString(kernelColumnVector.getString(getActualRowId(rowId)));
  }

  @Override
  public byte[] getBinary(int rowId) {
    return kernelColumnVector.getBinary(getActualRowId(rowId));
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException("getArray is not supported");
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    throw new UnsupportedOperationException("getMap is not supported");
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw new UnsupportedOperationException("getDecimal is not supported");
  }

  @Override
  public org.apache.spark.sql.vectorized.ColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException("getChild is not supported");
  }
}
