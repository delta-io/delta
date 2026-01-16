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

package io.delta.flink.table;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.util.List;

/**
 * A wrapper to provide a ColumnVector view backed by a nested list. Usage example: <code>
 *     val dataBuffer = java.util.List.of(
 *         java.util.List.of(1, "Jack"),
 *         java.util.List.of(2, "Amy"))
 *     val cv1 = new DataColumnVectorView(dataBuffer, 0, IntegerType.INTEGER)
 *     val cv2 = new DataColumnVectorView(dataBuffer, 1, StringType.STRING)
 * </code>
 */
public class DataColumnVectorView implements ColumnVector {

  private final List<List<Object>> rows;
  private final int colIdx;
  private final DataType dataType;

  public DataColumnVectorView(List<List<Object>> rows, int colIdx, DataType dataType) {
    this.rows = rows;
    this.colIdx = colIdx;
    this.dataType = dataType;
  }

  @Override
  public DataType getDataType() {
    return this.dataType;
  }

  @Override
  public int getSize() {
    return this.rows.size();
  }

  @Override
  public void close() {}

  @Override
  public boolean isNullAt(int rowId) {
    checkValidRowId(rowId);
    return rows.get(rowId).get(colIdx) == null;
  }

  protected void checkValidRowId(int rowId) {
    if (rowId < 0 || rowId >= getSize()) {
      throw new IllegalArgumentException("RowId out of range: " + rowId + " <-> " + getSize());
    }
  }

  protected void checkValidDataType(io.delta.kernel.types.DataType dataType) {
    if (!this.getDataType().equivalent(dataType)) {
      throw new UnsupportedOperationException("Invalid value request for data type");
    }
  }

  @Override
  public int getInt(int rowId) {
    checkValidRowId(rowId);
    checkValidDataType(IntegerType.INTEGER);
    return (Integer) rows.get(rowId).get(colIdx);
  }

  @Override
  public long getLong(int rowId) {
    checkValidRowId(rowId);
    checkValidDataType(LongType.LONG);
    return (Long) rows.get(rowId).get(colIdx);
  }

  @Override
  public String getString(int rowId) {
    checkValidRowId(rowId);
    checkValidDataType(StringType.STRING);
    return rows.get(rowId).get(colIdx).toString();
  }

  @Override
  public float getFloat(int rowId) {
    checkValidRowId(rowId);
    checkValidDataType(FloatType.FLOAT);
    return (Float) rows.get(rowId).get(colIdx);
  }

  @Override
  public double getDouble(int rowId) {
    checkValidRowId(rowId);
    checkValidDataType(DoubleType.DOUBLE);
    return (Double) rows.get(rowId).get(colIdx);
  }

  @Override
  public BigDecimal getDecimal(int rowId) {
    checkValidRowId(rowId);
    // Do not check precision and scale here because RowData support conversion
    if (!(this.getDataType() instanceof DecimalType)) {
      throw new UnsupportedOperationException("Invalid value request for data type");
    }
    DecimalType actualType = (DecimalType) dataType;
    return (BigDecimal) rows.get(rowId).get(colIdx);
  }
}
