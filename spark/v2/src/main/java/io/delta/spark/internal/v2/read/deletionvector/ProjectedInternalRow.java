/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.read.deletionvector;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;

/**
 * An InternalRow implementation that projects out a column without copying data.
 *
 * <p>This class wraps an existing row and remaps column indices to skip the excluded column. Each
 * method that retrieves data translates the provided column index, effectively filtering out the
 * excluded column. This approach allows efficient column projection without data copying.
 *
 * <p>Inspired by Iceberg's ColumnVectorWithFilter pattern.
 */
public class ProjectedInternalRow extends InternalRow {
  private final InternalRow delegate;
  private final int excludedColumnIndex;
  private final int outputColumnCount;

  public ProjectedInternalRow(InternalRow delegate, int excludedColumnIndex) {
    this.delegate = delegate;
    this.excludedColumnIndex = excludedColumnIndex;
    this.outputColumnCount = delegate.numFields() - 1;
  }

  /** Maps output column index to delegate column index, skipping the excluded column. */
  private int mapIndex(int ordinal) {
    return ordinal < excludedColumnIndex ? ordinal : ordinal + 1;
  }

  @Override
  public int numFields() {
    return outputColumnCount;
  }

  @Override
  public void setNullAt(int ordinal) {
    delegate.setNullAt(mapIndex(ordinal));
  }

  @Override
  public void update(int ordinal, Object value) {
    delegate.update(mapIndex(ordinal), value);
  }

  @Override
  public InternalRow copy() {
    return new ProjectedInternalRow(delegate.copy(), excludedColumnIndex);
  }

  @Override
  public boolean isNullAt(int ordinal) {
    return delegate.isNullAt(mapIndex(ordinal));
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return delegate.getBoolean(mapIndex(ordinal));
  }

  @Override
  public byte getByte(int ordinal) {
    return delegate.getByte(mapIndex(ordinal));
  }

  @Override
  public short getShort(int ordinal) {
    return delegate.getShort(mapIndex(ordinal));
  }

  @Override
  public int getInt(int ordinal) {
    return delegate.getInt(mapIndex(ordinal));
  }

  @Override
  public long getLong(int ordinal) {
    return delegate.getLong(mapIndex(ordinal));
  }

  @Override
  public float getFloat(int ordinal) {
    return delegate.getFloat(mapIndex(ordinal));
  }

  @Override
  public double getDouble(int ordinal) {
    return delegate.getDouble(mapIndex(ordinal));
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    return delegate.getDecimal(mapIndex(ordinal), precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    return delegate.getUTF8String(mapIndex(ordinal));
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return delegate.getBinary(mapIndex(ordinal));
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    return delegate.getInterval(mapIndex(ordinal));
  }

  @Override
  public VariantVal getVariant(int ordinal) {
    return delegate.getVariant(mapIndex(ordinal));
  }

  @Override
  public InternalRow getStruct(int ordinal, int numFields) {
    return delegate.getStruct(mapIndex(ordinal), numFields);
  }

  @Override
  public ArrayData getArray(int ordinal) {
    return delegate.getArray(mapIndex(ordinal));
  }

  @Override
  public MapData getMap(int ordinal) {
    return delegate.getMap(mapIndex(ordinal));
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    return delegate.get(mapIndex(ordinal), dataType);
  }
}
