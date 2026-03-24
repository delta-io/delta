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
package io.delta.spark.internal.v2.read.cdc;

import javax.annotation.Nullable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;

/**
 * An InternalRow wrapper that reorders and null-coalesces CDC columns.
 *
 * <p>The delegate row from buildReaderWithPartitionValues has layout: [readDataSchema, CDC,
 * partition]. This wrapper presents it as [tableSchema, CDC] to match DSv1's output order.
 *
 * <p>Uses a pre-computed ordinal mapping from output positions to delegate positions. CDC output
 * columns (at tableColCount, +1, +2) are filled from constants or delegate as appropriate.
 */
class CDCCoalesceRow extends InternalRow {

  private final InternalRow delegate;
  private final int[] outputToInternal;
  private final int tableColCount;
  private final int changeTypeInternalIdx;
  @Nullable private final UTF8String changeTypeValue;
  private final long commitVersionValue;
  private final long commitTimestampMicros;

  CDCCoalesceRow(
      InternalRow delegate,
      int[] outputToInternal,
      int tableColCount,
      int changeTypeInternalIdx,
      @Nullable UTF8String changeTypeValue,
      long commitVersionValue,
      long commitTimestampMicros) {
    this.delegate = delegate;
    this.outputToInternal = outputToInternal;
    this.tableColCount = tableColCount;
    this.changeTypeInternalIdx = changeTypeInternalIdx;
    this.changeTypeValue = changeTypeValue;
    this.commitVersionValue = commitVersionValue;
    this.commitTimestampMicros = commitTimestampMicros;
  }

  private int map(int ordinal) {
    return outputToInternal[ordinal];
  }

  private boolean isCDCChangeType(int ordinal) {
    return ordinal == tableColCount;
  }

  private boolean isCDCCommitVersion(int ordinal) {
    return ordinal == tableColCount + 1;
  }

  private boolean isCDCCommitTimestamp(int ordinal) {
    return ordinal == tableColCount + 2;
  }

  @Override
  public int numFields() {
    return outputToInternal.length;
  }

  @Override
  public boolean isNullAt(int ordinal) {
    if (isCDCChangeType(ordinal)) {
      return changeTypeValue == null ? delegate.isNullAt(changeTypeInternalIdx) : false;
    }
    if (isCDCCommitVersion(ordinal) || isCDCCommitTimestamp(ordinal)) return false;
    return delegate.isNullAt(map(ordinal));
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    if (isCDCChangeType(ordinal)) {
      return changeTypeValue != null
          ? changeTypeValue
          : delegate.get(changeTypeInternalIdx, dataType);
    }
    if (isCDCCommitVersion(ordinal)) return commitVersionValue;
    if (isCDCCommitTimestamp(ordinal)) return commitTimestampMicros;
    return delegate.get(map(ordinal), dataType);
  }

  @Override
  public long getLong(int ordinal) {
    if (isCDCCommitVersion(ordinal)) return commitVersionValue;
    if (isCDCCommitTimestamp(ordinal)) return commitTimestampMicros;
    return delegate.getLong(map(ordinal));
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    if (isCDCChangeType(ordinal)) {
      return changeTypeValue != null
          ? changeTypeValue
          : delegate.getUTF8String(changeTypeInternalIdx);
    }
    return delegate.getUTF8String(map(ordinal));
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return delegate.getBoolean(map(ordinal));
  }

  @Override
  public byte getByte(int ordinal) {
    return delegate.getByte(map(ordinal));
  }

  @Override
  public short getShort(int ordinal) {
    return delegate.getShort(map(ordinal));
  }

  @Override
  public int getInt(int ordinal) {
    return delegate.getInt(map(ordinal));
  }

  @Override
  public float getFloat(int ordinal) {
    return delegate.getFloat(map(ordinal));
  }

  @Override
  public double getDouble(int ordinal) {
    return delegate.getDouble(map(ordinal));
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    return delegate.getDecimal(map(ordinal), precision, scale);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return delegate.getBinary(map(ordinal));
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    return delegate.getInterval(map(ordinal));
  }

  @Override
  public InternalRow getStruct(int ordinal, int numFields) {
    return delegate.getStruct(map(ordinal), numFields);
  }

  @Override
  public ArrayData getArray(int ordinal) {
    return delegate.getArray(map(ordinal));
  }

  @Override
  public MapData getMap(int ordinal) {
    return delegate.getMap(map(ordinal));
  }

  @Override
  public VariantVal getVariant(int ordinal) {
    return delegate.getVariant(map(ordinal));
  }

  @Override
  public void update(int ordinal, Object value) {
    throw new UnsupportedOperationException("CDCCoalesceRow is read-only");
  }

  @Override
  public void setNullAt(int ordinal) {
    throw new UnsupportedOperationException("CDCCoalesceRow is read-only");
  }

  @Override
  public InternalRow copy() {
    return new CDCCoalesceRow(
        delegate.copy(),
        outputToInternal,
        tableColCount,
        changeTypeInternalIdx,
        changeTypeValue,
        commitVersionValue,
        commitTimestampMicros);
  }
}
