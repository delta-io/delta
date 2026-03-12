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
package io.delta.spark.internal.v2.write;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;
import java.math.BigDecimal;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;

/**
 * Adapts a Spark {@link InternalRow} to the Kernel {@link Row} interface so that Spark write data
 * can be passed to Kernel's {@link io.delta.kernel.Transaction#transformLogicalData}. Uses the
 * Spark schema (converted to Kernel schema) for type handling.
 */
public class InternalRowToKernelRowAdapter implements Row {

  private final InternalRow internalRow;
  private final org.apache.spark.sql.types.StructType sparkSchema;
  private final StructType kernelSchema;

  public InternalRowToKernelRowAdapter(
      InternalRow internalRow,
      org.apache.spark.sql.types.StructType sparkSchema,
      StructType kernelSchema) {
    this.internalRow = internalRow;
    this.sparkSchema = sparkSchema;
    this.kernelSchema = kernelSchema;
  }

  @Override
  public StructType getSchema() {
    return kernelSchema;
  }

  @Override
  public boolean isNullAt(int ordinal) {
    return internalRow.isNullAt(ordinal);
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return internalRow.getBoolean(ordinal);
  }

  @Override
  public byte getByte(int ordinal) {
    return internalRow.getByte(ordinal);
  }

  @Override
  public short getShort(int ordinal) {
    return internalRow.getShort(ordinal);
  }

  @Override
  public int getInt(int ordinal) {
    return internalRow.getInt(ordinal);
  }

  @Override
  public long getLong(int ordinal) {
    return internalRow.getLong(ordinal);
  }

  @Override
  public float getFloat(int ordinal) {
    return internalRow.getFloat(ordinal);
  }

  @Override
  public double getDouble(int ordinal) {
    return internalRow.getDouble(ordinal);
  }

  @Override
  public String getString(int ordinal) {
    if (internalRow.isNullAt(ordinal)) {
      throw new NullPointerException("Null at ordinal " + ordinal);
    }
    return internalRow.getString(ordinal);
  }

  @Override
  public BigDecimal getDecimal(int ordinal) {
    if (internalRow.isNullAt(ordinal)) {
      throw new NullPointerException("Null at ordinal " + ordinal);
    }
    org.apache.spark.sql.types.DecimalType dt =
        (org.apache.spark.sql.types.DecimalType) sparkSchema.fields()[ordinal].dataType();
    Decimal sparkDecimal = internalRow.getDecimal(ordinal, dt.precision(), dt.scale());
    return sparkDecimal != null ? sparkDecimal.toJavaBigDecimal() : null;
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return internalRow.getBinary(ordinal);
  }

  @Override
  public Row getStruct(int ordinal) {
    if (internalRow.isNullAt(ordinal)) {
      throw new NullPointerException("Null at ordinal " + ordinal);
    }
    InternalRow nested =
        internalRow.getStruct(ordinal, getNumFields(sparkSchema.fields()[ordinal].dataType()));
    org.apache.spark.sql.types.StructType nestedSparkSchema =
        (org.apache.spark.sql.types.StructType) sparkSchema.fields()[ordinal].dataType();
    io.delta.kernel.types.StructType nestedKernelSchema =
        (io.delta.kernel.types.StructType) kernelSchema.at(ordinal).getDataType();
    return new InternalRowToKernelRowAdapter(nested, nestedSparkSchema, nestedKernelSchema);
  }

  @Override
  public ArrayValue getArray(int ordinal) {
    throw new UnsupportedOperationException("Array type in write path not yet supported");
  }

  @Override
  public MapValue getMap(int ordinal) {
    throw new UnsupportedOperationException("Map type in write path not yet supported");
  }

  private static int getNumFields(DataType dataType) {
    if (dataType instanceof org.apache.spark.sql.types.StructType) {
      return ((org.apache.spark.sql.types.StructType) dataType).fields().length;
    }
    return 0;
  }
}
