/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.internal.data;

import java.math.BigDecimal;
import java.util.Optional;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;
import io.delta.kernel.defaults.internal.data.vector.*;


public class ColumnVectorConverter {
    public static ColumnVector convertToOutputType(ColumnVector c, DataType outputType) {
        DataType inputType = c.getDataType();
        if (inputType == outputType) {
            return c;
        }
        if (outputType instanceof BooleanType) {
            // TODO: how to convert string to boolean?
            // "true" "TRUE", "T" -> true ?
            throw new IllegalStateException("convert to boolean type is not yet supported");
        } else if (outputType instanceof ShortType) {
            return convertToShort(c);
        }  else if (outputType instanceof IntegerType) {
            return convertToInteger(c);
        } else if (outputType instanceof LongType) {
            return convertToLong(c);
        } else if (outputType instanceof DecimalType) {
            return convertToDecimal(c);
        } else if (outputType instanceof FloatType) {
            return convertToFloat(c);
        } else if (outputType instanceof DoubleType) {
            return convertToDouble(c);
        } else {
            String msg = String.format(
                    "`%s` is not supported to convert to data type %s`",
                    inputType, outputType);
            throw new IllegalStateException(msg);
        }
    }

    protected static ColumnVector convertToShort(ColumnVector c) {
        short[] values = new short[c.getSize()];
        boolean[] isNull = new boolean[c.getSize()];
        DataType d = c.getDataType();
        for (int i = 0; i < c.getSize(); i++) {
            if (c.isNullAt(i)) {
                isNull[i] = true;
            } else {
                if (d instanceof ByteType) {
                    values[i] = c.getByte(i);
                } else {
                    String msg = String.format(
                            "`%s` cannot convert to short`",
                            d);
                    throw new IllegalStateException(msg);
                }
            }
        }
        return new DefaultShortVector(c.getSize(), Optional.of(isNull), values);
    }
    protected static ColumnVector convertToInteger(ColumnVector c) {
        int[] values = new int[c.getSize()];
        boolean[] isNull = new boolean[c.getSize()];
        DataType d = c.getDataType();
        for (int i = 0; i < c.getSize(); i++) {
            if (c.isNullAt(i)) {
                isNull[i] = true;
            } else {
                if (d instanceof ByteType) {
                    values[i] = c.getByte(i);
                } else if (d instanceof ShortType) {
                    values[i] = c.getShort(i);
                } else {
                    String msg = String.format(
                            "`%s` cannot convert to int`",
                            d);
                    throw new IllegalStateException(msg);
                }
            }
        }
        return new DefaultIntVector(
                IntegerType.INTEGER, c.getSize(), Optional.of(isNull), values);
    }
    protected static ColumnVector convertToLong(ColumnVector c) {
        long[] values = new long[c.getSize()];
        boolean[] isNull = new boolean[c.getSize()];
        DataType d = c.getDataType();
        for (int i = 0; i < c.getSize(); i++) {
            if (c.isNullAt(i)) {
                isNull[i] = true;
            } else {
                if (d instanceof ByteType) {
                    values[i] = c.getByte(i);
                } else if (d instanceof ShortType) {
                    values[i] = c.getShort(i);
                } else if (d instanceof IntegerType) {
                    values[i] = c.getInt(i);
                } else if (d instanceof StringType) {
                    values[i] =  Long.parseLong(c.getString(i));
                } else {
                    String msg = String.format(
                            "`%s` cannot convert to long`",
                            d);
                    throw new IllegalStateException(msg);
                }
            }
        }
        return new DefaultLongVector(
                LongType.LONG, c.getSize(), Optional.of(isNull), values);
    }
    protected static ColumnVector convertToDecimal(ColumnVector c) {
        BigDecimal[] values = new BigDecimal[c.getSize()];
        boolean[] isNull = new boolean[c.getSize()];
        DataType d = c.getDataType();
        for (int i = 0; i < c.getSize(); i++) {
            if (d instanceof ByteType) {
                values[i] = new BigDecimal(c.getByte(i));
            } else if (d instanceof ShortType) {
                values[i] = new BigDecimal(c.getShort(i));
            } else if (d instanceof IntegerType) {
                values[i] = new BigDecimal(c.getInt(i));
            } else if (d instanceof LongType) {
                values[i] = new BigDecimal(c.getLong(i));
            } else {
                String msg = String.format(
                        "`%s` cannot convert to big decimal`",
                        d);
                throw new IllegalStateException(msg);
            }
        }
        return new DefaultDecimalVector(
                DecimalType.USER_DEFAULT,c.getSize(), values);
    }

    protected static ColumnVector convertToFloat(ColumnVector c) {
        float[] values = new float[c.getSize()];
        boolean[] isNull = new boolean[c.getSize()];
        DataType d = c.getDataType();
        for (int i = 0; i < c.getSize(); i++) {
            if (c.isNullAt(i)) {
                isNull[i] = true;
            } else {
                if (d instanceof ByteType) {
                    values[i] = c.getByte(i);
                } else if (d instanceof ShortType) {
                    values[i] = c.getShort(i);
                } else if (d instanceof IntegerType) {
                    values[i] = c.getInt(i);
                } else if (d instanceof LongType) {
                    values[i] = c.getLong(i);
                } else if (d instanceof DecimalType) {
                    values[i] = c.getDecimal(i).floatValue();
                } else {
                    String msg = String.format(
                            "`%s` cannot convert to float`",
                            d);
                    throw new IllegalStateException(msg);
                }
            }
        }
        return new DefaultFloatVector(c.getSize(), Optional.of(isNull), values);
    }

    protected static ColumnVector convertToDouble(ColumnVector c) {
        double[] values = new double[c.getSize()];
        boolean[] isNull = new boolean[c.getSize()];
        DataType d = c.getDataType();
        for (int i = 0; i < c.getSize(); i++) {
            if (c.isNullAt(i)) {
                isNull[i] = true;
            } else {
                if (d instanceof ByteType) {
                    values[i] = c.getByte(i);
                } else if (d instanceof ShortType) {
                    values[i] = c.getShort(i);
                } else if (d instanceof IntegerType) {
                    values[i] = c.getInt(i);
                } else if (d instanceof LongType) {
                    values[i] = c.getLong(i);
                } else if (d instanceof DecimalType) {
                    values[i] = c.getDecimal(i).doubleValue();
                } else if (d instanceof FloatType) {
                    values[i] = c.getFloat(i);
                } else if (d instanceof StringType) {
                    values[i] =  Double.parseDouble(c.getString(i));
                } else {
                    String msg = String.format(
                            "`%s` cannot convert to double`",
                            d);
                    throw new IllegalStateException(msg);
                }
            }
        }
        return new DefaultDoubleVector(c.getSize(), Optional.of(isNull), values);
    }
}
