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
package io.delta.kernel.defaults.internal.expressions;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.defaults.internal.DefaultKernelUtils;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.IntStream;

/**
 * An implicit cast expression to convert the input type to another given type. Here is the valid
 * list of casts
 *
 * <p>
 *
 * <ul>
 *   <li>{@code byte} to {@code short, int, long, float, double}
 *   <li>{@code short} to {@code int, long, float, double}
 *   <li>{@code int} to {@code long, float, double}
 *   <li>{@code long} to {@code float, double}
 *   <li>{@code float} to {@code double}
 *   <li>{@code date} to {@code timestamp_ntz}
 * </ul>
 *
 * <p>The above list is not exhaustive. Based on the need, we can add more casts.
 *
 * <p>Numeric values ({@code short, int, long, float, double, decimal}) can also be cast to {@code
 * decimal} as long as the target type has a precision and scale large enough to hold all values
 * from the source type.
 */
class CastExpressionEvaluator {

  /**
   * Evaluate the given column expression on the input {@link ColumnVector}.
   *
   * @param input {@link ColumnVector} data of the input to the cast expression.
   * @return {@link ColumnVector} result applying target type casting on every element in the input
   *     {@link ColumnVector}.
   */
  static ColumnVector eval(ColumnVector input, DataType outputType) {
    DataType inputType = input.getDataType();

    if (inputType instanceof ByteType) {
      return new ByteUpConverter(outputType, input);
    } else if (inputType instanceof ShortType) {
      return new ShortUpConverter(outputType, input);
    } else if (inputType instanceof IntegerType) {
      return new IntUpConverter(outputType, input);
    } else if (inputType instanceof LongType) {
      return new LongUpConverter(outputType, input);
    } else if (inputType instanceof FloatType) {
      return new FloatUpConverter(outputType, input);
    } else if (inputType instanceof DecimalType) {
      return new DecimalUpConverter(outputType, input);
    } else if (inputType instanceof DateType) {
      return new DateUpConverter(outputType, input);
    } else if (inputType instanceof StructType) {
      return new StructUpConverter(outputType, input);
    } else if (inputType instanceof ArrayType) {
      return new ArrayUpConverter(outputType, input);
    } else if (inputType instanceof MapType) {
      return new MapUpConverter(outputType, input);
    } else {
      throw new UnsupportedOperationException(format("Cast from %s is not supported", inputType));
    }
  }

  /**
   * Map containing the valid target types for each primitive types. Decimal is handled separately.
   */
  private static final Map<String, List<String>> UP_CASTABLE_PRIMITIVE_TYPE_TABLE =
      unmodifiableMap(
          new HashMap<String, List<String>>() {
            {
              this.put("byte", Arrays.asList("short", "integer", "long", "float", "double"));
              this.put("short", Arrays.asList("integer", "long", "float", "double"));
              this.put("integer", Arrays.asList("long", "float", "double"));
              this.put("long", Arrays.asList("float", "double"));
              this.put("float", Arrays.asList("double"));
              this.put("date", Arrays.asList("timestamp_ntz"));
            }
          });

  /**
   * Utility methods which return whether the given {@code from} type can be cast to {@code to}
   * type.
   */
  static boolean canCastTo(DataType from, DataType to) {
    if (to instanceof DecimalType) {
      return canCastToDecimal(from, (DecimalType) to);
    } else if (to instanceof StructType || to instanceof MapType || to instanceof ArrayType) {
      return canCastToNestedType(from, to);
    }

    if (!(from instanceof BasePrimitiveType) || !(to instanceof BasePrimitiveType)) return false;

    // TODO: The type name should be a first class method on `DataType` instead of getting it
    // using the `toString`.
    String fromStr = from.toString();
    String toStr = to.toString();
    return from.equals(to)
        || (UP_CASTABLE_PRIMITIVE_TYPE_TABLE.containsKey(fromStr)
            && UP_CASTABLE_PRIMITIVE_TYPE_TABLE.get(fromStr).contains(toStr));
  }

  private static boolean canCastToDecimal(DataType from, DecimalType to) {
    DecimalType fromDecimalType;
    if (from instanceof DecimalType) {
      fromDecimalType = (DecimalType) from;
    } else if (from instanceof ByteType) {
      fromDecimalType = DecimalType.BYTE_DECIMAL;
    } else if (from instanceof ShortType) {
      fromDecimalType = DecimalType.SHORT_DECIMAL;
    } else if (from instanceof IntegerType) {
      fromDecimalType = DecimalType.INT_DECIMAL;
    } else if (from instanceof LongType) {
      fromDecimalType = DecimalType.LONG_DECIMAL;
    } else {
      return false;
    }

    // We allow upcasting to a larger scale as long as the precision increases by at least as much
    // to accommodate all values.
    int precisionIncrease = to.getPrecision() - fromDecimalType.getPrecision();
    int scaleIncrease = to.getScale() - fromDecimalType.getScale();
    return scaleIncrease >= 0 && precisionIncrease >= scaleIncrease;
  }

  private static boolean canCastToNestedType(DataType from, DataType to) {
    if (from instanceof StructType && to instanceof StructType) {
      StructType fromStruct = (StructType) from;
      StructType toStruct = (StructType) to;
      return fromStruct.length() == toStruct.length()
          && IntStream.range(0, fromStruct.length())
              .mapToObj(
                  i -> canCastTo(fromStruct.at(i).getDataType(), toStruct.at(i).getDataType()))
              .allMatch(result -> result);
    } else if (from instanceof MapType && to instanceof MapType) {
      MapType fromMap = (MapType) from;
      MapType toMap = (MapType) to;
      return canCastTo(fromMap.getKeyType(), toMap.getKeyType())
          && canCastTo(fromMap.getValueType(), toMap.getValueType());
    } else if (from instanceof ArrayType && to instanceof ArrayType) {
      ArrayType fromArray = (ArrayType) from;
      ArrayType toArray = (ArrayType) to;
      return canCastTo(fromArray.getElementType(), toArray.getElementType());
    }
    return false;
  }

  /** Base class for up casting {@link ColumnVector} data. */
  private abstract static class UpConverter implements ColumnVector {
    protected final DataType targetType;
    protected final ColumnVector inputVector;

    UpConverter(DataType targetType, ColumnVector inputVector) {
      this.targetType = targetType;
      this.inputVector = inputVector;
    }

    @Override
    public DataType getDataType() {
      return targetType;
    }

    @Override
    public boolean isNullAt(int rowId) {
      return inputVector.isNullAt(rowId);
    }

    @Override
    public int getSize() {
      return inputVector.getSize();
    }

    @Override
    public void close() {
      inputVector.close();
    }

    protected void checkTargetType(Class<?> expectedType) {
      checkArgument(
          expectedType.isInstance(targetType),
          "Invalid value request for data type",
          targetType.toString());
    }
  }

  private static class ByteUpConverter extends UpConverter {
    ByteUpConverter(DataType targetType, ColumnVector inputVector) {
      super(targetType, inputVector);
    }

    @Override
    public short getShort(int rowId) {
      return inputVector.getByte(rowId);
    }

    @Override
    public int getInt(int rowId) {
      return inputVector.getByte(rowId);
    }

    @Override
    public long getLong(int rowId) {
      return inputVector.getByte(rowId);
    }

    @Override
    public float getFloat(int rowId) {
      return inputVector.getByte(rowId);
    }

    @Override
    public double getDouble(int rowId) {
      return inputVector.getByte(rowId);
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
      checkTargetType(DecimalType.class);
      DecimalType decimalType = (DecimalType) targetType;
      BigDecimal decimal = new BigDecimal(inputVector.getByte(rowId));
      return decimal.setScale(decimalType.getScale(), RoundingMode.UNNECESSARY);
    }
  }

  private static class ShortUpConverter extends UpConverter {
    ShortUpConverter(DataType targetType, ColumnVector inputVector) {
      super(targetType, inputVector);
    }

    @Override
    public int getInt(int rowId) {
      return inputVector.getShort(rowId);
    }

    @Override
    public long getLong(int rowId) {
      return inputVector.getShort(rowId);
    }

    @Override
    public float getFloat(int rowId) {
      return inputVector.getShort(rowId);
    }

    @Override
    public double getDouble(int rowId) {
      return inputVector.getShort(rowId);
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
      checkTargetType(DecimalType.class);
      DecimalType decimalType = (DecimalType) targetType;
      BigDecimal decimal = new BigDecimal(inputVector.getShort(rowId));
      return decimal.setScale(decimalType.getScale(), RoundingMode.UNNECESSARY);
    }
  }

  private static class IntUpConverter extends UpConverter {
    IntUpConverter(DataType targetType, ColumnVector inputVector) {
      super(targetType, inputVector);
    }

    @Override
    public long getLong(int rowId) {
      return inputVector.getInt(rowId);
    }

    @Override
    public float getFloat(int rowId) {
      return inputVector.getInt(rowId);
    }

    @Override
    public double getDouble(int rowId) {
      return inputVector.getInt(rowId);
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
      checkTargetType(DecimalType.class);
      DecimalType decimalType = (DecimalType) targetType;
      BigDecimal decimal = new BigDecimal(inputVector.getInt(rowId));
      return decimal.setScale(decimalType.getScale(), RoundingMode.UNNECESSARY);
    }
  }

  private static class LongUpConverter extends UpConverter {
    LongUpConverter(DataType targetType, ColumnVector inputVector) {
      super(targetType, inputVector);
    }

    @Override
    public float getFloat(int rowId) {
      return inputVector.getLong(rowId);
    }

    @Override
    public double getDouble(int rowId) {
      return inputVector.getLong(rowId);
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
      checkTargetType(DecimalType.class);
      DecimalType decimalType = (DecimalType) targetType;
      BigDecimal decimal = new BigDecimal(inputVector.getLong(rowId));
      return decimal.setScale(decimalType.getScale(), RoundingMode.UNNECESSARY);
    }
  }

  private static class FloatUpConverter extends UpConverter {
    FloatUpConverter(DataType targetType, ColumnVector inputVector) {
      super(targetType, inputVector);
    }

    @Override
    public double getDouble(int rowId) {
      return inputVector.getFloat(rowId);
    }
  }

  private static class DecimalUpConverter extends UpConverter {
    DecimalUpConverter(DataType targetType, ColumnVector inputVector) {
      super(targetType, inputVector);
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
      checkTargetType(DecimalType.class);
      DecimalType decimalType = (DecimalType) targetType;
      return inputVector
          .getDecimal(rowId)
          .setScale(decimalType.getScale(), RoundingMode.UNNECESSARY);
    }
  }

  private static class DateUpConverter extends UpConverter {
    DateUpConverter(DataType targetType, ColumnVector inputVector) {
      super(targetType, inputVector);
    }

    @Override
    public long getLong(int rowId) {
      int days = inputVector.getInt(rowId);
      return DefaultKernelUtils.daysToMicros(days, ZoneOffset.UTC);
    }
  }

  private static class StructUpConverter extends UpConverter {
    private final StructType structType;

    StructUpConverter(DataType targetType, ColumnVector inputVector) {
      super(targetType, inputVector);
      checkTargetType(StructType.class);
      this.structType = (StructType) targetType;
    }

    @Override
    public ColumnVector getChild(int rowId) {
      return CastExpressionEvaluator.eval(
          inputVector.getChild(rowId), structType.at(rowId).getDataType());
    }
  }

  private static class MapUpConverter extends UpConverter {
    private final DataType keyType;
    private final DataType valueType;

    MapUpConverter(DataType targetType, ColumnVector inputVector) {
      super(targetType, inputVector);
      checkTargetType(MapType.class);
      this.keyType = ((MapType) targetType).getKeyType();
      this.valueType = ((MapType) targetType).getValueType();
    }

    @Override
    public MapValue getMap(int rowId) {
      MapValue childValue = inputVector.getMap(rowId);
      return new MapValue() {
        @Override
        public int getSize() {
          return childValue.getSize();
        }

        @Override
        public ColumnVector getKeys() {
          return CastExpressionEvaluator.eval(childValue.getKeys(), keyType);
        }

        @Override
        public ColumnVector getValues() {
          return CastExpressionEvaluator.eval(childValue.getValues(), valueType);
        }
      };
    }
  }

  private static class ArrayUpConverter extends UpConverter {
    private final DataType elementType;

    ArrayUpConverter(DataType targetType, ColumnVector inputVector) {
      super(targetType, inputVector);
      checkTargetType(ArrayType.class);
      this.elementType = ((ArrayType) targetType).getElementType();
    }

    @Override
    public ArrayValue getArray(int rowId) {
      ArrayValue childValue = inputVector.getArray(rowId);
      return new ArrayValue() {
        @Override
        public int getSize() {
          return childValue.getSize();
        }

        @Override
        public ColumnVector getElements() {
          return CastExpressionEvaluator.eval(childValue.getElements(), elementType);
        }
      };
    }
  }
}
