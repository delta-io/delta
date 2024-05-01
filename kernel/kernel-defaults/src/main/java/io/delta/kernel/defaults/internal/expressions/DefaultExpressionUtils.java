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

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.defaults.internal.data.ValueComparator;
import io.delta.kernel.defaults.internal.data.vector.*;
import io.delta.kernel.types.*;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.expressions.Expression;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

/**
 * Utility methods used by the default expression evaluator.
 */
class DefaultExpressionUtils {

    private DefaultExpressionUtils() {}

    /**
     * Utility method that calculates the nullability result from given two vectors. Result is
     * null if at least one side is a null.
     */
    static boolean[] evalNullability(ColumnVector left, ColumnVector right) {
        int numRows = left.getSize();
        boolean[] nullability = new boolean[numRows];
        for (int rowId = 0; rowId < numRows; rowId++) {
            nullability[rowId] = left.isNullAt(rowId) || right.isNullAt(rowId);
        }
        return nullability;
    }

    /**
     * Wraps a child vector as a boolean {@link ColumnVector} with the given value and nullability
     * accessors.
     */
    static ColumnVector booleanWrapperVector(
        ColumnVector childVector,
        Function<Integer, Boolean> valueAccessor,
        Function<Integer, Boolean> nullabilityAccessor) {

        return new ColumnVector() {

            @Override
            public DataType getDataType() {
                return BooleanType.BOOLEAN;
            }

            @Override
            public int getSize() {
                return childVector.getSize();
            }

            @Override
            public void close() {
                childVector.close();
            }

            @Override
            public boolean isNullAt(int rowId) {
                return nullabilityAccessor.apply(rowId);
            }

            @Override
            public boolean getBoolean(int rowId) {
                return valueAccessor.apply(rowId);
            }
        };
    }

    /**
     * Utility method to create a column vector that lazily evaluate the
     * comparator ex. vectorComparator (ie. ==, >=, <=......) for left and right
     * column vector according to the natural ordering of numbers
     * <p>
     * Only primitive data types are supported.
     */
    static ColumnVector comparatorVector(
            ColumnVector left,
            ColumnVector right,
            VectorComparator vectorComparator) {
        checkArgument(
                left.getSize() == right.getSize(),
                "Left and right operand have different vector sizes.");

        DataType dataType = left.getDataType();
        ValueComparator valueComparator;
        if (dataType instanceof BooleanType) {
            valueComparator = new BooleanValueComparator(left, right, vectorComparator);
        } else if (dataType instanceof ByteType) {
            valueComparator = new ByteValueComparator(left, right, vectorComparator);
        } else if (dataType instanceof ShortType) {
            valueComparator = new ShortValueComparator(left, right, vectorComparator);
        } else if (dataType instanceof IntegerType || dataType instanceof DateType) {
            valueComparator = new IntegerValueComparator(left, right, vectorComparator);
        } else if (dataType instanceof LongType ||
                dataType instanceof TimestampType ||
                dataType instanceof TimestampNTZType) {
            valueComparator = new LongValueComparator(left, right, vectorComparator);
        } else if (dataType instanceof FloatType) {
            valueComparator = new FloatValueComparator(left, right, vectorComparator);
        } else if (dataType instanceof DoubleType) {
            valueComparator = new DoubleValueComparator(left, right, vectorComparator);
        } else if (dataType instanceof DecimalType) {
            valueComparator = new DecimalValueComparator(left, right, vectorComparator);
        } else if (dataType instanceof StringType) {
            valueComparator = new StringValueComparator(left, right, vectorComparator);
        } else if (dataType instanceof BinaryType) {
            valueComparator = new BinaryValueComparator(left, right, vectorComparator);
        } else {
            throw new UnsupportedOperationException(dataType + " can not be compared.");
        }

        return new ColumnVector() {

            @Override
            public DataType getDataType() {
                return BooleanType.BOOLEAN;
            }

            @Override
            public void close() {
                Utils.closeCloseables(left, right);
            }

            @Override
            public int getSize() {
                return left.getSize();
            }

            @Override
            public boolean isNullAt(int rowId) {
                return left.isNullAt(rowId) || right.isNullAt(rowId);
            }

            @Override
            public boolean getBoolean(int rowId) {
                if (isNullAt(rowId)) {
                    return false;
                }
                return valueComparator.getCompareResult(rowId);
            }
        };
    }

    static void compareBoolean(ColumnVector left, ColumnVector right, int[] result) {
        for (int rowId = 0; rowId < left.getSize(); rowId++) {
            if (!left.isNullAt(rowId) && !right.isNullAt(rowId)) {
                result[rowId] = Boolean.compare(left.getBoolean(rowId), right.getBoolean(rowId));
            }
        }
    }

    static void compareByte(ColumnVector left, ColumnVector right, int[] result) {
        for (int rowId = 0; rowId < left.getSize(); rowId++) {
            if (!left.isNullAt(rowId) && !right.isNullAt(rowId)) {
                result[rowId] = Byte.compare(left.getByte(rowId), right.getByte(rowId));
            }
        }
    }

    static void compareShort(ColumnVector left, ColumnVector right, int[] result) {
        for (int rowId = 0; rowId < left.getSize(); rowId++) {
            if (!left.isNullAt(rowId) && !right.isNullAt(rowId)) {
                result[rowId] = Short.compare(left.getShort(rowId), right.getShort(rowId));
            }
        }
    }

    static void compareInt(ColumnVector left, ColumnVector right, int[] result) {
        for (int rowId = 0; rowId < left.getSize(); rowId++) {
            if (!left.isNullAt(rowId) && !right.isNullAt(rowId)) {
                result[rowId] = Integer.compare(left.getInt(rowId), right.getInt(rowId));
            }
        }
    }

    static void compareLong(ColumnVector left, ColumnVector right, int[] result) {
        for (int rowId = 0; rowId < left.getSize(); rowId++) {
            if (!left.isNullAt(rowId) && !right.isNullAt(rowId)) {
                result[rowId] = Long.compare(left.getLong(rowId), right.getLong(rowId));
            }
        }
    }

    static void compareFloat(ColumnVector left, ColumnVector right, int[] result) {
        for (int rowId = 0; rowId < left.getSize(); rowId++) {
            if (!left.isNullAt(rowId) && !right.isNullAt(rowId)) {
                result[rowId] = Float.compare(left.getFloat(rowId), right.getFloat(rowId));
            }
        }
    }

    static void compareDouble(ColumnVector left, ColumnVector right, int[] result) {
        for (int rowId = 0; rowId < left.getSize(); rowId++) {
            if (!left.isNullAt(rowId) && !right.isNullAt(rowId)) {
                result[rowId] = Double.compare(left.getDouble(rowId), right.getDouble(rowId));
            }
        }
    }

    static void compareString(ColumnVector left, ColumnVector right, int[] result) {
        Comparator<String> comparator = Comparator.naturalOrder();
        for (int rowId = 0; rowId < left.getSize(); rowId++) {
            if (!left.isNullAt(rowId) && !right.isNullAt(rowId)) {
                result[rowId] = comparator.compare(left.getString(rowId), right.getString(rowId));
            }
        }
    }

    static void compareDecimal(ColumnVector left, ColumnVector right, int[] result) {
        Comparator<BigDecimal> comparator = Comparator.naturalOrder();
        for (int rowId = 0; rowId < left.getSize(); rowId++) {
            if (!left.isNullAt(rowId) && !right.isNullAt(rowId)) {
                result[rowId] = comparator.compare(left.getDecimal(rowId), right.getDecimal(rowId));
            }
        }
    }

    static void compareBinary(ColumnVector left, ColumnVector right, int[] result) {
        Comparator<byte[]> comparator = (leftOp, rightOp) -> {
            int i = 0;
            while (i < leftOp.length && i < rightOp.length) {
                if (leftOp[i] != rightOp[i]) {
                    return Byte.compare(leftOp[i], rightOp[i]);
                }
                i++;
            }
            return Integer.compare(leftOp.length, rightOp.length);
        };
        for (int rowId = 0; rowId < left.getSize(); rowId++) {
            if (!left.isNullAt(rowId) && !right.isNullAt(rowId)) {
                result[rowId] = comparator.compare(left.getBinary(rowId), right.getBinary(rowId));
            }
        }
    }

    static Expression childAt(Expression expression, int index) {
        return expression.getChildren().get(index);
    }

    /**
     * Combines a list of column vectors into one column vector based on the resolution of
     * idxToReturn
     * @param vectors List of ColumnVectors of the same data type with length >= 1
     * @param idxToReturn Function that takes in a rowId and returns the index of the column vector
     *                    to use as the return value
     */
    static ColumnVector combinationVector(
        List<ColumnVector> vectors,
        Function<Integer, Integer> idxToReturn) {
        return new ColumnVector() {
            // Store the last lookup value to avoid multiple looks up for same rowId.
            // The general pattern is call `isNullAt(rowId)` followed by `getBoolean(rowId)` or
            // some other value accessor. So the cache of one value is enough.
            private int lastLookupRowId = -1;
            private ColumnVector lastLookupVector = null;

            @Override
            public DataType getDataType() {
                return vectors.get(0).getDataType();
            }

            @Override
            public int getSize() {
                return vectors.get(0).getSize();
            }

            @Override
            public void close() {
                Utils.closeCloseables(vectors.toArray(new ColumnVector[0]));
            }

            @Override
            public boolean isNullAt(int rowId) {
                return getVector(rowId).isNullAt(rowId);
            }

            @Override
            public boolean getBoolean(int rowId) {
                return getVector(rowId).getBoolean(rowId);
            }

            @Override
            public byte getByte(int rowId) {
                return getVector(rowId).getByte(rowId);
            }

            @Override
            public short getShort(int rowId) {
                return getVector(rowId).getShort(rowId);
            }

            @Override
            public int getInt(int rowId) {
                return getVector(rowId).getInt(rowId);
            }

            @Override
            public long getLong(int rowId) {
                return getVector(rowId).getLong(rowId);
            }

            @Override
            public float getFloat(int rowId) {
                return getVector(rowId).getFloat(rowId);
            }

            @Override
            public double getDouble(int rowId) {
                return getVector(rowId).getDouble(rowId);
            }

            @Override
            public byte[] getBinary(int rowId) {
                return getVector(rowId).getBinary(rowId);
            }

            @Override
            public String getString(int rowId) {
                return getVector(rowId).getString(rowId);
            }

            @Override
            public BigDecimal getDecimal(int rowId) {
                return getVector(rowId).getDecimal(rowId);
            }

            @Override
            public MapValue getMap(int rowId) {
                return getVector(rowId).getMap(rowId);
            }

            @Override
            public ArrayValue getArray(int rowId) {
                return getVector(rowId).getArray(rowId);
            }

            @Override
            public ColumnVector getChild(int ordinal) {
                return combinationVector(
                    vectors.stream().map(v -> v.getChild(ordinal)).collect(Collectors.toList()),
                    idxToReturn
                );
            }

            private ColumnVector getVector(int rowId) {
                if (rowId == lastLookupRowId) {
                    return lastLookupVector;
                }
                lastLookupRowId = rowId;
                lastLookupVector = vectors.get(idxToReturn.apply(rowId));
                return lastLookupVector;
            }
        };
    }
}
