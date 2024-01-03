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
import static java.lang.String.format;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.types.*;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

/**
 * Utility methods used by the default expression evaluator.
 */
class ExpressionUtils {
    private ExpressionUtils() {}

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
     * Utility method to compare the left and right according to the natural ordering
     * and return an integer array where each row contains the comparison result (-1, 0, 1) for
     * corresponding rows in the input vectors compared.
     * <p>
     * Only primitive data types are supported.
     */
    static int[] compare(ColumnVector left, ColumnVector right) {
        checkArgument(
            left.getSize() == right.getSize(),
            "Left and right operand have different vector sizes.");
        DataType dataType = left.getDataType();

        int numRows = left.getSize();
        int[] result = new int[numRows];
        if (dataType instanceof BooleanType) {
            compareBoolean(left, right, result);
        } else if (dataType instanceof ByteType) {
            compareByte(left, right, result);
        } else if (dataType instanceof ShortType) {
            compareShort(left, right, result);
        } else if (dataType instanceof IntegerType || dataType instanceof DateType) {
            compareInt(left, right, result);
        } else if (dataType instanceof LongType || dataType instanceof TimestampType) {
            compareLong(left, right, result);
        } else if (dataType instanceof FloatType) {
            compareFloat(left, right, result);
        } else if (dataType instanceof DoubleType) {
            compareDouble(left, right, result);
        } else if (dataType instanceof DecimalType) {
            compareDecimal(left, right, result);
        } else if (dataType instanceof StringType) {
            compareString(left, right, result);
        } else if (dataType instanceof BinaryType) {
            compareBinary(left, right, result);
        } else {
            throw new UnsupportedOperationException(dataType + " can not be compared.");
        }
        return result;
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

    /**
     * Utility method to return the left child of the binary input expression
     */
    static Expression getLeft(Expression expression) {
        List<Expression> children = expression.getChildren();
        checkArgument(
            children.size() == 2,
            format("%s: expected two inputs, but got %s", expression, children.size()));
        return children.get(0);
    }

    /**
     * Utility method to return the right child of the binary input expression
     */
    static Expression getRight(Expression expression) {
        List<Expression> children = expression.getChildren();
        checkArgument(
            children.size() == 2,
            format("%s: expected two inputs, but got %s", expression, children.size()));
        return children.get(1);
    }

    static Expression childAt(Expression expression, int index) {
        return expression.getChildren().get(index);
    }
}
