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

import java.util.*;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.types.DataType;

import io.delta.kernel.defaults.client.DefaultExpressionHandler;

/**
 * An implicit cast expression to convert the input type to another given type. Here is the valid
 * list of casts
 * <p>
 *  <ul>
 *    <li>{@code byte} to {@code short, int, long, float, double}</li>
 *    <li>{@code short} to {@code int, long, float, double}</li>
 *    <li>{@code int} to {@code long, float, double}</li>
 *    <li>{@code long} to {@code float, double}</li>
 *    <li>{@code float} to {@code double}</li>
 *  </ul>
 *
 * <p>
 * The above list is not exhaustive. Based on the need, we can add more casts.
 * <p>
 * In {@link DefaultExpressionHandler} this is used when the operands of an expression are not of
 * the same type, but the evaluator expects same type inputs. There could be more use cases, but
 * for now this is the only use case.
 */
final class ImplicitCastExpression {
    private final Expression input;
    private final DataType outputType;

    /**
     * Create a cast around the given input expression to specified output data
     * type. It is the responsibility of the caller to validate the input expression can be cast
     * to the new type using {@link #canCastTo(DataType, DataType)}
     */
    ImplicitCastExpression(Expression input, DataType outputType) {
        this.input = requireNonNull(input, "input is null");
        this.outputType = requireNonNull(outputType, "outputType is null");
    }

    public Expression getInput() {
        return input;
    }

    /**
     * Evaluate the given column expression on the input {@link ColumnVector}.
     *
     * @param input {@link ColumnVector} data of the input to the cast expression.
     * @return {@link ColumnVector} result applying target type casting on every element in the
     * input {@link ColumnVector}.
     */
    ColumnVector eval(ColumnVector input) {
        String fromTypeStr = input.getDataType().toString();
        switch (fromTypeStr) {
            case "byte":
                return new ByteUpConverter(outputType, input);
            case "short":
                return new ShortUpConverter(outputType, input);
            case "integer":
                return new IntUpConverter(outputType, input);
            case "long":
                return new LongUpConverter(outputType, input);
            case "float":
                return new FloatUpConverter(outputType, input);
            default:
                throw new UnsupportedOperationException(
                    format("Cast from %s is not supported", fromTypeStr));
        }
    }

    /**
     * Map containing for each type what are the target cast types can be.
     */
    private static final Map<String, List<String>> UP_CASTABLE_TYPE_TABLE = unmodifiableMap(
        new HashMap<String, List<String>>() {
            {
                this.put("byte", Arrays.asList("short", "integer", "long", "float", "double"));
                this.put("short", Arrays.asList("integer", "long", "float", "double"));
                this.put("integer", Arrays.asList("long", "float", "double"));
                this.put("long", Arrays.asList("float", "double"));
                this.put("float", Arrays.asList("double"));
            }
        });

    /**
     * Utility method which returns whether the given {@code from} type can be cast to {@code to}
     * type.
     */
    static boolean canCastTo(DataType from, DataType to) {
        // TODO: The type name should be a first class method on `DataType` instead of getting it
        // using the `toString`.
        String fromStr = from.toString();
        String toStr = to.toString();
        return UP_CASTABLE_TYPE_TABLE.containsKey(fromStr) &&
            UP_CASTABLE_TYPE_TABLE.get(fromStr).contains(toStr);
    }

    /**
     * Base class for up casting {@link ColumnVector} data.
     */
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
}
