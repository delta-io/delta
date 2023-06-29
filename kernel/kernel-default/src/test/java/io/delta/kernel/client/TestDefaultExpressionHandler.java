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
package io.delta.kernel.client;

import static io.delta.kernel.DefaultKernelUtils.daysSinceEpoch;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.DefaultColumnarBatch;
import io.delta.kernel.data.vector.DefaultIntVector;
import io.delta.kernel.data.vector.DefaultLongVector;
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.EqualTo;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.*;

public class TestDefaultExpressionHandler
{
    /**
     * Evaluate literal expressions. This is used to populate the partition column vectors.
     */
    @Test
    public void evalLiterals()
    {
        StructType inputSchema = new StructType();
        ColumnVector[] data = new ColumnVector[0];

        List<Literal> testCases = new ArrayList<>();
        testCases.add(Literal.of(true));
        testCases.add(Literal.of(false));
        testCases.add(Literal.ofNull(BooleanType.INSTANCE));
        testCases.add(Literal.of((byte) 24));
        testCases.add(Literal.ofNull(ByteType.INSTANCE));
        testCases.add(Literal.of((short) 876));
        testCases.add(Literal.ofNull(ShortType.INSTANCE));
        testCases.add(Literal.of(2342342));
        testCases.add(Literal.ofNull(IntegerType.INSTANCE));
        testCases.add(Literal.of(234234223L));
        testCases.add(Literal.ofNull(LongType.INSTANCE));
        testCases.add(Literal.of(23423.4223f));
        testCases.add(Literal.ofNull(FloatType.INSTANCE));
        testCases.add(Literal.of(23423.422233d));
        testCases.add(Literal.ofNull(DoubleType.INSTANCE));
        testCases.add(Literal.of("string_val"));
        testCases.add(Literal.ofNull(StringType.INSTANCE));
        testCases.add(Literal.of("binary_val".getBytes()));
        testCases.add(Literal.ofNull(BinaryType.INSTANCE));
        testCases.add(Literal.of(new Date(234234234)));
        testCases.add(Literal.ofNull(DateType.INSTANCE));
        testCases.add(Literal.of(new Timestamp(2342342342232L)));
        testCases.add(Literal.ofNull(TimestampType.INSTANCE));

        ColumnarBatch[] inputBatches = new ColumnarBatch[] {
            new DefaultColumnarBatch(0, inputSchema, data),
            new DefaultColumnarBatch(25, inputSchema, data),
            new DefaultColumnarBatch(128, inputSchema, data)
        };

        for (Literal expression : testCases) {
            DataType outputDataType = expression.dataType();

            for (ColumnarBatch inputBatch : inputBatches) {
                ColumnVector outputVector = eval(inputSchema, inputBatch, expression);
                assertEquals(inputBatch.getSize(), outputVector.getSize());
                assertEquals(outputDataType, outputVector.getDataType());
                for (int rowId = 0; rowId < outputVector.getSize(); rowId++) {
                    if (expression.value() == null) {
                        assertTrue(outputVector.isNullAt(rowId));
                        continue;
                    }
                    Object expRowValue = expression.value();
                    if (outputDataType instanceof BooleanType) {
                        assertEquals(expRowValue, outputVector.getBoolean(rowId));
                    }
                    else if (outputDataType instanceof ByteType) {
                        assertEquals(expRowValue, outputVector.getByte(rowId));
                    }
                    else if (outputDataType instanceof ShortType) {
                        assertEquals(expRowValue, outputVector.getShort(rowId));
                    }
                    else if (outputDataType instanceof IntegerType) {
                        assertEquals(expRowValue, outputVector.getInt(rowId));
                    }
                    else if (outputDataType instanceof LongType) {
                        assertEquals(expRowValue, outputVector.getLong(rowId));
                    }
                    else if (outputDataType instanceof FloatType) {
                        assertEquals(expRowValue, outputVector.getFloat(rowId));
                    }
                    else if (outputDataType instanceof DoubleType) {
                        assertEquals(expRowValue, outputVector.getDouble(rowId));
                    }
                    else if (outputDataType instanceof StringType) {
                        assertEquals(expRowValue, outputVector.getString(rowId));
                    }
                    else if (outputDataType instanceof BinaryType) {
                        assertEquals(expRowValue, outputVector.getBinary(rowId));
                    }
                    else if (outputDataType instanceof DateType) {
                        assertEquals(
                            daysSinceEpoch((Date) expRowValue), outputVector.getInt(rowId));
                    }
                    else if (outputDataType instanceof TimestampType) {
                        Timestamp timestamp = (Timestamp) expRowValue;
                        long micros = timestamp.getTime() * 1000;
                        assertEquals(micros, outputVector.getLong(rowId));
                    }
                    else {
                        throw new UnsupportedOperationException(
                            "unsupported output type encountered: " + outputDataType);
                    }
                }
            }
        }
    }

    @Test
    public void evalBooleanExpressionSimple()
    {
        Expression expression = new EqualTo(
            new Column(0, "intType", IntegerType.INSTANCE),
            Literal.of(3));

        for (int size : Arrays.asList(26, 234, 567)) {
            StructType inputSchema = new StructType()
                .add("intType", IntegerType.INSTANCE);
            ColumnVector[] data = new ColumnVector[] {
                intVector(size)
            };

            ColumnarBatch inputBatch = new DefaultColumnarBatch(size, inputSchema, data);

            ColumnVector output = eval(inputSchema, inputBatch, expression);
            for (int rowId = 0; rowId < size; rowId++) {
                if (data[0].isNullAt(rowId)) {
                    // expect the output to be null as well
                    assertTrue(output.isNullAt(rowId));
                }
                else {
                    assertFalse(output.isNullAt(rowId));
                    boolean expValue = rowId % 7 == 3;
                    assertEquals(expValue, output.getBoolean(rowId));
                }
            }
        }
    }

    @Test
    public void evalBooleanExpressionComplex()
    {
        Expression expression = new And(
            new EqualTo(new Column(0, "intType", IntegerType.INSTANCE), Literal.of(3)),
            new EqualTo(new Column(1, "longType", LongType.INSTANCE), Literal.of(4L))
        );

        for (int size : Arrays.asList(26, 234, 567)) {
            StructType inputSchema = new StructType()
                .add("intType", IntegerType.INSTANCE)
                .add("longType", LongType.INSTANCE);
            ColumnVector[] data = new ColumnVector[] {
                intVector(size),
                longVector(size),
            };

            ColumnarBatch inputBatch = new DefaultColumnarBatch(size, inputSchema, data);

            ColumnVector output = eval(inputSchema, inputBatch, expression);
            for (int rowId = 0; rowId < size; rowId++) {
                if (data[0].isNullAt(rowId) || data[1].isNullAt(rowId)) {
                    // expect the output to be null as well
                    assertTrue(output.isNullAt(rowId));
                }
                else {
                    assertFalse(output.isNullAt(rowId));
                    boolean expValue = (rowId % 7 == 3) && (rowId * 200L / 87 == 4);
                    assertEquals(expValue, output.getBoolean(rowId));
                }
            }
        }
    }

    private static ColumnVector eval(
        StructType inputSchema, ColumnarBatch input, Expression expression)
    {
        return new DefaultExpressionHandler()
            .getEvaluator(inputSchema, expression)
            .eval(input);
    }

    private static ColumnVector intVector(int size)
    {
        int[] values = new int[size];
        boolean[] nullability = new boolean[size];

        for (int rowId = 0; rowId < size; rowId++) {
            if (rowId % 5 == 0) {
                nullability[rowId] = true;
            }
            else {
                values[rowId] = rowId % 7;
            }
        }

        return new DefaultIntVector(
            IntegerType.INSTANCE, size, Optional.of(nullability), values);
    }

    private static ColumnVector longVector(int size)
    {
        long[] values = new long[size];
        boolean[] nullability = new boolean[size];

        for (int rowId = 0; rowId < size; rowId++) {
            if (rowId % 5 == 0) {
                nullability[rowId] = true;
            }
            else {
                values[rowId] = rowId * 200L % 87;
            }
        }

        return new DefaultLongVector(size, Optional.of(nullability), values);
    }
}
