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
package io.delta.kernel.parquet;

import static io.delta.kernel.DefaultKernelUtils.checkArgument;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.vector.DefaultBinaryVector;
import io.delta.kernel.data.vector.DefaultBooleanVector;
import io.delta.kernel.data.vector.DefaultByteVector;
import io.delta.kernel.data.vector.DefaultConstantVector;
import io.delta.kernel.data.vector.DefaultDoubleVector;
import io.delta.kernel.data.vector.DefaultFloatVector;
import io.delta.kernel.data.vector.DefaultIntVector;
import io.delta.kernel.data.vector.DefaultLongVector;
import io.delta.kernel.data.vector.DefaultShortVector;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

class ParquetConverters
{
    public static Converter createConverter(
        int maxBatchSize,
        DataType typeFromClient,
        Type typeFromFile
    )
    {
        if (typeFromClient instanceof StructType) {
            return new RowConverter(
                maxBatchSize,
                (StructType) typeFromClient,
                (GroupType) typeFromFile);
        }
        else if (typeFromClient instanceof ArrayType) {
            return new ArrayConverter(
                maxBatchSize,
                (ArrayType) typeFromClient,
                (GroupType) typeFromFile
            );
        }
        else if (typeFromClient instanceof MapType) {
            return new MapConverter(
                maxBatchSize,
                (MapType) typeFromClient,
                (GroupType) typeFromFile);
        }
        else if (typeFromClient instanceof StringType || typeFromClient instanceof BinaryType) {
            return new BinaryColumnConverter(typeFromClient, maxBatchSize);
        }
        else if (typeFromClient instanceof BooleanType) {
            return new BooleanColumnConverter(maxBatchSize);
        }
        else if (typeFromClient instanceof IntegerType || typeFromClient instanceof DateType) {
            return new IntColumnConverter(typeFromClient, maxBatchSize);
        }
        else if (typeFromClient instanceof ByteType) {
            return new ByteColumnConverter(maxBatchSize);
        }
        else if (typeFromClient instanceof ShortType) {
            return new ShortColumnConverter(maxBatchSize);
        }
        else if (typeFromClient instanceof LongType) {
            return new LongColumnConverter(maxBatchSize);
        }
        else if (typeFromClient instanceof FloatType) {
            return new FloatColumnConverter(maxBatchSize);
        }
        else if (typeFromClient instanceof DoubleType) {
            return new DoubleColumnConverter(maxBatchSize);
        }
//        else if (typeFromClient instanceof DecimalType) {
//
//        }
//        else if (typeFromClient instanceof TimestampType) {
//
//        }

        throw new UnsupportedOperationException(typeFromClient + " is not supported");
    }

    public interface BaseConverter
    {
        ColumnVector getDataColumnVector(int batchSize);

        /**
         * Move the converter to accept the next row value.
         *
         * @return True if the last converted value is null, false otherwise
         */
        boolean moveToNextRow();

        void resizeIfNeeded();
    }

    public static class NonExistentColumnConverter
        extends PrimitiveConverter
        implements BaseConverter
    {
        private final DataType dataType;

        public NonExistentColumnConverter(DataType dataType)
        {
            this.dataType = Objects.requireNonNull(dataType, "dataType is null");
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize)
        {
            return new DefaultConstantVector(dataType, batchSize, null);
        }

        @Override
        public boolean moveToNextRow()
        {
            return true;
        }

        @Override
        public void resizeIfNeeded()
        {
            // nothing to resize
        }
    }

    public abstract static class BasePrimitiveColumnConverter
        extends PrimitiveConverter
        implements BaseConverter
    {
        // working state
        protected int currentRowIndex;
        protected boolean[] nullability;

        BasePrimitiveColumnConverter(int maxBatchSize)
        {
            checkArgument(maxBatchSize >= 0, "invalid maxBatchSize: %s", maxBatchSize);

            // Initialize the working state
            this.nullability = initNullabilityVector(maxBatchSize);
        }

        @Override
        public boolean moveToNextRow()
        {
            resizeIfNeeded();
            currentRowIndex++;
            return this.nullability[currentRowIndex - 1];
        }
    }

    public static class BooleanColumnConverter
        extends BasePrimitiveColumnConverter
    {
        // working state
        private boolean[] values;

        BooleanColumnConverter(int maxBatchSize)
        {
            super(maxBatchSize);
            this.values = new boolean[maxBatchSize];
        }

        @Override
        public void addBoolean(boolean value)
        {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize)
        {
            ColumnVector vector =
                new DefaultBooleanVector(batchSize, Optional.of(nullability), values);
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new boolean[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded()
        {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class ByteColumnConverter
        extends BasePrimitiveColumnConverter
    {

        // working state
        private byte[] values;

        ByteColumnConverter(int maxBatchSize)
        {
            super(maxBatchSize);
            this.values = new byte[maxBatchSize];
        }

        @Override
        public void addInt(int value)
        {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = (byte) value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize)
        {
            ColumnVector vector =
                new DefaultByteVector(batchSize, Optional.of(nullability), values);
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new byte[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded()
        {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class ShortColumnConverter
        extends BasePrimitiveColumnConverter
    {

        // working state
        private short[] values;

        ShortColumnConverter(int maxBatchSize)
        {
            super(maxBatchSize);
            this.values = new short[maxBatchSize];
        }

        @Override
        public void addInt(int value)
        {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = (short) value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize)
        {
            ColumnVector vector =
                new DefaultShortVector(batchSize, Optional.of(nullability), values);
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new short[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded()
        {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class IntColumnConverter
        extends BasePrimitiveColumnConverter
    {
        private final DataType dataType;
        // working state
        private int[] values;

        IntColumnConverter(DataType dataType, int maxBatchSize)
        {
            super(maxBatchSize);
            checkArgument(dataType instanceof IntegerType || dataType instanceof DataType);
            this.dataType = dataType;
            this.values = new int[maxBatchSize];
        }

        @Override
        public void addInt(int value)
        {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize)
        {
            ColumnVector vector =
                new DefaultIntVector(dataType, batchSize, Optional.of(nullability), values);
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new int[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded()
        {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class LongColumnConverter
        extends BasePrimitiveColumnConverter
    {

        // working state
        private long[] values;

        LongColumnConverter(int maxBatchSize)
        {
            super(maxBatchSize);
            this.values = new long[maxBatchSize];
        }

        @Override
        public void addLong(long value)
        {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize)
        {
            ColumnVector vector =
                new DefaultLongVector(batchSize, Optional.of(nullability), values);
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new long[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded()
        {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class FloatColumnConverter
        extends BasePrimitiveColumnConverter
    {
        // working state
        private float[] values;

        FloatColumnConverter(int maxBatchSize)
        {
            super(maxBatchSize);
            this.values = new float[maxBatchSize];
        }

        @Override
        public void addFloat(float value)
        {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize)
        {
            ColumnVector vector =
                new DefaultFloatVector(batchSize, Optional.of(nullability), values);
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new float[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded()
        {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class DoubleColumnConverter
        extends BasePrimitiveColumnConverter
    {

        // working state
        private double[] values;

        DoubleColumnConverter(int maxBatchSize)
        {
            super(maxBatchSize);
            this.values = new double[maxBatchSize];
        }

        @Override
        public void addDouble(double value)
        {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize)
        {
            ColumnVector vector =
                new DefaultDoubleVector(batchSize, Optional.of(nullability), values);
            // re-initialize the working space
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new double[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded()
        {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class BinaryColumnConverter
        extends BasePrimitiveColumnConverter
    {
        private final DataType dataType;

        // working state
        private byte[][] values;

        BinaryColumnConverter(DataType dataType, int maxBatchSize)
        {
            super(maxBatchSize);
            this.dataType = dataType;
            this.values = new byte[maxBatchSize][];
        }

        @Override
        public void addBinary(Binary value)
        {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = value.getBytes();
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize)
        {
            ColumnVector vector = new DefaultBinaryVector(dataType, batchSize, values);
            // re-initialize the working space
            this.values = new byte[values.length][];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded()
        {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    static boolean[] initNullabilityVector(int size)
    {
        boolean[] nullability = new boolean[size];
        // Initialize all values as null. As Parquet calls this converter only for non-null
        // values, make the corresponding value to false.
        Arrays.fill(nullability, true);

        return nullability;
    }

    static void setNullabilityToTrue(boolean[] nullability, int start, int end)
    {
        // Initialize all values as null. As Parquet calls this converter only for non-null
        // values, make the corresponding value to false.
        Arrays.fill(nullability, start, end, true);
    }
}
