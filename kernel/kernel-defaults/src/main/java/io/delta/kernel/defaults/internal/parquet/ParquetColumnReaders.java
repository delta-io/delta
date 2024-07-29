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
package io.delta.kernel.defaults.internal.parquet;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.*;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.defaults.internal.data.vector.*;
import static io.delta.kernel.defaults.internal.parquet.TimestampConverters.createTimestampConverter;

/**
 * Parquet column readers for materializing the column values from Parquet files into Kernels
 * {@link ColumnVector}.
 */
class ParquetColumnReaders {
    public static Converter createConverter(
        int initialBatchSize,
        DataType typeFromClient,
        Type typeFromFile
    ) {
        if (typeFromClient instanceof StructType) {
            return new RowColumnReader(
                initialBatchSize,
                (StructType) typeFromClient,
                (GroupType) typeFromFile);
        } else if (typeFromClient instanceof ArrayType) {
            return new ArrayColumnReader(
                initialBatchSize,
                (ArrayType) typeFromClient,
                (GroupType) typeFromFile
            );
        } else if (typeFromClient instanceof MapType) {
            return new MapColumnReader(
                initialBatchSize,
                (MapType) typeFromClient,
                (GroupType) typeFromFile);
        } else if (typeFromClient instanceof StringType || typeFromClient instanceof BinaryType) {
            return new BinaryColumnReader(typeFromClient, initialBatchSize);
        } else if (typeFromClient instanceof BooleanType) {
            return new BooleanColumnReader(initialBatchSize);
        } else if (typeFromClient instanceof IntegerType || typeFromClient instanceof DateType) {
            return new IntColumnReader(typeFromClient, initialBatchSize);
        } else if (typeFromClient instanceof ByteType) {
            return new ByteColumnReader(initialBatchSize);
        } else if (typeFromClient instanceof ShortType) {
            return new ShortColumnReader(initialBatchSize);
        } else if (typeFromClient instanceof LongType) {
            return new LongColumnReader(typeFromClient, initialBatchSize);
        } else if (typeFromClient instanceof FloatType) {
            return new FloatColumnReader(initialBatchSize);
        } else if (typeFromClient instanceof DoubleType) {
            return new DoubleColumnReader(initialBatchSize);
        } else if (typeFromClient instanceof DecimalType) {
            return DecimalColumnReader.createDecimalConverter(
                initialBatchSize, (DecimalType) typeFromClient, typeFromFile);
        } else if (typeFromClient instanceof TimestampType) {
            return createTimestampConverter(initialBatchSize, typeFromFile,
                    TimestampType.TIMESTAMP);
        } else if (typeFromClient instanceof TimestampNTZType) {
            return createTimestampConverter(initialBatchSize, typeFromFile,
                    TimestampNTZType.TIMESTAMP_NTZ);
        }

        throw new UnsupportedOperationException(typeFromClient + " is not supported");
    }

    static boolean[] initNullabilityVector(int size) {
        boolean[] nullability = new boolean[size];
        // Initialize all values as null. As Parquet calls this converter only for non-null
        // values, make the corresponding value to false.
        Arrays.fill(nullability, true);

        return nullability;
    }

    static void setNullabilityToTrue(boolean[] nullability, int start, int end) {
        // Initialize all values as null. As Parquet calls this converter only for non-null
        // values, make the corresponding value to false.
        Arrays.fill(nullability, start, end, true);
    }

    /**
     * Base column reader for all implementations of Parquet {@link Converter} to return data in
     * columnar batch. General operation flow is:
     *  - each reader implementation allocates state to receive a fixed number of column values
     *  - before accepting a new value the state is resized if it is not of sufficient size
     *  - after each row, {@link #finalizeCurrentRow(long)} is called to finalize the state of
     *    the last read row column value.
     */
    public interface BaseColumnReader {
        ColumnVector getDataColumnVector(int batchSize);

        /**
         * Finalize the current row:
         *  - close the state of the row that was read most recently.
         *  - reallocate the state to be of sufficient size for the current batch size. Generally
         *    the state value arrays are resized as part of setting the value, but method doesn't
         *    get called for null values which results in the state value arrays are not sufficient
         *    size for the current batch size.
         *
         * @param currentRowIndex Row index of the current row in the Parquet file.
         */
        void finalizeCurrentRow(long currentRowIndex);

        default void resizeIfNeeded() {}

        default void resetWorkingState() {}
    }

    public static class NonExistentColumnReader
        extends PrimitiveConverter
        implements BaseColumnReader {
        private final DataType dataType;

        NonExistentColumnReader(DataType dataType) {
            this.dataType = Objects.requireNonNull(dataType, "dataType is null");
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize) {
            return new DefaultConstantVector(dataType, batchSize, null);
        }

        @Override
        public void finalizeCurrentRow(long currentRowIndex) {}
    }

    public abstract static class BasePrimitiveColumnReader
        extends PrimitiveConverter
        implements BaseColumnReader {
        // working state
        protected int currentRowIndex;
        protected boolean[] nullability;

        BasePrimitiveColumnReader(int initialBatchSize) {
            checkArgument(initialBatchSize > 0, "invalid initialBatchSize: %s", initialBatchSize);
            // Initialize the working state
            this.nullability = initNullabilityVector(initialBatchSize);
        }

        @Override
        public void finalizeCurrentRow(long currentRowIndex) {
            resizeIfNeeded();
            this.currentRowIndex++;
        }
    }

    public static class BooleanColumnReader extends BasePrimitiveColumnReader {
        // working state
        private boolean[] values;

        BooleanColumnReader(int initialBatchSize) {
            super(initialBatchSize);
            this.values = new boolean[initialBatchSize];
        }

        @Override
        public void addBoolean(boolean value) {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize) {
            ColumnVector vector =
                new DefaultBooleanVector(batchSize, Optional.of(nullability), values);
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new boolean[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded() {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class ByteColumnReader extends BasePrimitiveColumnReader {
        // working state
        private byte[] values;

        ByteColumnReader(int initialBatchSize) {
            super(initialBatchSize);
            this.values = new byte[initialBatchSize];
        }

        @Override
        public void addInt(int value) {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = (byte) value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize) {
            ColumnVector vector =
                new DefaultByteVector(batchSize, Optional.of(nullability), values);
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new byte[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded() {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class ShortColumnReader extends BasePrimitiveColumnReader {
        // working state
        private short[] values;

        ShortColumnReader(int initialBatchSize) {
            super(initialBatchSize);
            this.values = new short[initialBatchSize];
        }

        @Override
        public void addInt(int value) {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = (short) value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize) {
            ColumnVector vector =
                new DefaultShortVector(batchSize, Optional.of(nullability), values);
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new short[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded() {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class IntColumnReader extends BasePrimitiveColumnReader {
        private final DataType dataType;
        // working state
        private int[] values;

        IntColumnReader(DataType dataType, int initialBatchSize) {
            super(initialBatchSize);
            checkArgument(dataType instanceof IntegerType || dataType instanceof DataType);
            this.dataType = dataType;
            this.values = new int[initialBatchSize];
        }

        @Override
        public void addInt(int value) {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize) {
            ColumnVector vector =
                new DefaultIntVector(dataType, batchSize, Optional.of(nullability), values);
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new int[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded() {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class LongColumnReader extends BasePrimitiveColumnReader {
        private final DataType dataType;
        // working state
        private long[] values;

        LongColumnReader(DataType dataType, int initialBatchSize) {
            super(initialBatchSize);
            checkArgument(dataType instanceof LongType ||
                    dataType instanceof TimestampType ||
                    dataType instanceof TimestampNTZType);
            this.dataType = dataType;
            this.values = new long[initialBatchSize];
        }

        @Override
        public void addLong(long value) {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize) {
            ColumnVector vector =
                new DefaultLongVector(dataType, batchSize, Optional.of(nullability), values);
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new long[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded() {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class FloatColumnReader extends BasePrimitiveColumnReader {
        // working state
        private float[] values;

        FloatColumnReader(int initialBatchSize) {
            super(initialBatchSize);
            this.values = new float[initialBatchSize];
        }

        @Override
        public void addFloat(float value) {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize) {
            ColumnVector vector =
                new DefaultFloatVector(batchSize, Optional.of(nullability), values);
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new float[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded() {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class DoubleColumnReader extends BasePrimitiveColumnReader {
        // working state
        private double[] values;

        DoubleColumnReader(int initialBatchSize) {
            super(initialBatchSize);
            this.values = new double[initialBatchSize];
        }

        @Override
        public void addDouble(double value) {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize) {
            ColumnVector vector =
                new DefaultDoubleVector(batchSize, Optional.of(nullability), values);
            // re-initialize the working space
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new double[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded() {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class BinaryColumnReader extends BasePrimitiveColumnReader {
        private final DataType dataType;

        // working state
        private byte[][] values;

        BinaryColumnReader(DataType dataType, int initialBatchSize) {
            super(initialBatchSize);
            this.dataType = dataType;
            this.values = new byte[initialBatchSize][];
        }

        @Override
        public void addBinary(Binary value) {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = value.getBytes();
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize) {
            ColumnVector vector = new DefaultBinaryVector(dataType, batchSize, values);
            // re-initialize the working space
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new byte[values.length][];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded() {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class FileRowIndexColumnReader extends LongColumnReader {
        FileRowIndexColumnReader(int initialBatchSize) {
            super(LongType.LONG, initialBatchSize);
        }

        @Override
        public void addLong(long value) {
            throw new UnsupportedOperationException("cannot add long to metadata column");
        }

        @Override
        public void finalizeCurrentRow(long currentRowIndex) {
            // Set the previous row index value as the value
            super.addLong(currentRowIndex);
            super.finalizeCurrentRow(currentRowIndex);
        }
    }
}
