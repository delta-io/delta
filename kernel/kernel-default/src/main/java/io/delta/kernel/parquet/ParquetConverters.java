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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.DefaultColumnarBatch;
import io.delta.kernel.data.vector.DefaultBinaryVector;
import io.delta.kernel.data.vector.DefaultBooleanVector;
import io.delta.kernel.data.vector.DefaultDoubleVector;
import io.delta.kernel.data.vector.DefaultFloatVector;
import io.delta.kernel.data.vector.DefaultIntVector;
import io.delta.kernel.data.vector.DefaultLongVector;
import io.delta.kernel.data.vector.DefaultStructVector;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.delta.kernel.DefaultKernelUtils.checkArgument;
import static io.delta.kernel.DefaultKernelUtils.findFieldType;
import static java.util.Objects.requireNonNull;

class ParquetConverters
{
    public static Converter createConverter(
            int maxBatchSize,
            DataType typeFromClient,
            Type typeFromFile
    ) {
        if (typeFromClient instanceof StructType) {
            return new RowRecordGroupConverter(
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
        else if (typeFromClient instanceof StringType) {
            return new BinaryColumnConverter(typeFromClient, maxBatchSize);
        }
        else if (typeFromClient instanceof BooleanType) {
            return new BooleanColumnConverter(maxBatchSize);
        }
        else if (typeFromClient instanceof IntegerType) {
            return new IntColumnConverter(maxBatchSize);
        }
        else if (typeFromClient instanceof LongType) {
            return new LongColumnConverter(maxBatchSize);
        }

        throw new UnsupportedOperationException(typeFromClient + " is not supported");
        // TODO: add support for float and double
    }

    public interface BaseConverter {
        ColumnVector getDataColumnVector(int batchSize);

        /**
         * Move the converter to accept the next row value.
         * @return True if the last converted value is null, false otherwise
         */
        boolean moveToNextRow();
    }

    public static class RowRecordGroupConverter
            extends GroupConverter
            implements BaseConverter
    {
        private final StructType readSchema;
        private final Converter[] converters;

        // Working state
        private int currentRowIndex;
        private final boolean[] nullability;

        public RowRecordGroupConverter(
                int maxBatchSize,
                StructType readSchema,
                GroupType fileSchema)
        {
            this.readSchema = requireNonNull(readSchema, "readSchema is not null");
            List<StructField> fields = readSchema.fields();
            this.converters = new Converter[fields.size()];

            // Initialize the working state
            this.nullability = new boolean[maxBatchSize];
            // Initialize all values as null. As Parquet calls this converter only for non-null
            // values, make the corresponding value to false.
            Arrays.fill(this.nullability, true);

            for (int i = 0; i < converters.length; i++) {
                final StructField field = fields.get(i);
                final DataType typeFromClient = field.getDataType();
                final Type typeFromFile = findFieldType(fileSchema, field);
                converters[i] = createConverter(maxBatchSize, typeFromClient, typeFromFile);
            }
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return converters[fieldIndex];
        }

        @Override
        public void start()
        {
            Arrays.stream(converters)
                    .filter(conv -> !conv.isPrimitive())
                    .forEach(conv -> ((GroupConverter) conv).start());
        }

        @Override
        public void end()
        {
            Arrays.stream(converters)
                    .filter(conv -> !conv.isPrimitive())
                    .forEach(conv -> ((GroupConverter) conv).end());
        }

        public ColumnarBatch getDataAsColumnarBatch(int batchSize)
        {
            ColumnVector[] memberVectors = collectMemberVectors(batchSize);
            return new DefaultColumnarBatch(batchSize, readSchema, memberVectors);
        }

        @Override
        public boolean moveToNextRow()
        {
            long memberNullCount = Arrays.stream(converters)
                    .map(converter -> (BaseConverter) converter)
                    .map(converters -> converters.moveToNextRow())
                    .filter(result -> result)
                    .count();

            boolean isNull = memberNullCount == converters.length;
            nullability[currentRowIndex] = isNull;
            currentRowIndex++;

            return isNull;
        }

        public ColumnVector getDataColumnVector(int batchSize) {
            ColumnVector[] memberVectors = collectMemberVectors(batchSize);
            return new DefaultStructVector(
                    batchSize,
                    readSchema,
                    Optional.of(nullability),
                    memberVectors
            );
        }

        private ColumnVector[] collectMemberVectors(int batchSize) {
            return Arrays.stream(converters)
                    .map(converter -> ((BaseConverter) converter).getDataColumnVector(batchSize))
                    .toArray(ColumnVector[]::new);
        }
    }

    public static abstract class BasePrimitiveColumnConverter
            extends PrimitiveConverter
            implements BaseConverter
    {
        // working state
        protected int currentRowIndex;
        protected boolean[] nullability;

        public BasePrimitiveColumnConverter(int maxBatchSize)
        {
            checkArgument(maxBatchSize >= 0, "invalid maxBatchSize: %s", maxBatchSize);

            // Initialize the working state
            this.nullability = new boolean[maxBatchSize];
            // Initialize all values as null. As Parquet calls this converter only for non-null
            // values, make the corresponding value to false.
            Arrays.fill(this.nullability, true);
        }

        @Override
        public boolean moveToNextRow()
        {
            currentRowIndex++;
            return this.nullability[currentRowIndex - 1];
        }
    }

    public static class BooleanColumnConverter
            extends BasePrimitiveColumnConverter
    {
        // working state
        private boolean[] values;

        public BooleanColumnConverter(int maxBatchSize)
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
            return new DefaultBooleanVector(batchSize, Optional.of(nullability), values);
        }

        private void resizeIfNeeded() {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
            }
        }
    }

    public static class IntColumnConverter
            extends BasePrimitiveColumnConverter
    {

        // working state
        private int[] values;

        public IntColumnConverter(int maxBatchSize)
        {
            super(maxBatchSize);
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
            return new DefaultIntVector(batchSize, Optional.of(nullability), values);
        }

        private void resizeIfNeeded() {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
            }
        }
    }

    public static class LongColumnConverter
            extends BasePrimitiveColumnConverter
    {

        // working state
        private long[] values;

        public LongColumnConverter(int maxBatchSize)
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
            return new DefaultLongVector(batchSize, Optional.of(nullability), values);
        }

        private void resizeIfNeeded() {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
            }
        }
    }

    public static class FloatColumnConverter
            extends BasePrimitiveColumnConverter
    {

        // working state
        private float[] values;

        public FloatColumnConverter(int maxBatchSize)
        {
            super(maxBatchSize);
            this.values = new float[maxBatchSize];
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
            return new DefaultFloatVector(batchSize, Optional.of(nullability), values);
        }

        private void resizeIfNeeded() {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
            }
        }
    }

    public static class DoubleColumnConverter
            extends BasePrimitiveColumnConverter
    {

        // working state
        private double[] values;

        public DoubleColumnConverter(int maxBatchSize)
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
            return new DefaultDoubleVector(batchSize, Optional.of(nullability), values);
        }

        private void resizeIfNeeded() {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
            }
        }
    }

    public static class BinaryColumnConverter
            extends BasePrimitiveColumnConverter
    {
        private final DataType dataType;

        // working state
        private byte[][] values;

        public BinaryColumnConverter(DataType dataType, int maxBatchSize)
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
            return new DefaultBinaryVector(dataType, batchSize, values);
        }

        private void resizeIfNeeded() {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
            }
        }
    }
}
