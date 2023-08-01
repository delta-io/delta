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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.vector.*;
import io.delta.kernel.types.*;
import static io.delta.kernel.DefaultKernelUtils.checkArgument;

class ParquetConverters
{
    public static Converter createConverter(
        int initialBatchSize,
        DataType typeFromClient,
        Type typeFromFile
    )
    {
        if (typeFromClient instanceof StructType) {
            return new RowConverter(
                initialBatchSize,
                (StructType) typeFromClient,
                (GroupType) typeFromFile);
        }
        else if (typeFromClient instanceof ArrayType) {
            return new ArrayConverter(
                initialBatchSize,
                (ArrayType) typeFromClient,
                (GroupType) typeFromFile
            );
        }
        else if (typeFromClient instanceof MapType) {
            return new MapConverter(
                initialBatchSize,
                (MapType) typeFromClient,
                (GroupType) typeFromFile);
        }
        else if (typeFromClient instanceof StringType || typeFromClient instanceof BinaryType) {
            return new BinaryColumnConverter(typeFromClient, initialBatchSize);
        }
        else if (typeFromClient instanceof BooleanType) {
            return new BooleanColumnConverter(initialBatchSize);
        }
        else if (typeFromClient instanceof IntegerType || typeFromClient instanceof DateType) {
            return new IntColumnConverter(typeFromClient, initialBatchSize);
        }
        else if (typeFromClient instanceof ByteType) {
            return new ByteColumnConverter(initialBatchSize);
        }
        else if (typeFromClient instanceof ShortType) {
            return new ShortColumnConverter(initialBatchSize);
        }
        else if (typeFromClient instanceof LongType) {
            return new LongColumnConverter(initialBatchSize);
        }
        else if (typeFromClient instanceof FloatType) {
            return new FloatColumnConverter(initialBatchSize);
        }
        else if (typeFromClient instanceof DoubleType) {
            return new DoubleColumnConverter(initialBatchSize);
        }
        else if (typeFromClient instanceof DecimalType) {
            PrimitiveType primType = typeFromFile.asPrimitiveType();
            LogicalTypeAnnotation typeAnnotation = primType.getLogicalTypeAnnotation();

            if (primType.getPrimitiveTypeName() == INT32) {
                // For INT32 backed decimals
                if (typeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
                            (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) typeAnnotation;
                    return new IntDictionaryAwareDecimalConverter(typeFromClient,
                            decimalType.getPrecision(), decimalType.getScale(), initialBatchSize);
                } else {
                    // If the column is a plain INT32, we should pick the precision that can host
                    // the largest INT32 value.
                    return new IntDictionaryAwareDecimalConverter(typeFromClient,
                            10, 0, initialBatchSize);
                }

            } else if (primType.getPrimitiveTypeName() == INT64) {
                // For INT64 backed decimals
                if (typeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
                            (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) typeAnnotation;
                    return new LongDictionaryAwareDecimalConverter(typeFromClient,
                            decimalType.getPrecision(), decimalType.getScale(), initialBatchSize);
                } else {
                    // If the column is a plain INT64, we should pick the precision that can host
                    // the largest INT64 value.
                    return new LongDictionaryAwareDecimalConverter(typeFromClient,
                            20, 0, initialBatchSize);
                }

            } else if (primType.getPrimitiveTypeName() == FIXED_LEN_BYTE_ARRAY ||
                    primType.getPrimitiveTypeName() == BINARY) {
                // For BINARY and FIXED_LEN_BYTE_ARRAY backed decimals
                if (typeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
                            (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) typeAnnotation;
                    return new BinaryDictionaryAwareDecimalConverter(typeFromClient,
                            decimalType.getPrecision(), decimalType.getScale(), initialBatchSize);
                } else {
                    throw new RuntimeException(String.format(
                            "Unable to create Parquet converter for DecimalType whose parquet " +
                                    "type is %s without decimal metadata.",
                            typeFromFile));
                }

            } else {
                throw new RuntimeException(String.format(
                        "Unable to create Parquet converter for DecimalType whose Parquet type " +
                                "is %s. Parquet DECIMAL type can only be backed by INT32, INT64, " +
                                "FIXED_LEN_BYTE_ARRAY, or BINARY",
                        typeFromFile));
            }
        }
        // else if (typeFromClient instanceof TimestampType) {
        // }

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

        default void resizeIfNeeded() {}

        default void resetWorkingState() {}
    }

    public static class NonExistentColumnConverter
        extends PrimitiveConverter
        implements BaseConverter
    {
        private final DataType dataType;

        NonExistentColumnConverter(DataType dataType)
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
    }

    public abstract static class BasePrimitiveColumnConverter
        extends PrimitiveConverter
        implements BaseConverter
    {
        // working state
        protected int currentRowIndex;
        protected boolean[] nullability;

        BasePrimitiveColumnConverter(int initialBatchSize)
        {
            checkArgument(initialBatchSize >= 0, "invalid initialBatchSize: %s", initialBatchSize);

            // Initialize the working state
            this.nullability = initNullabilityVector(initialBatchSize);
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

        BooleanColumnConverter(int initialBatchSize)
        {
            super(initialBatchSize);
            this.values = new boolean[initialBatchSize];
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

        ByteColumnConverter(int initialBatchSize)
        {
            super(initialBatchSize);
            this.values = new byte[initialBatchSize];
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

        ShortColumnConverter(int initialBatchSize)
        {
            super(initialBatchSize);
            this.values = new short[initialBatchSize];
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

        IntColumnConverter(DataType dataType, int initialBatchSize)
        {
            super(initialBatchSize);
            checkArgument(dataType instanceof IntegerType || dataType instanceof DataType);
            this.dataType = dataType;
            this.values = new int[initialBatchSize];
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

        LongColumnConverter(int initialBatchSize)
        {
            super(initialBatchSize);
            this.values = new long[initialBatchSize];
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

        FloatColumnConverter(int initialBatchSize)
        {
            super(initialBatchSize);
            this.values = new float[initialBatchSize];
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

        DoubleColumnConverter(int initialBatchSize)
        {
            super(initialBatchSize);
            this.values = new double[initialBatchSize];
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

        BinaryColumnConverter(DataType dataType, int initialBatchSize)
        {
            super(initialBatchSize);
            this.dataType = dataType;
            this.values = new byte[initialBatchSize][];
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
            this.nullability = initNullabilityVector(nullability.length);
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

    public abstract static class BaseDecimalConverter extends BasePrimitiveColumnConverter {

        // working state
        private BigDecimal[] values;

        private final DataType dataType;
        private final int scale;
        protected BigDecimal[] expandedDictionary;

        BaseDecimalConverter(DataType dataType, int precision, int scale, int initialBatchSize) {
            super(initialBatchSize);
            DecimalType decimalType = (DecimalType) dataType;
            checkArgument(
                    decimalType.getPrecision() == precision && decimalType.getScale() == scale,
                    String.format(
                            "Found Delta type %s but Parquet type has precision=%s and scale=%s",
                            decimalType, precision, scale));
            this.scale = scale;
            this.dataType = dataType;
            this.values = new BigDecimal[initialBatchSize];
        }

        @Override
        public boolean hasDictionarySupport() {
            return true;
        }

        protected void addDecimal(BigDecimal value) {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = value;
        }

        @Override
        public void addValueFromDictionary(int dictionaryId) {
            addDecimal(expandedDictionary[dictionaryId]);
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize)
        {
            ColumnVector vector = new DefaultDecimalVector(dataType, batchSize,
                    Optional.of(nullability),  values);
            // re-initialize the working space
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new BigDecimal[values.length];
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

        protected BigDecimal decimalFromLong(long value) {
            return BigDecimal.valueOf(value, scale);
        }

        protected BigDecimal decimalFromBinary(Binary value) {
            return new BigDecimal(new BigInteger(value.getBytes()), scale);
        }
    }

    public static class IntDictionaryAwareDecimalConverter extends BaseDecimalConverter {

        IntDictionaryAwareDecimalConverter(
                DataType dataType, int precision, int scale, int initialBatchSize) {
            super(dataType, precision, scale, initialBatchSize);
        }

        @Override
        public void setDictionary(Dictionary dictionary) {
            this.expandedDictionary = new BigDecimal[dictionary.getMaxId() + 1];
            for (int id = 0; id < dictionary.getMaxId() + 1; id ++) {
                this.expandedDictionary[id] = decimalFromLong(dictionary.decodeToInt(id));
            }
        }

        @Override
        // Converts decimals stored as INT32
        public void addInt(int value) {
            addDecimal(decimalFromLong(value));
        }
    }

    public static class LongDictionaryAwareDecimalConverter extends BaseDecimalConverter {

        LongDictionaryAwareDecimalConverter(
                DataType dataType, int precision, int scale, int initialBatchSize) {
            super(dataType, precision, scale, initialBatchSize);
        }

        @Override
        public void setDictionary(Dictionary dictionary) {
            this.expandedDictionary = new BigDecimal[dictionary.getMaxId() + 1];
            for (int id = 0; id < dictionary.getMaxId() + 1; id ++) {
                this.expandedDictionary[id] = decimalFromLong(dictionary.decodeToLong(id));
            }
        }

        @Override
        // Converts decimals stored as INT64
        public void addLong(long value) {
            addDecimal(decimalFromLong(value));
        }
    }

    public static class BinaryDictionaryAwareDecimalConverter extends BaseDecimalConverter {

        BinaryDictionaryAwareDecimalConverter(
                DataType dataType, int precision, int scale, int initialBatchSize) {
            super(dataType, precision, scale, initialBatchSize);
        }

        @Override
        public void setDictionary(Dictionary dictionary) {
            this.expandedDictionary = new BigDecimal[dictionary.getMaxId() + 1];
            for (int id = 0; id < dictionary.getMaxId() + 1; id ++) {
                this.expandedDictionary[id] = decimalFromBinary(dictionary.decodeToBinary(id));
            }
        }

        @Override
        // Converts decimals stored as either FIXED_LENGTH_BYTE_ARRAY or BINARY
        public void addBinary(Binary value) {
            addDecimal(decimalFromBinary(value));
        }
    }

    public static class FileRowIndexColumnConverter
            extends LongColumnConverter {

        FileRowIndexColumnConverter(int initialBatchSize) {
            super(initialBatchSize);
        }

        @Override
        public void addLong(long value) {
            throw new UnsupportedOperationException("cannot add long to metadata column");
        }

        /**
         * @param fileRowIndex the file row index of the row processed
         */
        // If moveToNextRow() is called instead the value will be null
        public boolean moveToNextRow(long fileRowIndex) {
            super.values[currentRowIndex] = fileRowIndex;
            this.nullability[currentRowIndex] = false;
            return moveToNextRow();
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
