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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import static java.util.Objects.requireNonNull;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

import io.delta.kernel.data.*;
import io.delta.kernel.types.*;

import io.delta.kernel.internal.util.Tuple2;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.defaults.internal.DefaultKernelUtils;

/**
 * Parquet column writers for writing columnar vectors to Parquet files using the
 * {@link RecordConsumer} interface.
 */
class ParquetColumnWriters {
    private ParquetColumnWriters() {
    }

    /**
     * Create column vector writers for the given columnar batch.
     *
     * @param batch the columnar batch
     * @return an array of column vector writers
     */
    static ColumnWriter[] createColumnVectorWriters(ColumnarBatch batch) {
        requireNonNull(batch, "batch is null");

        StructType schema = batch.getSchema();
        ColumnVector[] columnVectors = new ColumnVector[schema.length()];
        for (int fieldIndex = 0; fieldIndex < schema.length(); fieldIndex++) {
            columnVectors[fieldIndex] = batch.getColumnVector(fieldIndex);
        }

        return createColumnVectorWritersHelper(schema, columnVectors);
    }

    /**
     * Create column vector writers for the given struct column vector.
     * TODO: Having the ColumnarBatch as separate method complicates the code. ColumnarBatch is
     * also a ColumnVector of type STRUCT.
     *
     * @param structColumnVector the column vector
     * @return an array of column vector writers
     */
    static ColumnWriter[] createColumnVectorWriters(ColumnVector structColumnVector) {
        requireNonNull(structColumnVector, "batch is null");
        checkArgument(
                structColumnVector.getDataType() instanceof StructType,
                "ColumnVector is not a struct type");

        StructType schema = (StructType) structColumnVector.getDataType();
        ColumnVector[] columnVectors = new ColumnVector[schema.length()];
        for (int fieldIndex = 0; fieldIndex < schema.length(); fieldIndex++) {
            columnVectors[fieldIndex] = structColumnVector.getChild(fieldIndex);
        }

        return createColumnVectorWritersHelper(schema, columnVectors);
    }

    private static ColumnWriter[] createColumnVectorWritersHelper(
            StructType schema, ColumnVector[] columnVectors) {
        int numCols = schema.length();
        checkArgument(
                numCols == columnVectors.length,
                "Number of columns in schema does not match number of column vectors");

        ColumnWriter[] columnWriters = new ColumnWriter[numCols];
        for (int fieldIndex = 0; fieldIndex < numCols; fieldIndex++) {
            String colName = schema.at(fieldIndex).getName();
            ColumnVector columnVector = columnVectors[fieldIndex];
            columnWriters[fieldIndex] = createColumnWriter(colName, fieldIndex, columnVector);
        }
        return columnWriters;
    }

    private static ColumnWriter createColumnWriter(
            String colName,
            int fieldIndex,
            ColumnVector columnVector) {
        DataType dataType = columnVector.getDataType();

        if (dataType instanceof BooleanType) {
            return new BooleanWriter(colName, fieldIndex, columnVector);
        } else if (dataType instanceof ByteType) {
            return new ByteWriter(colName, fieldIndex, columnVector);
        } else if (dataType instanceof ShortType) {
            return new ShortWriter(colName, fieldIndex, columnVector);
        } else if (dataType instanceof IntegerType) {
            return new IntWriter(colName, fieldIndex, columnVector);
        } else if (dataType instanceof LongType) {
            return new LongWriter(colName, fieldIndex, columnVector);
        } else if (dataType instanceof FloatType) {
            return new FloatWriter(colName, fieldIndex, columnVector);
        } else if (dataType instanceof DoubleType) {
            return new DoubleWriter(colName, fieldIndex, columnVector);
        } else if (dataType instanceof StringType) {
            return new StringWriter(colName, fieldIndex, columnVector);
        } else if (dataType instanceof BinaryType) {
            return new BinaryWriter(colName, fieldIndex, columnVector);
        } else if (dataType instanceof DecimalType) {
            int precision = ((DecimalType) dataType).getPrecision();
            if (precision <= ParquetSchemaUtils.DECIMAL_MAX_DIGITS_IN_INT) {
                return new DecimalIntWriter(colName, fieldIndex, columnVector);
            } else if (precision <= ParquetSchemaUtils.DECIMAL_MAX_DIGITS_IN_LONG) {
                return new DecimalLongWriter(colName, fieldIndex, columnVector);
            }
            return new DecimalFixedBinaryWriter(colName, fieldIndex, columnVector);
        } else if (dataType instanceof DateType) {
            return new DateWriter(colName, fieldIndex, columnVector);
        } else if (dataType instanceof TimestampType) {
            return new TimestampWriter(colName, fieldIndex, columnVector);
        } else if (dataType instanceof ArrayType) {
            return new ArrayWriter(colName, fieldIndex, columnVector);
        } else if (dataType instanceof MapType) {
            return new MapWriter(colName, fieldIndex, columnVector);
        } else if (dataType instanceof StructType) {
            return new StructWriter(colName, fieldIndex, columnVector);
        }

        throw new IllegalArgumentException("Unsupported column vector type: " + dataType);
    }

    /**
     * Base class for column writers. Handles the common stuff such as null check, start/stop of
     * field and delegating the actual writing of non-null values to the subclass.
     */
    abstract static class ColumnWriter {
        protected final String colName;
        protected final int fieldIndex;
        protected final ColumnVector columnVector;

        ColumnWriter(String colName, int fieldIndex, ColumnVector columnVector) {
            this.colName = colName;
            this.fieldIndex = fieldIndex;
            this.columnVector = columnVector;
        }

        void writeRowValue(RecordConsumer recordConsumer, int rowId) {
            if (!columnVector.isNullAt(rowId)) {
                recordConsumer.startField(colName, fieldIndex);
                writeNonNullRowValue(recordConsumer, rowId);
                recordConsumer.endField(colName, fieldIndex);
            }
        }

        /**
         * Each specific column writer for data type, will implement to call appropriate methods
         * on the {@link RecordConsumer} to write the non-null value.
         */
        abstract void writeNonNullRowValue(RecordConsumer recordConsumer, int rowId);
    }

    static class BooleanWriter extends ColumnWriter {
        BooleanWriter(String name, int fieldId, ColumnVector columnVector) {
            super(name, fieldId, columnVector);
        }

        @Override
        void writeNonNullRowValue(RecordConsumer recordConsumer, int rowId) {
            recordConsumer.addBoolean(columnVector.getBoolean(rowId));
        }
    }

    static class ByteWriter extends ColumnWriter {
        ByteWriter(String name, int fieldId, ColumnVector columnVector) {
            super(name, fieldId, columnVector);
        }

        @Override
        void writeNonNullRowValue(RecordConsumer recordConsumer, int rowId) {
            recordConsumer.addInteger(columnVector.getByte(rowId));
        }
    }

    static class ShortWriter extends ColumnWriter {
        ShortWriter(String name, int fieldId, ColumnVector columnVector) {
            super(name, fieldId, columnVector);
        }

        @Override
        void writeNonNullRowValue(RecordConsumer recordConsumer, int rowId) {
            recordConsumer.addInteger(columnVector.getShort(rowId));
        }
    }

    static class IntWriter extends ColumnWriter {
        IntWriter(String name, int fieldId, ColumnVector columnVector) {
            super(name, fieldId, columnVector);
        }

        @Override
        void writeNonNullRowValue(RecordConsumer recordConsumer, int rowId) {
            recordConsumer.addInteger(columnVector.getInt(rowId));
        }
    }

    static class LongWriter extends ColumnWriter {
        LongWriter(String name, int fieldId, ColumnVector columnVector) {
            super(name, fieldId, columnVector);
        }

        @Override
        void writeNonNullRowValue(RecordConsumer recordConsumer, int rowId) {
            recordConsumer.addLong(columnVector.getLong(rowId));
        }
    }

    static class FloatWriter extends ColumnWriter {
        FloatWriter(String name, int fieldId, ColumnVector columnVector) {
            super(name, fieldId, columnVector);
        }

        @Override
        void writeNonNullRowValue(RecordConsumer recordConsumer, int rowId) {
            recordConsumer.addFloat(columnVector.getFloat(rowId));
        }
    }

    static class DoubleWriter extends ColumnWriter {
        DoubleWriter(String name, int fieldId, ColumnVector columnVector) {
            super(name, fieldId, columnVector);
        }

        @Override
        void writeNonNullRowValue(RecordConsumer recordConsumer, int rowId) {
            recordConsumer.addDouble(columnVector.getDouble(rowId));
        }
    }

    static class DecimalIntWriter extends ColumnWriter {
        private final int scale;

        DecimalIntWriter(String name, int fieldId, ColumnVector columnVector) {
            super(name, fieldId, columnVector);
            this.scale = ((DecimalType) columnVector.getDataType()).getScale();
        }

        @Override
        void writeNonNullRowValue(RecordConsumer recordConsumer, int rowId) {
            BigDecimal decimal = columnVector.getDecimal(rowId).movePointRight(scale);
            recordConsumer.addInteger(decimal.intValue());
        }
    }

    static class DecimalLongWriter extends ColumnWriter {
        private final int scale;

        DecimalLongWriter(String name, int fieldId, ColumnVector columnVector) {
            super(name, fieldId, columnVector);
            this.scale = ((DecimalType) columnVector.getDataType()).getScale();
        }

        @Override
        void writeNonNullRowValue(RecordConsumer recordConsumer, int rowId) {
            BigDecimal decimal = columnVector.getDecimal(rowId).movePointRight(scale);
            recordConsumer.addLong(decimal.longValue());
        }
    }

    static class DecimalFixedBinaryWriter extends ColumnWriter {
        private final int precision;
        private final int scale;

        DecimalFixedBinaryWriter(String name, int fieldId, ColumnVector columnVector) {
            super(name, fieldId, columnVector);
            DecimalType decimalType = (DecimalType) columnVector.getDataType();
            this.precision = decimalType.getPrecision();
            this.scale = decimalType.getScale();
        }

        @Override
        void writeNonNullRowValue(RecordConsumer recordConsumer, int rowId) {
            throw new UnsupportedOperationException("TODO: Implement DecimalFixedBinaryWriter");
        }
    }

    static class DateWriter extends ColumnWriter {
        DateWriter(String name, int fieldId, ColumnVector columnVector) {
            super(name, fieldId, columnVector);
        }

        @Override
        void writeNonNullRowValue(RecordConsumer recordConsumer, int rowId) {
            // TODO: Spark has various handling mode for DateType, need to check if it is needed
            // for Delta Kernel.
            recordConsumer.addInteger(columnVector.getInt(rowId)); // dates are stores as epoch days
        }
    }

    static class TimestampWriter extends ColumnWriter {
        // Reuse this buffer to avoid allocating a new buffer for each row
        private final byte[] reusedBuffer = new byte[12];

        TimestampWriter(String name, int fieldId, ColumnVector columnVector) {
            super(name, fieldId, columnVector);
        }

        @Override
        void writeNonNullRowValue(RecordConsumer recordConsumer, int rowId) {
            // TODO: Spark has various handling mode for DateType, need to check if it is needed
            // for Delta Kernel.

            // For now write as INT96 which is the most supported format for timestamps
            // Later on, depending upon the config, we can write either as INT64 or INT96
            long microsSinceEpochUTC = columnVector.getLong(rowId);
            Tuple2<Integer, Long> julianDayRemainingNanos =
                    DefaultKernelUtils.toJulianDay(microsSinceEpochUTC);

            ByteBuffer buffer = ByteBuffer.wrap(reusedBuffer);
            buffer.order(ByteOrder.LITTLE_ENDIAN)
                    .putLong(julianDayRemainingNanos._2) // timeOfDayNanos
                    .putInt(julianDayRemainingNanos._1); // julianDay
            recordConsumer.addBinary(Binary.fromReusedByteArray(reusedBuffer));
        }
    }

    static class StringWriter extends ColumnWriter {
        StringWriter(String name, int fieldId, ColumnVector columnVector) {
            super(name, fieldId, columnVector);
        }

        @Override
        void writeNonNullRowValue(RecordConsumer recordConsumer, int rowId) {
            Binary binary = Binary.fromConstantByteArray(
                    columnVector.getString(rowId).getBytes(StandardCharsets.UTF_8));
            recordConsumer.addBinary(binary);
        }
    }

    static class BinaryWriter extends ColumnWriter {
        BinaryWriter(String name, int fieldId, ColumnVector columnVector) {
            super(name, fieldId, columnVector);
        }

        @Override
        void writeNonNullRowValue(RecordConsumer recordConsumer, int rowId) {
            Binary binary = Binary.fromConstantByteArray(columnVector.getBinary(rowId));
            recordConsumer.addBinary(binary);
        }
    }

    static class ArrayWriter extends ColumnWriter {
        ArrayWriter(String name, int fieldId, ColumnVector columnVector) {
            super(name, fieldId, columnVector);
        }

        @Override
        void writeNonNullRowValue(RecordConsumer recordConsumer, int rowId) {
            // Write as 3-level representation. Later on, depending upon the config,
            // we can write either as 2-level or 3-level representation.
            recordConsumer.startGroup();
            ArrayValue arrayValue = columnVector.getArray(rowId);
            if (arrayValue.getSize() > 0) {
                // Use the fieldIndex as zero. Once we support Uniform compatible Parquet files,
                // the field index will come from the Delta schema.
                recordConsumer.startField("list", 0 /* fieldIndex */);
                ColumnVector elementVector = arrayValue.getElements();
                ColumnWriter elementWriter =
                        createColumnWriter("element", 0 /* fieldIndex */, elementVector);
                for (int i = 0; i < arrayValue.getSize(); i++) {
                    recordConsumer.startGroup();
                    if (!elementVector.isNullAt(i)) {
                        elementWriter.writeRowValue(recordConsumer, i);
                    }
                    recordConsumer.endGroup();
                }
                recordConsumer.endField("list", 0 /* fieldIndex */);
            }
            recordConsumer.endGroup();
        }
    }

    static class MapWriter extends ColumnWriter {
        MapWriter(String name, int fieldId, ColumnVector columnVector) {
            super(name, fieldId, columnVector);
        }

        @Override
        void writeNonNullRowValue(RecordConsumer recordConsumer, int rowId) {
            // Write as 3-level representation. Later on, depending upon the config,
            // we can write either as 2-level or 3-level representation.
            recordConsumer.startGroup();

            MapValue mapValue = columnVector.getMap(rowId);
            if (mapValue.getSize() > 0) {
                recordConsumer.startField("key_value", 0 /* fieldIndex */);

                // Use the fieldIndex as zero. Once we support Uniform compatible Parquet files,
                // the field index will come from the Delta schema.
                ColumnVector keyVector = mapValue.getKeys();
                ColumnWriter keyWriter = createColumnWriter("key", 0 /* fieldIndex */, keyVector);
                ColumnVector valueVector = mapValue.getValues();
                ColumnWriter valueWriter =
                        createColumnWriter("value", 1 /* fieldIndex */, valueVector);

                for (int i = 0; i < mapValue.getSize(); i++) {
                    recordConsumer.startGroup();
                    keyWriter.writeRowValue(recordConsumer, i);
                    if (!valueVector.isNullAt(i)) {
                        valueWriter.writeRowValue(recordConsumer, i);
                    }
                    recordConsumer.endGroup();
                }

                recordConsumer.endField("key_value", 0 /* fieldIndex */);
            }
            recordConsumer.endGroup();
        }
    }

    static class StructWriter extends ColumnWriter {
        private final ColumnWriter[] fieldWriters;

        StructWriter(String name, int fieldId, ColumnVector columnVector) {
            super(name, fieldId, columnVector);
            fieldWriters = createColumnVectorWriters(columnVector);
        }

        @Override
        void writeNonNullRowValue(RecordConsumer recordConsumer, int rowId) {
            recordConsumer.startGroup();
            for (ColumnWriter fieldWriter : fieldWriters) {
                fieldWriter.writeRowValue(recordConsumer, rowId);
            }
            recordConsumer.endGroup();
        }
    }
}
