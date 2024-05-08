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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;

import io.delta.kernel.types.*;
import static io.delta.kernel.types.TimestampNTZType.TIMESTAMP_NTZ;
import static io.delta.kernel.types.TimestampType.TIMESTAMP;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.defaults.internal.DefaultKernelUtils;

/**
 * Column readers for columns of types {@code timestmap, timestamp_ntz}. These both data types share
 * the same encoding methods except the logical type.
 */
public class TimestampConverters {

    /**
     * Create a {@code timestamp} column type reader
     *
     * @param initialBatchSize Initial batch size of the generated column vector
     * @param typeFromFile     Column type metadata from Parquet file
     * @return instance of {@link Converter}
     */
    public static Converter createTimestampConverter(int initialBatchSize, Type typeFromFile) {
        PrimitiveType primType = typeFromFile.asPrimitiveType();

        if (primType.getPrimitiveTypeName() == INT96) {
            return new TimestampBinaryConverter(TIMESTAMP, initialBatchSize);
        } else if (primType.getPrimitiveTypeName() == INT64) {
            LogicalTypeAnnotation typeAnnotation = primType.getLogicalTypeAnnotation();
            if (!(typeAnnotation instanceof TimestampLogicalTypeAnnotation)) {
                throw new RuntimeException(String.format(
                        "Unsupported timestamp column with Parquet type %s.",
                        typeFromFile));
            }
            TimestampLogicalTypeAnnotation timestamp =
                    (TimestampLogicalTypeAnnotation) typeAnnotation;

            checkArgument(timestamp.isAdjustedToUTC(),
                    "TimestampType must have Parquet TimestampType(isAdjustedToUTC=true)");

            switch (timestamp.getUnit()) {
                case MICROS:
                    return new ParquetColumnReaders.LongColumnReader(TIMESTAMP, initialBatchSize);
                case MILLIS:
                    return new TimestampMillisConverter(TIMESTAMP, initialBatchSize);
                default:
                    throw new UnsupportedOperationException(String.format(
                            "Unsupported Parquet TimeType unit=%s", timestamp.getUnit()));
            }
        } else {
            throw new RuntimeException(String.format(
                    "Unsupported timestamp column with Parquet type %s.",
                    typeFromFile));
        }
    }

    /**
     * Create a {@code timestamp_ntz} column type reader
     *
     * @param initialBatchSize Initial batch size of the generated column vector
     * @param typeFromFile     Column type metadata from Parquet file
     * @return instance of {@link Converter}
     */
    public static Converter createTimestampNtzConverter(int initialBatchSize, Type typeFromFile) {
        PrimitiveType primType = typeFromFile.asPrimitiveType();

        LogicalTypeAnnotation logicalType = primType.getLogicalTypeAnnotation();
        checkArgument(logicalType instanceof TimestampLogicalTypeAnnotation,
                "Invalid logical type annotation for timestamp_ntz type columns: " + logicalType);

        checkArgument(primType.getPrimitiveTypeName() == INT64,
                "Invalid storage type for timestamp_ntz columns: "
                        + primType.getPrimitiveTypeName());

        TimestampLogicalTypeAnnotation timestamp = (TimestampLogicalTypeAnnotation) logicalType;
        checkArgument(!timestamp.isAdjustedToUTC(),
                TIMESTAMP_NTZ + " must have Parquet TimestampType(isAdjustedToUTC=false)");

        switch (timestamp.getUnit()) {
            case MICROS:
                return new ParquetColumnReaders.LongColumnReader(TIMESTAMP_NTZ, initialBatchSize);
            case MILLIS:
                return new TimestampMillisConverter(TIMESTAMP_NTZ, initialBatchSize);
            default:
                throw new UnsupportedOperationException(String.format(
                        "Unsupported Parquet TimeType unit=%s", timestamp.getUnit()));
        }
    }

    public static class TimestampMillisConverter extends ParquetColumnReaders.LongColumnReader {

        TimestampMillisConverter(DataType dataType, int initialBatchSize) {
            super(validTimestampType(dataType), initialBatchSize);
        }

        @Override
        public void addLong(long value) {
            super.addLong(DefaultKernelUtils.millisToMicros(value));
        }
    }

    public static class TimestampBinaryConverter extends ParquetColumnReaders.LongColumnReader {

        TimestampBinaryConverter(DataType dataType, int initialBatchSize) {
            super(validTimestampType(dataType), initialBatchSize);
        }

        private long binaryToSQLTimestamp(Binary binary) {
            checkArgument(binary.length() == 12, String.format(
                    "Timestamps (with nanoseconds) are expected to be stored in 12-byte long " +
                            "binaries. Found a %s-byte binary instead.", binary.length()
            ));
            ByteBuffer buffer = binary.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
            long timeOfDayNanos = buffer.getLong();
            int julianDay = buffer.getInt();
            return DefaultKernelUtils.fromJulianDay(julianDay, timeOfDayNanos);
        }

        @Override
        public void addBinary(Binary value) {
            long julianMicros = binaryToSQLTimestamp(value);
            // we do not rebase timestamps
            long gregorianMicros = julianMicros;
            super.addLong(gregorianMicros);
        }

        @Override
        public void addLong(long value) {
            throw new UnsupportedOperationException(getClass().getName());
        }
    }

    private static DataType validTimestampType(DataType dataType) {
        checkArgument(dataType instanceof TimestampType ||
                dataType instanceof TimestampNTZType);
        return dataType;
    }
}
