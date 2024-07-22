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
     * @param typeFromClient  Column type from client
     * @return instance of {@link Converter}
     */
    public static Converter createTimestampConverter(int initialBatchSize,
                                                     Type typeFromFile,
                                                     DataType typeFromClient) {
        PrimitiveType primType = typeFromFile.asPrimitiveType();
        LogicalTypeAnnotation typeAnnotation = primType.getLogicalTypeAnnotation();
        boolean isTimestampTz = (typeFromClient instanceof TimestampType);

        if (primType.getPrimitiveTypeName() == INT96) {
            // INT96 does not have a logical type in both TIMESTAMP and TIMESTAMP_NTZ
            // Also, TimestampNTZ type does not require rebasing
            // due to its lack of time zone context.
            return new TimestampBinaryConverter(typeFromClient, initialBatchSize);
        } else if (primType.getPrimitiveTypeName() == INT64 &&
                typeAnnotation instanceof TimestampLogicalTypeAnnotation) {
            TimestampLogicalTypeAnnotation timestamp =
                    (TimestampLogicalTypeAnnotation) typeAnnotation;

            boolean isAdjustedUtc = timestamp.isAdjustedToUTC();
            if (!((isTimestampTz && isAdjustedUtc) || (!isTimestampTz && !isAdjustedUtc))) {
                throw new RuntimeException(String.format(
                        "Incompatible Utc adjustment for timestamp column. " +
                                "Client type: %s, File type: %s, isAdjustedUtc: %s",
                        typeFromClient, typeFromFile, isAdjustedUtc));
            }

            switch (timestamp.getUnit()) {
                case MICROS:
                    return new ParquetColumnReaders.LongColumnReader(typeFromClient,
                            initialBatchSize);
                case MILLIS:
                    return new TimestampMillisConverter(typeFromClient, initialBatchSize);
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
