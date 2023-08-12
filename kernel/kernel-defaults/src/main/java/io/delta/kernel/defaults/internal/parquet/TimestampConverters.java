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
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;

import io.delta.kernel.types.TimestampType;

import io.delta.kernel.defaults.internal.DefaultKernelUtils;
import static io.delta.kernel.defaults.internal.DefaultKernelUtils.checkArgument;

public class TimestampConverters {

    public static Converter createTimestampConverter(int initialBatchSize, Type typeFromFile) {
        PrimitiveType primType = typeFromFile.asPrimitiveType();

        if (primType.getPrimitiveTypeName() == INT96) {

            return new TimestampBinaryConverter(initialBatchSize);

        } else if (primType.getPrimitiveTypeName() == INT64) {

            LogicalTypeAnnotation typeAnnotation = primType.getLogicalTypeAnnotation();
            if (!(typeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation)) {
                throw new IllegalStateException(String.format(
                    "Invalid parquet type %s for timestamp column",
                    typeFromFile));
            }
            LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestamp =
                (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) typeAnnotation;
            checkArgument(timestamp.isAdjustedToUTC(),
                "TimestampType must have parquet TimeType(isAdjustedToUTC=true)");

            if (timestamp.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS) {
                return new ParquetConverters.LongColumnConverter(
                    TimestampType.INSTANCE, initialBatchSize);
            } else if (timestamp.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS) {
                return new TimestampMillisConverter(initialBatchSize);
            } else {
                throw new UnsupportedOperationException(String.format(
                    "Unsupported parquet TimeType unit=%s", timestamp.getUnit()));
            }

        } else {
            throw new IllegalStateException(String.format(
                "Invalid parquet type %s for timestamp column",
                typeFromFile));
        }
    }

    public static class TimestampMillisConverter extends ParquetConverters.LongColumnConverter {

        TimestampMillisConverter( int initialBatchSize) {
            super(TimestampType.INSTANCE, initialBatchSize);
        }

        @Override
        public void addLong(long value) {
            super.addLong(DefaultKernelUtils.millisToMicros(value));
        }
    }

    public static class TimestampBinaryConverter extends ParquetConverters.LongColumnConverter {

        TimestampBinaryConverter( int initialBatchSize) {
            super(TimestampType.INSTANCE, initialBatchSize);
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

}
