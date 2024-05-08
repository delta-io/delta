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
package io.delta.kernel.defaults.internal;

import java.time.LocalDate;
import java.util.concurrent.TimeUnit;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.util.DateTimeConstants;
import io.delta.kernel.internal.util.Tuple2;

public class DefaultKernelUtils {
    private static final LocalDate EPOCH = LocalDate.ofEpochDay(0);

    private DefaultKernelUtils() {
    }

    //////////////////////////////////////////////////////////////////////////////////
    // Below utils are adapted from org.apache.spark.sql.catalyst.util.DateTimeUtils
    //////////////////////////////////////////////////////////////////////////////////

    // See http://stackoverflow.com/questions/466321/convert-unix-timestamp-to-julian
    // It's 2440587.5, rounding up to be compatible with Hive.
    static int JULIAN_DAY_OF_EPOCH = 2440588;

    /**
     * Returns the number of microseconds since epoch from Julian day and nanoseconds in a day.
     */
    public static long fromJulianDay(int days, long nanos) {
        // use Long to avoid rounding errors
        return ((long) (days - JULIAN_DAY_OF_EPOCH)) * DateTimeConstants.MICROS_PER_DAY +
                nanos / DateTimeConstants.NANOS_PER_MICROS;
    }

    /**
     * Returns Julian day and remaining nanoseconds from the number of microseconds
     * <p>
     * Note: support timestamp since 4717 BC (without negative nanoseconds, compatible with Hive).
     */
    public static Tuple2<Integer, Long> toJulianDay(long micros) {
        long julianUs = micros + JULIAN_DAY_OF_EPOCH * DateTimeConstants.MICROS_PER_DAY;
        long days = julianUs / DateTimeConstants.MICROS_PER_DAY;
        long us = julianUs % DateTimeConstants.MICROS_PER_DAY;
        return new Tuple2<>((int) days, TimeUnit.MICROSECONDS.toNanos(us));
    }

    public static long millisToMicros(long millis) {
        return Math.multiplyExact(millis, DateTimeConstants.MICROS_PER_MILLIS);
    }

    /**
     * Search for the data type of the given column in the schema.
     *
     * @param schema the schema to search
     * @param column the column whose data type is to be found
     * @return the data type of the column
     * @throws IllegalArgumentException if the column is not found in the schema
     */
    public static DataType getDataType(StructType schema, Column column) {
        DataType dataType = schema;
        for (String part : column.getNames()) {
            if (!(dataType instanceof StructType)) {
                throw new IllegalArgumentException(
                        String.format("Cannot resolve column (%s) in schema: %s", column, schema));
            }
            StructType structType = (StructType) dataType;
            if (structType.fieldNames().contains(part)) {
                dataType = structType.get(part).getDataType();
            } else {
                throw new IllegalArgumentException(
                        String.format("Cannot resolve column (%s) in schema: %s", column, schema));
            }
        }
        return dataType;
    }
}
