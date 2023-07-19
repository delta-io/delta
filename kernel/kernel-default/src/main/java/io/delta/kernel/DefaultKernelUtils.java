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
package io.delta.kernel;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

public class DefaultKernelUtils
{
    private static final LocalDate EPOCH = LocalDate.ofEpochDay(0);

    private DefaultKernelUtils() {}

    /**
     * Given the file schema in Parquet file and selected columns by Delta, return
     * a subschema of the file schema.
     *
     * @param fileSchema
     * @param deltaType
     * @return
     */
    public static final MessageType pruneSchema(
        GroupType fileSchema, // parquet
        StructType deltaType) // delta-core
    {
        return new MessageType("fileSchema", pruneFields(fileSchema, deltaType));
    }

    private static List<Type> pruneFields(GroupType type, StructType deltaDataType) {
        // prune fields including nested pruning like in pruneSchema
        return deltaDataType.fields().stream()
                .map(column -> {
                    Type subType = findSubFieldType(type, column);
                    if (subType != null) {
                        return prunedType(subType, column.getDataType());
                    } else {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private static Type prunedType(Type type, DataType deltaType) {
        if (type instanceof GroupType && deltaType instanceof StructType) {
            GroupType groupType = (GroupType) type;
            StructType structType = (StructType) deltaType;
            return groupType.withNewFields(pruneFields(groupType, structType));
        } else {
            return type;
        }
    }

    /**
     * Search for the Parquet type in {@code groupType} of subfield which is equivalent to
     * given {@code field}.
     *
     * @param groupType Parquet group type coming from the file schema.
     * @param field Sub field given as Delta Kernel's {@link StructField}
     * @return {@link Type} of the Parquet field. Returns {@code null}, if not found.
     */
    public static Type findSubFieldType(GroupType groupType, StructField field)
    {
        // TODO: Need a way to search by id once we start supporting column mapping `id` mode.
        final String columnName = field.getName();
        if (groupType.containsField(columnName)) {
            return groupType.getType(columnName);
        }
        // Parquet is case-sensitive, but the engine that generated the parquet file may not be.
        // Check for direct match above but if no match found, try case-insensitive match.
        for (org.apache.parquet.schema.Type type : groupType.getFields()) {
            if (type.getName().equalsIgnoreCase(columnName)) {
                return type;
            }
        }

        return null;
    }

    /**
     * Precondition-style validation that throws {@link IllegalArgumentException}.
     *
     * @param isValid {@code true} if valid, {@code false} if an exception should be thrown
     * @throws IllegalArgumentException if {@code isValid} is false
     */
    public static void checkArgument(boolean isValid)
        throws IllegalArgumentException
    {
        if (!isValid) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Precondition-style validation that throws {@link IllegalArgumentException}.
     *
     * @param isValid {@code true} if valid, {@code false} if an exception should be thrown
     * @param message A String message for the exception.
     * @throws IllegalArgumentException if {@code isValid} is false
     */
    public static void checkArgument(boolean isValid, String message)
        throws IllegalArgumentException
    {
        if (!isValid) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Precondition-style validation that throws {@link IllegalArgumentException}.
     *
     * @param isValid {@code true} if valid, {@code false} if an exception should be thrown
     * @param message A String message for the exception.
     * @param args Objects used to fill in {@code %s} placeholders in the message
     * @throws IllegalArgumentException if {@code isValid} is false
     */
    public static void checkArgument(boolean isValid, String message, Object... args)
        throws IllegalArgumentException
    {
        if (!isValid) {
            throw new IllegalArgumentException(
                String.format(String.valueOf(message), args));
        }
    }

    /**
     * Precondition-style validation that throws {@link IllegalStateException}.
     *
     * @param isValid {@code true} if valid, {@code false} if an exception should be thrown
     * @param message A String message for the exception.
     * @throws IllegalStateException if {@code isValid} is false
     */
    public static void checkState(boolean isValid, String message)
        throws IllegalStateException
    {
        if (!isValid) {
            throw new IllegalStateException(message);
        }
    }

    /**
     * Utility method to get the number of days since epoch this given date is.
     *
     * @param date
     */
    public static int daysSinceEpoch(Date date)
    {
        LocalDate localDate = date.toLocalDate();
        return (int) ChronoUnit.DAYS.between(EPOCH, localDate);
    }

    /**
     * Utility method to get the number of microseconds since epoch this given timestamp is
     * w.r.t. to the system default timezone
     */
    public static long microsSinceEpoch(Timestamp timestamp) {
        LocalDateTime localTimestamp = timestamp.toLocalDateTime();
        LocalDateTime epoch = LocalDateTime.ofEpochSecond(
            0, 0,
            TimeZone.getDefault().toZoneId().getRules().getOffset(localTimestamp)
        );
        return ChronoUnit.MICROS.between(epoch, localTimestamp);

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

    public static long millisToMicros(long millis) {
        return Math.multiplyExact(millis, DateTimeConstants.MICROS_PER_MILLIS);
    }

    public static class DateTimeConstants {

        public static final int MONTHS_PER_YEAR = 12;

        public static final byte DAYS_PER_WEEK = 7;

        public static final long HOURS_PER_DAY = 24L;

        public static final long MINUTES_PER_HOUR = 60L;

        public static final long SECONDS_PER_MINUTE = 60L;
        public static final long SECONDS_PER_HOUR = MINUTES_PER_HOUR * SECONDS_PER_MINUTE;
        public static final long SECONDS_PER_DAY = HOURS_PER_DAY * SECONDS_PER_HOUR;

        public static final long MILLIS_PER_SECOND = 1000L;
        public static final long MILLIS_PER_MINUTE = SECONDS_PER_MINUTE * MILLIS_PER_SECOND;
        public static final long MILLIS_PER_HOUR = MINUTES_PER_HOUR * MILLIS_PER_MINUTE;
        public static final long MILLIS_PER_DAY = HOURS_PER_DAY * MILLIS_PER_HOUR;

        public static final long MICROS_PER_MILLIS = 1000L;
        public static final long MICROS_PER_SECOND = MILLIS_PER_SECOND * MICROS_PER_MILLIS;
        public static final long MICROS_PER_MINUTE = SECONDS_PER_MINUTE * MICROS_PER_SECOND;
        public static final long MICROS_PER_HOUR = MINUTES_PER_HOUR * MICROS_PER_MINUTE;
        public static final long MICROS_PER_DAY = HOURS_PER_DAY * MICROS_PER_HOUR;

        public static final long NANOS_PER_MICROS = 1000L;
        public static final long NANOS_PER_MILLIS = MICROS_PER_MILLIS * NANOS_PER_MICROS;
        public static final long NANOS_PER_SECOND = MILLIS_PER_SECOND * NANOS_PER_MILLIS;
    }
}
