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

import io.delta.kernel.internal.util.Tuple2;

public class DefaultKernelUtils {
    private static final LocalDate EPOCH = LocalDate.ofEpochDay(0);

    private DefaultKernelUtils() {}

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
     *
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
