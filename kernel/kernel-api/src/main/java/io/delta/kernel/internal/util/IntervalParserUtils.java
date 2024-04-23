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

package io.delta.kernel.internal.util;

import java.util.Locale;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import static io.delta.kernel.internal.util.DateTimeConstants.*;
import static io.delta.kernel.internal.util.IntervalParserUtils.ParseState.*;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

/**
 * Copy of `org/apache/spark/sql/catalyst/util/SparkIntervalUtils.scala` from Apache Spark.
 * Delta table properties store the interval format. We need a parser in order to parse these
 * values in Kernel.
 */
public class IntervalParserUtils {
    private IntervalParserUtils() {
    }

    /**
     * Parse the given interval string into milliseconds. For configs accepting an interval, we
     * require the user specified string must obey: - Doesn't use months or years, since an internal
     * like this is not deterministic. - The microseconds parsed from the string value must be a
     * non-negative value. - Doesn't use nanoseconds part as it too granular to use
     *
     * @return parsed interval as microseconds.
     */
    public static long safeParseIntervalAsMicros(String input) {
        checkArgument(input != null, "interval string cannot be null");
        String inputInLowerCase = input.trim().toLowerCase(Locale.ROOT);
        checkArgument(!inputInLowerCase.isEmpty(), "interval string cannot be empty");
        if (!inputInLowerCase.startsWith("interval ")) {
            inputInLowerCase = "interval " + inputInLowerCase;
        }
        return parseIntervalAsMicros(inputInLowerCase);
    }


    public static long parseIntervalAsMicros(String input) {
        return new IntervalParser(input).toMicroSeconds();
    }

    enum ParseState {
        PREFIX,
        TRIM_BEFORE_SIGN,
        SIGN,
        TRIM_BEFORE_VALUE,
        VALUE,
        VALUE_FRACTIONAL_PART,
        TRIM_BEFORE_UNIT,
        UNIT_BEGIN,
        UNIT_SUFFIX,
        UNIT_END;
    }

    private static final String intervalStr = "interval";
    private static final String yearStr = "year";
    private static final String monthStr = "month";
    private static final String weekStr = "week";
    private static final String dayStr = "day";
    private static final String hourStr = "hour";
    private static final String minuteStr = "minute";
    private static final String secondStr = "second";
    private static final String millisStr = "millisecond";
    private static final String microsStr = "microsecond";

    private static class IntervalParser {
        private final String input;
        private String s; // trimmed input in lowercase
        private ParseState state = ParseState.PREFIX;
        private int i = 0;
        private long currentValue = 0;
        private boolean isNegative = false;
        private int days = 0;
        private long microseconds = 0;
        private int fractionScale = 0;
        private int fraction = 0;
        private boolean pointPrefixed = false;

        // Expected trimmed lower case input string.
        IntervalParser(String input) {
            this.input = input;
            if (input == null) {
                throwIAE("interval string cannot be null");
            }
            String inputInLowerCase = input.trim().toLowerCase(Locale.ROOT);
            if (inputInLowerCase.isEmpty()) {
                throwIAE(format("Error parsing '%s' to interval", input));
            }
            this.s = inputInLowerCase;
        }

        long toMicroSeconds() {
            // UTF-8 encoded bytes of the trimmed input
            byte[] bytes = s.getBytes(UTF_8);
            checkArgument(bytes.length > 0, "Interval string cannot be empty");

            while (i < bytes.length) {
                byte b = bytes[i];
                int initialFractionScale = (int) (NANOS_PER_SECOND / 10);
                switch (state) {
                    case PREFIX:
                        if (s.startsWith(intervalStr)) {
                            if (s.length() == intervalStr.length()) {
                                throwIAE("interval string cannot be empty");
                            } else if (!Character.isWhitespace(bytes[i + intervalStr.length()])) {
                                throwIAE("invalid interval prefix " + currentWord());
                            } else {
                                i += intervalStr.length();
                            }
                        }
                        state = ParseState.TRIM_BEFORE_SIGN;
                        break;
                    case TRIM_BEFORE_SIGN:
                        trimToNextState(b, SIGN);
                        break;
                    case SIGN:
                        currentValue = 0;
                        fraction = 0;
                        // We preset next state from SIGN to TRIM_BEFORE_VALUE. If we meet '.'
                        // in the SIGN state, it means that the interval value we deal with here is
                        // a numeric with only fractional part, such as '.11 second', which can be
                        // parsed to 0.11 seconds. In this case, we need to reset next state to
                        // `VALUE_FRACTIONAL_PART` to go parse the fraction part of the interval
                        // value.
                        state = TRIM_BEFORE_VALUE;
                        fractionScale = -1;
                        if (b == '-' || b == '+') {
                            i++;
                            isNegative = b == '-';
                        } else if ('0' <= b && b <= '9') {
                            isNegative = false;
                        } else if (b == '.') {
                            isNegative = false;
                            fractionScale = initialFractionScale;
                            pointPrefixed = true;
                            i++;
                            state = VALUE_FRACTIONAL_PART;
                        } else {
                            throwIAE(format("unrecognized number '%s'", currentWord()));
                        }
                        break;
                    case TRIM_BEFORE_VALUE:
                        trimToNextState(b, VALUE);
                        break;
                    case VALUE:
                        if ('0' <= b && b <= '9') {
                            try {
                                currentValue = Math.addExact(
                                        Math.multiplyExact(10, currentValue), (b - '0'));
                            } catch (ArithmeticException e) {
                                throwIAE(e.getMessage(), e);
                            }
                        } else if (Character.isWhitespace(b)) {
                            state = TRIM_BEFORE_UNIT;
                        } else if (b == '.') {
                            fractionScale = initialFractionScale;
                            state = VALUE_FRACTIONAL_PART;
                        } else {
                            throwIAE(format("invalid value '%s'", currentWord()));
                        }
                        i++;
                        break;
                    case VALUE_FRACTIONAL_PART:
                        if ('0' <= b && b <= '9' && fractionScale > 0) {
                            fraction += (b - '0') * fractionScale;
                            fractionScale /= 10;
                        } else if (Character.isWhitespace(b) &&
                                (!pointPrefixed || fractionScale < initialFractionScale)) {
                            fraction /= ((int) NANOS_PER_MICROS);
                            state = TRIM_BEFORE_UNIT;
                        } else if ('0' <= b && b <= '9') {
                            throwIAE(format("interval can only support nanosecond " +
                                    "precision, '%s' is out of range", currentWord()));
                        } else {
                            throwIAE(format("invalid value '%s'", currentWord()));
                        }
                        i += 1;
                        break;
                    case TRIM_BEFORE_UNIT:
                        trimToNextState(b, UNIT_BEGIN);
                        break;
                    case UNIT_BEGIN:
                        // Checks that only seconds can have the fractional part
                        if (b != 's' && fractionScale >= 0) {
                            throwIAE(format("'%s' cannot have fractional part", currentWord()));
                        }
                        if (isNegative) {
                            currentValue = -currentValue;
                            fraction = -fraction;
                        }
                        try {
                            if (b == 'y' && matchAt(i, yearStr)) {
                                throwIAE("year is not supported, use days instead");
                            } else if (b == 'w' && matchAt(i, weekStr)) {
                                long daysInWeeks = Math.multiplyExact(DAYS_PER_WEEK, currentValue);
                                days = Math.toIntExact(Math.addExact(days, daysInWeeks));
                                i += weekStr.length();
                            } else if (b == 'd' && matchAt(i, dayStr)) {
                                days = Math.addExact(days, Math.toIntExact(currentValue));
                                i += dayStr.length();
                            } else if (b == 'h' && matchAt(i, hourStr)) {
                                long hoursUs = Math.multiplyExact(currentValue,
                                        MICROS_PER_HOUR);
                                microseconds = Math.addExact(microseconds, hoursUs);
                                i += hourStr.length();
                            } else if (b == 's' && matchAt(i, secondStr)) {
                                long secondsUs = Math.multiplyExact(
                                        currentValue,
                                        MICROS_PER_SECOND);
                                microseconds = Math.addExact(
                                        Math.addExact(microseconds, secondsUs), fraction);
                                i += secondStr.length();
                            } else if (b == 'm') {
                                if (matchAt(i, monthStr)) {
                                    throwIAE("month is not supported, use days instead");
                                } else if (matchAt(i, minuteStr)) {
                                    long minutesUs = Math.multiplyExact(
                                            currentValue,
                                            MICROS_PER_MINUTE);
                                    microseconds = Math.addExact(microseconds, minutesUs);
                                    i += minuteStr.length();
                                } else if (matchAt(i, millisStr)) {
                                    long millisUs = Math.multiplyExact(
                                            currentValue,
                                            MICROS_PER_MILLIS);
                                    microseconds = Math.addExact(microseconds, millisUs);
                                    i += millisStr.length();
                                } else if (matchAt(i, microsStr)) {
                                    microseconds = Math.addExact(microseconds, currentValue);
                                    i += microsStr.length();
                                } else {
                                    throwIAE(format("invalid unit '%s'", currentWord()));
                                }
                            } else {
                                throwIAE(format("invalid unit '%s'", currentWord()));
                            }
                        } catch (ArithmeticException e) {
                            throwIAE(e.getMessage(), e);
                        }
                        state = UNIT_SUFFIX;
                        break;
                    case UNIT_SUFFIX:
                        if (b == 's') {
                            state = UNIT_END;
                        } else if (Character.isWhitespace(b)) {
                            state = TRIM_BEFORE_SIGN;
                        } else {
                            throwIAE(format("invalid unit '%s'", currentWord()));
                        }
                        i++;
                        break;
                    case UNIT_END:
                        if (Character.isWhitespace(b)) {
                            i++;
                            state = TRIM_BEFORE_SIGN;
                        } else {
                            throwIAE(format("invalid unit '%s'", currentWord()));
                        }
                        break;
                    default:
                        throwIAE("invalid input: " + s);
                }
            }

            switch (state) {
                case UNIT_SUFFIX: // fall through
                case UNIT_END: // fall through
                case TRIM_BEFORE_SIGN:
                    return days * MICROS_PER_DAY + microseconds;
                case TRIM_BEFORE_VALUE:
                    throwIAE(format("expect a number after '%s' but hit EOL", currentWord()));
                    break;
                case VALUE:
                case VALUE_FRACTIONAL_PART:
                    throwIAE(format("expect a unit name after '%s' but hit EOL", currentWord()));
                    break;
                default:
                    throwIAE(format("unknown error when parsing '%s'", currentWord()));
            }

            throwIAE("invalid interval");
            return 0; // should never reach.
        }

        private void trimToNextState(byte b, ParseState next) {
            if (Character.isWhitespace(b)) {
                i++;
            } else {
                state = next;
            }
        }

        private String currentWord() {
            String sep = "\\s+";
            String[] strings = s.split(sep);
            int lenRight = s.substring(i).split(sep).length;
            return strings[strings.length - lenRight];
        }

        private boolean matchAt(int i, String str) {
            if (i + str.length() > s.length()) {
                return false;
            }
            return s.substring(i, i + str.length()).equals(str);
        }

        private void throwIAE(String msg, Exception e) {
            throw new IllegalArgumentException(
                    format("Error parsing '%s' to interval, %s", input, msg), e);
        }

        private void throwIAE(String msg) {
            throw new IllegalArgumentException(
                    format("Error parsing '%s' to interval, %s", input, msg));
        }
    }
}
