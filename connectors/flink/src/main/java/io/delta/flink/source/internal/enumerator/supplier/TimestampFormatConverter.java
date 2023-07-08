package io.delta.flink.source.internal.enumerator.supplier;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * An Util class that converts timestamps represented as String to long values.
 */
public final class TimestampFormatConverter {

    private static final DateTimeFormatter FORMATTER = new DateTimeFormatterBuilder()
        .appendOptional(DateTimeFormatter.ISO_LOCAL_DATE)
        .optionalStart().appendLiteral(' ').optionalEnd()
        .optionalStart().appendLiteral('T').optionalEnd()
        .appendOptional(DateTimeFormatter.ISO_LOCAL_TIME)
        .appendOptional(DateTimeFormatter.ofPattern(".SSS"))
        .optionalStart().appendLiteral('Z').optionalEnd()
        .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
        .toFormatter();

    /**
     * Converts a String representing Date or Date Time to timestamp value.
     * <p>
     * Supported formats are:
     * <ul>
     *      <li>2022-02-24</li>
     *      <li>2022-02-24 04:55:00</li>
     *      <li>2022-02-24 04:55:00.001</li>
     *      <li>2022-02-24T04:55:00</li>
     *      <li>2022-02-24T04:55:00.001</li>
     *      <li>2022-02-24T04:55:00.001Z</li>
     * </ul>
     *
     * @param timestamp A String representing a date or date-time to convert.
     * @return A UTC timestamp value as long.
     */
    public static long convertToTimestamp(String timestamp) {
        return LocalDateTime.parse(timestamp, FORMATTER).toInstant(ZoneOffset.UTC).toEpochMilli();
    }
}
