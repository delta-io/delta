package org.apache.spark.sql.delta.util

import java.text.ParseException
import java.time._
import java.time.format.DateTimeParseException
import java.time.temporal.TemporalQueries
import java.util.{Locale, TimeZone}

import org.apache.spark.sql.delta.util.DateTimeUtils.instantToMicros

/**
 * Forked from [[org.apache.spark.sql.catalyst.util.TimestampFormatter]]
 */
sealed trait TimestampFormatter extends Serializable {
  /**
   * Parses a timestamp in a string and converts it to microseconds.
   *
   * @param s - string with timestamp to parse
   * @return microseconds since epoch.
   * @throws ParseException can be thrown by legacy parser
   * @throws DateTimeParseException can be thrown by new parser
   * @throws DateTimeException unable to obtain local date or time
   */
  @throws(classOf[ParseException])
  @throws(classOf[DateTimeParseException])
  @throws(classOf[DateTimeException])
  def parse(s: String): Long
  def format(us: Long): String
}

class Iso8601TimestampFormatter(
    pattern: String,
    timeZone: TimeZone,
    locale: Locale) extends TimestampFormatter with DateTimeFormatterHelper {
  @transient
  protected lazy val formatter = getOrCreateFormatter(pattern, locale)

  private def toInstant(s: String): Instant = {
    val temporalAccessor = formatter.parse(s)
    if (temporalAccessor.query(TemporalQueries.offset()) == null) {
      toInstantWithZoneId(temporalAccessor, timeZone.toZoneId)
    } else {
      Instant.from(temporalAccessor)
    }
  }

  override def parse(s: String): Long = instantToMicros(toInstant(s))

  override def format(us: Long): String = {
    val instant = DateTimeUtils.microsToInstant(us)
    formatter.withZone(timeZone.toZoneId).format(instant)
  }
}

/**
 * The formatter parses/formats timestamps according to the pattern `yyyy-MM-dd HH:mm:ss.[..fff..]`
 * where `[..fff..]` is a fraction of second up to microsecond resolution. The formatter does not
 * output trailing zeros in the fraction. For example, the timestamp `2019-03-05 15:00:01.123400` is
 * formatted as the string `2019-03-05 15:00:01.1234`.
 *
 * @param timeZone the time zone in which the formatter parses or format timestamps
 */
class FractionTimestampFormatter(timeZone: TimeZone)
  extends Iso8601TimestampFormatter("", timeZone, TimestampFormatter.defaultLocale) {

  @transient
  override protected lazy val formatter = DateTimeFormatterHelper.fractionFormatter
}

object TimestampFormatter {
  val defaultPattern: String = "yyyy-MM-dd HH:mm:ss"
  val defaultLocale: Locale = Locale.US

  def apply(format: String, timeZone: TimeZone, locale: Locale): TimestampFormatter = {
    new Iso8601TimestampFormatter(format, timeZone, locale)
  }

  def apply(format: String, timeZone: TimeZone): TimestampFormatter = {
    apply(format, timeZone, defaultLocale)
  }

  def apply(timeZone: TimeZone): TimestampFormatter = {
    apply(defaultPattern, timeZone, defaultLocale)
  }

  def getFractionFormatter(timeZone: TimeZone): TimestampFormatter = {
    new FractionTimestampFormatter(timeZone)
  }
}
