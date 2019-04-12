package org.apache.spark.sql.delta.util

import java.time.{Instant, ZoneId}
import java.util.Locale

import org.apache.spark.sql.delta.util.DateTimeUtils.instantToDays

/**
 * Forked from [[org.apache.spark.sql.catalyst.util.DateFormatter]]
 */
sealed trait DateFormatter extends Serializable {
  def parse(s: String): Int // returns days since epoch
  def format(days: Int): String
}

class Iso8601DateFormatter(
    pattern: String,
    locale: Locale) extends DateFormatter with DateTimeFormatterHelper {

  @transient
  private lazy val formatter = getOrCreateFormatter(pattern, locale)
  private val UTC = ZoneId.of("UTC")

  private def toInstant(s: String): Instant = {
    val temporalAccessor = formatter.parse(s)
    toInstantWithZoneId(temporalAccessor, UTC)
  }

  override def parse(s: String): Int = instantToDays(toInstant(s))

  override def format(days: Int): String = {
    val instant = Instant.ofEpochSecond(days * DateTimeUtils.SECONDS_PER_DAY)
    formatter.withZone(UTC).format(instant)
  }
}

object DateFormatter {
  val defaultPattern: String = "yyyy-MM-dd"
  val defaultLocale: Locale = Locale.US

  def apply(format: String, locale: Locale): DateFormatter = {
    new Iso8601DateFormatter(format, locale)
  }

  def apply(format: String): DateFormatter = apply(format, defaultLocale)

  def apply(): DateFormatter = apply(defaultPattern)
}
