/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains code from the Apache Spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

/*
 * Copyright (2021) The Delta Lake Project Authors.
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
    timeZone: ZoneId,
    locale: Locale) extends TimestampFormatter with DateTimeFormatterHelper {
  @transient
  protected lazy val formatter = getOrCreateFormatter(pattern, locale)

  private def toInstant(s: String): Instant = {
    val temporalAccessor = formatter.parse(s)
    if (temporalAccessor.query(TemporalQueries.offset()) == null) {
      toInstantWithZoneId(temporalAccessor, timeZone)
    } else {
      Instant.from(temporalAccessor)
    }
  }

  override def parse(s: String): Long = instantToMicros(toInstant(s))

  override def format(us: Long): String = {
    val instant = DateTimeUtils.microsToInstant(us)
    formatter.withZone(timeZone).format(instant)
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
  extends Iso8601TimestampFormatter("", timeZone.toZoneId, TimestampFormatter.defaultLocale) {

  @transient
  override protected lazy val formatter = DateTimeFormatterHelper.fractionFormatter
}

object TimestampFormatter {
  val defaultPattern: String = "yyyy-MM-dd HH:mm:ss"
  val defaultLocale: Locale = Locale.US

  def apply(format: String, zoneId: ZoneId): TimestampFormatter = {
    new Iso8601TimestampFormatter(format, zoneId, defaultLocale)
  }

  def apply(format: String, timeZone: TimeZone, locale: Locale): TimestampFormatter = {
    new Iso8601TimestampFormatter(format, timeZone.toZoneId, locale)
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
