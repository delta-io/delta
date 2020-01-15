/*
 * Copyright 2019 Databricks, Inc.
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

package org.apache.spark.sql.delta

import java.sql.Timestamp

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, Literal, PreciseTimestampConversion}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, TimestampType}

/**
 * The specification to time travel a Delta Table to the given `timestamp` or `version`.
 * @param timestamp An expression that can be evaluated into a timestamp. The expression cannot
 *                  be a subquery.
 * @param version The version of the table to time travel to. Must be >= 0.
 * @param creationSource The API used to perform time travel, e.g. `atSyntax`, `dfReader` or SQL
 */
case class DeltaTimeTravelSpec(
    timestamp: Option[Expression],
    version: Option[Long],
    creationSource: Option[String]) {

  assert(version.isEmpty ^ timestamp.isEmpty,
    "Either the version or timestamp should be provided for time travel")

  /**
   * Compute the timestamp to use for time travelling the relation from the given expression for
   * the given time zone.
   */
  def getTimestamp(timeZone: String): Timestamp = {
    DateTimeUtils.toJavaTimestamp(
      Cast(timestamp.get, TimestampType, Option(timeZone)).eval().asInstanceOf[java.lang.Long])
  }
}

object DeltaTimeTravelSpec {
  /** A regex which looks for the pattern ...@v(some numbers) for extracting the version number */
  private val VERSION_URI_FOR_TIME_TRAVEL = ".*@[vV](\\d+)$".r

  /** The timestamp format which we accept after the `@` character. */
  private val TIMESTAMP_FORMAT = "yyyyMMddHHmmssSSS"

  /** Length of yyyyMMddHHmmssSSS */
  private val TIMESTAMP_FORMAT_LENGTH = TIMESTAMP_FORMAT.length

  /** A regex which looks for the pattern ...@(yyyyMMddHHmmssSSS) for extracting timestamps. */
  private val TIMESTAMP_URI_FOR_TIME_TRAVEL = s".*@(\\d{$TIMESTAMP_FORMAT_LENGTH})$$".r

  /** Returns whether the given table identifier may contain time travel syntax. */
  def isApplicable(conf: SQLConf, identifier: String): Boolean = {
    conf.getConf(DeltaSQLConf.RESOLVE_TIME_TRAVEL_ON_IDENTIFIER) &&
      identifierContainsTimeTravel(identifier)
  }

  /** Checks if the table identifier contains patterns that resemble time travel syntax. */
  private def identifierContainsTimeTravel(identifier: String): Boolean = identifier match {
    case TIMESTAMP_URI_FOR_TIME_TRAVEL(ts) => true
    case VERSION_URI_FOR_TIME_TRAVEL(v) => true
    case _ => false
  }

  /** Adds a time travel node based on the special syntax in the table identifier. */
  def resolvePath(conf: SQLConf, identifier: String): (DeltaTimeTravelSpec, String) = {
    identifier match {
      case TIMESTAMP_URI_FOR_TIME_TRAVEL(ts) =>
        val timestamp = parseTimestamp(ts, conf.sessionLocalTimeZone)
        // Drop the 18 characters in the right, which is the timestamp format and the @ character.
        val realIdentifier = identifier.dropRight(TIMESTAMP_FORMAT_LENGTH + 1)

        DeltaTimeTravelSpec(Some(timestamp), None, Some("atSyntax.path")) -> realIdentifier
      case VERSION_URI_FOR_TIME_TRAVEL(v) =>
        // Drop the version, and `@v` characters from the identifier
        val realIdentifier = identifier.dropRight(v.length + 2)
        DeltaTimeTravelSpec(None, Some(v.toLong), Some("atSyntax.path")) -> realIdentifier
    }
  }

  /**
   * Parse the given timestamp string into a proper Catalyst TimestampType. We support millisecond
   * level precision, therefore don't use standard SQL timestamp functions, which only support
   * second level precision.
   *
   * @throws `AnalysisException` when the timestamp format doesn't match our criteria
   */
  private def parseTimestamp(ts: String, timeZone: String): Expression = {
    val format = FastDateFormat.getInstance(TIMESTAMP_FORMAT, DateTimeUtils.getTimeZone(timeZone))

    try {
      val sqlTs = DateTimeUtils.fromJavaTimestamp(new java.sql.Timestamp(format.parse(ts).getTime))
      PreciseTimestampConversion(Literal(sqlTs), LongType, TimestampType)
    } catch {
      case e: java.text.ParseException =>
        throw new AnalysisException(
          s"The provided timestamp $ts doesn't match the expected syntax $TIMESTAMP_FORMAT.",
          cause = Some(e))
    }
  }
}
