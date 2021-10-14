/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package io.delta.standalone.internal.util

import java.nio.charset.StandardCharsets

private[internal] object IntervalUtils {

  object IntervalUnit extends Enumeration {
    type IntervalUnit = Value

    val NANOSECOND = Value(0, "nanosecond")
    val MICROSECOND = Value(1, "microsecond")
    val MILLISECOND = Value(2, "millisecond")
    val SECOND = Value(3, "second")
    val MINUTE = Value(4, "minute")
    val HOUR = Value(5, "hour")
    val DAY = Value(6, "day")
    val WEEK = Value(7, "week")
    val MONTH = Value(8, "month")
    val YEAR = Value(9, "year")
  }
  import IntervalUnit._

  private object ParseState extends Enumeration {
    type ParseState = Value

    val PREFIX,
    TRIM_BEFORE_SIGN,
    SIGN,
    TRIM_BEFORE_VALUE,
    VALUE,
    VALUE_FRACTIONAL_PART,
    TRIM_BEFORE_UNIT,
    UNIT_BEGIN,
    UNIT_SUFFIX,
    UNIT_END = Value
  }
  private final val intervalStr = "interval"
  private def unitToUtf8(unit: IntervalUnit): String = {
    unit.toString
  }
  private final val yearStr = unitToUtf8(YEAR)
  private final val monthStr = unitToUtf8(MONTH)
  private final val weekStr = unitToUtf8(WEEK)
  private final val dayStr = unitToUtf8(DAY)
  private final val hourStr = unitToUtf8(HOUR)
  private final val minuteStr = unitToUtf8(MINUTE)
  private final val secondStr = unitToUtf8(SECOND)
  private final val millisStr = unitToUtf8(MILLISECOND)
  private final val microsStr = unitToUtf8(MICROSECOND)

  /**
   * A safe version of `stringToInterval`. It returns null for invalid input string.
   */
  def safeStringToInterval(input: String): CalendarInterval = {
    try {
      stringToInterval(input)
    } catch {
      case _: IllegalArgumentException => null
    }
  }

  /**
   * Converts a string to [[CalendarInterval]] case-insensitively.
   *
   * @throws IllegalArgumentException if the input string is not in valid interval format.
   */
  def stringToInterval(input: String): CalendarInterval = {
    import ParseState._
    def throwIAE(msg: String, e: Exception = null) = {
      throw new IllegalArgumentException(s"Error parsing '$input' to interval, $msg", e)
    }

    if (input == null) {
      throwIAE("interval string cannot be null")
    }
    // scalastyle:off caselocale .toLowerCase
    val s = input.trim().toLowerCase
    // scalastyle:on
    val bytes = s.getBytes(StandardCharsets.UTF_8)
    if (bytes.isEmpty) {
      throwIAE("interval string cannot be empty")
    }
    var state = PREFIX
    var i = 0
    var currentValue: Long = 0
    var isNegative: Boolean = false
    var months: Int = 0
    var days: Int = 0
    var microseconds: Long = 0
    var fractionScale: Int = 0
    val initialFractionScale = (DateTimeConstants.NANOS_PER_SECOND / 10).toInt
    var fraction: Int = 0
    var pointPrefixed: Boolean = false

    def trimToNextState(b: Byte, next: ParseState): Unit = {
      if (Character.isWhitespace(b)) {
        i += 1
      } else {
        state = next
      }
    }

    def currentWord: String = {
      val sep = "\\s+"
      val strings = s.split(sep)
      val lenRight = s.substring(i, s.length).split(sep) .length
      strings(strings.length - lenRight)
    }

    def matchAt(i: Int, str: String): Boolean = {
      if (i + str.length > s.length) {
        false
      } else {
        s.substring(i, i + str.length) == str
      }
    }

    while (i < bytes.length) {
      val b = bytes(i)
      state match {
        case PREFIX =>
          if (s.startsWith(intervalStr)) {
            if (s.length ==
              intervalStr.length) {
              throwIAE("interval string cannot be empty")
            } else if (!Character.isWhitespace(
              bytes(i + intervalStr.length))) {
              throwIAE(s"invalid interval prefix $currentWord")
            } else {
              i += intervalStr.length + 1
            }
          }
          state = TRIM_BEFORE_SIGN
        case TRIM_BEFORE_SIGN => trimToNextState(b, SIGN)
        case SIGN =>
          currentValue = 0
          fraction = 0
          // We preset next state from SIGN to TRIM_BEFORE_VALUE. If we meet '.' in the SIGN state,
          // it means that the interval value we deal with here is a numeric with only fractional
          // part, such as '.11 second', which can be parsed to 0.11 seconds. In this case, we need
          // to reset next state to `VALUE_FRACTIONAL_PART` to go parse the fraction part of the
          // interval value.
          state = TRIM_BEFORE_VALUE
          // We preset the scale to an invalid value to track fraction presence in the UNIT_BEGIN
          // state. If we meet '.', the scale become valid for the VALUE_FRACTIONAL_PART state.
          fractionScale = -1
          pointPrefixed = false
          b match {
            case '-' =>
              isNegative = true
              i += 1
            case '+' =>
              isNegative = false
              i += 1
            case _ if '0' <= b && b <= '9' =>
              isNegative = false
            case '.' =>
              isNegative = false
              fractionScale = initialFractionScale
              pointPrefixed = true
              i += 1
              state = VALUE_FRACTIONAL_PART
            case _ => throwIAE( s"unrecognized number '$currentWord'")
          }
        case TRIM_BEFORE_VALUE => trimToNextState(b, VALUE)
        case VALUE =>
          b match {
            case _ if '0' <= b && b <= '9' =>
              try {
                currentValue = Math.addExact(Math.multiplyExact(10, currentValue), (b - '0'))
              } catch {
                case e: ArithmeticException => throwIAE(e.getMessage, e)
              }
            case _ if Character.isWhitespace(b) => state = TRIM_BEFORE_UNIT
            case '.' =>
              fractionScale = initialFractionScale
              state = VALUE_FRACTIONAL_PART
            case _ => throwIAE(s"invalid value '$currentWord'")
          }
          i += 1
        case VALUE_FRACTIONAL_PART =>
          if ('0' <= b && b <= '9' && fractionScale > 0) {
            fraction += (b - '0') * fractionScale
            fractionScale /= 10
          } else if (Character.isWhitespace(b) &&
            (!pointPrefixed || fractionScale < initialFractionScale)) {
            fraction /= DateTimeConstants.NANOS_PER_MICROS.toInt
            state = TRIM_BEFORE_UNIT
          } else if ('0' <= b && b <= '9') {
            throwIAE(s"interval can only support nanosecond precision, '$currentWord' is out" +
              s" of range")
          } else {
            throwIAE(s"invalid value '$currentWord'")
          }
          i += 1
        case TRIM_BEFORE_UNIT => trimToNextState(b, UNIT_BEGIN)
        case UNIT_BEGIN =>
          // Checks that only seconds can have the fractional part
          if (b != 's' && fractionScale >= 0) {
            throwIAE(s"'$currentWord' cannot have fractional part")
          }
          if (isNegative) {
            currentValue = -currentValue
            fraction = -fraction
          }
          try {
            b match {
              case 'y' if matchAt(i, yearStr) =>
                val monthsInYears = Math.multiplyExact(
                  DateTimeConstants.MONTHS_PER_YEAR,
                  currentValue)
                months = Math.toIntExact(Math.addExact(months, monthsInYears))
                i += yearStr.length
              case 'w' if matchAt(i, weekStr) =>
                val daysInWeeks = Math.multiplyExact(DateTimeConstants.DAYS_PER_WEEK, currentValue)
                days = Math.toIntExact(Math.addExact(days, daysInWeeks))
                i += weekStr.length
              case 'd' if matchAt(i, dayStr) =>
                days = Math.addExact(days, Math.toIntExact(currentValue))
                i += dayStr.length
              case 'h' if matchAt(i, hourStr) =>
                val hoursUs = Math.multiplyExact(currentValue, DateTimeConstants.MICROS_PER_HOUR)
                microseconds = Math.addExact(microseconds, hoursUs)
                i += hourStr.length
              case 's' if matchAt(i, secondStr) =>
                val secondsUs = Math.multiplyExact(
                  currentValue,
                  DateTimeConstants.MICROS_PER_SECOND)
                microseconds = Math.addExact(Math.addExact(microseconds, secondsUs), fraction)
                i += secondStr.length
              case 'm' =>
                if (matchAt(i, monthStr)) {
                  months = Math.addExact(months, Math.toIntExact(currentValue))
                  i += monthStr.length
                } else if (matchAt(i, minuteStr)) {
                  val minutesUs = Math.multiplyExact(
                    currentValue,
                    DateTimeConstants.MICROS_PER_MINUTE)
                  microseconds = Math.addExact(microseconds, minutesUs)
                  i += minuteStr.length
                } else if (matchAt(i, millisStr)) {
                  val millisUs = Math.multiplyExact(
                    currentValue,
                    DateTimeConstants.MICROS_PER_MILLIS)
                  microseconds = Math.addExact(microseconds, millisUs)
                  i += millisStr.length
                } else if (matchAt(i, microsStr)) {
                  microseconds = Math.addExact(microseconds, currentValue)
                  i += microsStr.length
                } else throwIAE(s"invalid unit '$currentWord'")
              case _ => throwIAE(s"invalid unit '$currentWord'")
            }
          } catch {
            case e: ArithmeticException => throwIAE(e.getMessage, e)
          }
          state = UNIT_SUFFIX
        case UNIT_SUFFIX =>
          b match {
            case 's' => state = UNIT_END
            case _ if Character.isWhitespace(b) => state = TRIM_BEFORE_SIGN
            case _ => throwIAE(s"invalid unit '$currentWord'")
          }
          i += 1
        case UNIT_END =>
          if (Character.isWhitespace(b) ) {
            i += 1
            state = TRIM_BEFORE_SIGN
          } else {
            throwIAE(s"invalid unit '$currentWord'")
          }
      }
    }

    val result = state match {
      case UNIT_SUFFIX | UNIT_END | TRIM_BEFORE_SIGN =>
        new CalendarInterval(months, days, microseconds)
      case TRIM_BEFORE_VALUE => throwIAE(s"expect a number after '$currentWord' but hit EOL")
      case VALUE | VALUE_FRACTIONAL_PART =>
        throwIAE(s"expect a unit name after '$currentWord' but hit EOL")
      case _ => throwIAE(s"unknown error when parsing '$currentWord'")
    }

    result
  }
}

