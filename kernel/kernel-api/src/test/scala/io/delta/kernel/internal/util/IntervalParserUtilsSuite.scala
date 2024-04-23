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
package io.delta.kernel.internal.util

import io.delta.kernel.internal.util.DateTimeConstants._
import io.delta.kernel.internal.util.IntervalParserUtils.parseIntervalAsMicros
import org.scalatest.funsuite.AnyFunSuite

/**
 * Subset of tests from Apache Spark's `org/apache/spark/sql/catalyst/util/IntervalUtilsSuite.scala`
 */
class IntervalParserUtilsSuite extends AnyFunSuite {
  test("string to interval: basic") {
    testSingleUnit("Week", 3, 21, 0)
    testSingleUnit("DAY", 3, 3, 0)
    testSingleUnit("HouR", 3, 0, 3 * MICROS_PER_HOUR)
    testSingleUnit("MiNuTe", 3, 0, 3 * MICROS_PER_MINUTE)
    testSingleUnit("Second", 3, 0, 3 * MICROS_PER_SECOND)
    testSingleUnit("MilliSecond", 3, 0, 3 * MICROS_PER_MILLIS)
    testSingleUnit("MicroSecond", 3, 0, 3)

    checkFromInvalidString(null, "cannot be null")

    Seq(
      "",
      "interval",
      "foo",
      "foo 1 day",
      "month 3",
      "year 3",
    ).foreach { input =>
      checkFromInvalidString(input, "Error parsing")
    }
  }

  test("string to interval: interval with dangling parts should not results null") {
    checkFromInvalidString("+", "expect a number after '+' but hit EOL")
    checkFromInvalidString("-", "expect a number after '-' but hit EOL")
    checkFromInvalidString("+ 2", "expect a unit name after '2' but hit EOL")
    checkFromInvalidString("- 1", "expect a unit name after '1' but hit EOL")
    checkFromInvalidString("1", "expect a unit name after '1' but hit EOL")
    checkFromInvalidString("1.2", "expect a unit name after '1.2' but hit EOL")
    checkFromInvalidString("1 day 2", "expect a unit name after '2' but hit EOL")
    checkFromInvalidString("1 day 2.2", "expect a unit name after '2.2' but hit EOL")
    checkFromInvalidString("1 day -", "expect a number after '-' but hit EOL")
    checkFromInvalidString("-.", "expect a unit name after '-.' but hit EOL")
  }

  test("string to interval: multiple units") {
    Seq(
      "interval -1 day +3 Microseconds" -> micros(-1, 3),
      "interval -   1 day +     3 Microseconds" -> micros(-1, 3),
      "  interval  123  weeks   -1 day " +
        "23 hours -22 minutes 1 second  -123  millisecond    567 microseconds " ->
        micros(860, 81480877567L)
    ).foreach { case (input, expected) =>
      checkFromString(input, expected)
    }
  }

  test("string to interval: special cases") {
    // Support any order of interval units
    checkFromString("1 microsecond 1 day", micros(1, 1))
    // Allow duplicated units and summarize their values
    checkFromString("1 day 10 day", micros(11, 0))
    // Only the seconds units can have the fractional part
    checkFromInvalidString("1.5 days", "'days' cannot have fractional part")
    checkFromInvalidString("1. hour", "'hour' cannot have fractional part")
    checkFromInvalidString("1 hourX", "invalid unit 'hourx'")
    checkFromInvalidString("~1 hour", "unrecognized number '~1'")
    checkFromInvalidString("1 Mour", "invalid unit 'mour'")
    checkFromInvalidString("1 aour", "invalid unit 'aour'")
    checkFromInvalidString("1a1 hour", "invalid value '1a1'")
    checkFromInvalidString("1.1a1 seconds", "invalid value '1.1a1'")
    checkFromInvalidString("2234567890 days", "integer overflow")
    checkFromInvalidString(". seconds", "invalid value '.'")
  }

  test("string to interval: whitespaces") {
    checkFromInvalidString(" ", "Error parsing ' ' to interval")
    checkFromInvalidString("\n", "Error parsing '\n' to interval")
    checkFromInvalidString("\t", "Error parsing '\t' to interval")
    checkFromString("1 \t day \n 2 \r hour", micros(1, 2 * MICROS_PER_HOUR))
    checkFromInvalidString("interval1 \t day \n 2 \r hour", "invalid interval prefix interval1")
    checkFromString("interval\r1\tday", micros(1, 0))
    // scalastyle:off nonascii
    checkFromInvalidString("中国 interval 1 day", "unrecognized number '中国'")
    checkFromInvalidString("interval浙江 1 day", "invalid interval prefix interval浙江")
    checkFromInvalidString("interval 1杭州 day", "invalid value '1杭州'")
    checkFromInvalidString("interval 1 滨江day", "invalid unit '滨江day'")
    checkFromInvalidString("interval 1 day长河", "invalid unit 'day长河'")
    checkFromInvalidString("interval 1 day 网商路", "unrecognized number '网商路'")
    // scalastyle:on nonascii
  }

  test("string to interval: seconds with fractional part") {
    checkFromString("0.1 seconds", micros(0, 100000))
    checkFromString("1. seconds", micros(0, 1000000))
    checkFromString("123.001 seconds", micros(0, 123001000))
    checkFromString("1.001001 seconds", micros(0, 1001001))
    checkFromString("1 minute 1.001001 seconds", micros(0, 61001001))
    checkFromString("-1.5 seconds", micros(0, -1500000))
    // truncate nanoseconds to microseconds
    checkFromString("0.999999999 seconds", micros(0, 999999))
    checkFromString(".999999999 seconds", micros(0, 999999))
    checkFromInvalidString("0.123456789123 seconds", "'0.123456789123' is out of range")
  }

  private def testSingleUnit(unit: String, number: Int, days: Int, microseconds: Long): Unit = {
    for (prefix <- Seq("interval ", "")) {
      val input1 = prefix + number + " " + unit
      val input2 = prefix + number + " " + unit + "s"
      val result = micros(days, microseconds)
      checkFromString(input1, result)
      checkFromString(input2, result)
    }
  }

  private def checkFromString(input: String, expected: Long): Unit = {
    assert(parseIntervalAsMicros(input) === expected)
  }

  private def checkFromInvalidString(input: String, errorMsg: String): Unit = {
    failFuncWithInvalidInput(input, errorMsg, s => parseIntervalAsMicros(s))
  }

  private def failFuncWithInvalidInput(
    input: String, errorMsg: String, converter: String => Long): Unit = {
    withClue(s"Expected to throw an exception for the invalid input: $input") {
      val e = intercept[IllegalArgumentException](converter(input))
      assert(e.getMessage.contains(errorMsg))
    }
  }

  private def micros(days: Long, microseconds: Long): Long = {
    days * MICROS_PER_DAY + microseconds
  }
}
