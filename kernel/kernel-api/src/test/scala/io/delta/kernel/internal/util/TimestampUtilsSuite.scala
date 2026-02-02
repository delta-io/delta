/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import java.time.{Instant, LocalDateTime, OffsetDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit

import org.scalatest.funsuite.AnyFunSuite

class TimestampUtilsSuite extends AnyFunSuite {
  // Expected micros for 9999-12-31T23:59:59Z
  private val FAR_FUTURE_MICROS = 253402300799000000L

  test("toEpochMicros(Instant) - epoch") {
    assert(TimestampUtils.toEpochMicros(Instant.EPOCH) === 0L)
  }

  test("toEpochMicros(Instant) - far future timestamp does not overflow") {
    val instant = Instant.parse("9999-12-31T23:59:59Z")
    assert(TimestampUtils.toEpochMicros(instant) === FAR_FUTURE_MICROS)
  }

  test("toEpochMicros(Instant) - preserves microsecond precision") {
    // 1000 seconds + 123456 microseconds (123456000 nanoseconds)
    val instant = Instant.ofEpochSecond(1000, 123456000)
    assert(TimestampUtils.toEpochMicros(instant) === 1000123456L)
  }

  test("toEpochMicros(OffsetDateTime) - epoch") {
    val dt = OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)
    assert(TimestampUtils.toEpochMicros(dt) === 0L)
  }

  test("toEpochMicros(OffsetDateTime) - far future timestamp does not overflow") {
    val dt = OffsetDateTime.parse("9999-12-31T23:59:59Z")
    assert(TimestampUtils.toEpochMicros(dt) === FAR_FUTURE_MICROS)
  }

  test("toEpochMicros(LocalDateTime) - epoch") {
    val dt = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC)
    assert(TimestampUtils.toEpochMicros(dt) === 0L)
  }

  test("toEpochMicros(LocalDateTime) - far future timestamp does not overflow") {
    val dt = LocalDateTime.parse("9999-12-31T23:59:59")
    assert(TimestampUtils.toEpochMicros(dt) === FAR_FUTURE_MICROS)
  }

  test("toEpochMicros(LocalDateTime) - preserves microsecond precision") {
    // 1000 seconds + 123456 microseconds (123456000 nanoseconds)
    val dt = LocalDateTime.ofEpochSecond(1000, 123456000, ZoneOffset.UTC)
    assert(TimestampUtils.toEpochMicros(dt) === 1000123456L)
  }

  test("ChronoUnit.MICROS.between() throws for far-future timestamps") {
    // This test documents why we need TimestampUtils instead of ChronoUnit.MICROS.between().
    // ChronoUnit.MICROS.between() internally computes (seconds * 1_000_000_000) / 1000,
    // where the intermediate nanoseconds value overflows for timestamps beyond ~292 years
    // from epoch.
    val farFutureInstant = Instant.parse("9999-12-31T23:59:59Z")

    // ChronoUnit throws ArithmeticException due to overflow
    intercept[ArithmeticException] {
      ChronoUnit.MICROS.between(Instant.EPOCH, farFutureInstant)
    }

    // TimestampUtils returns correct value
    assert(TimestampUtils.toEpochMicros(farFutureInstant) === FAR_FUTURE_MICROS)
  }
}
