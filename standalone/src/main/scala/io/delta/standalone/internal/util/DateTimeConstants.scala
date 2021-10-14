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

private[internal] object DateTimeConstants {

  val MONTHS_PER_YEAR = 12

  val DAYS_PER_WEEK = 7

  val HOURS_PER_DAY = 24L

  val MINUTES_PER_HOUR = 60L

  val SECONDS_PER_MINUTE = 60L
  val SECONDS_PER_HOUR: Long = MINUTES_PER_HOUR * SECONDS_PER_MINUTE
  val SECONDS_PER_DAY: Long = HOURS_PER_DAY * SECONDS_PER_HOUR

  val MILLIS_PER_SECOND = 1000L
  val MILLIS_PER_MINUTE: Long = SECONDS_PER_MINUTE * MILLIS_PER_SECOND
  val MILLIS_PER_HOUR: Long = MINUTES_PER_HOUR * MILLIS_PER_MINUTE
  val MILLIS_PER_DAY: Long = HOURS_PER_DAY * MILLIS_PER_HOUR

  val MICROS_PER_MILLIS = 1000L
  val MICROS_PER_SECOND: Long = MILLIS_PER_SECOND * MICROS_PER_MILLIS
  val MICROS_PER_MINUTE: Long = SECONDS_PER_MINUTE * MICROS_PER_SECOND
  val MICROS_PER_HOUR: Long = MINUTES_PER_HOUR * MICROS_PER_MINUTE
  val MICROS_PER_DAY: Long = HOURS_PER_DAY * MICROS_PER_HOUR

  val NANOS_PER_MICROS = 1000L
  val NANOS_PER_MILLIS: Long = MICROS_PER_MILLIS * NANOS_PER_MICROS
  val NANOS_PER_SECOND: Long = MILLIS_PER_SECOND * NANOS_PER_MILLIS

}
