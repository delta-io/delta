/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

/**
 * The class representing calendar intervals. The calendar interval is stored internally in
 * three components.
 * <ul>
 *   <li>an integer value representing the number of `months` in this interval,</li>
 *   <li>an integer value representing the number of `days` in this interval,</li>
 *   <li>a long value representing the number of `microseconds` in this interval.</li>
 * </ul>
 *
 * The `months` and `days` are not units of time with a constant length (unlike hours, seconds), so
 * they are two separated fields from microseconds. One month may be equal to 28, 29, 30 or 31 days
 * and one day may be equal to 23, 24 or 25 hours (daylight saving).
 *
 * @param months  an integer value representing the number of months in this interval
 * @param days  an integer value representing the number of days in this interval
 * @param microseconds  a long value representing the  number of microseconds in this interval
 */
private[internal] case class CalendarInterval(
    val months: Int,
    val days: Int,
    val microseconds: Long)

