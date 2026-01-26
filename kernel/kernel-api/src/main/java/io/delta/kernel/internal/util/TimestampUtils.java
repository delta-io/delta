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
package io.delta.kernel.internal.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Utilities for timestamp conversions that avoid overflow issues.
 *
 * <p>Note: {@code ChronoUnit.MICROS.between()} internally computes {@code (seconds * 1_000_000_000)
 * / 1000}, where the intermediate nanoseconds value overflows for timestamps beyond ~292 years from
 * epoch. These methods compute {@code seconds * 1_000_000} directly to avoid overflow.
 */
public final class TimestampUtils {

  private TimestampUtils() {}

  /** Converts an Instant to microseconds since epoch. */
  public static long toEpochMicros(Instant instant) {
    long microsFromSeconds = Math.multiplyExact(instant.getEpochSecond(), 1_000_000L);
    long microsFromNanos = instant.getNano() / 1000;
    return Math.addExact(microsFromSeconds, microsFromNanos);
  }

  /** Converts an OffsetDateTime to microseconds since epoch. */
  public static long toEpochMicros(OffsetDateTime dateTime) {
    long microsFromSeconds = Math.multiplyExact(dateTime.toEpochSecond(), 1_000_000L);
    long microsFromNanos = dateTime.getNano() / 1000;
    return Math.addExact(microsFromSeconds, microsFromNanos);
  }

  /** Converts a LocalDateTime (interpreted as UTC) to microseconds since epoch. */
  public static long toEpochMicros(LocalDateTime dateTime) {
    long microsFromSeconds = Math.multiplyExact(dateTime.toEpochSecond(ZoneOffset.UTC), 1_000_000L);
    long microsFromNanos = dateTime.getNano() / 1000;
    return Math.addExact(microsFromSeconds, microsFromNanos);
  }
}
