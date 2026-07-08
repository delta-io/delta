/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.sparkuctest.mock;

import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.server.service.credential.CredentialContext;

/**
 * Base for the server-side test credential generators. Each call mints a credential stamped with
 * the current time and valid for {@code [mint, mint + duration)} (so the paired {@link
 * CredentialTestFileSystem} can parse the mint timestamp back out and check validity).
 *
 * <p>The UC server instantiates generators reflectively with no access to Hadoop conf, so the clock
 * and validity duration are supplied through JVM-global config set by the test:
 *
 * <ul>
 *   <li><b>Manual clock, short duration</b> (renewal suites): {@link #useManualClock} points the
 *       generator at the same named in-JVM manual clock the connector resolves, so credentials
 *       rotate deterministically as the test advances the clock past each expiry.
 *   <li><b>System clock, long duration</b> (default; general suites): the credential simply stays
 *       valid for the test's duration, so a real vended credential is exercised without racing its
 *       expiry.
 * </ul>
 *
 * <p>Suites run sequentially within a forked JVM, so a test sets its mode in {@code @BeforeAll} and
 * restores the default in {@code @AfterAll} without racing another suite.
 *
 * @param <T> the cloud-specific credential type
 */
public abstract class TimeBasedCredGenerator<T> {
  /** Long validity duration for the system-clock (general) mode: a test never outlives its cred. */
  public static final long SYSTEM_CLOCK_DURATION_MILLIS = 3_600_000L;

  // JVM-global config (volatile: set on the driver thread, read on the server thread in the same
  // JVM). Default is the general-suite mode: system clock + long duration.
  private static volatile String clockName = null; // null => system clock
  private static volatile long durationMillis = SYSTEM_CLOCK_DURATION_MILLIS;

  /** Generate against the named in-JVM manual clock with the given validity duration (renewal). */
  public static void useManualClock(String name, long duration) {
    clockName = name;
    durationMillis = duration;
  }

  /**
   * Restore the default: system clock with {@link #SYSTEM_CLOCK_DURATION_MILLIS} (general suites).
   */
  public static void useSystemClock() {
    clockName = null;
    durationMillis = SYSTEM_CLOCK_DURATION_MILLIS;
  }

  public T generate(CredentialContext ignored) {
    Clock clock = clockName != null ? Clock.getManualClock(clockName) : Clock.systemClock();
    // The credential is valid from the instant it is minted for the configured duration. The
    // connector caches it until (near) expiry, so every access within its lifetime sees the same
    // mint timestamp -- which the filesystem uses both to check validity and, by counting distinct
    // mint timestamps, to count renewals.
    long mintMillis = clock.now().toEpochMilli();
    return newTimeBasedCred(mintMillis, durationMillis);
  }

  /**
   * Builds a credential minted at {@code mintMillis}, valid for {@code [mintMillis, +durMillis)}.
   */
  protected abstract T newTimeBasedCred(long mintMillis, long durMillis);
}
