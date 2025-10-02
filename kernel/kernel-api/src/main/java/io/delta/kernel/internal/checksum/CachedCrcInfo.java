/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.checksum;

import io.delta.kernel.internal.lang.Lazy;
import java.util.Optional;

/**
 * Represents cached CRC information. This is a wrapper around {@code Optional<Optional<CRCInfo>>}
 * to make the three-state nature more explicit and easier to work with.
 *
 * <p>The three states are:
 *
 * <ul>
 *   <li>Empty: CRC reading was never attempted
 *   <li>Some(Empty): CRC reading was tried but failed (parse error, I/O error, etc.)
 *   <li>Some(Some(Value)): CRC was successfully read and parsed
 * </ul>
 */
public class CachedCrcInfo {

  ////////////////////////////
  // Static factory methods //
  ////////////////////////////

  /** Creates a CachedCrcInfo indicating that CRC reading was not attempted. */
  public static CachedCrcInfo notAttempted() {
    return new CachedCrcInfo(Optional.empty());
  }

  /** Creates a CachedCrcInfo indicating that CRC file was read and parsed. */
  public static CachedCrcInfo success(CRCInfo crcInfo) {
    return new CachedCrcInfo(Optional.of(Optional.of(crcInfo)));
  }

  /** Creates a result from a Lazy CRC computation. */
  public static CachedCrcInfo fromLazy(Lazy<Optional<CRCInfo>> lazyCrcInfo) {
    return lazyCrcInfo.isPresent()
        ? new CachedCrcInfo(Optional.of(lazyCrcInfo.get())) // maybe successful or failed
        : notAttempted();
  }

  /////////////////////////////////
  // Instance fields and methods //
  /////////////////////////////////

  private final Optional<Optional<CRCInfo>> value;

  private CachedCrcInfo(Optional<Optional<CRCInfo>> value) {
    this.value = value;
  }

  /** @return true if CRC was successfully generated */
  public boolean hasParsedCrc() {
    return value.isPresent() && value.get().isPresent();
  }

  /** @return true if CRC reading was attempted but failed */
  public boolean didCrcReadFail() {
    return value.isPresent() && !value.get().isPresent();
  }

  /** @return true if CRC reading was attempted (regardless of success/fail) */
  public boolean wasCrcReadAttempted() {
    return value.isPresent();
  }

  /**
   * @return The value of the CRC read attempt.
   * @throws java.util.NoSuchElementException if CRC reading was never attempted. Callers must be
   *     sure to check {@link #wasCrcReadAttempted()} before invoking this method.
   */
  public Optional<CRCInfo> getCrcReadAttemptResult() {
    return value.get();
  }
}
