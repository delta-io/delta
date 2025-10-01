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
 * Represents the result of attempting to read and cache CRC information. This is a simple wrapper
 * around Optional<Optional<CRCInfo>> to make the three-state nature more explicit and easier to
 * work with.
 *
 * <p>The three states are:
 *
 * <ul>
 *   <li>Not attempted - CRC reading was never tried
 *   <li>Attempted with failure - CRC reading was tried but failed (parse error, I/O error, etc.)
 *   <li>Attempted with success - CRC was successfully read and parsed
 * </ul>
 */
public class CachedCrcInfoResult {

  ////////////////////////////
  // Static factory methods //
  ////////////////////////////

  /** Creates a result indicating CRC reading was not attempted. */
  public static CachedCrcInfoResult notAttempted() {
    return new CachedCrcInfoResult(Optional.empty());
  }

  /** Creates a result from a successfully read CRC file. */
  public static CachedCrcInfoResult success(CRCInfo crcInfo) {
    return new CachedCrcInfoResult(Optional.of(Optional.of(crcInfo)));
  }

  /** Creates a result from a Lazy CRC computation. */
  public static CachedCrcInfoResult fromLazy(Lazy<Optional<CRCInfo>> lazyCrcInfo) {
    return lazyCrcInfo.isPresent()
        ? new CachedCrcInfoResult(Optional.of(lazyCrcInfo.get()))
        : notAttempted();
  }

  /////////////////////////////////
  // Instance fields and methods //
  /////////////////////////////////

  private final Optional<Optional<CRCInfo>> value;

  private CachedCrcInfoResult(Optional<Optional<CRCInfo>> value) {
    this.value = value;
  }

  /** @return true if CRC reading was attempted (regardless of success) */
  public boolean wasAttempted() {
    return value.isPresent();
  }

  /** @return true if CRC was successfully read */
  public boolean wasSuccessful() {
    return value.isPresent() && value.get().isPresent();
  }

  /** @return The underlying Optional<Optional<CRCInfo>> for compatibility */
  public Optional<Optional<CRCInfo>> getValue() {
    return value;
  }

  /** @return The CRC info if successfully read, empty otherwise */
  public Optional<CRCInfo> getCrcInfo() {
    return value.orElse(Optional.empty());
  }
}
