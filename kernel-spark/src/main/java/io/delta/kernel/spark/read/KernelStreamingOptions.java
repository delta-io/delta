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
package io.delta.kernel.spark.read;

import java.sql.Timestamp;
import java.util.Map;

/**
 * Kernel-native options parser for Delta streaming. This is a simplified version that doesn't
 * depend on the main Delta Lake classes.
 */
public class KernelStreamingOptions {

  public static final String STARTING_VERSION_OPTION = "startingVersion";
  public static final String STARTING_TIMESTAMP_OPTION = "startingTimestamp";

  private final Map<String, String> options;

  public KernelStreamingOptions(Map<String, String> options) {
    this.options = options;
  }

  /**
   * Get the starting version if specified.
   *
   * @return Starting version, or null if not specified or "latest"
   */
  public Long getStartingVersion() {
    String versionStr = options.get(STARTING_VERSION_OPTION);
    if (versionStr == null) {
      return null;
    }

    if ("latest".equalsIgnoreCase(versionStr)) {
      return -1L; // Special marker for latest
    }

    try {
      return Long.parseLong(versionStr);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid starting version: " + versionStr + ". Must be a number or 'latest'");
    }
  }

  /**
   * Get the starting timestamp if specified.
   *
   * @return Starting timestamp, or null if not specified
   */
  public Timestamp getStartingTimestamp() {
    String timestampStr = options.get(STARTING_TIMESTAMP_OPTION);
    if (timestampStr == null) {
      return null;
    }

    try {
      return Timestamp.valueOf(timestampStr);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid timestamp format: "
              + timestampStr
              + ". Expected format: yyyy-MM-dd HH:mm:ss[.S]",
          e);
    }
  }

  /** Check if starting version is specified. */
  public boolean hasStartingVersion() {
    return options.containsKey(STARTING_VERSION_OPTION);
  }

  /** Check if starting timestamp is specified. */
  public boolean hasStartingTimestamp() {
    return options.containsKey(STARTING_TIMESTAMP_OPTION);
  }
}
