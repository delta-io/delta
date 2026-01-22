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
package io.delta.kernel.exceptions;

import io.delta.kernel.annotation.Evolving;

/**
 * Exception thrown when Kernel encounters unsupported protocol versions.
 *
 * @since 4.1.0
 */
@Evolving
public class UnsupportedProtocolVersionException extends KernelException {

  /** Enum representing the type of Delta protocol version. */
  public enum ProtocolVersionType {
    /** Reader protocol version */
    READER,
    /** Writer protocol version */
    WRITER
  }

  private final String tablePath;
  private final int version;
  private final ProtocolVersionType versionType;

  public UnsupportedProtocolVersionException(
      String tablePath, int version, ProtocolVersionType versionType) {
    super(
        String.format(
            "Unsupported Delta protocol %s version: table `%s` requires %s version %s "
                + "which is unsupported by this version of Delta Kernel.",
            versionType.name().toLowerCase(),
            tablePath,
            versionType.name().toLowerCase(),
            version));
    this.tablePath = tablePath;
    this.version = version;
    this.versionType = versionType;
  }

  /** @return the table path where the unsupported protocol was encountered */
  public String getTablePath() {
    return tablePath;
  }

  /** @return the unsupported protocol version */
  public int getVersion() {
    return version;
  }

  /** @return the type of protocol version (READER or WRITER) */
  public ProtocolVersionType getVersionType() {
    return versionType;
  }
}
