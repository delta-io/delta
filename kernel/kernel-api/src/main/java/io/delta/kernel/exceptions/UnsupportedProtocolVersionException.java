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
  private final int minReaderVersion;
  private final int minWriterVersion;
  private final ProtocolVersionType versionType;

  public UnsupportedProtocolVersionException(
      String tablePath,
      int minReaderVersion,
      int minWriterVersion,
      ProtocolVersionType versionType) {
    super(
        String.format(
            "Unsupported Delta protocol %s version: table `%s` requires %s version %s "
                + "which is unsupported by this version of Delta Kernel.",
            versionType.name().toLowerCase(),
            tablePath,
            versionType.name().toLowerCase(),
            versionType == ProtocolVersionType.READER ? minReaderVersion : minWriterVersion));
    this.tablePath = tablePath;
    this.minReaderVersion = minReaderVersion;
    this.minWriterVersion = minWriterVersion;
    this.versionType = versionType;
  }

  /** @return the table path where the unsupported protocol was encountered */
  public String getTablePath() {
    return tablePath;
  }

  /** @return the table's required minimum reader protocol version */
  public int getMinReaderVersion() {
    return minReaderVersion;
  }

  /** @return the table's required minimum writer protocol version */
  public int getMinWriterVersion() {
    return minWriterVersion;
  }

  /** @return the unsupported protocol version */
  public int getVersion() {
    return versionType == ProtocolVersionType.READER ? minReaderVersion : minWriterVersion;
  }

  /** @return the type of protocol version that is unsupported */
  public ProtocolVersionType getVersionType() {
    return versionType;
  }
}
