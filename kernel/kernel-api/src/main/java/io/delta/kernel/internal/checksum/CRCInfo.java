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
package io.delta.kernel.internal.checksum;

import static io.delta.kernel.internal.checksum.ChecksumUtils.CRC_FILE_SCHEMA;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CRCInfo {
  private static final Logger logger = LoggerFactory.getLogger(CRCInfo.class);

  public static Optional<CRCInfo> fromColumnarBatch(
      long version, ColumnarBatch batch, int rowId, String crcFilePath) {
    Protocol protocol =
        Protocol.fromColumnVector(
            batch.getColumnVector(CRC_FILE_SCHEMA.indexOf("protocol")), rowId);
    Metadata metadata =
        Metadata.fromColumnVector(
            batch.getColumnVector(CRC_FILE_SCHEMA.indexOf("metadata")), rowId);
    long tableSizeBytes =
        batch.getColumnVector(CRC_FILE_SCHEMA.indexOf("tableSizeBytes")).getLong(rowId);
    long numFiles = batch.getColumnVector(CRC_FILE_SCHEMA.indexOf("numFiles")).getLong(rowId);
    //  protocol and metadata are nullable per fromColumnVector's implementation.
    if (protocol == null || metadata == null) {
      logger.warn("Invalid checksum file missing protocol and/or metadata: {}", crcFilePath);
      return Optional.empty();
    }
    return Optional.of(new CRCInfo(version, metadata, protocol, tableSizeBytes, numFiles));
  }

  private final long version;
  private final Metadata metadata;
  private final Protocol protocol;
  private final long tableSizeBytes;
  private final long numFiles;

  protected CRCInfo(
      long version, Metadata metadata, Protocol protocol, long tableSizeBytes, long numFiles) {
    this.version = version;
    this.metadata = requireNonNull(metadata);
    this.protocol = requireNonNull(protocol);
    this.tableSizeBytes = tableSizeBytes;
    this.numFiles = numFiles;
  }

  /** The version of the Delta table that this CRCInfo represents. */
  public long getVersion() {
    return version;
  }

  /** The {@link Metadata} stored in this CRCInfo. */
  public Metadata getMetadata() {
    return metadata;
  }

  /** The {@link Protocol} stored in this CRCInfo. */
  public Protocol getProtocol() {
    return protocol;
  }

  public long getNumFiles() {
    return numFiles;
  }

  public long getTableSizeBytes() {
    return tableSizeBytes;
  }
}
