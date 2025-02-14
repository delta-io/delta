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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CRCInfo {
  private static final Logger logger = LoggerFactory.getLogger(CRCInfo.class);

  // Constants for schema field names
  public static final String TABLE_SIZE_BYTES = "tableSizeBytes";
  public static final String NUM_FILES = "numFiles";
  public static final String NUM_METADATA = "numMetadata";
  public static final String NUM_PROTOCOL = "numProtocol";
  public static final String METADATA = "metadata";
  public static final String PROTOCOL = "protocol";
  public static final String TXN_ID = "txnId";

  public static final StructType CRC_FILE_SCHEMA =
      new StructType()
          .add(TABLE_SIZE_BYTES, LongType.LONG)
          .add(NUM_FILES, LongType.LONG)
          .add(NUM_METADATA, LongType.LONG)
          .add(NUM_PROTOCOL, LongType.LONG)
          .add(METADATA, Metadata.FULL_SCHEMA)
          .add(PROTOCOL, Protocol.FULL_SCHEMA)
          .add(TXN_ID, StringType.STRING, /*nullable*/ true);

  public static Optional<CRCInfo> fromColumnarBatch(
      long version, ColumnarBatch batch, int rowId, String crcFilePath) {
    Protocol protocol =
        Protocol.fromColumnVector(batch.getColumnVector(CRC_FILE_SCHEMA.indexOf(PROTOCOL)), rowId);
    Metadata metadata =
        Metadata.fromColumnVector(batch.getColumnVector(CRC_FILE_SCHEMA.indexOf(METADATA)), rowId);
    long tableSizeBytes =
        batch.getColumnVector(CRC_FILE_SCHEMA.indexOf(TABLE_SIZE_BYTES)).getLong(rowId);
    long numFiles = batch.getColumnVector(CRC_FILE_SCHEMA.indexOf(NUM_FILES)).getLong(rowId);
    Optional<String> txnId =
        Optional.ofNullable(
            batch.getColumnVector(CRC_FILE_SCHEMA.indexOf(TXN_ID)).getString(rowId));
    //  protocol and metadata are nullable per fromColumnVector's implementation.
    if (protocol == null || metadata == null) {
      logger.warn("Invalid checksum file missing protocol and/or metadata: {}", crcFilePath);
      return Optional.empty();
    }
    return Optional.of(new CRCInfo(version, metadata, protocol, tableSizeBytes, numFiles, txnId));
  }

  private final long version;
  private final Metadata metadata;
  private final Protocol protocol;
  private final long tableSizeBytes;
  private final long numFiles;
  private final Optional<String> txnId;

  public CRCInfo(
      long version,
      Metadata metadata,
      Protocol protocol,
      long tableSizeBytes,
      long numFiles,
      Optional<String> txnId) {
    this.version = version;
    this.metadata = requireNonNull(metadata);
    this.protocol = requireNonNull(protocol);
    this.tableSizeBytes = tableSizeBytes;
    this.numFiles = numFiles;
    this.txnId = txnId;
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

  public Optional<String> getTxnId() {
    return txnId;
  }
}
