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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.InternalUtils;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CRCInfo {
  private static final Logger logger = LoggerFactory.getLogger(CRCInfo.class);

  // Constants for schema field names
  private static final String TABLE_SIZE_BYTES = "tableSizeBytes";
  private static final String NUM_FILES = "numFiles";
  private static final String NUM_METADATA = "numMetadata";
  private static final String NUM_PROTOCOL = "numProtocol";
  private static final String DOMAIN_METADATA = "domainMetadata";
  private static final String METADATA = "metadata";
  private static final String PROTOCOL = "protocol";
  private static final String TXN_ID = "txnId";

  public static final StructType CRC_FILE_SCHEMA =
      new StructType()
          .add(TABLE_SIZE_BYTES, LongType.LONG)
          .add(NUM_FILES, LongType.LONG)
          .add(NUM_METADATA, LongType.LONG)
          .add(NUM_PROTOCOL, LongType.LONG)
          .add(METADATA, Metadata.FULL_SCHEMA)
          .add(PROTOCOL, Protocol.FULL_SCHEMA)
          .add(DOMAIN_METADATA, new ArrayType(DomainMetadata.FULL_SCHEMA, false), /*nullable*/ true)
          .add(TXN_ID, StringType.STRING, /*nullable*/ true);

  public static Optional<CRCInfo> fromColumnarBatch(
      long version, ColumnarBatch batch, int rowId, String crcFilePath) {
    // Read required fields.
    Protocol protocol =
        Protocol.fromColumnVector(batch.getColumnVector(getSchemaIndex(PROTOCOL)), rowId);
    Metadata metadata =
        Metadata.fromColumnVector(batch.getColumnVector(getSchemaIndex(METADATA)), rowId);
    long tableSizeBytes =
        InternalUtils.requireNonNull(
                batch.getColumnVector(getSchemaIndex(TABLE_SIZE_BYTES)), rowId, TABLE_SIZE_BYTES)
            .getLong(rowId);
    long numFiles =
        InternalUtils.requireNonNull(
                batch.getColumnVector(getSchemaIndex(NUM_FILES)), rowId, NUM_FILES)
            .getLong(rowId);

    // Read optional fields
    ColumnVector txnIdColumnVector = batch.getColumnVector(getSchemaIndex(TXN_ID));
    Optional<String> txnId =
        txnIdColumnVector.isNullAt(rowId)
            ? Optional.empty()
            : Optional.of(txnIdColumnVector.getString(rowId));

    ColumnVector domainMetadataVector = batch.getColumnVector(getSchemaIndex(DOMAIN_METADATA));
    Optional<List<DomainMetadata>> domainMetadata =
        domainMetadataVector.isNullAt(rowId)
            ? Optional.empty()
            : Optional.of(VectorUtils.toJavaList(domainMetadataVector.getArray(rowId)));

    //  protocol and metadata are nullable per fromColumnVector's implementation.
    if (protocol == null || metadata == null) {
      logger.warn("Invalid checksum file missing protocol and/or metadata: {}", crcFilePath);
      return Optional.empty();
    }
    return Optional.of(
        new CRCInfo(version, metadata, protocol, tableSizeBytes, numFiles, domainMetadata, txnId));
  }

  private final long version;
  private final Metadata metadata;
  private final Protocol protocol;
  private final long tableSizeBytes;
  private final long numFiles;
  private final Optional<List<DomainMetadata>> domainMetadata;
  private final Optional<String> txnId;

  public CRCInfo(
      long version,
      Metadata metadata,
      Protocol protocol,
      long tableSizeBytes,
      long numFiles,
      Optional<List<DomainMetadata>> domainMetadata,
      Optional<String> txnId) {
    checkArgument(tableSizeBytes >= 0);
    checkArgument(numFiles >= 0);
    this.version = version;
    this.metadata = requireNonNull(metadata);
    this.protocol = requireNonNull(protocol);
    this.tableSizeBytes = tableSizeBytes;
    this.numFiles = numFiles;
    this.domainMetadata = domainMetadata;
    this.txnId = requireNonNull(txnId);
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

  public Optional<List<DomainMetadata>> getDomainMetadata() {
    return domainMetadata;
  }

  public Optional<String> getTxnId() {
    return txnId;
  }

  /**
   * Encode as a {@link Row} object with the schema {@link CRCInfo#CRC_FILE_SCHEMA}.
   *
   * @return {@link Row} object with the schema {@link CRCInfo#CRC_FILE_SCHEMA}
   */
  public Row toRow() {
    Map<Integer, Object> values = new HashMap<>();
    // Add required fields
    values.put(getSchemaIndex(TABLE_SIZE_BYTES), tableSizeBytes);
    values.put(getSchemaIndex(NUM_FILES), numFiles);
    values.put(getSchemaIndex(NUM_METADATA), 1L);
    values.put(getSchemaIndex(NUM_PROTOCOL), 1L);
    values.put(getSchemaIndex(METADATA), metadata.toRow());
    values.put(getSchemaIndex(PROTOCOL), protocol.toRow());

    // Add optional fields
    domainMetadata.ifPresent(
        domainMetadataList ->
            values.put(
                getSchemaIndex(DOMAIN_METADATA),
                VectorUtils.buildArrayValue(domainMetadataList, DomainMetadata.FULL_SCHEMA)));
    txnId.ifPresent(txn -> values.put(getSchemaIndex(TXN_ID), txn));
    return new GenericRow(CRC_FILE_SCHEMA, values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        version, metadata, protocol, tableSizeBytes, numFiles, domainMetadata, txnId);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CRCInfo)) {
      return false;
    }
    CRCInfo other = (CRCInfo) o;
    return version == other.version
        && tableSizeBytes == other.tableSizeBytes
        && numFiles == other.numFiles
        && metadata.equals(other.metadata)
        && protocol.equals(other.protocol)
        && domainMetadata.equals(other.domainMetadata)
        && txnId.equals(other.txnId);
  }

  private static int getSchemaIndex(String fieldName) {
    return CRC_FILE_SCHEMA.indexOf(fieldName);
  }
}
