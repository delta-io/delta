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
import io.delta.kernel.internal.data.StructRow;
import io.delta.kernel.internal.stats.FileSizeHistogram;
import io.delta.kernel.internal.util.InternalUtils;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CRCInfo {
  private static final Logger logger = LoggerFactory.getLogger(CRCInfo.class);

  // Constants for schema field names
  private static final String TABLE_SIZE_BYTES = "tableSizeBytes";
  private static final String NUM_FILES = "numFiles";
  private static final String NUM_METADATA = "numMetadata";
  private static final String NUM_PROTOCOL = "numProtocol";
  private static final String METADATA = "metadata";
  private static final String PROTOCOL = "protocol";
  private static final String TXN_ID = "txnId";
  private static final String DOMAIN_METADATA = "domainMetadata";
  private static final String FILE_SIZE_HISTOGRAM = "fileSizeHistogram";

  public static final StructType CRC_FILE_SCHEMA =
      new StructType()
          .add(TABLE_SIZE_BYTES, LongType.LONG)
          .add(NUM_FILES, LongType.LONG)
          .add(NUM_METADATA, LongType.LONG)
          .add(NUM_PROTOCOL, LongType.LONG)
          .add(METADATA, Metadata.FULL_SCHEMA)
          .add(PROTOCOL, Protocol.FULL_SCHEMA)
          .add(TXN_ID, StringType.STRING, /*nullable*/ true)
          .add(DOMAIN_METADATA, new ArrayType(DomainMetadata.FULL_SCHEMA, false), /*nullable*/ true)
          .add(FILE_SIZE_HISTOGRAM, FileSizeHistogram.FULL_SCHEMA, /*nullable*/ true);

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
            : Optional.of(
                VectorUtils.toJavaList(domainMetadataVector.getArray(rowId)).stream()
                    .map(row -> DomainMetadata.fromRow((StructRow) row))
                    .collect(Collectors.toList()));
    Optional<FileSizeHistogram> fileSizeHistogram =
        FileSizeHistogram.fromColumnVector(
            batch.getColumnVector(getSchemaIndex(FILE_SIZE_HISTOGRAM)), rowId);

    //  protocol and metadata are nullable per fromColumnVector's implementation.
    if (protocol == null || metadata == null) {
      logger.warn("Invalid checksum file missing protocol and/or metadata: {}", crcFilePath);
      return Optional.empty();
    }
    return Optional.of(
        new CRCInfo(
            version,
            metadata,
            protocol,
            tableSizeBytes,
            numFiles,
            txnId,
            domainMetadata,
            fileSizeHistogram));
  }

  private final long version;
  private final Metadata metadata;
  private final Protocol protocol;
  private final long tableSizeBytes;
  private final long numFiles;
  private final Optional<String> txnId;
  private final Optional<List<DomainMetadata>> domainMetadata;
  private final Optional<FileSizeHistogram> fileSizeHistogram;

  public CRCInfo(
      long version,
      Metadata metadata,
      Protocol protocol,
      long tableSizeBytes,
      long numFiles,
      Optional<String> txnId,
      Optional<List<DomainMetadata>> domainMetadata,
      Optional<FileSizeHistogram> fileSizeHistogram) {
    checkArgument(tableSizeBytes >= 0);
    checkArgument(numFiles >= 0);
    // Live Domain Metadata actions at this version, excluding tombstones.
    domainMetadata.ifPresent(dms -> dms.forEach(dm -> checkArgument(!dm.isRemoved())));
    this.version = version;
    this.metadata = requireNonNull(metadata);
    this.protocol = requireNonNull(protocol);
    this.tableSizeBytes = tableSizeBytes;
    this.numFiles = numFiles;
    this.txnId = requireNonNull(txnId);
    this.domainMetadata = requireNonNull(domainMetadata);
    this.fileSizeHistogram = requireNonNull(fileSizeHistogram);
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

  public Optional<List<DomainMetadata>> getDomainMetadata() {
    return domainMetadata;
  }

  /** The {@link FileSizeHistogram} stored in this CRCInfo. */
  public Optional<FileSizeHistogram> getFileSizeHistogram() {
    return fileSizeHistogram;
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
    txnId.ifPresent(txn -> values.put(getSchemaIndex(TXN_ID), txn));
    domainMetadata.ifPresent(
        domainMetadataList ->
            values.put(
                getSchemaIndex(DOMAIN_METADATA),
                VectorUtils.buildArrayValue(
                    domainMetadataList.stream()
                        .map(DomainMetadata::toRow)
                        .collect(Collectors.toList()),
                    DomainMetadata.FULL_SCHEMA)));
    fileSizeHistogram.ifPresent(
        fileSizeHistogram ->
            values.put(getSchemaIndex(FILE_SIZE_HISTOGRAM), fileSizeHistogram.toRow()));
    return new GenericRow(CRC_FILE_SCHEMA, values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        version,
        metadata,
        protocol,
        tableSizeBytes,
        numFiles,
        txnId,
        domainMetadata,
        fileSizeHistogram);
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
        && txnId.equals(other.txnId)
        && domainMetadata.equals(other.domainMetadata)
        && fileSizeHistogram.equals(other.fileSizeHistogram);
  }

  private static int getSchemaIndex(String fieldName) {
    return CRC_FILE_SCHEMA.indexOf(fieldName);
  }
}
