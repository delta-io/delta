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
package io.delta.kernel.internal.replay;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.types.StructType;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CRCInfo {
  private static final Logger logger = LoggerFactory.getLogger(CRCInfo.class);

  public static Optional<CRCInfo> fromColumnarBatch(
      Engine engine, long version, ColumnarBatch batch, int rowId, String crcFilePath) {
    Protocol protocol = Protocol.fromColumnVector(batch.getColumnVector(PROTOCOL_ORDINAL), rowId);
    Metadata metadata = Metadata.fromColumnVector(batch.getColumnVector(METADATA_ORDINAL), rowId);
    //  protocol and metadata are nullable per fromColumnVector's implementation.
    if (protocol == null || metadata == null) {
      logger.warn("Invalid checksum file missing protocol and/or metadata: {}", crcFilePath);
      return Optional.empty();
    }
    return Optional.of(new CRCInfo(version, metadata, protocol));
  }

  // We can add additional fields later
  public static final StructType FULL_SCHEMA =
      new StructType().add("protocol", Protocol.FULL_SCHEMA).add("metadata", Metadata.FULL_SCHEMA);

  private static final int PROTOCOL_ORDINAL = 0;
  private static final int METADATA_ORDINAL = 1;

  private final long version;
  private final Metadata metadata;
  private final Protocol protocol;

  protected CRCInfo(long version, Metadata metadata, Protocol protocol) {
    this.version = version;
    this.metadata = requireNonNull(metadata);
    this.protocol = requireNonNull(protocol);
  }

  /** The version of the Delta table that this VersionStats represents. */
  public long getVersion() {
    return version;
  }

  /** The {@link Metadata} stored in this VersionStats. May be null. */
  public Metadata getMetadata() {
    return metadata;
  }

  /** The {@link Protocol} stored in this VersionStats. May be null. */
  public Protocol getProtocol() {
    return protocol;
  }
}
