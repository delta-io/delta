/*
 * Copyright (2023) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.snapshot;

import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.checksum.CRCInfo;
import java.util.Optional;
import java.util.Set;

/** Contains summary information of a {@link io.delta.kernel.Snapshot}. */
public class SnapshotHint {
  private final long version;
  private final Protocol protocol;
  private final Metadata metadata;
  private final Optional<Set<DomainMetadata>> domainMetadata;

  /** Constructs a new SnapshotHint based on a CRCInfo */
  public static SnapshotHint fromCrcInfo(CRCInfo crcInfo) {
    return new SnapshotHint(
        crcInfo.getVersion(),
        crcInfo.getProtocol(),
        crcInfo.getMetadata(),
        crcInfo.getDomainMetadata());
  }

  public SnapshotHint(
      long version,
      Protocol protocol,
      Metadata metadata,
      Optional<Set<DomainMetadata>> domainMetadata) {
    this.version = version;
    this.protocol = protocol;
    this.metadata = metadata;
    this.domainMetadata = domainMetadata;
  }

  public long getVersion() {
    return version;
  }

  public Protocol getProtocol() {
    return protocol;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public Optional<Set<DomainMetadata>> getDomainMetadata() {
    return domainMetadata;
  }
}
