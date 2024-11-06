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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import java.util.Optional;

/** Contains summary information of a {@link io.delta.kernel.Snapshot}. */
public class SnapshotDescriptor {
  private final long version;
  private final Protocol protocol;
  private final Metadata metadata;

  /** Should be non-empty if and only if ICT is enabled. */
  private final Optional<Long> inCommitTimestampOpt;

  public SnapshotDescriptor(
      long version, Protocol protocol, Metadata metadata, Optional<Long> inCommitTimestampOpt) {
    requireNonNull(protocol, "protocol is null");
    requireNonNull(metadata, "metadata is null");
    requireNonNull(inCommitTimestampOpt, "inCommitTimestampOpt is null");

    if (TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(metadata)) {
      checkArgument(
          inCommitTimestampOpt.isPresent(),
          "ICT is enabled but inCommitTimestampOpt is not present");
      this.inCommitTimestampOpt = inCommitTimestampOpt;
    } else {
      this.inCommitTimestampOpt = Optional.empty();
    }

    this.version = version;
    this.protocol = protocol;
    this.metadata = metadata;
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

  public Optional<Long> getInCommitTimestampOpt() {
    return inCommitTimestampOpt;
  }
}
