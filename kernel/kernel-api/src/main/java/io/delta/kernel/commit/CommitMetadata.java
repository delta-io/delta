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

package io.delta.kernel.commit;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.annotation.Experimental;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Optional;

/**
 * Contains all information required to commit changes to a Delta table. // TODO: out of date
 *
 * <p>The {@code CommitPayload} encapsulates the complete state needed for a Delta commit operation,
 * including all Delta actions, metadata changes, and contextual information. This payload is passed
 * from Kernel to {@link Committer} implementations, which use the information to perform
 * catalog-specific or filesystem-specific commit operations.
 *
 * <p><strong>Important:</strong> The actions iterator should only be consumed once. Committer
 * implementations must materialize actions if multiple passes are needed.
 */
@Experimental
public class CommitMetadata {

  private final long version;
  private final String logPath;
  private final CommitInfo commitInfo;
  private final Optional<Protocol> readProtocolOpt;
  private final Optional<Metadata> readMetadataOpt;
  private final Optional<Protocol> newProtocolOpt;
  private final Optional<Metadata> newMetadataOpt;

  public CommitMetadata(
      long version,
      String logPath,
      CloseableIterator<Row> finalizedActions,
      CommitInfo commitInfo,
      Optional<Protocol> readProtocolOpt,
      Optional<Metadata> readMetadataOpt,
      Optional<Protocol> newProtocolOpt,
      Optional<Metadata> newMetadataOpt) {
    checkArgument(version >= 0, "version must be non-negative: %d", version);
    this.version = version;
    this.logPath = requireNonNull(logPath, "logPath is null");
    this.commitInfo = requireNonNull(commitInfo, "commitInfo is null");
    this.readProtocolOpt = requireNonNull(readProtocolOpt, "readProtocolOpt is null");
    this.readMetadataOpt = requireNonNull(readMetadataOpt, "readMetadataOpt is null");
    this.newProtocolOpt = requireNonNull(newProtocolOpt, "newProtocolOpt is null");
    this.newMetadataOpt = requireNonNull(newMetadataOpt, "newMetadataOpt is null");
  }

  /** The version of the Delta table this commit is targeting. */
  public long getVersion() {
    return version;
  }

  /** The path to the Delta log directory. */
  public String getLogPath() {
    return logPath;
  }

  /** The {@link CommitInfo} that is being written as part of this commit. */
  public CommitInfo getCommitInfo() {
    return commitInfo;
  }

  /**
   * The {@link Protocol} that was read at the beginning of the commit. Empty if a new table is
   * being created.
   */
  public Optional<Protocol> getReadProtocolOpt() {
    return readProtocolOpt;
  }

  /**
   * The {@link Metadata} that was read at the beginning of the commit. Empty if a new table is
   * being created.
   */
  public Optional<Metadata> getReadMetadataOpt() {
    return readMetadataOpt;
  }

  /**
   * The {@link Protocol} that is being written as part of this commit. Empty if the protocol is not
   * being changed.
   */
  public Optional<Protocol> getNewProtocolOpt() {
    return newProtocolOpt;
  }

  /**
   * The {@link Metadata} that is being written as part of this commit. Empty if the metadata is not
   * being changed.
   */
  public Optional<Metadata> getNewMetadataOpt() {
    return newMetadataOpt;
  }
}
