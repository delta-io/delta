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
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.FileNames;
import java.util.Optional;

/**
 * Contains all information (excluding the iterator of finalized actions) required to commit changes
 * to a Delta table.
 */
@Experimental
public class CommitMetadata {

  /**
   * Represents the different types of commits based on filesystem-managed and catalog-managed state
   * transitions.
   */
  public enum CommitType {
    /** Creating a new filesystem-managed table */
    FILESYSTEM_CREATE,
    /** Creating a new catalog-managed table */
    CATALOG_CREATE,
    /** Writing to an existing filesystem-managed table */
    FILESYSTEM_WRITE,
    /** Writing to an existing catalog-managed table */
    CATALOG_WRITE,
    /** Upgrading a filesystem-managed table to a catalog-managed table */
    FILESYSTEM_UPGRADE_TO_CATALOG,
    /** Downgrading a catalog-managed table to a filesystem-managed table */
    CATALOG_DOWNGRADE_TO_FILESYSTEM
  }

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
      CommitInfo commitInfo,
      Optional<Protocol> readProtocolOpt,
      Optional<Metadata> readMetadataOpt,
      Optional<Protocol> newProtocolOpt,
      Optional<Metadata> newMetadataOpt) {
    checkArgument(version >= 0, "version must be non-negative: %d", version);
    checkICTPresentIfCatalogManaged(requireNonNull(commitInfo, "commitInfo is null"));
    this.version = version;
    this.logPath = requireNonNull(logPath, "logPath is null");
    this.commitInfo = commitInfo;
    this.readProtocolOpt = requireNonNull(readProtocolOpt, "readProtocolOpt is null");
    this.readMetadataOpt = requireNonNull(readMetadataOpt, "readMetadataOpt is null");
    this.newProtocolOpt = requireNonNull(newProtocolOpt, "newProtocolOpt is null");
    this.newMetadataOpt = requireNonNull(newMetadataOpt, "newMetadataOpt is null");
  }

  /** The version of the Delta table this commit is targeting. */
  public long getVersion() {
    return version;
  }

  /** The path to the Delta log directory, located at {@code <table_root>/_delta_log}. */
  public String getDeltaLogDirPath() {
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

  public Protocol getEffectiveProtocol() {
    return newProtocolOpt.orElseGet(() -> readProtocolOpt.get());
  }

  public Metadata getEffectiveMetadata() {
    return newMetadataOpt.orElseGet(() -> readMetadataOpt.get());
  }

  public CommitType getCommitType() {
    final boolean isCreate = !readProtocolOpt.isPresent() && !readMetadataOpt.isPresent();
    final boolean lastVersionCatalogManaged =
        readProtocolOpt.map(TableFeatures::isCatalogManagedSupported).orElse(false);
    final boolean nextVersionCatalogManaged =
        TableFeatures.isCatalogManagedSupported(getEffectiveProtocol());

    if (isCreate && nextVersionCatalogManaged) {
      return CommitType.CATALOG_CREATE;
    } else if (isCreate && !nextVersionCatalogManaged) {
      return CommitType.FILESYSTEM_CREATE;
    } else if (lastVersionCatalogManaged && nextVersionCatalogManaged) {
      return CommitType.CATALOG_WRITE;
    } else if (lastVersionCatalogManaged && !nextVersionCatalogManaged) {
      return CommitType.CATALOG_DOWNGRADE_TO_FILESYSTEM;
    } else if (!lastVersionCatalogManaged && nextVersionCatalogManaged) {
      return CommitType.FILESYSTEM_UPGRADE_TO_CATALOG;
    } else {
      return CommitType.FILESYSTEM_WRITE;
    }
  }

  public String getNewStagedCommitFilePath() {
    return FileNames.stagedCommitFile(logPath, version);
  }

  private void checkICTPresentIfCatalogManaged(CommitInfo commitInfo) {
    if (TableFeatures.isCatalogManagedSupported(getEffectiveProtocol())) {
      checkArgument(
          commitInfo.getInCommitTimestamp().isPresent(),
          "InCommitTimestamp must be present catalogManaged commits");
    }
  }
}
