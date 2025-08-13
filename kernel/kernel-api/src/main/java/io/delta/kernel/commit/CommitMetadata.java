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
    this.version = version;
    this.logPath = requireNonNull(logPath, "logPath is null");
    this.commitInfo = requireNonNull(commitInfo, "commitInfo is null");
    this.readProtocolOpt = requireNonNull(readProtocolOpt, "readProtocolOpt is null");
    this.readMetadataOpt = requireNonNull(readMetadataOpt, "readMetadataOpt is null");
    this.newProtocolOpt = requireNonNull(newProtocolOpt, "newProtocolOpt is null");
    this.newMetadataOpt = requireNonNull(newMetadataOpt, "newMetadataOpt is null");

    checkArgument(
        readProtocolOpt.isPresent() == readMetadataOpt.isPresent(),
        "readProtocolOpt and readMetadataOpt must either both be present or both be absent");
    checkArgument(
        readProtocolOpt.isPresent() || newProtocolOpt.isPresent(),
        "At least one of readProtocolOpt or newProtocolOpt must be present");
    checkArgument(
        readMetadataOpt.isPresent() || newMetadataOpt.isPresent(),
        "At least one of readMetadataOpt or newMetadataOpt must be present");

    checkICTPresentIfCatalogManaged();
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

  /**
   * Returns the effective {@link Protocol} that will be in place after this commit. If a new
   * protocol is being written as part of this commit, returns the new protocol. Otherwise, returns
   * the protocol that was read at the beginning of the commit.
   */
  public Protocol getEffectiveProtocol() {
    return newProtocolOpt.orElseGet(() -> readProtocolOpt.get());
  }

  /**
   * Returns the effective {@link Metadata} that will be in place after this commit. If new metadata
   * is being written as part of this commit, returns the new metadata. Otherwise, returns the
   * metadata that was read at the beginning of the commit.
   */
  public Metadata getEffectiveMetadata() {
    return newMetadataOpt.orElseGet(() -> readMetadataOpt.get());
  }

  /**
   * Determines the type of commit based on whether this is a table creation and the catalog-managed
   * status of the table before and after the commit.
   */
  public CommitType getCommitType() {
    final boolean isCreate = !readProtocolOpt.isPresent() && !readMetadataOpt.isPresent();
    final boolean readVersionCatalogManaged =
        readProtocolOpt.map(TableFeatures::isCatalogManagedSupported).orElse(false);
    final boolean writeVersionCatalogManaged =
        TableFeatures.isCatalogManagedSupported(getEffectiveProtocol());

    if (isCreate && writeVersionCatalogManaged) {
      return CommitType.CATALOG_CREATE;
    } else if (isCreate && !writeVersionCatalogManaged) {
      return CommitType.FILESYSTEM_CREATE;
    } else if (readVersionCatalogManaged && writeVersionCatalogManaged) {
      return CommitType.CATALOG_WRITE;
    } else if (readVersionCatalogManaged && !writeVersionCatalogManaged) {
      return CommitType.CATALOG_DOWNGRADE_TO_FILESYSTEM;
    } else if (!readVersionCatalogManaged && writeVersionCatalogManaged) {
      return CommitType.FILESYSTEM_UPGRADE_TO_CATALOG;
    } else {
      return CommitType.FILESYSTEM_WRITE;
    }
  }

  private void checkICTPresentIfCatalogManaged() {
    if (TableFeatures.isCatalogManagedSupported(getEffectiveProtocol())) {
      checkArgument(
          commitInfo.getInCommitTimestamp().isPresent(),
          "InCommitTimestamp must be present for commits to catalogManaged tables");
    }
  }
}
