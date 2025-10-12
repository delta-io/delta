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
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Tuple2;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

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
  private final List<DomainMetadata> commitDomainMetadatas;
  private final Supplier<Map<String, String>> committerProperties;
  private final Optional<Tuple2<Protocol, Metadata>> readPandMOpt;
  private final Optional<Protocol> newProtocolOpt;
  private final Optional<Metadata> newMetadataOpt;

  public CommitMetadata(
      long version,
      String logPath,
      CommitInfo commitInfo,
      List<DomainMetadata> commitDomainMetadatas,
      Supplier<Map<String, String>> committerProperties,
      Optional<Tuple2<Protocol, Metadata>> readPandMOpt,
      Optional<Protocol> newProtocolOpt,
      Optional<Metadata> newMetadataOpt) {
    checkArgument(version >= 0, "version must be non-negative: %d", version);
    this.version = version;
    this.logPath = requireNonNull(logPath, "logPath is null");
    this.commitInfo = requireNonNull(commitInfo, "commitInfo is null");
    this.commitDomainMetadatas =
        Collections.unmodifiableList(
            requireNonNull(commitDomainMetadatas, "txnDomainMetadatas is null"));
    this.committerProperties = requireNonNull(committerProperties, "committerProperties is null");
    this.readPandMOpt = requireNonNull(readPandMOpt, "readPandMOpt is null");
    this.newProtocolOpt = requireNonNull(newProtocolOpt, "newProtocolOpt is null");
    this.newMetadataOpt = requireNonNull(newMetadataOpt, "newMetadataOpt is null");

    checkArgument(
        readPandMOpt.isPresent() || newProtocolOpt.isPresent(),
        "At least one of readPandMOpt.protocol or newProtocolOpt must be present");
    checkArgument(
        readPandMOpt.isPresent() || newMetadataOpt.isPresent(),
        "At least one of readPandMOpt.metadata or newMetadataOpt must be present");

    checkReadStateAbsentIfAndOnlyIfVersion0();
    checkInCommitTimestampPresentIfCatalogManaged();
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
   * The {@link DomainMetadata}s that are being written as part of this commit. Includes those that
   * are being explicitly added and those that are being explicitly removed (tombstoned).
   *
   * <p>Does not include the domain metadatas that already exist in the transaction's read snapshot,
   * if any.
   */
  public List<DomainMetadata> getCommitDomainMetadatas() {
    return commitDomainMetadatas;
  }

  /**
   * Returns custom properties provided by the connector to be passed through to the committer.
   * These properties are not inspected by Kernel and are used for catalog-specific functionality.
   */
  public Supplier<Map<String, String>> getCommitterProperties() {
    return committerProperties;
  }

  /**
   * The {@link Protocol} that was read at the beginning of the commit. Empty if a new table is
   * being created.
   */
  public Optional<Protocol> getReadProtocolOpt() {
    return readPandMOpt.map(x -> x._1);
  }

  /**
   * The {@link Metadata} that was read at the beginning of the commit. Empty if a new table is
   * being created.
   */
  public Optional<Metadata> getReadMetadataOpt() {
    return readPandMOpt.map(x -> x._2);
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
    return newProtocolOpt.orElseGet(() -> getReadProtocolOpt().get());
  }

  /**
   * Returns the effective {@link Metadata} that will be in place after this commit. If new metadata
   * is being written as part of this commit, returns the new metadata. Otherwise, returns the
   * metadata that was read at the beginning of the commit.
   */
  public Metadata getEffectiveMetadata() {
    return newMetadataOpt.orElseGet(() -> getReadMetadataOpt().get());
  }

  /**
   * Determines the type of commit based on whether this is a table creation and the catalog-managed
   * status of the table before and after the commit.
   */
  public CommitType getCommitType() {
    final boolean isCreate = version == 0;
    final boolean readVersionCatalogManaged =
        readPandMOpt.map(x -> x._1).map(TableFeatures::isCatalogManagedSupported).orElse(false);
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

  /**
   * Returns the corresponding published Delta log file path for this commit, which is in the form
   * of {@code <table_path>/_delta_log/0000000000000000000<version>.json}.
   *
   * <p>Usages:
   *
   * <ul>
   *   <li>Filesystem-managed committers must write to this file path.
   *   <li>Catalog-managed committers must publish to this file path, if/when they so choose.
   * </ul>
   */
  public String getPublishedDeltaFilePath() {
    return FileNames.deltaFile(logPath, version);
  }

  /**
   * Returns a new staged commit file path with a unique UUID for this commit. Each invocation
   * returns a new, unique value, in the form of {@code
   * <table_path>/_delta_log/_staged_commits/0000000000000000000<version>.<uuid>.json}
   *
   * <p>Catalog-managed committers may use this path to write new staged commits.
   */
  public String generateNewStagedCommitFilePath() {
    return FileNames.stagedCommitFile(logPath, version);
  }

  private void checkReadStateAbsentIfAndOnlyIfVersion0() {
    checkArgument(
        (version == 0) == (!readPandMOpt.isPresent()),
        "Table creation (version 0) requires absent readPandMOpt, while existing table writes "
            + "(version > 0) require present readPandMOpt");
  }

  private void checkInCommitTimestampPresentIfCatalogManaged() {
    if (TableFeatures.isCatalogManagedSupported(getEffectiveProtocol())) {
      checkArgument(
          commitInfo.getInCommitTimestamp().isPresent(),
          "InCommitTimestamp must be present for commits to catalogManaged tables");
    }
  }
}
