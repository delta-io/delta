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
package io.delta.kernel.defaults.catalog;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Catalog-facing contract for catalog-managed commits (CCv2).
 *
 * <p>This interface is intentionally free of Spark types so engines like Flink/Trino can
 * implement it. Implementations should mirror the POST/GET semantics from the CCv2 protocol
 * (e.g. Unity Catalog, Glue, Polaris) and return only lightweight commit metadata.</p>
 */
public interface CatalogWithManagedCommits {

  /**
   * Extract the catalog-managed table identifier from table properties.
   *
   * <p>Implementations should not throw if the id is missing; returning empty allows callers to
   * fall back to path-based snapshots.</p>
   */
  Optional<String> extractTableId(Map<String, String> tableProperties);

  /**
   * Retrieve ratified commits starting at {@code startVersion}. Implementations can paginate but
   * must return the latest table version alongside the current page.
   */
  GetCommitsResult getCommits(
      String tableId, String tablePath, long startVersion, Optional<Long> endVersion);

  /**
   * Propose a commit for ratification. Mirrors the CCv2 POST payload (commit info + optional
   * metadata/protocol updates). Implementations should surface catalog errors via exceptions.
   */
  ProposeCommitResult proposeCommit(
      String tableId, String tablePath, CommitProposal proposal, Map<String, String> options);

  /**
   * Minimal commit metadata required to replay the Delta log.
   */
  final class CommitEntry {
    private final long version;
    private final String fileName;
    private final long fileSize;
    private final long fileModificationTime;
    private final long commitTimestamp;
    private final boolean isDisownCommit;
    private final Optional<String> commitType;

    public CommitEntry(
        long version,
        String fileName,
        long fileSize,
        long fileModificationTime,
        long commitTimestamp,
        boolean isDisownCommit,
        Optional<String> commitType) {
      if (version < 0) {
        throw new IllegalArgumentException("version must be non-negative");
      }
      this.version = version;
      this.fileName = Objects.requireNonNull(fileName, "fileName is null");
      this.fileSize = fileSize;
      this.fileModificationTime = fileModificationTime;
      this.commitTimestamp = commitTimestamp;
      this.isDisownCommit = isDisownCommit;
      this.commitType = commitType == null ? Optional.empty() : commitType;
    }

    public long getVersion() {
      return version;
    }

    public String getFileName() {
      return fileName;
    }

    public long getFileSize() {
      return fileSize;
    }

    public long getFileModificationTime() {
      return fileModificationTime;
    }

    public long getCommitTimestamp() {
      return commitTimestamp;
    }

    public boolean isDisownCommit() {
      return isDisownCommit;
    }

    public Optional<String> getCommitType() {
      return commitType;
    }
  }

  /**
   * Response from {@link #getCommits(String, String, long, Optional)}.
   */
  final class GetCommitsResult {
    private final List<CommitEntry> commits;
    private final long latestTableVersion;

    public GetCommitsResult(List<CommitEntry> commits, long latestTableVersion) {
      this.commits = Collections.unmodifiableList(commits);
      this.latestTableVersion = latestTableVersion;
    }

    public List<CommitEntry> getCommits() {
      return commits;
    }

    public long getLatestTableVersion() {
      return latestTableVersion;
    }
  }

  /**
   * Proposal payload for {@link #proposeCommit(String, String, CommitProposal, Map)}.
   */
  final class CommitProposal {
    private final String fileName;
    private final long fileSize;
    private final long fileModificationTime;
    private final long commitTimestamp;
    private final long version;
    private final Optional<Long> latestBackfilledVersion;
    private final Optional<Map<String, String>> metadataJson;
    private final Optional<Map<String, String>> protocolJson;
    private final Optional<Map<String, String>> commitInfoJson;

    public CommitProposal(
        String fileName,
        long fileSize,
        long fileModificationTime,
        long commitTimestamp,
        long version,
        Optional<Long> latestBackfilledVersion,
        Optional<Map<String, String>> metadataJson,
        Optional<Map<String, String>> protocolJson,
        Optional<Map<String, String>> commitInfoJson) {
      if (version < 0) {
        throw new IllegalArgumentException("version must be non-negative");
      }
      this.fileName = Objects.requireNonNull(fileName, "fileName is null");
      this.fileSize = fileSize;
      this.fileModificationTime = fileModificationTime;
      this.commitTimestamp = commitTimestamp;
      this.version = version;
      this.latestBackfilledVersion = latestBackfilledVersion == null ? Optional.empty() : latestBackfilledVersion;
      this.metadataJson = metadataJson == null ? Optional.empty() : metadataJson;
      this.protocolJson = protocolJson == null ? Optional.empty() : protocolJson;
      this.commitInfoJson = commitInfoJson == null ? Optional.empty() : commitInfoJson;
    }

    public String getFileName() {
      return fileName;
    }

    public long getFileSize() {
      return fileSize;
    }

    public long getFileModificationTime() {
      return fileModificationTime;
    }

    public long getCommitTimestamp() {
      return commitTimestamp;
    }

    public long getVersion() {
      return version;
    }

    public Optional<Long> getLatestBackfilledVersion() {
      return latestBackfilledVersion;
    }

    public Optional<Map<String, String>> getMetadataJson() {
      return metadataJson;
    }

    public Optional<Map<String, String>> getProtocolJson() {
      return protocolJson;
    }

    public Optional<Map<String, String>> getCommitInfoJson() {
      return commitInfoJson;
    }
  }

  /**
   * Result from {@link #proposeCommit(String, String, CommitProposal, Map)}.
   */
  final class ProposeCommitResult {
    private final boolean accepted;
    private final Optional<String> errorMessage;

    public ProposeCommitResult(boolean accepted, Optional<String> errorMessage) {
      this.accepted = accepted;
      this.errorMessage = errorMessage == null ? Optional.empty() : errorMessage;
    }

    public boolean isAccepted() {
      return accepted;
    }

    public Optional<String> getErrorMessage() {
      return errorMessage;
    }
  }
}
