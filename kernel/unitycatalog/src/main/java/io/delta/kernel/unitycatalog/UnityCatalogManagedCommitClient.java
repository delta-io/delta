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
package io.delta.kernel.unitycatalog;

import io.delta.kernel.defaults.catalog.CatalogWithManagedCommits;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorException;
import io.delta.storage.commit.CommitFailedException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Example implementation of {@link CatalogWithManagedCommits} backed by Unity Catalog's CCv2 HTTP
 * endpoints via {@link UCClient}. This keeps UC-specific logic out of the core connector while
 * allowing engines to plug in a concrete implementation.
 */
public class UnityCatalogManagedCommitClient implements CatalogWithManagedCommits {

  private final UCClient ucClient;

  public UnityCatalogManagedCommitClient(UCClient ucClient) {
    this.ucClient = ucClient;
  }

  @Override
  public Optional<String> extractTableId(Map<String, String> tableProperties) {
    return Optional.ofNullable(tableProperties.get(UCCatalogManagedClient.UC_TABLE_ID_KEY));
  }

  @Override
  public GetCommitsResult getCommits(
      String tableId, String tablePath, long startVersion, Optional<Long> endVersion) {
    try {
      GetCommitsResponse response =
          ucClient.getCommits(
              tableId, new Path(tablePath).toUri(), Optional.of(startVersion), endVersion);

      List<CommitEntry> commits =
          response.getCommits().stream()
              .map(UnityCatalogManagedCommitClient::toCommitEntry)
              .collect(Collectors.toList());

      return new GetCommitsResult(commits, response.getLatestTableVersion());
    } catch (IOException | UCCommitCoordinatorException ex) {
      throw new RuntimeException("Failed to fetch commits from Unity Catalog", ex);
    }
  }

  @Override
  public ProposeCommitResult proposeCommit(
      String tableId, String tablePath, CommitProposal proposal, Map<String, String> options) {
    Path commitPath = new Path(new Path(tablePath), "_delta_log/" + proposal.getFileName());
    FileStatus fileStatus =
        new FileStatus(
            proposal.getFileSize(),
            false,
            1,
            0L,
            proposal.getFileModificationTime(),
            commitPath);

    Commit commit = new Commit(proposal.getVersion(), fileStatus, proposal.getCommitTimestamp());

    try {
      ucClient.commit(
          tableId,
          new Path(tablePath).toUri(),
          Optional.of(commit),
          proposal.getLatestBackfilledVersion(),
          false,
          Optional.empty(),
          Optional.empty());
      return new ProposeCommitResult(true, Optional.empty());
    } catch (IOException | CommitFailedException | UCCommitCoordinatorException ex) {
      return new ProposeCommitResult(false, Optional.ofNullable(ex.getMessage()));
    }
  }

  private static CommitEntry toCommitEntry(Commit commit) {
    FileStatus fs = commit.getFileStatus();
    return new CommitEntry(
        commit.getVersion(),
        fs.getPath().getName(),
        fs.getLen(),
        fs.getModificationTime(),
        commit.getCommitTimestamp(),
        false,
        Optional.empty());
  }
}
