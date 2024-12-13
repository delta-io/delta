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

package io.delta.kernel.internal.snapshot;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.coordinatedcommits.CommitCoordinatorClientHandler;
import io.delta.kernel.internal.coordinatedcommits.CommitFailedException;
import io.delta.kernel.internal.coordinatedcommits.CommitResponse;
import io.delta.kernel.internal.coordinatedcommits.GetCommitsResponse;
import io.delta.kernel.internal.coordinatedcommits.UpdatedActions;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.Map;

/**
 * A wrapper around {@link CommitCoordinatorClientHandler} that provides a more user-friendly API
 * for committing/ accessing commits to a specific table. This class takes care of passing the table
 * specific configuration to the underlying {@link CommitCoordinatorClientHandler} e.g. logPath /
 * coordinatedCommitsTableConf.
 */
public class TableCommitCoordinatorClientHandler {
  private final CommitCoordinatorClientHandler commitCoordinatorClientHandler;
  private final String logPath;
  private final Map<String, String> tableConf;

  public TableCommitCoordinatorClientHandler(
      CommitCoordinatorClientHandler commitCoordinatorClientHandler,
      String logPath,
      Map<String, String> tableConf) {
    this.commitCoordinatorClientHandler = commitCoordinatorClientHandler;
    this.logPath = logPath;
    this.tableConf = tableConf;
  }

  public CommitResponse commit(
      long commitVersion, CloseableIterator<Row> actions, UpdatedActions updatedActions)
      throws CommitFailedException {
    return commitCoordinatorClientHandler.commit(
        logPath, tableConf, commitVersion, actions, updatedActions);
  }

  public GetCommitsResponse getCommits(Long startVersion, Long endVersion) {
    return commitCoordinatorClientHandler.getCommits(logPath, tableConf, startVersion, endVersion);
  }

  public void backfillToVersion(long version, Long lastKnownBackfilledVersion) throws IOException {
    commitCoordinatorClientHandler.backfillToVersion(
        logPath, tableConf, version, lastKnownBackfilledVersion);
  }

  public boolean semanticEquals(
      CommitCoordinatorClientHandler otherCommitCoordinatorClientHandler) {
    return commitCoordinatorClientHandler.semanticEquals(otherCommitCoordinatorClientHandler);
  }

  public boolean semanticEquals(TableCommitCoordinatorClientHandler otherCommitCoordinatorClient) {
    return semanticEquals(otherCommitCoordinatorClient.commitCoordinatorClientHandler);
  }
}
