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

package io.delta.unity;

import io.delta.kernel.ResolvedTable;
import io.delta.kernel.TableManager;
import io.delta.kernel.annotation.Experimental;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorException;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for interacting with Unity Catalog (UC) catalog-managed Delta tables.
 *
 * @see UCClient
 * @see ResolvedTable
 */
@Experimental
public class UCCatalogManagedClient {
  private static final Logger logger = LoggerFactory.getLogger(UCCatalogManagedClient.class);

  private final UCClient ucClient;
  private final String ucTableId;
  private final String tablePath;

  public UCCatalogManagedClient(UCClient ucClient, String ucTableId, String tablePath) {
    this.ucClient = Objects.requireNonNull(ucClient, "ucClient is null");
    this.ucTableId = Objects.requireNonNull(ucTableId, "ucTableId is null");
    this.tablePath = Objects.requireNonNull(tablePath, "tablePath is null");
  }

  /**
   * Loads a Kernel {@link ResolvedTable} at a specific version.
   *
   * @param engine The Delta Kernel {@link Engine} to use for loading the table.
   * @param version The version of the table to load.
   * @throws IOException if there's an error during the commit process, such as network issues.
   * @throws UCCommitCoordinatorException if there's an error specific to Unity Catalog such as the
   *     table not being found.
   */
  public ResolvedTable loadTable(Engine engine, long version)
      throws UCCommitCoordinatorException, IOException {
    Objects.requireNonNull(engine, "engine is null");

    logger.info("[{}] Loading table at version {}", ucTableId, version);

    final GetCommitsResponse response = getRatifiedCommitsFromUC(version);

    validateLoadTableVersionExists(version, response.getLatestTableVersion());

    final List<ParsedLogData> logData =
        getSortedKernelLogDataFromRatifiedCommits(ucTableId, response.getCommits());

    return TableManager.loadTable(tablePath).atVersion(version).withLogData(logData).build(engine);
  }

  private GetCommitsResponse getRatifiedCommitsFromUC(long version)
      throws IOException, UCCommitCoordinatorException {
    logger.info(
        "[{}] Invoking the UCClient to get ratified commits at version {}", ucTableId, version);

    return ucClient.getCommits(
        ucTableId,
        new Path(tablePath).toUri(),
        Optional.empty() /* startVersion */,
        Optional.of(version) /* endVersion */);
  }

  private void validateLoadTableVersionExists(long tableVersionToLoad, long maxRatifiedVersion) {
    if (tableVersionToLoad > maxRatifiedVersion) {
      throw new IllegalArgumentException(
          String.format(
              "[%s] Cannot load table version %s as the latest version ratified by UC is %s",
              ucTableId, tableVersionToLoad, maxRatifiedVersion));
    }
  }

  @VisibleForTesting
  static List<ParsedLogData> getSortedKernelLogDataFromRatifiedCommits(
      String ucTableId, List<Commit> commits) {
    final List<ParsedLogData> result =
        commits.stream()
            .sorted(Comparator.comparingLong(Commit::getVersion))
            .map(
                commit -> {
                  final org.apache.hadoop.fs.FileStatus hadoopFS = commit.getFileStatus();
                  final io.delta.kernel.utils.FileStatus kernelFS =
                      FileStatus.of(
                          hadoopFS.getPath().toString(),
                          hadoopFS.getLen(),
                          hadoopFS.getModificationTime());
                  return ParsedLogData.forFileStatus(kernelFS);
                })
            .collect(Collectors.toList());

    logger.debug("[{}] Created ParsedLogData from ratified commits: {}", ucTableId, result);

    return result;
  }
}
