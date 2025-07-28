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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.ResolvedTable;
import io.delta.kernel.TableManager;
import io.delta.kernel.annotation.Experimental;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
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

  public UCCatalogManagedClient(UCClient ucClient) {
    this.ucClient = Objects.requireNonNull(ucClient, "ucClient is null");
  }

  // TODO: [delta-io/delta#4817] loadTable API that takes in a UC TableInfo object

  /**
   * Loads a Kernel {@link ResolvedTable} at a specific version.
   *
   * @param engine The Delta Kernel {@link Engine} to use for loading the table.
   * @param ucTableId The Unity Catalog table ID, which is a unique identifier for the table in UC.
   * @param tablePath The path to the Delta table in the underlying storage system.
   * @param version The version of the table to load.
   */
  public ResolvedTable loadTable(Engine engine, String ucTableId, String tablePath, long version) {
    Objects.requireNonNull(engine, "engine is null");
    Objects.requireNonNull(ucTableId, "ucTableId is null");
    Objects.requireNonNull(tablePath, "tablePath is null");
    checkArgument(version >= 0, "version must be non-negative");

    logger.info("[{}] Resolving table at version {}", ucTableId, version);
    final GetCommitsResponse response = getRatifiedCommitsFromUC(ucTableId, tablePath, version);
    validateLoadTableVersionExists(ucTableId, version, response.getLatestTableVersion());
    final List<ParsedLogData> logData =
        getSortedKernelLogDataFromRatifiedCommits(ucTableId, response.getCommits());

    return timeOperation(
        "TableManager.loadTable",
        ucTableId,
        () ->
            TableManager.loadTable(tablePath)
                .atVersion(version)
                .withLogData(logData)
                .build(engine));
  }

  private GetCommitsResponse getRatifiedCommitsFromUC(
      String ucTableId, String tablePath, long version) {
    logger.info(
        "[{}] Invoking the UCClient to get ratified commits at version {}", ucTableId, version);

    return timeOperation(
        "UCClient.getCommits",
        ucTableId,
        () -> {
          try {
            return ucClient.getCommits(
                ucTableId,
                new Path(tablePath).toUri(),
                Optional.empty() /* startVersion */,
                Optional.of(version) /* endVersion */);
          } catch (IOException ex) {
            throw new UncheckedIOException(ex);
          } catch (UCCommitCoordinatorException ex) {
            throw new RuntimeException(ex);
          }
        });
  }

  private void validateLoadTableVersionExists(
      String ucTableId, long tableVersionToLoad, long maxRatifiedVersion) {
    if (tableVersionToLoad > maxRatifiedVersion) {
      throw new IllegalArgumentException(
          String.format(
              "[%s] Cannot load table version %s as the latest version ratified by UC is %s",
              ucTableId, tableVersionToLoad, maxRatifiedVersion));
    }
  }

  /**
   * Converts a list of ratified commits into a sorted list of {@link ParsedLogData} for use in
   * loading a Delta table.
   */
  @VisibleForTesting
  static List<ParsedLogData> getSortedKernelLogDataFromRatifiedCommits(
      String ucTableId, List<Commit> commits) {
    final List<ParsedLogData> result =
        timeOperation(
            "Sort and convert UC ratified commits into Kernel ParsedLogData",
            ucTableId,
            () ->
                commits.stream()
                    .sorted(Comparator.comparingLong(Commit::getVersion))
                    .map(
                        commit ->
                            ParsedLogData.forFileStatus(
                                hadoopFileStatusToKernelFileStatus(commit.getFileStatus())))
                    .collect(Collectors.toList()));

    logger.debug("[{}] Created ParsedLogData from ratified commits: {}", ucTableId, result);

    return result;
  }

  private static io.delta.kernel.utils.FileStatus hadoopFileStatusToKernelFileStatus(
      org.apache.hadoop.fs.FileStatus hadoopFS) {
    return io.delta.kernel.utils.FileStatus.of(
        hadoopFS.getPath().toString(), hadoopFS.getLen(), hadoopFS.getModificationTime());
  }

  /** Times an operation and logs the duration. */
  private static <T> T timeOperation(
      String operationName, String ucTableId, Supplier<T> operation) {
    final long startTime = System.nanoTime();
    try {
      final T result = operation.get();
      final long durationMs = (System.nanoTime() - startTime) / 1_000_000;
      logger.info("[{}] {} completed in {} ms", ucTableId, operationName, durationMs);
      return result;
    } catch (Exception e) {
      final long durationMs = (System.nanoTime() - startTime) / 1_000_000;
      logger.warn(
          "[{}] {} failed after {} ms: {}", ucTableId, operationName, durationMs, e.getMessage());
      throw e;
    }
  }
}
