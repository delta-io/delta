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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.unitycatalog.utils.OperationTimer.timeUncheckedOperation;

import io.delta.kernel.CommitRange;
import io.delta.kernel.CommitRangeBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.SnapshotBuilder;
import io.delta.kernel.TableManager;
import io.delta.kernel.annotation.Experimental;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.files.ParsedCatalogCommitData;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import io.delta.kernel.unitycatalog.metrics.UcLoadSnapshotTelemetry;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for interacting with Unity Catalog (UC) catalog-managed Delta tables.
 *
 * @see UCClient
 * @see Snapshot
 */
@Experimental
public class UCCatalogManagedClient {
  private static final Logger logger = LoggerFactory.getLogger(UCCatalogManagedClient.class);

  public static final String UC_PROPERTY_NAMESPACE_PREFIX = "io.unitycatalog.";

  /** Key for identifying Unity Catalog table ID. */
  public static final String UC_TABLE_ID_KEY = UC_PROPERTY_NAMESPACE_PREFIX + "tableId";

  protected final UCClient ucClient;

  public UCCatalogManagedClient(UCClient ucClient) {
    this.ucClient = Objects.requireNonNull(ucClient, "ucClient is null");
  }

  // TODO: [delta-io/delta#4817] loadSnapshot API that takes in a UC TableInfo object

  /////////////////
  // Public APIs //
  /////////////////

  /**
   * Loads a Kernel {@link Snapshot}. If no version is specified, the latest version of the table is
   * loaded.
   *
   * @param engine The Delta Kernel {@link Engine} to use for loading the table.
   * @param ucTableId The Unity Catalog table ID, which is a unique identifier for the table in UC.
   * @param tablePath The path to the Delta table in the underlying storage system.
   * @param versionOpt The optional version to time-travel to when loading the table. This must be
   *     mutually exclusive with timestampOpt.
   * @param timestampOpt The optional timestamp to time-travel to when loading the table. This must
   *     be mutually exclusive with versionOpt.
   * @throws IllegalArgumentException if a negative version or timestamp is provided
   * @throws IllegalArgumentException if both versionOpt and timestampOpt are defined
   */
  public Snapshot loadSnapshot(
      Engine engine,
      String ucTableId,
      String tablePath,
      Optional<Long> versionOpt,
      Optional<Long> timestampOpt) {
    Objects.requireNonNull(engine, "engine is null");
    Objects.requireNonNull(ucTableId, "ucTableId is null");
    Objects.requireNonNull(tablePath, "tablePath is null");
    Objects.requireNonNull(versionOpt, "versionOpt is null");
    Objects.requireNonNull(timestampOpt, "timestampOpt is null");
    versionOpt.ifPresent(version -> checkArgument(version >= 0, "version must be non-negative"));
    checkArgument(
        !timestampOpt.isPresent() || !versionOpt.isPresent(),
        "cannot provide both timestamp and version");

    logger.info(
        "[{}] Loading Snapshot at {}",
        ucTableId,
        getVersionOrTimestampString(versionOpt, timestampOpt));

    final UcLoadSnapshotTelemetry telemetry =
        new UcLoadSnapshotTelemetry(ucTableId, tablePath, versionOpt, timestampOpt);

    final UcLoadSnapshotTelemetry.MetricsCollector metricsCollector =
        telemetry.getMetricsCollector();

    try {
      final Snapshot result =
          metricsCollector.totalSnapshotLoadTimer.timeChecked(
              () -> {
                final GetCommitsResponse response =
                    metricsCollector.getCommitsTimer.timeChecked(
                        () -> getRatifiedCommitsFromUC(ucTableId, tablePath, versionOpt));

                metricsCollector.setNumCatalogCommits(response.getCommits().size());

                final long maxUcTableVersion = response.getLatestTableVersion();

                versionOpt.ifPresent(
                    version ->
                        validateTimeTravelVersionNotPastMax(ucTableId, version, maxUcTableVersion));

                final List<ParsedLogData> logData =
                    getSortedKernelParsedDeltaDataFromRatifiedCommits(
                        ucTableId, response.getCommits());

                return metricsCollector.kernelSnapshotBuildTimer.timeChecked(
                    () -> {
                      SnapshotBuilder snapshotBuilder = TableManager.loadSnapshot(tablePath);

                      if (versionOpt.isPresent()) {
                        snapshotBuilder = snapshotBuilder.atVersion(versionOpt.get());
                      }

                      if (timestampOpt.isPresent()) {
                        // If timestampOpt is present, we know versionOpt is not present. This means
                        // logData was not requested with an endVersion and thus it can be re-used
                        // to load the latest snapshot
                        Snapshot latestSnapshot =
                            metricsCollector.loadLatestSnapshotForTimestampTimeTravelTimer
                                .timeChecked(
                                    () ->
                                        loadLatestSnapshotForTimestampResolution(
                                            engine,
                                            ucTableId,
                                            tablePath,
                                            logData,
                                            maxUcTableVersion));
                        snapshotBuilder =
                            snapshotBuilder.atTimestamp(timestampOpt.get(), latestSnapshot);
                      }

                      Snapshot snapshot =
                          snapshotBuilder
                              .withCommitter(createUCCommitter(ucClient, ucTableId, tablePath))
                              .withLogData(logData)
                              .withMaxCatalogVersion(maxUcTableVersion)
                              .build(engine);
                      metricsCollector.setResolvedSnapshotVersion(snapshot.getVersion());
                      return snapshot;
                    });
              });

      final UcLoadSnapshotTelemetry.Report successReport = telemetry.createSuccessReport();
      engine.getMetricsReporters().forEach(r -> r.report(successReport));
      return result;
    } catch (Exception e) {
      final UcLoadSnapshotTelemetry.Report failureReport = telemetry.createFailureReport(e);
      engine.getMetricsReporters().forEach(r -> r.report(failureReport));
      throw e;
    }
  }

  /**
   * Builds a create table transaction for a Unity Catalog managed Delta table.
   *
   * <p>Configures the transaction with a {@link UCCatalogManagedCommitter} and required table
   * properties for catalog-managed table enablement.
   *
   * <p>This assumes the table is being created in a staging location as per UC semantics. Once this
   * transaction is built and committed, creating 000.json, you must call {@code
   * TablesApi::createTable} to inform Unity Catalog of the successful table creation.
   *
   * @param ucTableId The Unity Catalog table ID.
   * @param tablePath The staging path to the Delta table.
   * @param schema The table schema.
   * @param engineInfo Information about the creating engine.
   * @return A {@link CreateTableTransactionBuilder} configured for UC managed tables.
   */
  public CreateTableTransactionBuilder buildCreateTableTransaction(
      String ucTableId, String tablePath, StructType schema, String engineInfo) {
    Objects.requireNonNull(ucTableId, "ucTableId is null");
    Objects.requireNonNull(tablePath, "tablePath is null");
    Objects.requireNonNull(schema, "schema is null");
    Objects.requireNonNull(engineInfo, "engineInfo is null");

    return TableManager.buildCreateTableTransaction(tablePath, schema, engineInfo)
        .withCommitter(createUCCommitter(ucClient, ucTableId, tablePath))
        .withTableProperties(getRequiredTablePropertiesForCreate(ucTableId));
  }

  /**
   * Loads a Kernel {@link CommitRange} for the provided boundaries. If no end boundary is provided,
   * defaults to the latest version.
   *
   * <p>A start boundary is required and must be specified using either {@code startVersionOpt} or
   * {@code startTimestampOpt}. These parameters are mutually exclusive and at least one must be
   * provided.
   *
   * @param engine The Delta Kernel {@link Engine} to use for loading the table.
   * @param ucTableId The Unity Catalog table ID, which is a unique identifier for the table in UC.
   * @param tablePath The path to the Delta table in the underlying storage system.
   * @param startVersionOpt The optional start version boundary. This must be mutually exclusive
   *     with startTimestampOpt. Either this or startTimestampOpt must be provided.
   * @param startTimestampOpt The optional start timestamp boundary. This must be mutually exclusive
   *     with startVersionOpt. Either this or startVersionOpt must be provided.
   * @param endVersionOpt The optional end version boundary. This must be mutually exclusive with
   *     endTimestampOpt.
   * @param endTimestampOpt The optional end timestamp boundary. This must be mutually exclusive
   *     with endVersionOpt.
   * @throws IllegalArgumentException if neither startVersionOpt nor startTimestampOpt is provided
   * @throws IllegalArgumentException if both startVersionOpt and startTimestampOpt are defined
   * @throws IllegalArgumentException if both endVersionOpt and endTimestampOpt are defined
   * @throws IllegalArgumentException if either startVersionOpt or endVersionOpt is provided and is
   *     greater than the latest ratified version from UC
   */
  public CommitRange loadCommitRange(
      Engine engine,
      String ucTableId,
      String tablePath,
      Optional<Long> startVersionOpt,
      Optional<Long> startTimestampOpt,
      Optional<Long> endVersionOpt,
      Optional<Long> endTimestampOpt) {
    Objects.requireNonNull(engine, "engine is null");
    Objects.requireNonNull(ucTableId, "ucTableId is null");
    Objects.requireNonNull(tablePath, "tablePath is null");
    Objects.requireNonNull(startVersionOpt, "startVersionOpt is null");
    Objects.requireNonNull(startTimestampOpt, "startTimestampOpt is null");
    Objects.requireNonNull(endVersionOpt, "endVersionOpt is null");
    Objects.requireNonNull(endTimestampOpt, "endTimestampOpt is null");
    checkArgument(
        !startVersionOpt.isPresent() || !startTimestampOpt.isPresent(),
        "Cannot provide both a start timestamp and start version");
    checkArgument(
        !endVersionOpt.isPresent() || !endTimestampOpt.isPresent(),
        "Cannot provide both an end timestamp and start version");
    checkArgument(
        startVersionOpt.isPresent() || startTimestampOpt.isPresent(),
        "Must provide either a start timestamp or start version");
    if (startVersionOpt.isPresent() && endVersionOpt.isPresent()) {
      checkArgument(
          startVersionOpt.get() <= endVersionOpt.get(),
          "Cannot provide a start version greater than the end version");
    }
    if (startTimestampOpt.isPresent() && endTimestampOpt.isPresent()) {
      checkArgument(
          startTimestampOpt.get() <= endTimestampOpt.get(),
          "Cannot provide a start timestamp greater than the end timestamp");
    }

    logger.info(
        "[{}] Loading CommitRange for {}",
        ucTableId,
        getCommitRangeBoundariesString(
            startVersionOpt, startTimestampOpt, endVersionOpt, endTimestampOpt));
    // If we have a timestamp-based boundary we need to build the latest snapshot, don't provide
    // an endVersion
    Optional<Long> endVersionOptForCommitQuery =
        endVersionOpt.filter(v -> !startTimestampOpt.isPresent());
    final GetCommitsResponse response =
        getRatifiedCommitsFromUC(ucTableId, tablePath, endVersionOptForCommitQuery);
    final long ucTableVersion = response.getLatestTableVersion();
    validateVersionBoundariesExist(ucTableId, startVersionOpt, endVersionOpt, ucTableVersion);
    final List<ParsedLogData> logData =
        getSortedKernelParsedDeltaDataFromRatifiedCommits(ucTableId, response.getCommits());
    final Lazy<Snapshot> latestSnapshot =
        new Lazy<>(
            () ->
                loadLatestSnapshotForTimestampResolution(
                    engine, ucTableId, tablePath, logData, ucTableVersion));

    return timeUncheckedOperation(
        logger,
        "TableManager.loadCommitRange",
        ucTableId,
        () -> {
          // Determine the start boundary (required - validated above)
          CommitRangeBuilder.CommitBoundary startBoundary;
          if (startVersionOpt.isPresent()) {
            startBoundary = CommitRangeBuilder.CommitBoundary.atVersion(startVersionOpt.get());
          } else {
            // startTimestampOpt must be present due to validation above
            startBoundary =
                CommitRangeBuilder.CommitBoundary.atTimestamp(
                    startTimestampOpt.get(), latestSnapshot.get());
          }

          CommitRangeBuilder commitRangeBuilder =
              TableManager.loadCommitRange(tablePath, startBoundary)
                  .withMaxCatalogVersion(ucTableVersion);

          if (endVersionOpt.isPresent()) {
            commitRangeBuilder =
                commitRangeBuilder.withEndBoundary(
                    CommitRangeBuilder.CommitBoundary.atVersion(endVersionOpt.get()));
          }
          if (endTimestampOpt.isPresent()) {
            commitRangeBuilder =
                commitRangeBuilder.withEndBoundary(
                    CommitRangeBuilder.CommitBoundary.atTimestamp(
                        endTimestampOpt.get(), latestSnapshot.get()));
          }

          return commitRangeBuilder.withLogData(logData).build(engine);
        });
  }

  /////////////////////////////////////////
  // Protected Methods for Extensibility //
  /////////////////////////////////////////

  /**
   * Creates a UC committer instance for the specified table.
   *
   * <p>This method allows subclasses to provide custom committer implementations for specialized
   * use cases.
   */
  protected Committer createUCCommitter(UCClient ucClient, String ucTableId, String tablePath) {
    return new UCCatalogManagedCommitter(ucClient, ucTableId, tablePath);
  }

  ////////////////////
  // Helper Methods //
  ////////////////////

  private String getVersionString(Optional<Long> versionOpt) {
    return versionOpt.map(String::valueOf).orElse("latest");
  }

  private String getVersionOrTimestampString(
      Optional<Long> versionOpt, Optional<Long> timestampOpt) {
    if (versionOpt.isPresent()) {
      return "version=" + versionOpt.get();
    } else if (timestampOpt.isPresent()) {
      return "timestamp=" + timestampOpt.get();
    } else {
      return "latest";
    }
  }

  private String getCommitRangeBoundariesString(
      Optional<Long> startVersionOpt,
      Optional<Long> startTimestampOpt,
      Optional<Long> endVersionOpt,
      Optional<Long> endTimestampOpt) {
    String startBound;
    if (startVersionOpt.isPresent()) {
      startBound = startVersionOpt.get() + "(version)";
    } else if (startTimestampOpt.isPresent()) {
      startBound = startTimestampOpt.get() + "(timestamp)";
    } else {
      startBound = "0(default)";
    }
    String endBound;
    if (endVersionOpt.isPresent()) {
      endBound = endVersionOpt.get() + "(version)";
    } else if (endTimestampOpt.isPresent()) {
      endBound = endTimestampOpt.get() + "(timestamp)";
    } else {
      endBound = "latestVersion(default)";
    }
    return String.format("startBoundary=%s and endBoundary=%s", startBound, endBound);
  }

  private GetCommitsResponse getRatifiedCommitsFromUC(
      String ucTableId, String tablePath, Optional<Long> versionOpt) {
    logger.info(
        "[{}] Invoking the UCClient to get ratified commits at version {}",
        ucTableId,
        getVersionString(versionOpt));

    // TODO: We can remove timeUncheckedOperation when the commitRange code integrates with metrics
    final GetCommitsResponse response =
        timeUncheckedOperation(
            logger,
            "UCClient.getCommits",
            ucTableId,
            () -> {
              try {
                return ucClient.getCommits(
                    ucTableId,
                    new Path(tablePath).toUri(),
                    Optional.empty() /* startVersion */,
                    versionOpt /* endVersion */);
              } catch (IOException ex) {
                throw new UncheckedIOException(ex);
              } catch (UCCommitCoordinatorException ex) {
                throw new RuntimeException(ex);
              }
            });

    logger.info(
        "[{}] Number of ratified commits: {}, Max ratified version in UC: {}",
        ucTableId,
        response.getCommits().size(),
        response.getLatestTableVersion());

    return response;
  }

  private void validateTimeTravelVersionNotPastMax(
      String ucTableId, long tableVersionToLoad, long maxRatifiedVersion) {
    if (tableVersionToLoad > maxRatifiedVersion) {
      throw new IllegalArgumentException(
          String.format(
              "[%s] Cannot load table version %s as the latest version ratified by UC is %s",
              ucTableId, tableVersionToLoad, maxRatifiedVersion));
    }
  }

  private void validateVersionBoundariesExist(
      String ucTableId,
      Optional<Long> startVersion,
      Optional<Long> endVersion,
      long maxRatifiedVersion) {
    BiConsumer<Long, String> validateVersion =
        (version, type) -> {
          if (version > maxRatifiedVersion) {
            throw new IllegalArgumentException(
                String.format(
                    "[%s] Cannot load commit range with %s version %d as the latest version "
                        + "ratified by UC is %d",
                    ucTableId, type, version, maxRatifiedVersion));
          }
        };
    startVersion.ifPresent(v -> validateVersion.accept(v, "start"));
    endVersion.ifPresent(v -> validateVersion.accept(v, "end"));
  }

  private Map<String, String> getRequiredTablePropertiesForCreate(String ucTableId) {
    final Map<String, String> requiredProperties = new HashMap<>();

    requiredProperties.put(
        TableFeatures.CATALOG_MANAGED_RW_FEATURE.getTableFeatureSupportKey(),
        TableFeatures.SET_TABLE_FEATURE_SUPPORTED_VALUE);
    requiredProperties.put(
        TableFeatures.VACUUM_PROTOCOL_CHECK_RW_FEATURE.getTableFeatureSupportKey(),
        TableFeatures.SET_TABLE_FEATURE_SUPPORTED_VALUE);
    requiredProperties.put(UC_TABLE_ID_KEY, ucTableId);

    return requiredProperties;
  }

  /**
   * Converts a list of ratified commits into a sorted list of {@link ParsedLogData} for use in
   * loading a Delta table.
   */
  @VisibleForTesting
  static List<ParsedLogData> getSortedKernelParsedDeltaDataFromRatifiedCommits(
      String ucTableId, List<Commit> commits) {
    final List<ParsedLogData> result =
        timeUncheckedOperation(
            logger,
            "Sort and convert UC ratified commits into Kernel ParsedLogData",
            ucTableId,
            () ->
                commits.stream()
                    .sorted(Comparator.comparingLong(Commit::getVersion))
                    .map(
                        commit ->
                            ParsedCatalogCommitData.forFileStatus(
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

  /**
   * Helper method to load the latest snapshot and time the operation. This is used to load the
   * latest snapshot for timestamp resolution queries. Reuses existing logData that has already been
   * queried from the catalog (it is required that this includes the latest commits from the catalog
   * and were not queried with an endVersion).
   */
  private Snapshot loadLatestSnapshotForTimestampResolution(
      Engine engine,
      String ucTableId,
      String tablePath,
      List<ParsedLogData> logData,
      long ucTableVersion) {
    // TODO: We can remove timeUncheckedOperation when the commitRange code integrates with metrics
    return timeUncheckedOperation(
        logger,
        "TableManager.loadSnapshot at latest for time-travel query",
        ucTableId,
        () ->
            TableManager.loadSnapshot(tablePath)
                .withCommitter(new UCCatalogManagedCommitter(ucClient, ucTableId, tablePath))
                .withLogData(logData)
                .withMaxCatalogVersion(ucTableVersion)
                .build(engine));
  }
}
