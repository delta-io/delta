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
package io.delta.spark.internal.v2.read;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;

import io.delta.kernel.Snapshot;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.utils.FileStatus;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.delta.StateReconstructionUtils;
import org.apache.spark.sql.types.*;

/**
 * Helper class to perform distributed log replay using Spark DataFrame API, following the same
 * algorithm as V1's Snapshot.stateReconstruction.
 *
 * <p>The core state reconstruction steps (canonicalize paths, repartition+sort, reconstruct
 * structs) are shared with V1 via {@link StateReconstructionUtils}. V2 uses window-function-based
 * deduplication instead of V1's InMemoryLogReplay mapPartitions.
 */
public class DistributedLogReplayHelper {

  // Use the same column names as V1 (DeltaLogFileIndex.COMMIT_VERSION_COLUMN,
  // Snapshot.ADD_STATS_TO_USE_COL_NAME) so DefaultStateProvider works identically.
  private static final String COMMIT_VERSION_COLUMN =
      org.apache.spark.sql.delta.DeltaLogFileIndex.COMMIT_VERSION_COLUMN();
  private static final String ADD_STATS_TO_USE_COL =
      org.apache.spark.sql.delta.Snapshot.ADD_STATS_TO_USE_COL_NAME();

  /**
   * Load raw log actions (checkpoint + delta files) as a DataFrame suitable for state
   * reconstruction. The returned DataFrame has the SingleAction schema plus {@code version} and
   * {@code add_stats_to_use} columns (same names as V1).
   *
   * <p>This is the V2 equivalent of V1's {@code Snapshot.loadActions}. It is consumed by {@link
   * org.apache.spark.sql.delta.stats.DefaultStateProvider} which handles the full state
   * reconstruction pipeline.
   *
   * @param spark SparkSession
   * @param snapshot Kernel snapshot (must be SnapshotImpl)
   * @return DataFrame of raw log actions ready for state reconstruction
   */
  public static Dataset<Row> loadActionsV2(SparkSession spark, Snapshot snapshot) {
    if (!(snapshot instanceof SnapshotImpl)) {
      throw new IllegalArgumentException("Snapshot must be SnapshotImpl to access LogSegment");
    }
    SnapshotImpl snapshotImpl = (SnapshotImpl) snapshot;
    LogSegment logSegment = snapshotImpl.getLogSegment();

    // Register canonicalizePath UDF (V2's path normalization)
    spark
        .udf()
        .register(
            "canonicalizePath",
            (UDF1<String, String>) DistributedLogReplayHelper::canonicalizePath,
            StringType);

    return loadActions(spark, logSegment);
  }

  /**
   * Performs distributed log replay following V1's Snapshot.stateReconstruction algorithm.
   *
   * <p><b>Note:</b> For batch scans, prefer {@link #loadActionsV2} + {@link
   * org.apache.spark.sql.delta.stats.DefaultStateProvider} which shares the full pipeline with V1.
   * This method is retained for streaming use cases.
   *
   * @param spark SparkSession
   * @param snapshot Kernel snapshot (must be SnapshotImpl)
   * @param numPartitions Number of partitions (default 50, same as V1)
   * @return Dataset of SingleAction rows (final state after log replay)
   */
  public static Dataset<Row> stateReconstructionV2(
      SparkSession spark, Snapshot snapshot, int numPartitions) {

    // Cast to SnapshotImpl to access internal LogSegment
    if (!(snapshot instanceof SnapshotImpl)) {
      throw new IllegalArgumentException("Snapshot must be SnapshotImpl to access LogSegment");
    }

    SnapshotImpl snapshotImpl = (SnapshotImpl) snapshot;
    LogSegment logSegment = snapshotImpl.getLogSegment();

    // Step 1: Load checkpoint and delta files as DataFrame (equivalent to V1's loadActions)
    Dataset<Row> rawActions = loadActions(spark, logSegment);

    // Step 2-5: Apply state reconstruction algorithm using shared utils
    return applyStateReconstructionAlgorithm(spark, rawActions, numPartitions);
  }

  /**
   * V1's loadActions: loads checkpoint + deltas with COMMIT_VERSION_COLUMN.
   *
   * <p>V1 Reference: Snapshot.scala:532-536 Returns DataFrame with SingleAction schema +
   * commitVersion column
   */
  private static Dataset<Row> loadActions(SparkSession spark, LogSegment logSegment) {
    Dataset<Row> checkpointDF = loadCheckpointFiles(spark, logSegment);
    Dataset<Row> deltaDF = loadDeltaFiles(spark, logSegment);

    // Union and ensure commitVersion column exists
    if (checkpointDF.isEmpty()) {
      return deltaDF;
    } else if (deltaDF.isEmpty()) {
      return checkpointDF;
    } else {
      return checkpointDF.unionAll(deltaDF);
    }
  }

  /**
   * Apply state reconstruction using shared StateReconstructionUtils.
   *
   * <p>Steps 2-4 (canonicalize paths, repartition+sort, reconstruct structs) are delegated to the
   * shared {@link StateReconstructionUtils#canonicalizeRepartitionAndReconstruct}. Step 5
   * (deduplication) uses InMemoryLogReplay via {@link StateReconstructionUtils#replayPerPartition}.
   */
  private static Dataset<Row> applyStateReconstructionAlgorithm(
      SparkSession spark, Dataset<Row> loadActions, int numPartitions) {

    // Register canonicalizePath UDF (V2's path normalization)
    spark
        .udf()
        .register(
            "canonicalizePath",
            (UDF1<String, String>) DistributedLogReplayHelper::canonicalizePath,
            StringType);

    // Steps 2-4: Shared with V1 via StateReconstructionUtils
    Dataset<Row> reconstructed =
        StateReconstructionUtils.canonicalizeRepartitionAndReconstruct(
            loadActions,
            numPartitions,
            c -> callUDF("canonicalizePath", c),
            COMMIT_VERSION_COLUMN,
            ADD_STATS_TO_USE_COL);

    // Step 5: Apply InMemoryLogReplay per partition (same as V1).
    // InMemoryLogReplay correctly handles:
    //   - (path, deletionVector.uniqueId) as primary key (not just path)
    //   - Add/Remove reconciliation
    //   - Tombstone expiry
    //   - SetTransaction / Protocol / Metadata dedup
    // V2 uses None for retention timestamps (keeps all tombstones).
    scala.Option<Object> noRetention = scala.Option.empty();
    Dataset<org.apache.spark.sql.delta.actions.SingleAction> replayed =
        StateReconstructionUtils.replayPerPartition(reconstructed, noRetention, noRetention);

    return replayed.toDF();
  }

  /**
   * Canonicalize file path (same as V1's canonicalPath UDF).
   *
   * <p>V1 uses deltaLog.getCanonicalPathUdf() which normalizes the path. For simplicity, we do
   * basic canonicalization here.
   */
  private static String canonicalizePath(String path) {
    if (path == null) {
      return null;
    }
    try {
      // Normalize path: remove .., convert to lowercase for deduplication
      URI uri = new URI(path);
      String normalized = uri.normalize().toString();
      // V1 also does case normalization for deduplication
      return normalized;
    } catch (URISyntaxException e) {
      return path.trim(); // fallback
    }
  }

  /**
   * Load checkpoint parquet files as DataFrame (V1's loadIndex for checkpoint).
   *
   * <p>V1: Snapshot.loadIndex reads checkpoint with SingleAction schema.
   */
  private static Dataset<Row> loadCheckpointFiles(SparkSession spark, LogSegment logSegment) {

    List<FileStatus> checkpoints = logSegment.getCheckpoints();

    if (checkpoints.isEmpty()) {
      return spark.createDataFrame(new ArrayList<Row>(), getSingleActionSchema());
    }

    // Get checkpoint file paths
    String[] checkpointPaths = checkpoints.stream().map(FileStatus::getPath).toArray(String[]::new);

    // Read checkpoint as parquet with SingleAction schema
    Dataset<Row> checkpointDF =
        spark
            .read()
            .format("parquet")
            .schema(getSingleActionSchema()) // Enforce schema
            .load(checkpointPaths);

    // Add commit version column
    long checkpointVersion =
        logSegment
            .getCheckpointVersionOpt()
            .orElseThrow(() -> new IllegalStateException("Checkpoint version not found"));

    // Add ADD_STATS_TO_USE_COL (V1 uses add.stats directly)
    return checkpointDF
        .withColumn(COMMIT_VERSION_COLUMN, lit(checkpointVersion))
        .withColumn(ADD_STATS_TO_USE_COL, col("add.stats"));
  }

  /** Load delta JSON files as DataFrame. */
  private static Dataset<Row> loadDeltaFiles(SparkSession spark, LogSegment logSegment) {

    List<FileStatus> deltas = logSegment.getDeltas();

    if (deltas.isEmpty()) {
      return spark.emptyDataFrame();
    }

    // Get delta file paths with their versions
    Map<String, Long> deltaPathToVersion =
        deltas.stream()
            .collect(
                Collectors.toMap(FileStatus::getPath, fs -> extractDeltaVersion(fs.getPath())));

    String[] deltaPaths = deltaPathToVersion.keySet().toArray(new String[0]);

    // Read deltas as JSON with Delta's SingleAction schema
    Dataset<Row> deltaDF =
        spark.read().format("json").schema(getSingleActionSchema()).load(deltaPaths);

    // Add commit version based on input file name
    // Use input_file_name() to get the source file path
    deltaDF = deltaDF.withColumn("_input_file", input_file_name());

    // Create UDF to extract version from file path
    spark
        .udf()
        .register(
            "extractVersionFromPath",
            (UDF1<String, Long>) DistributedLogReplayHelper::extractDeltaVersion,
            LongType);

    return deltaDF
        .withColumn(COMMIT_VERSION_COLUMN, callUDF("extractVersionFromPath", col("_input_file")))
        .withColumn(ADD_STATS_TO_USE_COL, col("add.stats")) // Use add.stats directly
        .drop("_input_file");
  }

  /** Extract delta version from file path (e.g., "00000000000000000123.json" -> 123) */
  private static Long extractDeltaVersion(String filePath) {
    String fileName = filePath.substring(filePath.lastIndexOf('/') + 1);
    // Remove .json extension
    String versionStr = fileName.replace(".json", "");
    try {
      return Long.parseLong(versionStr);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid delta file name: " + fileName, e);
    }
  }

  /**
   * Get Delta's SingleAction schema for reading log files (JSON deltas and Parquet checkpoints).
   *
   * <p>Uses V1's {@code Action.logSchema} which is auto-derived from the SingleAction case class,
   * ensuring the schema stays in sync with the actual action definitions (including all fields like
   * deletionVector.maxRowIndex, etc.).
   */
  private static StructType getSingleActionSchema() {
    return org.apache.spark.sql.delta.actions.Action.logSchema();
  }

  // NOTE: Stats schema construction, stats parsing, verifyStatsForFilter, and data skipping
  // filter application are now in the shared DataFiltersBuilderUtils (V1 spark module),
  // accessed via DataFiltersBuilderV2. No data-skipping logic should live here.

  // ==================== STREAMING SUPPORT ====================

  /**
   * Get initial snapshot files for streaming **with proper sorting**.
   *
   * <p>V1 Reference: DeltaSourceSnapshot.filteredFiles (DeltaSourceSnapshot.scala:60-85)
   *
   * <p>Streaming uses DIFFERENT algorithm than batch: - Batch: repartition(path) +
   * sortWithinPartitions(commitVersion) - for deduplication - Streaming:
   * repartitionByRange(modificationTime, path) + sort - for time ordering
   *
   * <p>Why different? - Streaming needs files ordered by time for incremental processing - Batch
   * needs files grouped by path for deduplication
   *
   * @param spark SparkSession
   * @param snapshot Kernel snapshot
   * @param numPartitions Number of partitions
   * @return Dataset of files sorted by modification time and path
   */
  public static Dataset<Row> getInitialSnapshotForStreaming(
      SparkSession spark, Snapshot snapshot, int numPartitions) {

    // Step 1: Get all files from state reconstruction (distributed log replay)
    // This returns DataFrame with "add" struct column
    Dataset<Row> allFiles = stateReconstructionV2(spark, snapshot, numPartitions);

    // Step 2: Apply DeltaSource's sorting algorithm
    // V1 Code (DeltaSourceSnapshot.scala:64-68):
    // snapshot.allFiles
    //   .repartitionByRange(numPartitions, col("modificationTime"), col("path"))
    //   .sort("modificationTime", "path")
    //
    // Note: Need to access fields inside "add" struct

    Dataset<Row> sortedFiles =
        allFiles
            .repartitionByRange(numPartitions, col("add.modificationTime"), col("add.path"))
            .sort("add.modificationTime", "add.path");

    // Step 3: Add index for tracking (V1: zipWithIndex)
    // In Spark SQL, we can use row_number() window function
    sortedFiles =
        sortedFiles.withColumn(
            "index",
            row_number()
                .over(
                    org.apache.spark.sql.expressions.Window.orderBy(
                        "add.modificationTime", "add.path"))
                .minus(lit(1))); // 0-based index

    // Step 4: Null out stats for streaming (V1: line 73)
    // Streaming doesn't need stats, saves memory
    sortedFiles =
        sortedFiles.withColumn("add", col("add").withField("stats", lit(null).cast(StringType)));

    return sortedFiles;
  }

  /**
   * Apply partition filters for streaming initial snapshot.
   *
   * <p>V1 Reference: DeltaSourceSnapshot.filteredFiles (line 80-84) Uses DeltaLog.filterFileList
   * for partition filtering.
   *
   * @param sortedFiles Files sorted by time
   * @param partitionSchema Partition schema
   * @param partitionFilters Partition filter expressions (SQL strings)
   * @return Filtered dataset
   */
  public static Dataset<Row> applyPartitionFiltersForStreaming(
      Dataset<Row> sortedFiles, StructType partitionSchema, String[] partitionFilters) {

    if (partitionFilters == null || partitionFilters.length == 0) {
      return sortedFiles;
    }

    // Apply each partition filter
    Dataset<Row> filtered = sortedFiles;
    for (String filter : partitionFilters) {
      if (filter != null && !filter.isEmpty()) {
        filtered = filtered.where(filter);
      }
    }

    return filtered;
  }
}
