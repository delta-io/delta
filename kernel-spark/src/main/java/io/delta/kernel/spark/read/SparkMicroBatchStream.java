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
package io.delta.kernel.spark.read;

import io.delta.kernel.CommitRange;
import io.delta.kernel.CommitRangeBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.spark.utils.PartitionUtils;
import io.delta.kernel.spark.utils.SchemaUtils;
import io.delta.kernel.spark.utils.SparkRuntimeUtils;
import io.delta.kernel.utils.CloseableIterator;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.paths.SparkPath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;

public class SparkMicroBatchStream implements MicroBatchStream {

  private static final Logger logger = LoggerFactory.getLogger(SparkMicroBatchStream.class);

  private final String tablePath;
  private final Map<String, String> options;
  private final Engine engine;
  private final KernelStreamingOptions kernelOptions;
  // Note: SparkSession will be available at runtime, but not in kernel-only compilation

  /** Snapshot at the time this source was initialized. */
  private final Snapshot snapshotAtSourceInit;

  private final String tableId;

  /** Cache for CommitRange data to avoid duplicate processing */
  private final Map<String, CommitRangeData> commitRangeCache = new HashMap<>();

  /** Helper class to store CommitRange processing results */
  private static class CommitRangeData {
    final long lastVersionWithChanges;
    final long lastIndex;
    final List<PartitionedFile> partitionedFiles;

    CommitRangeData(
        long lastVersionWithChanges, long lastIndex, List<PartitionedFile> partitionedFiles) {
      this.lastVersionWithChanges = lastVersionWithChanges;
      this.lastIndex = lastIndex;
      this.partitionedFiles = partitionedFiles;
    }
  }

  public SparkMicroBatchStream(String tablePath, Map<String, String> options) {
    this.tablePath = tablePath;
    this.options = options;
    // Create engine with default Hadoop configuration
    Configuration hadoopConf = new Configuration();
    this.engine = DefaultEngine.create(hadoopConf);
    this.kernelOptions = new KernelStreamingOptions(options);

    this.snapshotAtSourceInit = TableManager.loadSnapshot(tablePath).build(engine);
    // Generate table ID from table path since getMetadata() is not exposed
    this.tableId = tablePath.hashCode() + "_" + System.currentTimeMillis();
  }

  ////////////
  // offset //
  ////////////

  @Override
  public Offset initialOffset() {
    Long startingVersion = getStartingVersion();

    if (startingVersion == null) {
      // No starting version specified, start from the latest snapshot
      return getStartingOffsetFromSpecificVersion(
          snapshotAtSourceInit.getVersion(), /*isInitialSnapshot=*/ true);
    } else if (startingVersion < 0) {
      // Negative version means no offset should be returned
      return null;
    } else {
      // Use the specified starting version
      return getStartingOffsetFromSpecificVersion(startingVersion, /*isInitialSnapshot=*/ false);
    }
  }

  /**
   * Extracts the user-provided options to time travel to a specific version or timestamp. Supports
   * options: startingVersion and startingTimestamp.
   *
   * @return Starting version, or null if should start from latest
   */
  private Long getStartingVersion() {
    if (kernelOptions.hasStartingVersion()) {
      Long startingVersion = kernelOptions.getStartingVersion();
      if (startingVersion == -1L) {
        // "latest" was specified - get the latest version + 1
        Snapshot latestSnapshot = TableManager.loadSnapshot(tablePath).build(engine);
        return latestSnapshot.getVersion() + 1;
      } else {
        // Specific version was specified
        validateProtocolAt(startingVersion);
        return startingVersion;
      }
    } else if (kernelOptions.hasStartingTimestamp()) {
      Timestamp timestamp = kernelOptions.getStartingTimestamp();
      return getStartingVersionFromTimestamp(timestamp);
    }

    return null;
  }

  /**
   * This method analyzes file changes from a specific version and creates an appropriate offset
   * based on the last file change found.
   *
   * @return KernelSourceOffset or null if no file changes exist
   */
  private KernelSourceOffset getStartingOffsetFromSpecificVersion(
      long fromVersion, boolean isInitialSnapshot) {
    try {
      if (isInitialSnapshot) {
        // For initial snapshot, we want to start reading from the files in this version
        // Create an offset that represents the end of this version's files
        return KernelSourceOffset.apply(
            tableId,
            fromVersion,
            KernelSourceOffset.END_INDEX, // Use END_INDEX to indicate end of version
            isInitialSnapshot);
      } else {
        // Load the snapshot at the starting version to ensure it's valid
        Snapshot startSnapshot =
            TableManager.loadSnapshot(tablePath).atVersion(fromVersion).build(engine);
        // For non-initial snapshots, we want to find the next version with file changes
        // Look for the next commit that has add file actions
        Snapshot latestSnapshot = TableManager.loadSnapshot(tablePath).build(engine);
        long endVersion = latestSnapshot.getVersion();

        if (fromVersion > endVersion) {
          // No commits beyond the requested version
          return null;
        }

        // Use the helper method to process CommitRange and get offset information
        CommitRangeData data = processCommitRange(fromVersion, endVersion);

        if (data.lastVersionWithChanges == fromVersion
            && data.lastIndex == KernelSourceOffset.BASE_INDEX) {
          // No file changes found
          return null;
        }

        return KernelSourceOffset.apply(
            tableId, data.lastVersionWithChanges, data.lastIndex, false // Not an initial snapshot
            );
      }
    } catch (Exception e) {
      logger.error("Failed to get starting offset from version " + fromVersion, e);
      throw new RuntimeException("Failed to get starting offset", e);
    }
  }

  /**
   * - If a commit version exactly matches the provided timestamp, we return it. - Otherwise, we
   * return the earliest commit version with a timestamp greater than the provided one. - If the
   * provided timestamp is larger than the timestamp of any committed version, we throw an error.
   *
   * <p>When the timestamp exceeds the latest commit timestamp, this method throws an exception
   * matching the DSv1 behavior (when DELTA_CDF_ALLOW_OUT_OF_RANGE_TIMESTAMP=false, which is the
   * default).
   */
  private Long getStartingVersionFromTimestamp(Timestamp timestamp) {
    // TODO(parity): support canExceedLatest when CDF is supported
    long millisSinceEpoch = timestamp.getTime();

    // TODO(parity): consider using Kernel's getActiveCommitAtTime if supported.
    try {
      Snapshot snapshotAtTimestamp =
          TableManager.loadSnapshot(tablePath)
              .atTimestamp(millisSinceEpoch, snapshotAtSourceInit)
              .build(engine);
      return snapshotAtTimestamp.getVersion();
    } catch (Exception kernelException) {
      // Handle the case where timestamp exceeds latest commit timestamp
      // Re-throw with kernel-compatible error
      throw new IllegalArgumentException(
          "Timestamp "
              + timestamp
              + " is greater than latest commit timestamp "
              + new Timestamp(snapshotAtSourceInit.getTimestamp(engine)));
    }
  }

  /**
   * Validate the protocol at a given version. If the snapshot reconstruction fails for any other
   * reason than table feature exception, we suppress it. This allows to fallback to previous
   * behavior where the starting version/timestamp was not mandatory to point to reconstructable
   * snapshot.
   *
   * <p>Returns true when the validation was performed and succeeded.
   */
  public boolean validateProtocolAt(long version) {
    // For kernel-based implementation, always validate protocol for now
    // In a full implementation, this could read from kernel-specific configuration

    try {
      // We attempt to construct a snapshot at the startingVersion in order to validate the
      // protocol. If snapshot reconstruction fails, fall back to the old behavior where the
      // only requirement was for the commit to exist.
      TableManager.loadSnapshot(this.tablePath).atVersion(version).build(this.engine);
      return true;
    } catch (Exception e) {
      logger.error("Protocol validation failed for version " + version, e);
      // For kernel implementation, we're more strict about version validation
      throw new RuntimeException("Failed to validate protocol at version " + version, e);
    }
  }

  @Override
  public Offset latestOffset() {
    try {
      // Get the latest snapshot
      Snapshot latestSnapshot = TableManager.loadSnapshot(tablePath).build(engine);
      long latestVersion = latestSnapshot.getVersion();

      // For the latest offset, we want to include all files up to the latest version
      return KernelSourceOffset.apply(
          tableId, latestVersion, KernelSourceOffset.END_INDEX, false // Not an initial snapshot
          );
    } catch (Exception e) {
      logger.error("Failed to get latest offset", e);
      throw new RuntimeException("Failed to get latest offset", e);
    }
  }

  @Override
  public Offset deserializeOffset(String json) {
    try {
      // Use KernelSourceOffset's built-in deserialization capability
      return KernelSourceOffset.fromJson(tableId, json);
    } catch (Exception e) {
      logger.error("Failed to deserialize offset: " + json, e);
      throw new RuntimeException("Failed to deserialize offset", e);
    }
  }

  ////////////
  /// data ///
  ////////////

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {
    try {
      KernelSourceOffset startOffset = (KernelSourceOffset) start;
      KernelSourceOffset endOffset = (KernelSourceOffset) end;

      if (startOffset == null) {
        // No start offset, return empty partitions
        return new InputPartition[0];
      }

      // Validate table IDs match
      if (!startOffset.getTableId().equals(tableId) || !endOffset.getTableId().equals(tableId)) {
        throw new IllegalArgumentException("Offset table ID mismatch");
      }

      long startVersion = startOffset.getVersion();
      long endVersion = endOffset.getVersion();

      // Get the add files from the commit range
      List<PartitionedFile> partitionedFiles =
          getPartitionedFilesFromCommitRange(startVersion, endVersion);

      if (partitionedFiles.isEmpty()) {
        logger.info("No files found for version range [{}, {}]", startVersion, endVersion);
        return new InputPartition[0];
      }

      // Use Spark's FilePartition to split files appropriately, same as SparkBatch
      // Use runtime utils to avoid compile-time SparkSession dependency but get optimal split
      // calculation
      Object sparkSession = SparkRuntimeUtils.getActiveSparkSession();
      long maxSplitBytes = PartitionUtils.calculateMaxSplitBytes(sparkSession, partitionedFiles);

      InputPartition[] partitions =
          SparkRuntimeUtils.getFilePartitions(partitionedFiles, maxSplitBytes);

      logger.info(
          "Created {} input partitions for version range [{}, {}] with {} files",
          partitions.length,
          startVersion,
          endVersion,
          partitionedFiles.size());

      return partitions;
    } catch (Exception e) {
      logger.error("Failed to plan input partitions", e);
      throw new RuntimeException("Failed to plan input partitions", e);
    }
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    try {
      // Use the same approach as SparkBatch - get schema and create SparkReaderFactory
      Snapshot currentSnapshot = TableManager.loadSnapshot(tablePath).build(engine);
      org.apache.spark.sql.types.StructType dataSchema =
          SchemaUtils.convertKernelSchemaToSparkSchema(currentSnapshot.getSchema());
      org.apache.spark.sql.types.StructType partitionSchema =
          new org.apache.spark.sql.types.StructType(); // No partitioning for now

      // Configure Parquet reader the same way as SparkBatch
      boolean enableVectorizedReader =
          ParquetUtils.isBatchReadSupportedForSchema(SQLConf.get(), dataSchema);

      scala.collection.immutable.Map<String, String> optionsWithBatch =
          scala.collection.immutable.Map$.MODULE$
              .<String, String>empty()
              .$plus(
                  new Tuple2<>(
                      FileFormat$.MODULE$.OPTION_RETURNING_BATCH(),
                      String.valueOf(enableVectorizedReader)));

      // Create read function using Parquet format, same as SparkBatch
      Function1<PartitionedFile, Iterator<InternalRow>> readFunc =
          new ParquetFileFormat()
              .buildReaderWithPartitionValues(
                  (org.apache.spark.sql.SparkSession) SparkRuntimeUtils.getActiveSparkSession(),
                  dataSchema,
                  partitionSchema,
                  dataSchema, // readDataSchema same as dataSchema for now
                  JavaConverters.asScalaBuffer(Arrays.<Filter>asList())
                      .toSeq(), // No data filters for now
                  optionsWithBatch,
                  new Configuration());

      return new SparkReaderFactory(readFunc, enableVectorizedReader);
    } catch (Exception e) {
      logger.error("Failed to create reader factory", e);
      throw new RuntimeException("Failed to create reader factory", e);
    }
  }

  /**
   * Helper method that processes a CommitRange and extracts both offset information and partitioned
   * files. This eliminates duplication between offset calculation and file extraction.
   */
  private CommitRangeData processCommitRange(long fromVersion, long endVersion) throws Exception {
    String cacheKey = fromVersion + "-" + endVersion;

    // Check cache first
    if (commitRangeCache.containsKey(cacheKey)) {
      return commitRangeCache.get(cacheKey);
    }

    List<PartitionedFile> partitionedFiles = new ArrayList<>();
    long lastVersionWithChanges = fromVersion;
    long lastIndex = KernelSourceOffset.BASE_INDEX;

    // Load snapshot at start version for reading
    Snapshot startSnapshot =
        TableManager.loadSnapshot(tablePath).atVersion(fromVersion).build(engine);

    // Create commit range for the version range
    CommitRange commitRange =
        TableManager.loadCommitRange(tablePath)
            .withStartBoundary(CommitRangeBuilder.CommitBoundary.atVersion(fromVersion))
            .withEndBoundary(CommitRangeBuilder.CommitBoundary.atVersion(endVersion))
            .build(engine);

    // Get add file actions from the commit range
    Set<io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction> actionTypes =
        Collections.singleton(io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction.ADD);
    try (CloseableIterator<ColumnarBatch> actionIter =
        commitRange.getActions(engine, startSnapshot, actionTypes)) {

      while (actionIter.hasNext()) {
        ColumnarBatch batch = actionIter.next();
        if (batch.getSize() > 0) {
          // Get the version from this batch for offset calculation
          ColumnVector versionVector = batch.getColumnVector(0); // version is first column

          // Process each add file action in the batch
          try (CloseableIterator<Row> rowIterator = batch.getRows()) {
            int rowIndex = 0;
            while (rowIterator.hasNext()) {
              Row row = rowIterator.next();
              if (row != null) {
                // Update offset information
                long version = versionVector.getLong(rowIndex);
                lastVersionWithChanges = Math.max(lastVersionWithChanges, version);
                lastIndex = rowIndex; // Use the row index as the file index

                // Extract add file information from the row
                // Assuming the add column is at index 2 (after version and timestamp)
                if (!row.isNullAt(2)) {
                  Row addRow = row.getStruct(2);
                  AddFile addFile = new AddFile(addRow);

                  // Create PartitionedFile from AddFile
                  SparkPath sparkPath = SparkPath.fromPathString(addFile.getPath());
                  InternalRow partitionRow = InternalRow.empty(); // Empty partition for now
                  PartitionedFile partitionedFile =
                      new PartitionedFile(
                          partitionRow,
                          sparkPath,
                          0L, // start offset
                          addFile.getSize(),
                          new String[0], // no partition values for now
                          addFile.getModificationTime(),
                          addFile.getSize(),
                          scala.collection.immutable.Map$.MODULE$
                              .<String, Object>empty() // no partition spec
                          );

                  partitionedFiles.add(partitionedFile);
                }
                rowIndex++;
              }
            }
          }
        }
      }
    }

    // Create and cache the result
    CommitRangeData result =
        new CommitRangeData(lastVersionWithChanges, lastIndex, partitionedFiles);
    commitRangeCache.put(cacheKey, result);
    return result;
  }

  /** Extract PartitionedFile objects from the commit range for the given version range. */
  private List<PartitionedFile> getPartitionedFilesFromCommitRange(
      long startVersion, long endVersion) throws Exception {
    // Use the helper method that processes CommitRange and caches results
    CommitRangeData data = processCommitRange(startVersion, endVersion);
    return data.partitionedFiles;
  }

  ///////////////
  // lifecycle //
  ///////////////

  @Override
  public void commit(Offset end) {
    // Validate the offset
    if (end != null) {
      KernelSourceOffset kernelEnd = (KernelSourceOffset) end;
      if (!kernelEnd.getTableId().equals(tableId)) {
        throw new IllegalArgumentException("Offset table ID mismatch during commit");
      }
      logger.debug("Committed offset: {}", end);
    }
    // For streaming sources, commit typically just validates the offset
    // The actual checkpoint management is handled by Spark's streaming engine
  }

  @Override
  public void stop() {
    logger.info("Stopping Delta streaming source for table: {}", tablePath);
    // Cleanup any resources if needed
    // Engine cleanup is handled by the DefaultEngine implementation
  }
}
