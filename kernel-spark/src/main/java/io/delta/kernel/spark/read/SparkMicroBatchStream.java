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

import io.delta.kernel.*;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.UnsupportedTableFeatureException;
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.spark.utils.StreamingHelper;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.delta.DeltaErrors;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.DeltaStartingVersion;
import org.apache.spark.sql.delta.StartingVersion;
import org.apache.spark.sql.delta.StartingVersionLatest$;
import org.apache.spark.sql.delta.sources.DeltaSQLConf;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

public class SparkMicroBatchStream implements MicroBatchStream {

  private static final Logger logger = LoggerFactory.getLogger(SparkMicroBatchStream.class);

  private static final Set<DeltaAction> ACTION_SET =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList(DeltaAction.ADD, DeltaAction.REMOVE)));

  private final Engine engine;
  private final String tablePath;
  private final DeltaOptions options;
  private final StreamingHelper streamingHelper;
  private final SparkSession spark;

  public SparkMicroBatchStream(String tablePath, Configuration hadoopConf) {
    this(
        tablePath,
        hadoopConf,
        SparkSession.active(),
        new DeltaOptions(
            scala.collection.immutable.Map$.MODULE$.empty(),
            SparkSession.active().sessionState().conf()));
  }

  public SparkMicroBatchStream(
      String tablePath, Configuration hadoopConf, SparkSession spark, DeltaOptions options) {
    this.spark = spark;
    this.tablePath = tablePath;
    this.engine = DefaultEngine.create(hadoopConf);
    this.options = options;
    this.streamingHelper = new StreamingHelper(tablePath, hadoopConf);
  }

  ////////////
  // offset //
  ////////////

  @Override
  public Offset initialOffset() {
    throw new UnsupportedOperationException("initialOffset is not supported");
  }

  @Override
  public Offset latestOffset() {
    throw new UnsupportedOperationException("latestOffset is not supported");
  }

  @Override
  public Offset deserializeOffset(String json) {
    throw new UnsupportedOperationException("deserializeOffset is not supported");
  }

  ////////////
  /// data ///
  ////////////

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {
    throw new UnsupportedOperationException("planInputPartitions is not supported");
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    throw new UnsupportedOperationException("createReaderFactory is not supported");
  }

  ///////////////
  // lifecycle //
  ///////////////

  @Override
  public void commit(Offset end) {
    throw new UnsupportedOperationException("commit is not supported");
  }

  @Override
  public void stop() {
    throw new UnsupportedOperationException("stop is not supported");
  }

  ///////////////////////
  // getStartingVersion //
  ///////////////////////

  /**
   * Extracts whether users provided the option to time travel a relation. If a query restarts from
   * a checkpoint and the checkpoint has recorded the offset, this method should never be called.
   *
   * <p>This is the DSv2 Kernel-based implementation of DeltaSource.getStartingVersion.
   */
  Optional<Long> getStartingVersion() {
    // TODO(#5319): DeltaSource.scala uses `allowOutOfRange` parameter from
    // DeltaSQLConf.DELTA_CDF_ALLOW_OUT_OF_RANGE_TIMESTAMP.
    if (options.startingVersion().isDefined()) {
      DeltaStartingVersion startingVersion = options.startingVersion().get();
      if (startingVersion instanceof StartingVersionLatest$) {
        Snapshot latestSnapshot = streamingHelper.loadLatestSnapshot();
        // "latest": start reading from the next commit
        return Optional.of(latestSnapshot.getVersion() + 1);
      } else if (startingVersion instanceof StartingVersion) {
        long version = ((StartingVersion) startingVersion).version();
        if (!validateProtocolAt(spark, tablePath, engine, version)) {
          // When starting from a given version, we don't require that the snapshot of this
          // version can be reconstructed, even though the input table is technically in an
          // inconsistent state. If the snapshot cannot be reconstructed, then the protocol
          // check is skipped, so this is technically not safe, but we keep it this way for
          // historical reasons.
          streamingHelper.checkVersionExists(
              version, /* mustBeRecreatable= */ false, /* allowOutOfRange= */ false);
        }
        return Optional.of(version);
      }
    }
    // TODO(#5319): Implement startingTimestamp support
    return Optional.empty();
  }

  /**
   * Validate the protocol at a given version. If the snapshot reconstruction fails for any other
   * reason than unsupported feature exception, we suppress it. This allows fallback to previous
   * behavior where the starting version/timestamp was not mandatory to point to reconstructable
   * snapshot.
   *
   * <p>This is the DSv2 Kernel-based implementation of DeltaSource.validateProtocolAt.
   *
   * <p>Returns true when the validation was performed and succeeded.
   */
  private static boolean validateProtocolAt(
      SparkSession spark, String tablePath, Engine engine, long version) {
    boolean alwaysValidateProtocol =
        (Boolean)
            spark
                .sessionState()
                .conf()
                .getConf(DeltaSQLConf.FAST_DROP_FEATURE_STREAMING_ALWAYS_VALIDATE_PROTOCOL());
    if (!alwaysValidateProtocol) {
      return false;
    }

    try {
      // Attempt to construct a snapshot at the startingVersion to validate the protocol
      // If snapshot reconstruction fails, fall back to old behavior where the only
      // requirement was for the commit to exist
      TableManager.loadSnapshot(tablePath).atVersion(version).build(engine);
      return true;
    } catch (UnsupportedTableFeatureException e) {
      // Re-throw fatal unsupported table feature exceptions
      throw e;
    } catch (Exception e) {
      // Suppress non-fatal exceptions
      logger.warn("Protocol validation failed at version {} with: {}", version, e.getMessage());
      return false;
    }
  }

  ////////////////////
  // getFileChanges //
  ////////////////////

  /**
   * Get file changes between fromVersion/fromIndex and endOffset. This is the Kernel-based
   * implementation of DeltaSource.getFileChanges.
   *
   * <p>Package-private for testing.
   *
   * @param fromVersion The starting version (exclusive with fromIndex)
   * @param fromIndex The starting index within fromVersion (exclusive)
   * @param isInitialSnapshot Whether this is the initial snapshot
   * @param endOffset The end offset (inclusive), or empty to read all available commits
   * @return An iterator of IndexedFile representing the file changes
   */
  CloseableIterator<IndexedFile> getFileChanges(
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Option<DeltaSourceOffset> endOffset) {

    CloseableIterator<IndexedFile> result;

    if (isInitialSnapshot) {
      // TODO(#5318): Implement initial snapshot
      throw new UnsupportedOperationException("initial snapshot is not supported yet");
    } else {
      result = filterDeltaLogs(fromVersion, endOffset);
    }

    // Check start boundary (exclusive)
    result =
        result.filter(
            file ->
                file.getVersion() > fromVersion
                    || (file.getVersion() == fromVersion && file.getIndex() > fromIndex));

    // Check end boundary (inclusive)
    if (endOffset.isDefined()) {
      DeltaSourceOffset bound = endOffset.get();
      result =
          result.takeWhile(
              file ->
                  file.getVersion() < bound.reservoirVersion()
                      || (file.getVersion() == bound.reservoirVersion()
                          && file.getIndex() <= bound.index()));
    }

    return result;
  }

  // TODO(#5318): implement lazy loading (one batch at a time).
  private CloseableIterator<IndexedFile> filterDeltaLogs(
      long startVersion, Option<DeltaSourceOffset> endOffset) {
    List<IndexedFile> allIndexedFiles = new ArrayList<>();
    // StartBoundary (inclusive)
    CommitRangeBuilder builder =
        TableManager.loadCommitRange(tablePath)
            .withStartBoundary(CommitRangeBuilder.CommitBoundary.atVersion(startVersion));
    if (endOffset.isDefined()) {
      // EndBoundary (inclusive)
      builder =
          builder.withEndBoundary(
              CommitRangeBuilder.CommitBoundary.atVersion(endOffset.get().reservoirVersion()));
    }
    CommitRange commitRange = builder.build(engine);
    // Required by kernel: perform protocol validation by creating a snapshot at startVersion.
    // TODO(#5318): This is not working with ccv2 table
    Snapshot startSnapshot =
        TableManager.loadSnapshot(tablePath).atVersion(startVersion).build(engine);
    try (CloseableIterator<ColumnarBatch> actionsIter =
        commitRange.getActions(engine, startSnapshot, ACTION_SET)) {
      // Each ColumnarBatch belongs to a single commit version,
      // but a single version may span multiple ColumnarBatches.
      long currentVersion = -1;
      long currentIndex = 0;
      List<IndexedFile> currentVersionFiles = new ArrayList<>();

      while (actionsIter.hasNext()) {
        ColumnarBatch batch = actionsIter.next();
        if (batch.getSize() == 0) {
          // TODO(#5318): this shouldn't happen, empty commits will still have a non-empty row
          // with the version set. Make sure the kernel API is explicit about this.
          continue;
        }
        long version = StreamingHelper.getVersion(batch);
        // When version changes, flush the completed version
        if (currentVersion != -1 && version != currentVersion) {
          flushVersion(currentVersion, currentVersionFiles, allIndexedFiles);
          currentVersionFiles.clear();
          currentIndex = 0;
        }

        // Validate the commit before processing files from this batch
        // TODO(#5318): migrate to kernel's commit-level iterator (WIP).
        // The current one-pass algorithm assumes REMOVE actions proceed ADD actions
        // in a commit; we should implement a proper two-pass approach once kernel API is ready.
        validateCommit(batch, version, endOffset);

        currentVersion = version;
        currentIndex =
            extractIndexedFilesFromBatch(batch, version, currentIndex, currentVersionFiles);
      }

      // Flush the last version
      if (currentVersion != -1) {
        flushVersion(currentVersion, currentVersionFiles, allIndexedFiles);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read commit range", e);
    }
    // TODO(#5318): implement lazy loading (only load a batch into memory if needed).
    return Utils.toCloseableIterator(allIndexedFiles.iterator());
  }

  /**
   * Flushes a completed version by adding BEGIN/END sentinels around data files.
   *
   * <p>Sentinels are IndexedFiles with null addFile that mark version boundaries. They serve
   * several purposes:
   *
   * <ul>
   *   <li>Enable offset tracking at version boundaries (before any files or after all files)
   *   <li>Allow streaming to resume at the start or end of a version
   *   <li>Handle versions with only metadata/protocol changes (no data files)
   * </ul>
   *
   * <p>This mimics DeltaSource.addBeginAndEndIndexOffsetsForVersion
   */
  private void flushVersion(
      long version, List<IndexedFile> versionFiles, List<IndexedFile> output) {
    // Add BEGIN sentinel
    output.add(new IndexedFile(version, DeltaSourceOffset.BASE_INDEX(), /* addFile= */ null));
    // TODO(#5319): implement getMetadataOrProtocolChangeIndexedFileIterator.
    // Add all data files
    output.addAll(versionFiles);
    // Add END sentinel
    output.add(new IndexedFile(version, DeltaSourceOffset.END_INDEX(), /* addFile= */ null));
  }

  /**
   * Validates a commit and fail the stream if it's invalid. Mimics
   * DeltaSource.validateCommitAndDecideSkipping in Scala.
   *
   * @throws RuntimeException if the commit is invalid.
   */
  private void validateCommit(
      ColumnarBatch batch, long version, Option<DeltaSourceOffset> endOffsetOpt) {
    // If endOffset is at the beginning of this version, exit early.
    if (endOffsetOpt.isDefined()) {
      DeltaSourceOffset endOffset = endOffsetOpt.get();
      if (endOffset.reservoirVersion() == version
          && endOffset.index() == DeltaSourceOffset.BASE_INDEX()) {
        return;
      }
    }
    int numRows = batch.getSize();
    // TODO(#5319): Implement ignoreChanges & skipChangeCommits & ignoreDeletes (legacy)
    // TODO(#5318): validate METADATA actions
    for (int rowId = 0; rowId < numRows; rowId++) {
      // RULE 1: If commit has RemoveFile(dataChange=true), fail this stream.
      Optional<RemoveFile> removeOpt = StreamingHelper.getDataChangeRemove(batch, rowId);
      if (removeOpt.isPresent()) {
        RemoveFile removeFile = removeOpt.get();
        Throwable error =
            DeltaErrors.deltaSourceIgnoreDeleteError(version, removeFile.getPath(), tablePath);
        if (error instanceof RuntimeException) {
          throw (RuntimeException) error;
        } else {
          throw new RuntimeException(error);
        }
      }
    }
  }

  /**
   * Extracts IndexedFiles from a batch of actions for a given version and adds them to the output
   * list. Assigns an index to each IndexedFile.
   *
   * @return The next available index after processing this batch
   */
  private long extractIndexedFilesFromBatch(
      ColumnarBatch batch, long version, long startIndex, List<IndexedFile> output) {
    long index = startIndex;
    for (int rowId = 0; rowId < batch.getSize(); rowId++) {
      Optional<AddFile> addOpt = StreamingHelper.getDataChangeAdd(batch, rowId);
      if (addOpt.isPresent()) {
        AddFile addFile = addOpt.get();
        output.add(new IndexedFile(version, index++, addFile));
      }
    }

    return index;
  }
}
