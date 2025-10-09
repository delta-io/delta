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
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.spark.utils.StreamingHelper;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.delta.DeltaErrors;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import scala.Option;

public class SparkMicroBatchStream implements MicroBatchStream {

  private final Engine engine;
  private final String tablePath;

  public SparkMicroBatchStream(String tablePath, Configuration hadoopConf) {
    this.tablePath = tablePath;
    this.engine = DefaultEngine.create(hadoopConf);
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

  ////////////////////
  // getFileChanges //
  ////////////////////

  /**
   * Get file changes between fromVersion/fromIndex and endOffset. This is the Kernel-based
   * implementation of DeltaSource.getFileChanges.
   *
   * <p>Package-private for testing.
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

  // TODO(M1): implement lazy loading (one batch at a time).
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
    Snapshot startSnapshot =
        TableManager.loadSnapshot(tablePath).atVersion(startVersion).build(engine);
    // TODO(M1): This is not working with ccv2 table
    Set<DeltaAction> actionSet = new HashSet<>(Arrays.asList(DeltaAction.ADD, DeltaAction.REMOVE));
    try (CloseableIterator<ColumnarBatch> actionsIter =
        commitRange.getActions(engine, startSnapshot, actionSet)) {
      // Each batch is guaranteed to have the same commit version.
      // A version can be split across multiple batches.
      long currentVersion = -1;
      long currentIndex = 0;
      List<IndexedFile> currentVersionFiles = new ArrayList<>();

      while (actionsIter.hasNext()) {
        ColumnarBatch batch = actionsIter.next();
        if (batch.getSize() == 0) {
          continue;
        }
        long version = StreamingHelper.getVersion(batch);
        // TODO(M1): migrate to kernel's commit-level iterator (WIP).
        // The current one-pass algorithm assumes REMOVE actions proceed ADD actions
        // in a commit; we should implement a proper two-pass approach once kernel API is ready.

        if (currentVersion != -1 && version != currentVersion) {
          // Process the previous version's files
          List<IndexedFile> currentVersionFilesWithSentinels =
              addBeginAndEndIndexOffsetsForVersion(currentVersion, currentVersionFiles);
          allIndexedFiles.addAll(currentVersionFilesWithSentinels);
          // Reset for the new version -- we've added all version files from this version to
          // allIndexedFiles.
          currentVersionFiles.clear();
          currentIndex = 0;
        }

        // Validate the commit before processing files from this batch
        validateCommit(batch, version, endOffset);

        // Update current version tracking
        currentVersion = version;
        List<IndexedFile> batchFiles = extractIndexedFilesFromBatch(batch, version, currentIndex);
        currentIndex += batchFiles.size();
        currentVersionFiles.addAll(batchFiles);
      }

      // Process the last version's files if any
      if (currentVersion != -1) {
        List<IndexedFile> versionFilesWithSentinels =
            addBeginAndEndIndexOffsetsForVersion(currentVersion, currentVersionFiles);
        allIndexedFiles.addAll(versionFilesWithSentinels);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read commit range", e);
    }
    return Utils.toCloseableIterator(allIndexedFiles.iterator());
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
   * Extracts IndexedFiles from a batch of actions for a given version. Assigns an index to each
   * IndexedFile.
   */
  private List<IndexedFile> extractIndexedFilesFromBatch(
      ColumnarBatch batch, long version, long startIndex) {
    List<IndexedFile> indexedFiles = new ArrayList<>();
    long index = startIndex;
    for (int rowId = 0; rowId < batch.getSize(); rowId++) {
      Optional<AddFile> addOpt = StreamingHelper.getDataChangeAdd(batch, rowId);
      if (addOpt.isPresent()) {
        AddFile addFile = addOpt.get();
        indexedFiles.add(new IndexedFile(version, index++, addFile));
      }
    }

    return indexedFiles;
  }

  /**
   * Adds BEGIN and END sentinel IndexedFiles for a version. Sentinels are used to mark the
   * beginning and end of a version.
   *
   * <p>This mimics DeltaSource.addBeginAndEndIndexOffsetsForVersion and
   * getMetadataOrProtocolChangeIndexedFileIterator.
   */
  private List<IndexedFile> addBeginAndEndIndexOffsetsForVersion(
      long version, List<IndexedFile> dataFiles) {

    List<IndexedFile> result = new ArrayList<>();

    // Add BEGIN sentinel
    result.add(new IndexedFile(version, DeltaSourceOffset.BASE_INDEX(), /* addFile= */ null));

    // TODO(M2): check trackingMetadataChange flag and compare with stream metadata.

    result.addAll(dataFiles);

    // Add END sentinel
    result.add(new IndexedFile(version, DeltaSourceOffset.END_INDEX(), /* addFile= */ null));

    return result;
  }
}
