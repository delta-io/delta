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
package io.delta.kernel.spark.utils;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Preconditions.checkState;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.data.StructRow;
import io.delta.kernel.spark.exception.VersionNotFoundException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.annotation.Experimental;

/**
 * Helper class for managing Delta table snapshots in streaming scenarios.
 *
 * <p>This class provides utilities to load, update, and query Delta table snapshots using the Delta
 * Kernel API. It maintains a cached snapshot that can be accessed and updated as needed.
 */
@Experimental
public class StreamingHelper {

  private final String tablePath;
  private final AtomicReference<Snapshot> snapshotAtomicReference;
  private final Engine kernelEngine;

  /**
   * Constructs a new StreamingHelper for the specified Delta table.
   *
   * @param tablePath the path to the Delta table
   * @param hadoopConf the Hadoop configuration to use for file system access
   */
  public StreamingHelper(String tablePath, Configuration hadoopConf) {
    this.tablePath = tablePath;
    this.snapshotAtomicReference = new AtomicReference<>();
    this.kernelEngine = DefaultEngine.create(hadoopConf);
  }

  /**
   * Returns the cached snapshot without guaranteeing its freshness.
   *
   * @return the cached snapshot, or a newly loaded snapshot if none exists
   */
  public Snapshot unsafeVolatileSnapshot() {
    Snapshot unsafeVolatileSnapshot = snapshotAtomicReference.get();
    if (unsafeVolatileSnapshot == null) {
      return loadLatestSnapshot();
    }
    return unsafeVolatileSnapshot;
  }

  /**
   * Loads and caches the latest snapshot of the Delta table.
   *
   * @return the newly loaded snapshot
   */
  public Snapshot loadLatestSnapshot() {
    Snapshot snapshot = TableManager.loadSnapshot(tablePath).build(kernelEngine);
    snapshotAtomicReference.set(snapshot);
    return snapshot;
  }

  /**
   * Finds the active commit at a specific timestamp.
   *
   * <p>This method searches the Delta table's commit history to find the commit that was active at
   * the specified timestamp.
   *
   * @param timeStamp the timestamp to query
   * @param canReturnLastCommit if true, returns the last commit if the timestamp is after all
   *     commits
   * @param mustBeRecreatable if true, only considers commits that can be recreated (i.e., all
   *     necessary log files are available)
   * @param canReturnEarliestCommit if true, returns the earliest commit if the timestamp is before
   *     all commits
   * @return the commit that was active at the specified timestamp
   */
  public DeltaHistoryManager.Commit getActiveCommitAtTime(
      Timestamp timeStamp,
      Boolean canReturnLastCommit,
      Boolean mustBeRecreatable,
      Boolean canReturnEarliestCommit) {
    SnapshotImpl snapshot = (SnapshotImpl) loadLatestSnapshot();
    return DeltaHistoryManager.getActiveCommitAtTimestamp(
        kernelEngine,
        snapshot,
        snapshot.getLogPath(),
        timeStamp.getTime(),
        mustBeRecreatable,
        canReturnLastCommit,
        canReturnEarliestCommit,
        new ArrayList<>());
  }

  /**
   * Checks if a specific version of the Delta table exists and is accessible.
   *
   * @param version the version to check
   * @param mustBeRecreatable if true, requires that the version can be fully recreated from
   *     available log files
   * @param allowOutOfRange if true, allows versions greater than the latest version without
   *     throwing an exception
   * @throws VersionNotFoundException if the version is not available
   */
  public void checkVersionExists(Long version, Boolean mustBeRecreatable, Boolean allowOutOfRange)
      throws VersionNotFoundException {
    SnapshotImpl snapshot = (SnapshotImpl) loadLatestSnapshot();
    long earliest =
        mustBeRecreatable
            ? DeltaHistoryManager.getEarliestRecreatableCommit(
                kernelEngine,
                snapshot.getLogPath(),
                Optional.empty() /*earliestRatifiedCommitVersion*/)
            : DeltaHistoryManager.getEarliestDeltaFile(
                kernelEngine,
                snapshot.getLogPath(),
                Optional.empty() /*earliestRatifiedCommitVersion*/);

    long latest = snapshot.getVersion();
    if (version < earliest || ((version > latest) && !allowOutOfRange)) {
      throw new VersionNotFoundException(version, earliest, latest);
    }
  }

  /**
   * Returns the index of the field with the given name in the schema of the batch. Throws an {@link
   * IllegalArgumentException} if the field is not found.
   */
  private static int getFieldIndex(ColumnarBatch batch, String fieldName) {
    int index = batch.getSchema().indexOf(fieldName);
    checkArgument(index >= 0, "Field '%s' not found in schema: %s", fieldName, batch.getSchema());
    return index;
  }

  /**
   * Get the version from a batch. Assumes all rows in the batch have the same version, so it reads
   * from the first row (rowId=0).
   */
  public static long getVersion(ColumnarBatch batch) {
    int versionColIdx = getFieldIndex(batch, "version");
    return batch.getColumnVector(versionColIdx).getLong(0);
  }

  /** Get AddFile action from a batch at the specified row, if present and has dataChange=true. */
  public static Optional<AddFile> getDataChangeAdd(ColumnarBatch batch, int rowId) {
    int addIdx = getFieldIndex(batch, "add");
    ColumnVector addVector = batch.getColumnVector(addIdx);
    if (addVector.isNullAt(rowId)) {
      return Optional.empty();
    }

    Row addFileRow = StructRow.fromStructVector(addVector, rowId);
    checkState(
        addFileRow != null,
        "Failed to extract AddFile struct from batch at rowId=%d.",
        rowId);

    AddFile addFile = new AddFile(addFileRow);
    return addFile.getDataChange() ? Optional.of(addFile) : Optional.empty();
  }

  /**
   * Get RemoveFile action from a batch at the specified row, if present and has dataChange=true.
   */
  public static Optional<RemoveFile> getDataChangeRemove(ColumnarBatch batch, int rowId) {
    int removeIdx = getFieldIndex(batch, "remove");
    ColumnVector removeVector = batch.getColumnVector(removeIdx);
    if (removeVector.isNullAt(rowId)) {
      return Optional.empty();
    }

    Row removeFileRow = StructRow.fromStructVector(removeVector, rowId);
    checkState(
        removeFileRow != null,
        "Failed to extract RemoveFile struct from batch at rowId=%d.",
        rowId);

    RemoveFile removeFile = new RemoveFile(removeFileRow);
    return removeFile.getDataChange() ? Optional.of(removeFile) : Optional.empty();
  }
}
