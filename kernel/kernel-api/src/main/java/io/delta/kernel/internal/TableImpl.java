/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal;

import static io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Collections.emptyList;

import io.delta.kernel.*;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.CheckpointAlreadyExistsException;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.checkpoints.Checkpointer;
import io.delta.kernel.internal.checksum.ChecksumUtils;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.metrics.SnapshotQueryContext;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.snapshot.SnapshotManager;
import io.delta.kernel.internal.util.Clock;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableImpl implements Table {

  private static final Logger logger = LoggerFactory.getLogger(TableImpl.class);

  public static Table forPath(Engine engine, String path) {
    return forPath(engine, path, System::currentTimeMillis);
  }

  /**
   * Instantiate a table object for the Delta Lake table at the given path. It takes an additional
   * parameter called {@link Clock} which helps in testing.
   *
   * @param engine {@link Engine} instance to use in Delta Kernel.
   * @param path location of the table.
   * @param clock {@link Clock} instance to use for time-related operations.
   * @return an instance of {@link Table} representing the Delta table at the given path
   */
  public static Table forPath(Engine engine, String path, Clock clock) {
    String resolvedPath;
    try {
      resolvedPath =
          wrapEngineExceptionThrowsIO(
              () -> engine.getFileSystemClient().resolvePath(path), "Resolving path %s", path);
    } catch (IOException io) {
      throw new UncheckedIOException(io);
    }
    return new TableImpl(resolvedPath, clock);
  }

  private final String tablePath;
  private final Checkpointer checkpointer;
  private final SnapshotManager snapshotManager;
  private final Clock clock;

  public TableImpl(String tablePath, Clock clock) {
    this.tablePath = tablePath;
    final Path dataPath = new Path(tablePath);
    final Path logPath = new Path(dataPath, "_delta_log");
    this.checkpointer = new Checkpointer(logPath);
    this.snapshotManager = new SnapshotManager(dataPath);
    this.clock = clock;
  }

  @Override
  public String getPath(Engine engine) {
    return tablePath;
  }

  @Override
  public SnapshotImpl getLatestSnapshot(Engine engine) throws TableNotFoundException {
    SnapshotQueryContext snapshotContext = SnapshotQueryContext.forLatestSnapshot(tablePath);
    return loadSnapshotWithMetrics(
        engine,
        () -> snapshotManager.buildLatestSnapshot(engine, snapshotContext),
        snapshotContext);
  }

  @Override
  public SnapshotImpl getSnapshotAsOfVersion(Engine engine, long versionId)
      throws TableNotFoundException {
    SnapshotQueryContext snapshotContext =
        SnapshotQueryContext.forVersionSnapshot(tablePath, versionId);
    return loadSnapshotWithMetrics(
        engine,
        () -> snapshotManager.getSnapshotAt(engine, versionId, snapshotContext),
        snapshotContext);
  }

  @Override
  public SnapshotImpl getSnapshotAsOfTimestamp(Engine engine, long millisSinceEpochUTC)
      throws TableNotFoundException {
    SnapshotQueryContext snapshotContext =
        SnapshotQueryContext.forTimestampSnapshot(tablePath, millisSinceEpochUTC);
    SnapshotImpl latestSnapshot = getLatestSnapshot(engine);
    return loadSnapshotWithMetrics(
        engine,
        () ->
            snapshotManager.getSnapshotForTimestamp(
                engine, latestSnapshot, millisSinceEpochUTC, snapshotContext),
        snapshotContext);
  }

  @Override
  public void checkpoint(Engine engine, long version)
      throws TableNotFoundException, CheckpointAlreadyExistsException, IOException {
    final SnapshotImpl snapshotToCheckpoint = getSnapshotAsOfVersion(engine, version);
    checkpointer.checkpoint(engine, clock, snapshotToCheckpoint);
  }

  @Override
  public void checksum(Engine engine, long version) throws TableNotFoundException, IOException {
    final LogSegment logSegmentAtVersion =
        snapshotManager.getLogSegmentForVersion(engine, Optional.of(version));
    ChecksumUtils.computeStateAndWriteChecksum(engine, logSegmentAtVersion);
  }

  @Override
  public TransactionBuilder createTransactionBuilder(
      Engine engine, String engineInfo, Operation operation) {
    return new TransactionBuilderImpl(this, engineInfo, operation);
  }

  public TransactionBuilder createReplaceTableTransactionBuilder(Engine engine, String engineInfo) {
    return new ReplaceTableTransactionBuilderImpl(this, engineInfo);
  }

  public Clock getClock() {
    return clock;
  }

  /**
   * Returns delta actions for each version between startVersion and endVersion. Only returns the
   * actions requested in actionSet.
   *
   * <p>For the returned columnar batches:
   *
   * <ul>
   *   <li>Each row within the same batch is guaranteed to have the same commit version
   *   <li>The batch commit versions are monotonically increasing
   *   <li>The top-level columns include "version", "timestamp", and the actions requested in
   *       actionSet. "version" and "timestamp" are the first and second columns in the schema,
   *       respectively. The remaining columns are based on the actions requested and each have the
   *       schema found in {@code DeltaAction.schema}.
   * </ul>
   *
   * @param engine {@link Engine} instance to use in Delta Kernel.
   * @param startVersion start version (inclusive)
   * @param endVersion end version (inclusive)
   * @param actionSet the actions to read and return from the JSON log files
   * @return an iterator of batches where each row in the batch has exactly one non-null action and
   *     its commit version and timestamp
   * @throws TableNotFoundException if the table does not exist or if it is not a delta table
   * @throws KernelException if a commit file does not exist for any of the versions in the provided
   *     range
   * @throws KernelException if provided an invalid version range
   * @throws KernelException if the version range contains a version with reader protocol that is
   *     unsupported by Kernel
   */
  public CloseableIterator<ColumnarBatch> getChanges(
      Engine engine,
      long startVersion,
      long endVersion,
      Set<DeltaLogActionUtils.DeltaAction> actionSet) {
    checkArgument(startVersion >= 0, "startVersion must be >= 0");
    checkArgument(startVersion <= endVersion, "startVersion must be <= endVersion");

    List<FileStatus> commitFiles =
        DeltaLogActionUtils.getCommitFilesForVersionRange(
            engine, new Path(tablePath), startVersion, Optional.of(endVersion));

    // Get CommitActions for each file
    CloseableIterator<io.delta.kernel.CommitActions> commits =
        DeltaLogActionUtils.getActionsFromCommitFilesWithProtocolValidation(
            engine, tablePath, commitFiles, actionSet);

    // Flatten and add version/timestamp columns
    return TableChangesUtils.flattenCommitsAndAddMetadata(engine, commits);
  }

  protected Path getDataPath() {
    return new Path(tablePath);
  }

  protected Path getLogPath() {
    return new Path(tablePath, "_delta_log");
  }

  /**
   * Returns the latest version that was committed before or at {@code millisSinceEpochUTC}. If no
   * version exists, throws a {@link KernelException}
   *
   * <p>Specifically:
   *
   * <ul>
   *   <li>if a commit version exactly matches the provided timestamp, we return it
   *   <li>else, we return the latest commit version with a timestamp less than the provided one
   *   <li>If the provided timestamp is less than the timestamp of any committed version, we throw
   *       an error.
   * </ul>
   *
   * .
   *
   * @param millisSinceEpochUTC the number of milliseconds since midnight, January 1, 1970 UTC
   * @return latest commit that happened before or at {@code timestamp}.
   * @throws KernelException if the timestamp is less than the timestamp of any committed version
   * @throws TableNotFoundException if no delta table is found
   */
  public long getVersionBeforeOrAtTimestamp(Engine engine, long millisSinceEpochUTC) {
    SnapshotImpl latestSnapshot = (SnapshotImpl) getLatestSnapshot(engine);
    return DeltaHistoryManager.getVersionBeforeOrAtTimestamp(
        engine,
        getLogPath(),
        millisSinceEpochUTC,
        latestSnapshot,
        emptyList() /* catalogCommits */);
  }

  /**
   * Returns the latest version that was committed at or after {@code millisSinceEpochUTC}. If no
   * version exists, throws a {@link KernelException}
   *
   * <p>Specifically:
   *
   * <ul>
   *   <li>if a commit version exactly matches the provided timestamp, we return it
   *   <li>else, we return the earliest commit version with a timestamp greater than the provided
   *       one
   *   <li>If the provided timestamp is larger than the timestamp of any committed version, we throw
   *       an error.
   * </ul>
   *
   * .
   *
   * @param millisSinceEpochUTC the number of milliseconds since midnight, January 1, 1970 UTC
   * @return latest commit that happened at or before {@code timestamp}.
   * @throws KernelException if the timestamp is more than the timestamp of any committed version
   * @throws TableNotFoundException if no delta table is found
   */
  public long getVersionAtOrAfterTimestamp(Engine engine, long millisSinceEpochUTC) {
    SnapshotImpl latestSnapshot = (SnapshotImpl) getLatestSnapshot(engine);
    return DeltaHistoryManager.getVersionAtOrAfterTimestamp(
        engine,
        getLogPath(),
        millisSinceEpochUTC,
        latestSnapshot,
        emptyList() /* catalogCommits */);
  }

  /** Helper method that loads a snapshot with proper metrics recording, logging, and reporting. */
  private SnapshotImpl loadSnapshotWithMetrics(
      Engine engine, Supplier<SnapshotImpl> loadSnapshot, SnapshotQueryContext snapshotContext)
      throws TableNotFoundException {
    try {
      final SnapshotImpl snapshot =
          snapshotContext.getSnapshotMetrics().loadSnapshotTotalTimer.time(loadSnapshot);

      logger.info(
          "[{}] Took {}ms to load snapshot (version = {}) for snapshot query {}",
          tablePath,
          snapshotContext.getSnapshotMetrics().loadSnapshotTotalTimer.totalDurationMs(),
          snapshot.getVersion(),
          snapshotContext.getQueryDisplayStr());

      engine
          .getMetricsReporters()
          .forEach(reporter -> reporter.report(snapshot.getSnapshotReport()));

      return snapshot;
    } catch (Exception e) {
      snapshotContext.recordSnapshotErrorReport(engine, e);
      throw e;
    }
  }
}
