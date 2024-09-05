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

import io.delta.kernel.*;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.CheckpointAlreadyExistsException;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.snapshot.SnapshotManager;
import io.delta.kernel.internal.util.Clock;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class TableImpl implements Table {
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

  private final SnapshotManager snapshotManager;
  private final String tablePath;
  private final Clock clock;

  public TableImpl(String tablePath, Clock clock) {
    this.tablePath = tablePath;
    final Path dataPath = new Path(tablePath);
    final Path logPath = new Path(dataPath, "_delta_log");
    this.snapshotManager = new SnapshotManager(logPath, dataPath);
    this.clock = clock;
  }

  @Override
  public String getPath(Engine engine) {
    return tablePath;
  }

  @Override
  public Snapshot getLatestSnapshot(Engine engine) throws TableNotFoundException {
    return snapshotManager.buildLatestSnapshot(engine);
  }

  @Override
  public Snapshot getSnapshotAsOfVersion(Engine engine, long versionId)
      throws TableNotFoundException {
    return snapshotManager.getSnapshotAt(engine, versionId);
  }

  @Override
  public Snapshot getSnapshotAsOfTimestamp(Engine engine, long millisSinceEpochUTC)
      throws TableNotFoundException {
    return snapshotManager.getSnapshotForTimestamp(engine, millisSinceEpochUTC);
  }

  @Override
  public void checkpoint(Engine engine, long version)
      throws TableNotFoundException, CheckpointAlreadyExistsException, IOException {
    snapshotManager.checkpoint(engine, version);
  }

  @Override
  public TransactionBuilder createTransactionBuilder(
      Engine engine, String engineInfo, Operation operation) {
    return new TransactionBuilderImpl(this, engineInfo, operation);
  }

  public Clock getClock() {
    return clock;
  }

  protected Path getDataPath() {
    return new Path(tablePath);
  }

  protected Path getLogPath() {
    return new Path(tablePath, "_delta_log");
  }

  /**
   * Returns the raw delta actions for each version between startVersion and endVersion. Only reads
   * the actions requested in actionSet from the JSON log files.
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
   */
  private CloseableIterator<ColumnarBatch> getRawChanges(
      Engine engine,
      long startVersion,
      long endVersion,
      Set<DeltaLogActionUtils.DeltaAction> actionSet) {

    List<FileStatus> commitFiles =
        DeltaLogActionUtils.getCommitFilesForVersionRange(
            engine, new Path(tablePath), startVersion, endVersion);

    StructType readSchema =
        new StructType(
            actionSet.stream()
                .map(action -> new StructField(action.colName, action.schema, true))
                .collect(Collectors.toList()));

    return DeltaLogActionUtils.readCommitFiles(engine, commitFiles, readSchema);
  }

  /**
   * Returns the raw delta actions for each version between startVersion and endVersion. Only reads
   * the actions requested in actionSet from the JSON log files. When {@link
   * DeltaLogActionUtils.DeltaAction#PROTOCOL} is present in {@code actionSet} Kernel also performs
   * read protocol checks on any protocol actions read.
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
   * @throws KernelException if {@link DeltaLogActionUtils.DeltaAction#PROTOCOL} is in actionSet and
   *     a Protocol action that is not read supported by Kernel is read
   */
  public CloseableIterator<ColumnarBatch> getChanges(
      Engine engine,
      long startVersion,
      long endVersion,
      Set<DeltaLogActionUtils.DeltaAction> actionSet) {
    if (actionSet.contains(DeltaLogActionUtils.DeltaAction.PROTOCOL)) {
      return getRawChanges(engine, startVersion, endVersion, actionSet)
          .map(
              batch -> {
                int protocolIdx =
                    batch.getSchema().indexOf("protocol"); // must exist due to outer check
                ColumnVector protocolVector = batch.getColumnVector(protocolIdx);
                for (int rowId = 0; rowId < protocolVector.getSize(); rowId++) {
                  if (!protocolVector.isNullAt(rowId)) {
                    Protocol protocol = Protocol.fromColumnVector(protocolVector, rowId);
                    TableFeatures.validateReadSupportedTable(
                        protocol, Optional.empty(), getDataPath().toString());
                  }
                }
                return batch;
              });
    } else {
      return getRawChanges(engine, startVersion, endVersion, actionSet);
    }
  }
}
