/*
 *  Copyright (2021) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.kernel;

import static io.delta.flink.kernel.ColumnVectorUtils.child;
import static io.delta.kernel.internal.checkpoints.Checkpointer.LAST_CHECKPOINT_FILE_NAME;
import static io.delta.kernel.internal.util.FileNames.checkpointFileSingular;
import static io.delta.kernel.internal.util.FileNames.v2CheckpointSidecarFile;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import io.delta.kernel.CommitActions;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.actions.SetTransaction;
import io.delta.kernel.internal.checkpoints.CheckpointMetaData;
import io.delta.kernel.internal.checkpoints.CheckpointMetadataAction;
import io.delta.kernel.internal.checkpoints.SidecarFile;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.InternalUtils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a Flink specialized checkpoint creator. It requires V2 checkpoint to be enabled on the
 * table. Considering that Flink only supports blind append, it generates a new sidecar file for all
 * actions between last_checkpoint and the current version.
 */
public class Checkpoint {

  public static final StructType CHECKPOINT_SCHEMA =
      new StructType()
          .add("checkpointMetadata", CheckpointMetadataAction.FULL_SCHEMA)
          .add("metaData", Metadata.FULL_SCHEMA)
          .add("protocol", Protocol.FULL_SCHEMA)
          .add("txn", SetTransaction.FULL_SCHEMA)
          .add("sidecar", SidecarFile.READ_SCHEMA);

  private static final Logger LOG = LoggerFactory.getLogger(Checkpoint.class);

  private final Engine engine;
  private final SnapshotImpl snapshot;
  private final Path lastCheckpointFilePath;

  private Map<String, Long> transactionIds = new HashMap<>();
  private AtomicInteger addFileCounter = new AtomicInteger();

  public Checkpoint(Engine engine, Snapshot snapshot) {
    this.engine = engine;
    this.snapshot = (SnapshotImpl) snapshot;
    this.lastCheckpointFilePath = new Path(this.snapshot.getLogPath(), LAST_CHECKPOINT_FILE_NAME);
  }

  /**
   * Assume that a V2 checkpoint with sidecar already exists [TODO] Evolve existing checkpoints to
   * V2 checkpoint when possible
   */
  public void write() throws IOException {
    transactionIds.clear();
    Path logPath = snapshot.getLogPath();
    long version = snapshot.getVersion();

    Optional<CheckpointMetaData> prevCheckpointMeta = getLastCheckpointInfo();
    long prevCheckpointVersion = prevCheckpointMeta.map(c -> c.version).orElse(-1L);

    Path prevCheckpointPath = checkpointFileSingular(logPath, prevCheckpointVersion);
    Path newCheckpointPath = checkpointFileSingular(logPath, snapshot.getVersion());
    String newSidecarName = UUID.randomUUID().toString();
    Path newSidecarPath = v2CheckpointSidecarFile(logPath, newSidecarName);

    // Write the new sidecar file
    List<FileStatus> deltaFiles =
        LongStream.range(prevCheckpointVersion + 1, version)
            .mapToObj(v -> FileStatus.of(FileNames.deltaFile(snapshot.getLogPath(), v)))
            .collect(Collectors.toList());

    // Fetch new txn and addfiles
    try (CloseableIterator<FilteredColumnarBatch> actions =
        DeltaLogActionUtils.getActionsFromCommitFilesWithProtocolValidation(
                engine, snapshot.getPath(), deltaFiles, Set.of(DeltaAction.ADD, DeltaAction.TXN))
            .flatMap(CommitActions::getActions)
            .map(filterActions("add", addFileCounter))
            .map(child("add"))) {
      // Write AddFiles
      engine
          .getParquetHandler()
          .writeParquetFileAtomically(String.valueOf(newSidecarPath), actions);
    }

    // Existing sidecars from old checkpoint needs to be merged into the new checkpoint
    CloseableIterator<FilteredColumnarBatch> existingSidecars =
        prevCheckpointMeta
            .map(
                metadata -> {
                  try {
                    return engine
                        .getParquetHandler()
                        .readParquetFiles(
                            singletonCloseableIterator(getFileStatus(prevCheckpointPath)),
                            CHECKPOINT_SCHEMA,
                            Optional.empty())
                        .map(FileReadResult::getData);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                })
            .orElse(toCloseableIterator(Collections.emptyIterator()))
            .map(filterActions("sidecar", null));

    // Write the new checkpoint, which include the following parts:
    // * checkpointMetadata
    // * protocol
    // * metadata
    // * newSidecar
    // * existingSidecars
    // * computed txn

    FileStatus newSidecarFileStatus = getFileStatus(newSidecarPath);

    CheckpointMetadataAction checkpointMetadata =
        new CheckpointMetadataAction(snapshot.getVersion(), Map.of());
    SidecarFile newSidecar =
        new SidecarFile(
            String.format(
                "%s.parquet", newSidecarName), // Kernel does not support absolute sidecar paths
            newSidecarFileStatus.getSize(),
            newSidecarFileStatus.getModificationTime());

    List<Row> actionRows =
        Stream.concat(
                Stream.of(
                        snapshot.getProtocol(),
                        snapshot.getMetadata(),
                        checkpointMetadata,
                        newSidecar)
                    .map(CheckpointActionRow::new),
                transactionIds.entrySet().stream()
                    .map(
                        entry ->
                            new SetTransaction(entry.getKey(), entry.getValue(), Optional.empty()))
                    .map(CheckpointActionRow::new))
            .collect(Collectors.toList());

    FilteredColumnarBatch checkpointContent =
        new FilteredColumnarBatch(
            new DefaultRowBasedColumnarBatch(CHECKPOINT_SCHEMA, actionRows), Optional.empty());

    engine
        .getParquetHandler()
        .writeParquetFileAtomically(
            String.valueOf(newCheckpointPath),
            singletonCloseableIterator(checkpointContent).combine(existingSidecars));

    // Write _last_checkpoint file
    engine
        .getJsonHandler()
        .writeJsonFileAtomically(
            lastCheckpointFilePath.toString(),
            singletonCloseableIterator(
                new CheckpointMetaData(
                        snapshot.getVersion(),
                        prevCheckpointMeta.map(meta -> meta.size).orElse(0L) + addFileCounter.get(),
                        Optional.empty())
                    .toRow()),
            true /* overwrite */);
  }

  /**
   * Filter the records that is not null at the given column, and count the num of return rows.
   *
   * @param notNullName the column to be filtered as not null
   * @param counter line counter. null if no need to count
   * @return mapping function to perform the filtering
   */
  private Function<ColumnarBatch, FilteredColumnarBatch> filterActions(
      String notNullName, AtomicInteger counter) {
    return (columnarBatch) -> {
      int txnOrdinal = columnarBatch.getSchema().indexOf("txn");
      int notNullOrdinal = columnarBatch.getSchema().indexOf(notNullName);
      return new FilteredColumnarBatch(
          columnarBatch,
          ColumnVectorUtils.filter(
              columnarBatch.getSize(),
              (index) -> {
                ColumnVector txnVector = columnarBatch.getColumnVector(txnOrdinal);
                if (!txnVector.isNullAt(index)) {
                  String appId = txnVector.getChild(0).getString(index);
                  Long version = txnVector.getChild(1).getLong(index);
                  transactionIds.merge(appId, version, Math::max);
                }
                if (!columnarBatch.getColumnVector(notNullOrdinal).isNullAt(index)) {
                  if (counter != null) {
                    counter.incrementAndGet();
                  }
                  return true;
                }
                return false;
              }));
    };
  }

  /**
   * Read the last checkpoint info from _last_checkpoint file
   *
   * @return previous checkpoint metadata if it exists
   */
  private Optional<CheckpointMetaData> getLastCheckpointInfo() {
    try (CloseableIterator<ColumnarBatch> jsonIter =
        engine
            .getJsonHandler()
            .readJsonFiles(
                singletonCloseableIterator(FileStatus.of(lastCheckpointFilePath.toString())),
                CheckpointMetaData.READ_SCHEMA,
                Optional.empty())) {
      Optional<Row> checkpointRow = InternalUtils.getSingularRow(jsonIter);
      return checkpointRow.map(CheckpointMetaData::fromRow);
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private FileStatus getFileStatus(Path path) throws IOException {
    return engine.getFileSystemClient().getFileStatus(path.toString());
  }
}
