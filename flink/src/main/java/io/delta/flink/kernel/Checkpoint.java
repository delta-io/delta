/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.flink.kernel;

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
import io.delta.kernel.internal.tablefeatures.TableFeatures;
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
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink-specialized Delta v2 checkpoint writer.
 *
 * <p>This writer assumes the table is configured to use v2 checkpoints with sidecar files and is
 * most efficient when used for incremental checkpoint creation on Flink sink tables.
 *
 * <p>Because the Flink sink performs blind appends, the writer generates a new sidecar file
 * containing all {@code AddFile} and {@code SetTransaction} actions in the range {@code
 * (previousCheckpointVersion + 1, currentSnapshotVersion]}. It then writes a new singular v2
 * checkpoint that includes:
 *
 * <ul>
 *   <li>protocol, metadata, and checkpointMetadata actions
 *   <li>the newly generated sidecar file
 *   <li>all sidecars referenced by the previous checkpoint, if that checkpoint was written by this
 *       class
 *   <li>aggregated {@code SetTransaction} actions computed from commits in the version range
 * </ul>
 *
 * <p>Checkpoints not tagged with {@link #TAG_FLINK_DELTASINK} in {@code _last_checkpoint} are
 * ignored.
 *
 * <p>If this writer is invoked on a snapshot version earlier than the version recorded in {@code
 * _last_checkpoint}, all existing checkpoints are ignored and a new checkpoint is created with a
 * single sidecar containing all prior actions.
 */
public class Checkpoint {

  /**
   * Tag written into {@code _last_checkpoint} to indicate the checkpoint was produced by DeltaSink.
   */
  public static final String TAG_FLINK_DELTASINK = "flink.deltaSink";

  /** Schema used to read/write v2 checkpoint files produced by this writer. */
  public static final StructType CHECKPOINT_SCHEMA =
      new StructType()
          .add("checkpointMetadata", CheckpointMetadataAction.FULL_SCHEMA)
          .add("metaData", Metadata.FULL_SCHEMA)
          .add("protocol", Protocol.FULL_SCHEMA)
          .add("txn", SetTransaction.FULL_SCHEMA)
          .add("sidecar", SidecarFile.READ_SCHEMA);

  private static final Logger LOG = LoggerFactory.getLogger(Checkpoint.class);
  private static final CheckpointMetaData EMPTY_META =
      new CheckpointMetaData(-1L, 0, Optional.empty(), Map.of());

  private final Engine engine;
  private final SnapshotImpl snapshot;
  private final Path lastCheckpointFilePath;

  private final CheckpointMetaData lastCheckpointMeta;
  private final boolean lastCheckpointByMe;

  /** Guard to prevent reusing a single writer instance. */
  private boolean used = false;

  /** Max transaction version per appId observed while scanning commits. */
  private final Map<String, Long> transactionIds = new HashMap<>();

  /** Counts {@code AddFile} actions written into the newly generated sidecar. */
  private final AtomicInteger addFileCounter = new AtomicInteger();

  /**
   * Creates a checkpoint writer bound to a snapshot.
   *
   * @param engine kernel engine
   * @param snapshot snapshot to checkpoint
   */
  public Checkpoint(Engine engine, Snapshot snapshot) {
    this.engine = engine;
    this.snapshot = (SnapshotImpl) snapshot;
    this.lastCheckpointFilePath = new Path(this.snapshot.getLogPath(), LAST_CHECKPOINT_FILE_NAME);
    Preconditions.checkArgument(
        this.snapshot.getProtocol().supportsFeature(TableFeatures.CHECKPOINT_V2_RW_FEATURE));

    lastCheckpointMeta = readLastCheckpointInfo();
    lastCheckpointByMe =
        Boolean.parseBoolean(lastCheckpointMeta.tags.getOrDefault(TAG_FLINK_DELTASINK, "false"));
  }

  /** Reads {@code _last_checkpoint} and returns the checkpoint metadata. */
  private CheckpointMetaData readLastCheckpointInfo() {
    try (CloseableIterator<ColumnarBatch> jsonIter =
        engine
            .getJsonHandler()
            .readJsonFiles(
                singletonCloseableIterator(FileStatus.of(lastCheckpointFilePath.toString())),
                CheckpointMetaData.READ_SCHEMA,
                Optional.empty())) {
      return InternalUtils.getSingularRow(jsonIter)
          .map(CheckpointMetaData::fromRow)
          .orElse(EMPTY_META);
    } catch (Exception ignore) {
      // Best-effort: absence or parse errors mean "no usable previous checkpoint."
      return EMPTY_META;
    }
  }

  /**
   * Writes an incremental v2 checkpoint for the bound snapshot.
   *
   * <p>If the existing {@code _last_checkpoint} was not written by this class (i.e. missing {@link
   * #TAG_FLINK_DELTASINK}), the writer behaves as if no previous checkpoint exists.
   *
   * <p><b>Note:</b> This method is single-use. Calling it more than once on the same instance
   * throws {@link IllegalStateException}.
   *
   * @throws IOException if reading commit files or writing checkpoint/sidecar files fails
   */
  public void write() throws IOException {
    if (used) {
      throw new IllegalStateException("Checkpoint writer must not be reused.");
    }
    used = true;

    transactionIds.clear();
    addFileCounter.set(0);

    Path logPath = snapshot.getLogPath();
    long version = snapshot.getVersion();

    Optional<CheckpointMetaData> prevMetaByMe =
        (lastCheckpointByMe && lastCheckpointMeta.version < version)
            ? Optional.of(lastCheckpointMeta)
            : Optional.empty();
    long prevVersionByMe = prevMetaByMe.map(m -> m.version).orElse(-1L);

    Path prevCheckpointPath = checkpointFileSingular(logPath, prevVersionByMe);
    Path newCheckpointPath = checkpointFileSingular(logPath, version);

    String newSidecarName = UUID.randomUUID().toString();
    Path newSidecarPath = v2CheckpointSidecarFile(logPath, newSidecarName);

    // Collect delta commit files in (prevCheckpointVersion, version].
    List<FileStatus> deltaFiles =
        LongStream.range(prevVersionByMe + 1, version + 1)
            .mapToObj(v -> FileStatus.of(FileNames.deltaFile(snapshot.getLogPath(), v)))
            .collect(Collectors.toList());

    // Read AddFile and txn actions from commit files, write AddFiles to the new sidecar,
    // and aggregate txn versions in-memory via filterActions(...).
    try (CloseableIterator<FilteredColumnarBatch> actions =
        DeltaLogActionUtils.getActionsFromCommitFilesWithProtocolValidation(
                engine, snapshot.getPath(), deltaFiles, Set.of(DeltaAction.ADD, DeltaAction.TXN))
            .flatMap(CommitActions::getActions)
            .map(filterActions("add", addFileCounter))) {
      engine
          .getParquetHandler()
          .writeParquetFileAtomically(String.valueOf(newSidecarPath), actions);
    }

    // Merge existing sidecars from the previous checkpoint (only if it was written by this class).
    CloseableIterator<FilteredColumnarBatch> existingSidecars =
        prevMetaByMe
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

    // Build new checkpoint content: protocol, metadata, checkpointMetadata, new sidecar, and txn
    // set.
    FileStatus newSidecarFileStatus = getFileStatus(newSidecarPath);
    CheckpointMetadataAction checkpointMetadata = new CheckpointMetadataAction(version, Map.of());
    SidecarFile newSidecar =
        new SidecarFile(
            String.format("%s.parquet", newSidecarName), // Kernel does not support absolute paths.
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
                    .map(e -> new SetTransaction(e.getKey(), e.getValue(), Optional.empty()))
                    .map(CheckpointActionRow::new))
            .collect(Collectors.toList());

    FilteredColumnarBatch checkpointContent =
        new FilteredColumnarBatch(
            new DefaultRowBasedColumnarBatch(CHECKPOINT_SCHEMA, actionRows), Optional.empty());

    try (CloseableIterator<FilteredColumnarBatch> merged =
        singletonCloseableIterator(checkpointContent).combine(existingSidecars)) {
      engine
          .getParquetHandler()
          .writeParquetFileAtomically(String.valueOf(newCheckpointPath), merged);
    }

    // Write _last_checkpoint file with our tag, so we can recognize our own checkpoints later.
    if (version > lastCheckpointMeta.version) {
      engine
          .getJsonHandler()
          .writeJsonFileAtomically(
              lastCheckpointFilePath.toString(),
              singletonCloseableIterator(
                  new CheckpointMetaData(
                          version,
                          lastCheckpointMeta.size + addFileCounter.get(),
                          Optional.empty(),
                          Map.of(TAG_FLINK_DELTASINK, "true"))
                      .toRow()),
              true /* overwrite */);
    }
  }

  /**
   * Returns a mapping function that:
   *
   * <ul>
   *   <li>Aggregates {@code txn} actions into {@link #transactionIds} (max version per appId)
   *   <li>Filters rows where {@code notNullName} is non-null
   *   <li>Optionally increments {@code counter} for each retained row
   * </ul>
   *
   * <p>This is used both for selecting {@code AddFile} rows ("add") when generating the sidecar and
   * for selecting {@code sidecar} rows when merging sidecar references from a prior checkpoint.
   *
   * @param notNullName name of the column that must be non-null for a row to be retained
   * @param counter optional counter incremented for each retained row; may be null
   * @return mapping function that applies the filter (and aggregation side-effects) to a batch
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
              (rowId) -> {
                ColumnVector txnVector = columnarBatch.getColumnVector(txnOrdinal);
                if (!txnVector.isNullAt(rowId)) {
                  String appId = txnVector.getChild(0).getString(rowId);
                  long txnVersion = txnVector.getChild(1).getLong(rowId);
                  transactionIds.merge(appId, txnVersion, Math::max);
                }

                if (!columnarBatch.getColumnVector(notNullOrdinal).isNullAt(rowId)) {
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
   * Resolves the {@link FileStatus} for a path using the engine filesystem client.
   *
   * @param path file path
   * @return file status
   * @throws IOException if the filesystem lookup fails
   */
  private FileStatus getFileStatus(Path path) throws IOException {
    return engine.getFileSystemClient().getFileStatus(path.toString());
  }
}
