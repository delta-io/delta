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

import static io.delta.flink.kernel.CheckpointActionRow.CHECKPOINT_SCHEMA;
import static io.delta.kernel.internal.checkpoints.Checkpointer.LAST_CHECKPOINT_FILE_NAME;
import static io.delta.kernel.internal.util.FileNames.*;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import io.delta.flink.table.ExceptionUtils;
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
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DomainMetadata;
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
 * If the writer detects that the number of existing sidecar files exceeds a threshold, it will
 * merge them into a new single sidecar files to reduce the total number of files.
 *
 * <p>## Fallback If any of the condition is true, the writer will ignore all existing sidecars, and
 * create a new checkpoint with a single sidecar containing all actions up to the version.
 *
 * <ul>
 *   <li>No previous checkpoint exists
 *   <li>{@code _last_checkpoint} is not tagged with {@link #TAG_DELTASINK_CHECKPOINT}.
 *   <li>Remove files appear since the last checkpoint.
 *   <li>this writer is invoked on a snapshot version earlier than the version recorded in {@code *
 *       _last_checkpoint}
 * </ul>
 *
 * We choose to not support reading checkpoints written by other writers assuming that the case is
 * rare. The support can be added in the future if being requested.
 *
 * <p>## Limitations This writer does not support tables with domain metadata feature. The support
 * will be added in the future.
 */
public class CheckpointWriter {

  /**
   * Tag written into {@code _last_checkpoint} to indicate the checkpoint was produced by DeltaSink.
   */
  public static final String TAG_DELTASINK_CHECKPOINT = "io.delta.flink.sink.checkpoint";

  public static final String TAG_SIDECAR_COUNT = "io.delta.flink.num_sidecar";

  private static final Logger LOG = LoggerFactory.getLogger(CheckpointWriter.class);

  private static final StructType SIDECAR_SCHEMA = new StructType().add("add", AddFile.FULL_SCHEMA);
  private static final CheckpointMetaData EMPTY_META =
      new CheckpointMetaData(-1L, 0, Optional.empty(), Map.of());

  private static <T> CloseableIterator<T> EMPTY_ITERATOR() {
    return toCloseableIterator(Collections.emptyIterator());
  }

  private final Engine engine;
  private final SnapshotImpl snapshot;
  private final Path lastCheckpointFilePath;
  private final int sidecarMergeThreshold;

  private final CheckpointMetaData lastCheckpointMeta;
  private final boolean lastCheckpointByMe;
  private int lastSidecarCount;
  /** Guard to prevent reusing a single writer instance. */
  private boolean used = false;

  /** Max transaction version per appId observed while scanning commits. */
  private final Map<String, Long> transactionIds = new HashMap<>();

  private final Map<String, String> domainMetadatas = new HashMap<>();

  /**
   * Creates a checkpoint writer bound to a snapshot.
   *
   * @param engine kernel engine
   * @param snapshot snapshot to checkpoint
   * @param sidecarMergeThreshold threshold to merge sidecars. Set negative to disable merging.
   */
  public CheckpointWriter(Engine engine, Snapshot snapshot, int sidecarMergeThreshold) {
    this.engine = engine;
    this.snapshot = (SnapshotImpl) snapshot;

    Preconditions.checkArgument(
        this.snapshot.getProtocol().supportsFeature(TableFeatures.CHECKPOINT_V2_RW_FEATURE));
    if (this.snapshot.getProtocol().supportsFeature(TableFeatures.CATALOG_MANAGED_RW_FEATURE)) {
      // Make sure we are creating checkpoint for a published version
      Preconditions.checkArgument(
          this.snapshot.getLogSegment().getMaxPublishedDeltaVersion().orElse(-1L)
              >= this.snapshot.getVersion());
    }

    Preconditions.checkArgument(sidecarMergeThreshold < 0 || sidecarMergeThreshold >= 2);
    this.sidecarMergeThreshold = sidecarMergeThreshold;

    // Read information from _last_checkpoint
    this.lastCheckpointFilePath = new Path(this.snapshot.getLogPath(), LAST_CHECKPOINT_FILE_NAME);
    lastCheckpointMeta = readLastCheckpointInfo();
    lastCheckpointByMe =
        Boolean.parseBoolean(
            lastCheckpointMeta.tags.getOrDefault(TAG_DELTASINK_CHECKPOINT, "false"));
    lastSidecarCount = 0;
    try {
      lastSidecarCount =
          Integer.parseInt(lastCheckpointMeta.tags.getOrDefault(TAG_SIDECAR_COUNT, "0"));
    } catch (Exception ignore) {
    }
  }

  public CheckpointWriter(Engine engine, Snapshot snapshot) {
    this(engine, snapshot, -1);
  }

  /**
   * Writes an incremental v2 checkpoint for the bound snapshot.
   *
   * <p>This operation includes the following steps:
   *
   * <ul>
   *   <li>1. Determine the baseCheckpoint.
   *   <li>2. Fetch the actions between baseCheckpoint and current version.
   *   <li>3. If the actions do not contain remove files, generate a new sidecar from them, and read
   *       existing sidecars from baseCheckpoint. Otherwise, generate a new sidecar for all actions
   *       in current snapshot, and DO NOT read existing sidecars.
   *   <li>4. Write existing sidecars and new sidecar as a new V2 checkpoint.
   *   <li>5. Update _last_checkpoint
   * </ul>
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

    Path logPath = snapshot.getLogPath();
    long version = snapshot.getVersion();

    // ===========
    // Step 1:
    // ===========
    // Use _last_checkpoint as baseCheckpoint when
    // 1. It is written by this writer
    // 2. _last_checkpoint version is smaller than this snapshot version
    // Otherwise assume there's no baseCheckpoint
    // ====================================================================
    Optional<CheckpointMetaData> baseCheckpointMeta =
        (lastCheckpointByMe && lastCheckpointMeta.version < version)
            ? Optional.of(lastCheckpointMeta)
            : Optional.empty();
    long baseVersion = baseCheckpointMeta.map(m -> m.version).orElse(-1L);

    Path baseCheckpointPath = checkpointFileSingular(logPath, baseVersion);
    Path newCheckpointPath = checkpointFileSingular(logPath, version);

    // ============
    // Step 2
    // ============
    // Read actions between (baseCheckpoint.version, version]. We can assume this since we require
    // that the snapshot be fully published on catalog-managed tables
    List<FileStatus> deltaFiles =
        LongStream.range(baseVersion + 1, version + 1)
            .mapToObj(v -> FileStatus.of(FileNames.deltaFile(snapshot.getLogPath(), v)))
            .collect(Collectors.toList());

    AtomicInteger addFileCounter = new AtomicInteger();
    AtomicInteger removeFileCounter = new AtomicInteger();

    // ===========
    // Step 3
    // ===========
    // Read AddFile and txn actions from incremental commit files, and generate a new sidecar
    // including AddFiles. It also checks if remove file exists.
    SidecarFile newSidecar;
    CloseableIterator<FilteredColumnarBatch> existingSidecars = EMPTY_ITERATOR();

    try (CloseableIterator<FilteredColumnarBatch> actions =
        DeltaLogActionUtils.getActionsFromCommitFilesWithProtocolValidation(
                engine,
                snapshot.getPath(),
                deltaFiles,
                Set.of(
                    DeltaAction.ADD,
                    DeltaAction.REMOVE,
                    DeltaAction.TXN,
                    DeltaAction.DOMAINMETADATA))
            .flatMap(CommitActions::getActions)
            .map(filterActions(Map.of("add", addFileCounter, "remove", removeFileCounter)))) {
      newSidecar = sidecarFromAddFiles(actions);
      // If remove file exists, fallback to generating a new sidecar including everything.
      if (removeFileCounter.get() > 0) {
        try (CloseableIterator<FilteredColumnarBatch> allActions =
            snapshot.getCreateCheckpointIterator(engine).map(filterAddFiles())) {
          newSidecar = sidecarFromAddFiles(allActions);
        }
      } else if (baseCheckpointMeta.isPresent()) {
        // When there's no remove files, read existing sidecars from the base checkpoint if it
        // exists
        existingSidecars = sidecarsFromCheckpoint(baseCheckpointPath);
      }
    }
    // ==========
    // Step 4
    // ==========
    // Build new checkpoint including:
    // - protocol
    // - metadata
    // - checkpointMetadata
    // - txn
    // - domainMetadata
    // - existing sidecars
    // - new sidecar.
    CheckpointMetadataAction checkpointMetadata = new CheckpointMetadataAction(version, Map.of());

    try (CloseableIterator<FilteredColumnarBatch> merged =
        rowsToBatch(
                Stream.of(
                    snapshot.getProtocol(), snapshot.getMetadata(), checkpointMetadata, newSidecar))
            .combine(existingSidecars)
            .combine(rowsToBatch(getTransactions()))
            .combine(rowsToBatch(getDomainMetadatas()))) {
      engine
          .getParquetHandler()
          .writeParquetFileAtomically(String.valueOf(newCheckpointPath), merged);
    }

    // ==========
    // Step 5
    // ==========
    // Write _last_checkpoint file with our tag, so we can recognize our own checkpoints later.
    if (version > lastCheckpointMeta.version) {
      engine
          .getJsonHandler()
          .writeJsonFileAtomically(
              lastCheckpointFilePath.toString(),
              singletonCloseableIterator(
                  new CheckpointMetaData(
                          version,
                          lastCheckpointMeta.size + addFileCounter.get() - removeFileCounter.get(),
                          Optional.empty(),
                          Map.of(
                              TAG_DELTASINK_CHECKPOINT,
                              "true",
                              TAG_SIDECAR_COUNT,
                              String.valueOf(lastSidecarCount + 1)))
                      .toRow()),
              true /* overwrite */);
    }
  }

  /**
   * Read existing sidecars from the given checkpoint Path. If the total number of sidecars exceeds
   * the threshold, merge existing sidecars into one to reduce the number of files to read.
   *
   * @param checkpointPath path for the checkpoint
   * @return an iterator of sidecars fetched from the checkpoint.
   * @throws IOException when exception happens during read or write.
   */
  private CloseableIterator<FilteredColumnarBatch> sidecarsFromCheckpoint(Path checkpointPath)
      throws IOException {
    AtomicInteger sidecarCounter = new AtomicInteger();
    CloseableIterator<FilteredColumnarBatch> existingSidecars;
    existingSidecars =
        engine
            .getParquetHandler()
            .readParquetFiles(
                singletonCloseableIterator(getFileStatus(checkpointPath)),
                CHECKPOINT_SCHEMA,
                Optional.empty())
            .map(FileReadResult::getData)
            .map(filterActions(Map.of("sidecar", sidecarCounter)));

    if (sidecarMergeThreshold > 0 && lastSidecarCount >= sidecarMergeThreshold - 1) {
      // Too many existing sidecars. Merge them into one.
      try (CloseableIterator<FileStatus> sidecarFiles =
              existingSidecars
                  .flatMap(FilteredColumnarBatch::getRows)
                  .map(
                      row -> {
                        Row sidecar = row.getStruct(CHECKPOINT_SCHEMA.indexOf("sidecar"));
                        String path = sidecar.getString(SidecarFile.READ_SCHEMA.indexOf("path"));
                        Path fullPath = new Path(sidecarFile(snapshot.getLogPath(), path));
                        return getFileStatus(fullPath);
                      });
          CloseableIterator<FilteredColumnarBatch> addFileRows =
              engine
                  .getParquetHandler()
                  .readParquetFiles(sidecarFiles, SIDECAR_SCHEMA, Optional.empty())
                  .map(FileReadResult::getData)
                  .map(ColumnVectorUtils::wrap)) {
        existingSidecars = rowsToBatch(Stream.of(sidecarFromAddFiles(addFileRows)));
        lastSidecarCount = 1;
      }
    }
    return existingSidecars;
  }

  /**
   * Return a mapping function that further filter add files from a filtered column batch
   *
   * @return a mapping function that applies to a filtered column batch
   */
  private Function<FilteredColumnarBatch, FilteredColumnarBatch> filterAddFiles() {
    return (input) -> {
      int addOrdinal = input.getData().getSchema().indexOf("add");
      return new FilteredColumnarBatch(
          input.getData(),
          ColumnVectorUtils.filter(
              input.getData().getSize(),
              (rowId) ->
                  input.getSelectionVector().map(cv -> cv.getBoolean(rowId)).orElse(true)
                      && !input.getData().getColumnVector(addOrdinal).isNullAt(rowId)));
    };
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
   * @param nameToCounters a map of key: name of the column that must be non-null for a row to be
   *     retained. value: counter incremented for each retained row;
   * @return mapping function that applies the filter (and aggregation side-effects) to a batch
   */
  private Function<ColumnarBatch, FilteredColumnarBatch> filterActions(
      Map<String, AtomicInteger> nameToCounters) {
    return (columnarBatch) -> {
      int txnOrdinal = columnarBatch.getSchema().indexOf("txn");
      int dmOrdinal = columnarBatch.getSchema().indexOf("domainMetadata");

      var entries = new ArrayList<>(nameToCounters.entrySet());
      Integer[] ordinals = new Integer[nameToCounters.size()];
      AtomicInteger[] counters = new AtomicInteger[nameToCounters.size()];
      for (int i = 0; i < entries.size(); i++) {
        ordinals[i] = columnarBatch.getSchema().indexOf(entries.get(i).getKey());
        counters[i] = entries.get(i).getValue();
      }

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
                ColumnVector dmVector = columnarBatch.getColumnVector(dmOrdinal);
                if (!dmVector.isNullAt(rowId)) {
                  String domain = dmVector.getChild(0).getString(rowId);
                  String configuration = dmVector.getChild(1).getString(rowId);
                  boolean removed = dmVector.getChild(2).getBoolean(rowId);
                  if (removed) {
                    domainMetadatas.remove(domain);
                  } else {
                    domainMetadatas.put(domain, configuration);
                  }
                }
                for (int i = 0; i < ordinals.length; i++) {
                  if (!columnarBatch.getColumnVector(ordinals[i]).isNullAt(rowId)) {
                    counters[i].incrementAndGet();
                    return true;
                  }
                }
                return false;
              }));
    };
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

  /* Wrap a stream of actions as column batches */
  private CloseableIterator<FilteredColumnarBatch> rowsToBatch(Stream<Object> input) {
    FilteredColumnarBatch checkpointContent =
        new FilteredColumnarBatch(
            new DefaultRowBasedColumnarBatch(
                CHECKPOINT_SCHEMA,
                input.map(CheckpointActionRow::new).collect(Collectors.toList())),
            Optional.empty());
    return singletonCloseableIterator(checkpointContent);
  }

  /* write a new sidecar from the given addfiles */
  private SidecarFile sidecarFromAddFiles(CloseableIterator<FilteredColumnarBatch> actions)
      throws IOException {
    String sidecarName = UUID.randomUUID().toString();
    Path sidecarPath = v2CheckpointSidecarFile(snapshot.getLogPath(), sidecarName);
    engine.getParquetHandler().writeParquetFileAtomically(String.valueOf(sidecarPath), actions);
    FileStatus fileStatus = getFileStatus(sidecarPath);
    return new SidecarFile(
        String.format("%s.parquet", sidecarName), // Kernel does not support absolute paths.
        fileStatus.getSize(),
        fileStatus.getModificationTime());
  }

  /**
   * Resolves the {@link FileStatus} for a path using the engine filesystem client.
   *
   * @param path file path
   * @return file status
   */
  private FileStatus getFileStatus(Path path) {
    try {
      return engine.getFileSystemClient().getFileStatus(path.toString());
    } catch (IOException e) {
      throw ExceptionUtils.wrap(e);
    }
  }

  private Stream<Object> getTransactions() {
    return transactionIds.entrySet().stream()
        .map(e -> new SetTransaction(e.getKey(), e.getValue(), Optional.empty()));
  }

  private Stream<Object> getDomainMetadatas() {
    return domainMetadatas.entrySet().stream()
        .map(e -> new DomainMetadata(e.getKey(), e.getValue(), false));
  }
}
