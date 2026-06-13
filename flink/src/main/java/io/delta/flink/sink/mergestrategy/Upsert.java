/*
 *  Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.flink.sink.mergestrategy;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.delta.flink.Conf;
import io.delta.flink.kernel.ColumnVectorUtils;
import io.delta.flink.sink.*;
import io.delta.flink.table.AbstractKernelTable;
import io.delta.flink.table.DeltaTable;
import io.delta.flink.table.ExceptionUtils;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Abstract base for upsert merge strategies. Owns the per-checkpoint PK bookkeeping (upserted /
 * deleted PKs, touched partitions, lookup index) and orchestrates {@link #merge}: ask the
 * configured {@link RowLocator} for candidate files, hand each one to {@link #deleteRecords},
 * flatten the resulting Delta actions, and clear internal state.
 *
 * <p>Subclasses choose how to materialize deletes, for example by rewriting matched files or by
 * writing deletion vectors over them.
 */
public abstract class Upsert
    implements MergeStrategy, RemovalListener<Map<String, String>, DeltaWriterTask> {
  /**
   * Primary-key values (in {@link DeltaSinkConf#getPrimaryKeyOrdinals()} order) of every {@code
   * INSERT}/{@code UPDATE_AFTER} row recorded during the current checkpoint.
   */
  private final List<List<Literal>> upsertedPrimaryKeys = new ArrayList<>();

  /** Primary-key values of every {@code DELETE} row recorded during the current checkpoint. */
  private final List<List<Literal>> deletedPrimaryKeys = new ArrayList<>();

  /** Indices for quick search of pks. */
  private final Set<List<String>> primaryKeyIndices = new HashSet<>();

  protected DeltaSinkWriter writer;
  protected AbstractKernelTable table;

  private final RowLocator rowLocator;

  private final Cache<Map<String, String>, DeltaUpsertWriterTask> writerTasksByPartition;

  private final List<DeltaWriterResult> completedWrites;

  /**
   * @param rowLocator selects which existing files to scan for PKs that need pre-image removal when
   *     {@link #merge} runs at checkpoint boundaries.
   */
  protected Upsert(RowLocator rowLocator) {
    this.rowLocator = Objects.requireNonNull(rowLocator, "rowLocator");
    this.completedWrites = new ArrayList<>();
    this.writerTasksByPartition =
        Caffeine.newBuilder()
            .executor(Runnable::run)
            .maximumSize(Conf.getInstance().getSinkWriterNumConcurrentFiles())
            .removalListener(this)
            .build();
  }

  @Override
  public void init(DeltaSinkWriter writer) {
    this.writer = writer;
    table = (AbstractKernelTable) writer.getTable();
  }

  @Override
  public void insert(
      List<Literal> primaryKey,
      Map<String, Literal> partitionValues,
      RowData element,
      SinkWriter.Context context) {
    try {
      // Route INSERT through the 3-arg, PK-aware write so the row participates in same-batch
      // dedup with any later UPDATE_AFTER / DELETE on the same PK. The dump-time guard in
      // DeltaUpsertWriterTask short-circuits when keyToPositionInMemory is empty, so failing to
      // track the PK here would silently drop the row at commit time.
      getWriterTask(partitionValues).write(primaryKey, element, context);
    } catch (Exception e) {
      throw ExceptionUtils.wrap(e);
    }
  }

  @Override
  public void upsert(
      List<Literal> primaryKey,
      Map<String, Literal> partitionValues,
      RowData element,
      SinkWriter.Context context) {
    try {
      if (!getWriterTask(partitionValues).write(primaryKey, element, context)) {
        upsertedPrimaryKeys.add(primaryKey);
        cachePrimaryKey(primaryKey);
      }
    } catch (Exception e) {
      throw ExceptionUtils.wrap(e);
    }
  }

  @Override
  public void delete(List<Literal> primaryKey, Map<String, Literal> partitionValues) {
    try {
      if (!getWriterTask(partitionValues).erase(primaryKey)) {
        deletedPrimaryKeys.add(primaryKey);
        cachePrimaryKey(primaryKey);
      }
    } catch (Exception e) {
      throw ExceptionUtils.wrap(e);
    }
  }

  @Override
  public List<DeltaWriterResult> merge() throws IOException {
    AbstractKernelTable table = (AbstractKernelTable) writer.getTable();
    DeltaSinkConf conf = writer.getConf();

    // Dump all Writer Tasks
    writerTasksByPartition.invalidateAll();

    if (!upsertedPrimaryKeys.isEmpty() || !deletedPrimaryKeys.isEmpty()) {
      try {
        // 1. Locate all data files that pending scan.
        List<List<Literal>> pkToDelete = new ArrayList<>();
        pkToDelete.addAll(upsertedPrimaryKeys);
        pkToDelete.addAll(deletedPrimaryKeys);
        CloseableIterator<Row> addFiles =
            rowLocator.find(table, conf.getPrimaryKeyOrdinals(), pkToDelete);

        // 2. Delete rows from found data files.
        BiPredicate<ColumnarBatch, Integer> filter =
            (batch, rowId) ->
                primaryKeyIndices.contains(
                    extractPrimaryKey(conf.getPrimaryKeyOrdinals(), batch, rowId));
        completedWrites.add(
            new DeltaWriterResult(
                addFiles.flatMap(addFile -> deleteRecords(addFile, filter)).toInMemoryList(),
                new WriterResultContext()));
      } finally {
        upsertedPrimaryKeys.clear();
        deletedPrimaryKeys.clear();
        primaryKeyIndices.clear();
      }
    }
    return this.completedWrites;
  }

  @Override
  public void onRemoval(
      @Nullable Map<String, String> key, @Nullable DeltaWriterTask value, RemovalCause cause) {
    try {
      if (value != null) {
        completedWrites.addAll(value.complete());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected DeltaUpsertWriterTask getWriterTask(Map<String, Literal> partitionValues) {
    return writerTasksByPartition.get(
        MergeStrategy.writerKey(partitionValues),
        (key) -> (DeltaUpsertWriterTask) writer.newWriterTask(partitionValues));
  }

  /**
   * Emit the Delta actions needed to logically delete from {@code addFile} every row matched by
   * {@code filter}. Called once per candidate file returned by {@link RowLocator#find}; the
   * returned iterator is flattened into {@link #merge}'s overall result.
   *
   * @param addFile scan-file row in Kernel's {@code Scan.getScanFiles(...)} shape
   * @param filter row-level predicate; {@code true} to remove the row, {@code false} to keep it
   * @return single-action rows already wrapped as {@code SingleAction}s
   */
  protected abstract CloseableIterator<Row> deleteRecords(
      Row addFile, BiPredicate<ColumnarBatch, Integer> filter);

  /**
   * Mark rows at {@code removePositions} as deleted within a staged (not-yet-committed) file and
   * return the replacement action row. Callers must ensure at least one row survives (i.e. {@code
   * removePositions.size() < totalRowsInFile}); the all-deleted case must be handled before this
   * call.
   */
  public abstract Row markRemovesOnStaged(
      DeltaTable table, Row stagedAddFile, Set<Integer> removePositions);

  private void cachePrimaryKey(List<Literal> primaryKey) {
    primaryKeyIndices.add(
        primaryKey.stream()
            .map(key -> Optional.ofNullable(key).map(Object::toString).orElse(""))
            .collect(Collectors.toList()));
  }

  private List<String> extractPrimaryKey(int[] ordinals, ColumnarBatch data, Integer rowId) {
    List<String> key = new ArrayList<>(ordinals.length);
    for (int ord : ordinals) {
      key.add(
          Optional.ofNullable(ColumnVectorUtils.get(data.getColumnVector(ord), rowId))
              .map(Object::toString)
              .orElse(""));
    }
    return key;
  }
}
