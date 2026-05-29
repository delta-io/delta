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

import io.delta.flink.sink.DeltaSinkConf;
import io.delta.flink.sink.MergeStrategy;
import io.delta.flink.table.AbstractKernelTable;
import io.delta.flink.table.DeltaTable;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * Abstract base for upsert merge strategies. Owns the per-checkpoint PK bookkeeping (upserted /
 * deleted PKs, touched partitions, lookup index) and orchestrates {@link #merge}: ask the
 * configured {@link RowLocator} for candidate files, hand each one to {@link #deleteRecords},
 * flatten the resulting Delta actions, and clear internal state.
 *
 * <p>Subclasses choose how to materialize deletes, for example by rewriting matched files or by
 * writing deletion vectors over them.
 */
public abstract class Upsert implements MergeStrategy {
  /**
   * Primary-key values (in {@link DeltaSinkConf#getPrimaryKeyOrdinals()} order) of every {@code
   * INSERT}/{@code UPDATE_AFTER} row recorded during the current checkpoint.
   */
  private final List<List<Literal>> upsertedPrimaryKeys = new ArrayList<>();

  /** Primary-key values of every {@code DELETE} row recorded during the current checkpoint. */
  private final List<List<Literal>> deletedPrimaryKeys = new ArrayList<>();

  /** Indices for quick search of pks. */
  private final Set<List<String>> primaryKeyIndices = new HashSet<>();

  /**
   * Distinct partition value maps touched during the checkpoint. Keyed by a stringified form of the
   * partition map so we deduplicate cheaply; the value is the original {@code Literal}-based map
   * used to bound the scan in {@link #merge}.
   */
  private final Map<Map<String, String>, Map<String, Literal>> touchedPartitions =
      new LinkedHashMap<>();

  protected transient AbstractKernelTable table;

  private final RowLocator rowLocator;

  protected Upsert(RowLocator rowLocator) {
    this.rowLocator = Objects.requireNonNull(rowLocator, "rowLocator");
  }

  @Override
  public void recordUpsert(List<Literal> primaryKey, Map<String, Literal> partitionValues) {
    upsertedPrimaryKeys.add(primaryKey);
    cachePrimaryKey(primaryKey);
    cachePartition(partitionValues);
  }

  @Override
  public void recordDelete(List<Literal> primaryKey, Map<String, Literal> partitionValues) {
    deletedPrimaryKeys.add(primaryKey);
    cachePrimaryKey(primaryKey);
    cachePartition(partitionValues);
  }

  @Override
  public List<Row> merge(DeltaTable table, DeltaSinkConf conf) throws IOException {
    this.table = (AbstractKernelTable) table;
    boolean hasWork = !upsertedPrimaryKeys.isEmpty() || !deletedPrimaryKeys.isEmpty();
    try {
      if (!hasWork) {
        return Collections.emptyList();
      }

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
      return addFiles.flatMap(addFile -> deleteRecords(addFile, filter)).toInMemoryList();
    } finally {
      upsertedPrimaryKeys.clear();
      deletedPrimaryKeys.clear();
      primaryKeyIndices.clear();
      touchedPartitions.clear();
    }
  }

  /**
   * Emit the Delta actions needed to logically delete from {@code addFile} every row matched by
   * {@code filter}. Called once per candidate file returned by {@link RowLocator#find}; the
   * returned iterator is flattened into {@link #merge}'s overall result. Implementations may assume
   * {@link #table} has been initialized by the surrounding {@code merge(...)} call.
   *
   * @param addFile scan-file row in Kernel's {@code Scan.getScanFiles(...)} shape
   * @param filter row-level predicate; {@code true} to remove the row, {@code false} to keep it
   * @return single-action rows already wrapped as {@code SingleAction}s
   */
  protected abstract CloseableIterator<Row> deleteRecords(
      Row addFile, BiPredicate<ColumnarBatch, Integer> filter);

  private void cachePrimaryKey(List<Literal> primaryKey) {
    primaryKeyIndices.add(
        primaryKey.stream()
            .map(key -> Optional.ofNullable(key).map(Object::toString).orElse(""))
            .collect(Collectors.toList()));
  }

  private void cachePartition(Map<String, Literal> partitionValues) {
    Map<String, String> dedupKey =
        partitionValues.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue())));
    touchedPartitions.putIfAbsent(dedupKey, partitionValues);
  }

  private List<String> extractPrimaryKey(int[] ordinals, ColumnarBatch data, Integer rowId) {
    List<String> key = new ArrayList<>(ordinals.length);
    for (int ord : ordinals) {
      key.add(
          Optional.ofNullable(columnVectorValue(data.getColumnVector(ord), rowId))
              .map(Object::toString)
              .orElse(""));
    }
    return key;
  }

  private Object columnVectorValue(ColumnVector input, int rowId) {
    if (input.isNullAt(rowId)) {
      return null;
    }
    DataType type = input.getDataType();
    if (type.equivalent(BooleanType.BOOLEAN)) {
      return input.getBoolean(rowId);
    } else if (type.equivalent(ByteType.BYTE)) {
      return input.getByte(rowId);
    } else if (type.equivalent(ShortType.SHORT)) {
      return input.getShort(rowId);
    } else if (type.equivalent(IntegerType.INTEGER)) {
      return input.getInt(rowId);
    } else if (type.equivalent(LongType.LONG)) {
      return input.getLong(rowId);
    } else if (type.equivalent(FloatType.FLOAT)) {
      return input.getFloat(rowId);
    } else if (type.equivalent(DoubleType.DOUBLE)) {
      return input.getDouble(rowId);
    } else if (type.equivalent(StringType.STRING)) {
      return input.getString(rowId);
    } else if (type.equivalent(BinaryType.BINARY)) {
      return input.getBinary(rowId);
    } else if (type.equivalent(DateType.DATE)) {
      return input.getInt(rowId);
    } else if (type.equivalent(TimestampType.TIMESTAMP)) {
      return input.getLong(rowId);
    } else if (type.equivalent(TimestampNTZType.TIMESTAMP_NTZ)) {
      return input.getLong(rowId);
    } else if (type instanceof DecimalType) {
      return input.getDecimal(rowId);
    } else {
      throw new UnsupportedOperationException("Unsupported column vector type: " + type);
    }
  }
}
