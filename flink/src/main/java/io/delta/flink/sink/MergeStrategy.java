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

package io.delta.flink.sink;

import io.delta.flink.table.DeltaTable;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Performing the row-level merge that turns an incoming upsert/delete stream into Delta {@code
 * Add}/{@code Remove} file actions.
 *
 * <p>A strategy owns the per-checkpoint bookkeeping it needs to do its job. {@link DeltaSinkWriter}
 * pushes primary-key information into the strategy as rows arrive ({@link #recordUpsert} for {@code
 * INSERT}/{@code UPDATE_AFTER}; {@link #recordDelete} for {@code DELETE}) and calls {@link #merge}
 * once per checkpoint to materialize that bookkeeping into Delta actions and reset internal state.
 *
 * <p>Implementations are responsible for emitting:
 *
 * <ul>
 *   <li>{@code RemoveFile} actions for any existing data file that contains rows whose PK matches
 *       an upserted or deleted PK;
 *   <li>{@code AddFile} actions for any rewritten (kept) rows from those files.
 * </ul>
 *
 * <p>The newly-written upsert rows themselves are appended by the writer via the standard {@link
 * DeltaTable#writeParquet} path and are <em>not</em> part of the strategy's responsibility.
 *
 * <p>Implementations must be {@link Serializable} so the writer can be shipped to TaskManagers.
 * Implementations should keep their internal state empty between checkpoints (i.e. {@link #merge}
 * must clear whatever {@link #recordUpsert} / {@link #recordDelete} accumulated).
 */
public interface MergeStrategy extends Serializable {

  /**
   * Records the primary key of an {@code INSERT} or {@code UPDATE_AFTER} row written during the
   * current checkpoint, so the strategy can later remove any pre-existing row that shares this key.
   * The new row itself has already been appended by the writer via {@link DeltaTable#writeParquet}.
   *
   * @param primaryKey the primary-key values, not null but individual elements may be null
   * @param partitionValues the partition column values of the row; empty for unpartitioned tables
   */
  void recordUpsert(List<Literal> primaryKey, Map<String, Literal> partitionValues);

  /**
   * Records the primary key of a {@code DELETE} row observed during the current checkpoint. The
   * {@code DELETE} row itself is <em>not</em> appended to the table — the strategy must remove the
   * existing row with this key.
   *
   * @param primaryKey the primary-key values, not null but individual elements may be null
   * @param partitionValues the partition column values of the row; empty for unpartitioned tables
   */
  void recordDelete(List<Literal> primaryKey, Map<String, Literal> partitionValues);

  /**
   * Resolves the recorded checkpoint work into Delta actions and clears internal state.
   *
   * <p>If no records were observed since the last call, returns an empty list.
   *
   * @param table the target Delta table; implementations may use it to read the current snapshot
   *     and to write replacement data files via {@link DeltaTable#writeParquet}
   * @param conf the sink configuration, providing the schema, primary-key columns, and rolling
   *     strategy
   * @return additional actions to include in the commit. Empty if no existing files need
   *     modification.
   * @throws IOException if reading existing files or writing replacement files fails
   */
  List<Row> merge(DeltaTable table, DeltaSinkConf conf) throws IOException;
}
