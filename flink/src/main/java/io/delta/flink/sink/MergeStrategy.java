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
import io.delta.kernel.expressions.Literal;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;

/**
 * Performing the row-level merge that turns an incoming upsert/delete stream into Delta {@code
 * Add}/{@code Remove} file actions.
 *
 * <p>A strategy owns the per-checkpoint bookkeeping it needs to do its job. {@link DeltaSinkWriter}
 * pushes primary-key information into the strategy as rows arrive ({@link #upsert} for {@code
 * INSERT}/{@code UPDATE_AFTER}; {@link #delete} for {@code DELETE}) and calls {@link #merge} once
 * per checkpoint to materialize that bookkeeping into Delta actions and reset internal state.
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
 * must clear whatever {@link #upsert} / {@link #delete} accumulated).
 */
public interface MergeStrategy extends Serializable {

  /**
   * Stable cache key for a per-partition writer task; collapses {@link Literal} partition values to
   * a canonical string so logically-equal partitions hash to the same bucket. A null-valued {@link
   * Literal} is encoded distinctly from a literal whose string value is {@code "null"}.
   */
  static Map<String, String> writerKey(Map<String, Literal> partitionValues) {
    return partitionValues.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> encodeLiteral(entry.getValue())));
  }

  /**
   * Canonical, lossless string key for a composite primary key. Values are already in primary-key
   * ordinal order, so no sorting is needed; each value is escaped so distinct keys cannot collide
   * on the {@code ;} delimiter. A null-valued {@link Literal} is encoded with a marker that
   * escaping can never produce, so it is distinguished from a literal whose string value is {@code
   * "null"}.
   */
  static String keyString(List<Literal> keys) {
    return keys.stream().map(MergeStrategy::encodeLiteral).collect(Collectors.joining(";"));
  }

  /**
   * Encodes a single non-null {@link Literal} to a canonical string fragment: a null-valued literal
   * becomes the marker {@code \0} (which escaping can never produce), every other value is its
   * string form with {@code \} and {@code ;} escaped.
   */
  private static String encodeLiteral(Literal literal) {
    Objects.requireNonNull(literal, "literal must not be null");
    return literal.getValue() == null
        ? "\\0"
        : literal.toString().replace("\\", "\\\\").replace(";", "\\;");
  }

  /**
   * Binds the strategy to the owning writer for the lifetime of the writer (called once after
   * construction). Strategies that need access to the table, schema or partition columns read them
   * from {@code writer}.
   */
  void init(DeltaSinkWriter writer);

  /**
   * Records an {@code INSERT} row written during the current checkpoint. The new row is appended to
   * the partition's writer task with no pre-image bookkeeping; see {@link #upsert} for the update
   * path that also schedules pre-image removal.
   *
   * @param primaryKey the primary-key values, not null and does not contain null elements.
   * @param partitionValues the partition column values of the row; empty for unpartitioned tables
   * @param content the actual row written
   */
  void insert(
      List<Literal> primaryKey,
      Map<String, Literal> partitionValues,
      RowData content,
      SinkWriter.Context context);
  /**
   * Records an {@code UPDATE_AFTER} row written during the current checkpoint, so the strategy can
   * later remove any pre-existing row that shares this key.
   *
   * @param primaryKey the primary-key values, not null and does not contain null elements
   * @param partitionValues the partition column values of the row; empty for unpartitioned tables
   * @param content the actual row written
   */
  void upsert(
      List<Literal> primaryKey,
      Map<String, Literal> partitionValues,
      RowData content,
      SinkWriter.Context context);

  /**
   * Records the primary key of a {@code DELETE} row observed during the current checkpoint. The
   * {@code DELETE} row itself is <em>not</em> appended to the table — the strategy must remove the
   * existing row with this key.
   *
   * @param primaryKey the primary-key values, not null and does not contain null elements
   * @param partitionValues the partition column values of the row; empty for unpartitioned tables
   */
  void delete(List<Literal> primaryKey, Map<String, Literal> partitionValues);

  /**
   * Resolves the recorded checkpoint work into Delta actions and clears internal state.
   *
   * <p>If no records were observed since the last call, returns an empty list.
   *
   * @return actions to include in the commit.
   * @throws IOException if reading existing files or writing replacement files fails
   */
  List<DeltaWriterResult> merge() throws IOException;
}
