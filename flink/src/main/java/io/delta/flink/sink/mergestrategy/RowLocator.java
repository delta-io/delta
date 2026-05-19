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

import io.delta.flink.table.DeltaTable;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.utils.CloseableIterator;
import java.util.List;

/**
 * Pluggable strategy for finding Delta data files that may contain rows whose primary key matches a
 * given set of PK tuples. The "where do I look" half of a merge — {@link Upsert} pairs a locator
 * with a row-level filter that does the exact intersection.
 *
 * <p>Implementations must not return false negatives (would leave stale rows). False positives are
 * fine — the row-level filter discards non-matches.
 */
public interface RowLocator {

  /**
   * Return scan-file rows for files that may contain at least one row whose PK is in {@code pks}.
   *
   * @param table the Delta table to search
   * @param pkIndices ordinals of the PK columns in the table schema, in PK order
   * @param pks PK tuples to locate; each inner list has the same arity as {@code pkIndices}. May be
   *     empty, in which case implementations should return an empty iterator.
   * @return iterator of scan-file rows in Kernel's {@code Scan.getScanFiles(...)} shape
   */
  CloseableIterator<Row> find(DeltaTable table, int[] pkIndices, List<List<Literal>> pks);
}
