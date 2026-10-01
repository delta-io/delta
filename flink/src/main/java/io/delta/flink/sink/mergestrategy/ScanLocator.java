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

import static io.delta.flink.kernel.ExpressionUtils.in;

import io.delta.flink.table.DeltaTable;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Default {@link RowLocator}: builds an {@code (pkCols) IN (pks)} predicate via {@link
 * io.delta.flink.kernel.ExpressionUtils#in} and runs it through Kernel's snapshot scan-files step,
 * so only files whose stats overlap the PK set are returned.
 *
 * <p>Correct on any Delta table, no extra index required. Pruning quality depends on the Parquet
 * min/max stats; tight for time-ordered PKs, degrades toward a full scan for UUID-like PKs (use
 * {@link IndexLocator} once implemented for that case).
 */
public class ScanLocator implements RowLocator {

  @Override
  public CloseableIterator<Row> find(DeltaTable table, int[] pkIndices, List<List<Literal>> pks) {

    List<String> pkNames =
        Arrays.stream(pkIndices)
            .mapToObj(idx -> table.getSchema().fieldNames().get(idx))
            .collect(Collectors.toList());
    Predicate filterPk = in(pkNames, pks);

    return table.scan(filterPk);
  }
}
