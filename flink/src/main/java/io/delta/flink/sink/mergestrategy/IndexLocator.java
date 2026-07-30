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
 * {@link RowLocator} that consults an externally-maintained secondary index (Hudi-Bloom-style) over
 * the PK columns to skip files Bloom-misses. Intended for tables where PKs aren't clustered by file
 * and {@link ScanLocator} degrades to a full scan.
 *
 * <p><b>Status:</b> not implemented. {@link #find} throws; needs the index storage, write path, and
 * lookup logic to be built out.
 */
public class IndexLocator implements RowLocator {

  @Override
  public CloseableIterator<Row> find(DeltaTable table, int[] pkIndices, List<List<Literal>> pks) {
    throw new UnsupportedOperationException("Not implemented. WIP");
  }
}
