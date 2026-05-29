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

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;
import java.util.function.BiPredicate;

/**
 * Merge-on-read {@link Upsert} (stub). Logically deletes rows by writing deletion vectors over the
 * existing data files; readers combine each base file with its DV at scan time.
 *
 * <p><b>Status:</b> not implemented. {@link #deleteRecords} throws; needs DV writing, the {@code
 * deletionVectors} table feature, and concurrent-commit retry handling before it can ship.
 */
public class MoRUpsert extends Upsert {

  public MoRUpsert() {
    super(new ScanLocator());
  }

  @Override
  protected CloseableIterator<Row> deleteRecords(
      Row addFile, BiPredicate<ColumnarBatch, Integer> filter) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
