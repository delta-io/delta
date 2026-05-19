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
import io.delta.flink.table.DeltaTable;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * No-op {@link MergeStrategy} used by append-mode sinks.
 *
 * <p>In append mode every incoming row is written via {@link DeltaTable#writeParquet} and no
 * existing files need to be modified. The writer must never call {@link #recordUpsert} or {@link
 * #recordDelete} in append mode; if it does, that's a programming error and we fail loudly.
 */
public class AppendOnly implements MergeStrategy {

  @Override
  public void recordUpsert(List<Literal> primaryKey, Map<String, Literal> partitionValues) {
    throw new IllegalStateException(
        "AppendOnlyMergeStrategy received an upsert record; this should only happen in "
            + "upsert mode.");
  }

  @Override
  public void recordDelete(List<Literal> primaryKey, Map<String, Literal> partitionValues) {
    throw new IllegalStateException(
        "AppendOnlyMergeStrategy received a delete record; this should only happen in "
            + "upsert mode.");
  }

  @Override
  public List<Row> merge(DeltaTable table, DeltaSinkConf conf) {
    return Collections.emptyList();
  }
}
