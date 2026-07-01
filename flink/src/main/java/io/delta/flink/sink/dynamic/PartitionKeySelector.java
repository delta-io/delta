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

package io.delta.flink.sink.dynamic;

import java.util.Arrays;
import java.util.Objects;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Flink {@link KeySelector} for {@link DynamicRow} that combines the table name and partition
 * values so rows with the same table and the same partition land on the same writer subtask.
 *
 * <p>This mirrors the intent of {@link io.delta.flink.sink.PartitionKeySelector} for the
 * single-table sink, but uses the string partition values already carried on {@link DynamicRow}
 * instead of extracting them from {@code RowData} with a fixed schema.
 *
 * <p>The key is {@code Objects.hash(tableName, Arrays.hashCode(partitionValues))}. A {@code null}
 * {@link DynamicRow#getPartitionValues()} is treated like an empty partition column list (same as
 * {@code new String[0]}) for stable routing.
 *
 * <p>A concrete class is used (not a lambda) so Flink does not need to capture serializable lambdas
 * in the job graph.
 */
public class PartitionKeySelector implements KeySelector<DynamicRow, Integer> {

  private static final String[] EMPTY_PARTITION_VALUES = new String[0];

  @Override
  public Integer getKey(DynamicRow value) throws Exception {
    String[] partitionValues = value.getPartitionValues();
    if (partitionValues == null) {
      partitionValues = EMPTY_PARTITION_VALUES;
    }
    return Objects.hash(value.getTableName(), Arrays.hashCode(partitionValues));
  }
}
