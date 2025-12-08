/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink.sink;

import java.util.Collection;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * We need a concrete KeySelector instead of a lambda here to prevent Flink from serializing a
 * lambda
 */
public class PartitionKeySelector implements KeySelector<RowData, Integer> {

  private final RowType schema;
  private final Collection<String> fieldNames;

  public PartitionKeySelector(RowType schema, Collection<String> names) {
    this.schema = schema;
    this.fieldNames = names;
  }

  @Override
  public Integer getKey(RowData value) throws Exception {
    return Conversions.FlinkToJava.partitionValues(schema, fieldNames, value).hashCode();
  }
}
