/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

/**
 * Component 1 interface: Planner.
 *
 * <p>Reads log segments, builds a DataFrame, obtains rewritten predicates from the {@link
 * ScanPredicateBuilder}, and applies them to the Spark plan.
 *
 * <p>Effective Java Item 64: refer to objects by their interfaces.
 *
 * @see DistributedScanPlanner distributed implementation
 * @see ScanPredicateBuilder Component 2: PredicateBuilder
 * @see ScanExecutor Component 3: Executor
 */
public interface ScanPlanner {

  /**
   * Plans a scan: reads log segments, applies state reconstruction, rewrites and applies pushdown
   * predicates.
   *
   * @param pushdownFilters all pushable Spark filters (partition + data); empty = no filtering
   * @param partitionSchema partition schema for partition filter rewriting (non-null)
   * @return planned DataFrame with schema {@code {add: {path, partitionValues, stats, ...}}}
   */
  Dataset<Row> plan(Filter[] pushdownFilters, StructType partitionSchema);
}
