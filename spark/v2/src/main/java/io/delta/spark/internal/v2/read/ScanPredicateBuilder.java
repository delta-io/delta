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

/**
 * Component 2 interface: PredicateBuilder.
 *
 * <p>Takes pushdown predicates and the kernel's stats provider, then generates partition pruning
 * and data skipping filter expressions that can be applied via {@code df.filter()}.
 *
 * <p>Effective Java Item 64: refer to objects by their interfaces.
 *
 * @see DeltaScanPredicateBuilder default implementation
 * @see ScanPlanner Component 1: Planner
 * @see ScanExecutor Component 3: Executor
 */
public interface ScanPredicateBuilder {

  /**
   * Rewrites the given pushdown filters and applies them to the DataFrame.
   *
   * <p>The input DataFrame must have schema {@code {add: {path, partitionValues, stats, ...}}}.
   * Filters are split into:
   *
   * <ul>
   *   <li><b>Partition filters</b> — rewritten to reference {@code partitionValues.<colName>}
   *   <li><b>Data filters</b> — converted to stats-based predicates via {@code
   *       constructDataFilters}
   * </ul>
   *
   * @param wrappedDF DataFrame with schema {@code {add: struct}} from log replay
   * @param filters all pushable Spark filters (partition + data); empty array = no-op
   * @return filtered DataFrame with same wrapped schema {@code {add: {...}}}
   */
  Dataset<Row> apply(Dataset<Row> wrappedDF, Filter[] filters);
}
