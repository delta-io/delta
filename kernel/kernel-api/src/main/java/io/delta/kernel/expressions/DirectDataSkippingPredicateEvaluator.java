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
package io.delta.kernel.expressions;

import java.util.Optional;

/**
 * A data skipping predicate evaluator that directly applies data skipping, resolving column
 * references to scalar stats values such as those provided by Parquet footer stats.
 *
 * <p>This evaluator is used for Parquet row group skipping, where stats are available directly
 * (e.g., from Parquet file footers) and can be evaluated immediately.
 *
 * <p><b>Semantics:</b>
 *
 * <ul>
 *   <li>{@code Optional.empty()} means the predicate evaluated to NULL or stats are missing
 *   <li>{@code Optional.of(true)} means the row group should be included
 *   <li>{@code Optional.of(false)} means the row group can be skipped
 * </ul>
 *
 * <p><b>Rust Equivalent:</b>
 *
 * <pre>{@code
 * pub type DirectDataSkippingPredicateEvaluator<'a> =
 *     dyn DataSkippingPredicateEvaluator<Output = bool, ColumnStat = Scalar> + 'a;
 * }</pre>
 *
 * @since 4.1.0
 */
@FunctionalInterface
public interface DirectDataSkippingPredicateEvaluator {
  /**
   * Evaluates a predicate for data skipping using available stats.
   *
   * @param predicate the predicate to evaluate
   * @param inverted whether to invert the result (apply NOT)
   * @return {@code Optional.of(true/false)} if evaluation succeeded, or {@code Optional.empty()} if
   *     stats are missing or result is NULL
   */
  Optional<Boolean> eval(Predicate predicate, boolean inverted);
}
