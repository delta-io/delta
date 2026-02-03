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
 * A data skipping predicate evaluator that rewrites the input to a predicate that performs data
 * skipping over column stats for all referenced columns.
 *
 * <p>This evaluator is used for stats-based file pruning during log replay. Instead of evaluating
 * the predicate immediately, it transforms column references like {@code columnName} into stats
 * column references like {@code stats.minValues.columnName}, {@code stats.maxValues.columnName},
 * etc. The resulting predicate can be evaluated against batches of column stats at some future
 * point.
 *
 * <p><b>Example Transformation:</b>
 *
 * <pre>{@code
 * Input:  price > 100
 * Output: stats.minValues.price > 100  // File could contain values > 100
 * }</pre>
 *
 * <p><b>Semantics:</b>
 *
 * <ul>
 *   <li>{@code Optional.empty()} means the predicate cannot be converted to a data skipping
 *       predicate
 *   <li>{@code Optional.of(predicate)} is the transformed predicate that references stats columns
 * </ul>
 *
 * <p><b>Rust Equivalent:</b>
 *
 * <pre>{@code
 * pub type IndirectDataSkippingPredicateEvaluator<'a> =
 *     dyn DataSkippingPredicateEvaluator<Output = Pred, ColumnStat = Expr> + 'a;
 * }</pre>
 *
 * @since 4.1.0
 */
@FunctionalInterface
public interface IndirectDataSkippingPredicateEvaluator {
  /**
   * Transforms a predicate to reference stats columns instead of data columns.
   *
   * @param predicate the predicate to transform
   * @param inverted whether to invert the result (apply NOT)
   * @return {@code Optional} containing the transformed predicate if conversion succeeded, or
   *     {@code Optional.empty()} if the predicate cannot be converted for data skipping
   */
  Optional<Predicate> transform(Predicate predicate, boolean inverted);
}
