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

import io.delta.kernel.exceptions.KernelException;
import java.util.Optional;

/**
 * A predicate evaluator that directly evaluates predicates, resolving column references to scalar
 * values.
 *
 * <p>This evaluator is used for partition pruning and evaluating opaque data skipping predicates
 * produced by {@link IndirectDataSkippingPredicateEvaluator}.
 *
 * <p><b>Semantics:</b>
 *
 * <ul>
 *   <li>{@code Optional.empty()} means the predicate evaluated to NULL
 *   <li>{@code Optional.of(true/false)} is the boolean result of evaluation
 * </ul>
 *
 * <p><b>Rust Equivalent:</b>
 *
 * <pre>{@code
 * pub type DirectPredicateEvaluator<'a> = dyn KernelPredicateEvaluator<Output = bool> + 'a;
 * }</pre>
 *
 * @since 4.1.0
 */
@FunctionalInterface
public interface DirectPredicateEvaluator {
  /**
   * Evaluates a predicate to a boolean value.
   *
   * @param predicate the predicate to evaluate
   * @param inverted whether to invert the result (apply NOT)
   * @return {@code Optional.of(true/false)} if evaluation succeeded, or {@code Optional.empty()} if
   *     the result is NULL
   * @throws KernelException if the predicate cannot be evaluated
   */
  Optional<Boolean> eval(Predicate predicate, boolean inverted) throws KernelException;
}
