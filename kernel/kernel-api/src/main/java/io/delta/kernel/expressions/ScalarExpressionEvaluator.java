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
 * A Kernel-supplied scalar expression evaluator that can convert column references (i.e., {@link
 * Expression#column}) to scalar values.
 *
 * <p>This interface is used by {@link OpaqueExpressionOp#evalExprScalar} and {@link
 * OpaquePredicateOp#evalPredScalar} to evaluate sub-expressions.
 *
 * <p><b>Semantics:</b>
 *
 * <ul>
 *   <li>{@code Optional.empty()} means Kernel was unable to evaluate the input expression
 *   <li>{@code Optional.of(value)} is the result of evaluation (may be {@code null} if the output
 *       was NULL)
 * </ul>
 *
 * <p><b>Rust Equivalent:</b>
 *
 * <pre>{@code
 * pub type ScalarExpressionEvaluator<'a> = dyn Fn(&Expression) -> Option<Scalar> + 'a;
 * }</pre>
 *
 * @since 4.1.0
 */
@FunctionalInterface
public interface ScalarExpressionEvaluator {
  /**
   * Evaluates an expression to a scalar value, typically by resolving column references to their
   * values in the current row.
   *
   * @param expr the expression to evaluate
   * @return {@code Optional.of(value)} if evaluation succeeded (value may be null for NULL), or
   *     {@code Optional.empty()} if Kernel couldn't evaluate the expression
   */
  Optional<Object> eval(Expression expr);
}
