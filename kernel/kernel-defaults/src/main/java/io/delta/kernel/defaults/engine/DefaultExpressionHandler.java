/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.engine;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.engine.ExpressionHandler;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.expressions.PredicateEvaluator;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import java.util.Arrays;
import java.util.Optional;

/** Default implementation of {@link ExpressionHandler} */
public class DefaultExpressionHandler implements ExpressionHandler {

  @Override
  public ExpressionEvaluator getEvaluator(
      StructType inputSchema, Expression expression, DataType outputType) {
    return new io.delta.kernel.defaults.internal.expressions.DefaultExpressionEvaluator(
        inputSchema, expression, outputType);
  }

  @Override
  public PredicateEvaluator getPredicateEvaluator(StructType inputSchema, Predicate predicate) {
    return new io.delta.kernel.defaults.internal.expressions.DefaultPredicateEvaluator(
        inputSchema, predicate);
  }

  @Override
  public ColumnVector createSelectionVector(boolean[] values, int from, int to) {
    requireNonNull(values, "values is null");
    int length = to - from;
    checkArgument(
        length >= 0 && values.length > from && values.length >= to,
        "invalid range from=%s, to=%s, values length=%s",
        from,
        to,
        values.length);

    // Make a copy of the `values` array.
    boolean[] valuesCopy = Arrays.copyOfRange(values, from, to);
    return new io.delta.kernel.defaults.internal.data.vector.DefaultBooleanVector(
        length, Optional.empty(), valuesCopy);
  }
}
