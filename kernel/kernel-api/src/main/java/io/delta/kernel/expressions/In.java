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
package io.delta.kernel.expressions;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.types.CollationIdentifier;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code IN} expression
 *
 * <p>Definition:
 *
 * <ul>
 *   <li>SQL semantic: {@code expr IN (expr1, expr2, ...) [COLLATE collationIdentifier]}
 *   <li>Requires the value expression to be evaluated against a list of literal expressions.
 *   <li>Result is true if the value matches any element in the list, false if no matches and no
 *       nulls, null if the value is null or any comparison results in null.
 *   <li>Supports collation for string comparisons.
 * </ul>
 *
 * @since 4.0.0
 */
@Evolving
public final class In extends Predicate {

  /**
   * Creates an IN predicate expression.
   *
   * @param valueExpression The expression to evaluate (left side of IN)
   * @param inListElements The list of literal expressions to check against
   */
  public In(Expression valueExpression, List<Expression> inListElements) {
    super("IN", buildChildren(valueExpression, inListElements));
  }

  /**
   * Creates an IN predicate expression with collation support.
   *
   * @param valueExpression The expression to evaluate (left side of IN)
   * @param inListElements The list of literal expressions to check against
   * @param collationIdentifier The collation identifier for string comparisons
   */
  public In(
      Expression valueExpression,
      List<Expression> inListElements,
      CollationIdentifier collationIdentifier) {
    super("IN", buildChildren(valueExpression, inListElements), collationIdentifier);
  }

  /** @return The value expression to be evaluated (left side of IN). */
  public Expression getValueExpression() {
    return getChildren().get(0);
  }

  /** @return The list of expressions to check against (right side of IN). */
  public List<Expression> getInListElements() {
    return getChildren().subList(1, getChildren().size());
  }

  private static List<Expression> buildChildren(
      Expression valueExpression, List<Expression> inListElements) {
    List<Expression> children = new ArrayList<>();
    children.add(valueExpression);
    children.addAll(inListElements);
    return children;
  }
}
