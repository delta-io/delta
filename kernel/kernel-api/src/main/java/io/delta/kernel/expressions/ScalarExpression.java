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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.annotation.Evolving;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Scalar SQL expressions which take zero or more inputs and for each input row generate one output
 * value. A subclass of these expressions are of type {@link Predicate} whose result type is
 * `boolean`. See {@link Predicate} for predicate type scalar expressions. Supported non-predicate
 * type scalar expressions are listed below.
 *
 * <ol>
 *   <li>Name: <code>element_at</code>
 *       <ul>
 *         <li>Semantic: <code>element_at(map, key)</code>. Return the value of given <i>key</i>
 *             from the <i>map</i> type input. Returns <i>null</i> if the given <i>key</i> is not in
 *             the <i>map</i> Ex: `element_at(map(1, 'a', 2, 'b'), 2)` returns 'b'
 *         <li>Since version: 3.0.0
 *       </ul>
 *   <li>Name: <code>COALESCE</code>
 *       <ul>
 *         <li>Semantic: <code>COALESCE(expr1, ..., exprN)</code> Return the first non-null
 *             argument. If all arguments are null returns null
 *         <li>Since version: 3.1.0
 *       </ul>
 *   <li>Name: <code>TIMEADD</code>
 *       <ul>
 *         <li>Semantic: <code>TIMEADD(colExpr, milliseconds)</code>. Add the specified number of
 *             milliseconds to the timestamp represented by <i>colExpr</i>. The adjustment does not
 *             alter the original value but returns a new timestamp increased by the given
 *             milliseconds. Ex: `TIMEADD(timestampColumn, 1000)` returns a timestamp 1 second
 *             later.
 *         <li>Since version: 3.3.0
 *       </ul>
 *   <li>Name: <code>SUBSTRING</code>
 *       <ul>
 *         <li>Semantic: <code>SUBSTRING(colExpr, pos, len)</code>. Substring starts at pos and is
 *             of length len when str is String type or returns the slice of byte array that starts
 *             at pos in byte and is of length len when str is Binary type.
 *         <li>Since version: 3.4.0
 *       </ul>
 * </ol>
 *
 * @since 3.0.0
 */
@Evolving
public class ScalarExpression implements Expression {
  protected final String name;
  protected final List<Expression> children;

  public ScalarExpression(String name, List<Expression> children) {
    this.name = requireNonNull(name, "name is null").toUpperCase(Locale.ENGLISH);
    this.children = Collections.unmodifiableList(new ArrayList<>(children));
  }

  @Override
  public String toString() {
    return String.format(
        "%s(%s)", name, children.stream().map(Object::toString).collect(Collectors.joining(", ")));
  }

  public String getName() {
    return name;
  }

  @Override
  public List<Expression> getChildren() {
    return children;
  }
}
