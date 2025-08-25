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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.engine.ExpressionHandler;
import io.delta.kernel.types.CollationIdentifier;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Defines predicate scalar expression which is an extension of {@link ScalarExpression} that
 * evaluates to true, false, or null for each input row.
 *
 * <p>Currently, implementations of {@link ExpressionHandler} requires support for at least the
 * following scalar expressions.
 *
 * <ol>
 *   <li>Name: <code>=</code>
 *       <ul>
 *         <li>SQL semantic: <code>expr1 = expr2 [COLLATE collationIdentifier]</code>
 *         <li>Since version: 3.0.0
 *       </ul>
 *   <li>Name: <code>&lt;</code>
 *       <ul>
 *         <li>SQL semantic: <code>expr1 &lt; expr2 [COLLATE collationIdentifier]</code>
 *         <li>Since version: 3.0.0
 *       </ul>
 *   <li>Name: <code>&lt;=</code>
 *       <ul>
 *         <li>SQL semantic: <code>expr1 &lt;= expr2 [COLLATE collationIdentifier]</code>
 *         <li>Since version: 3.0.0
 *       </ul>
 *   <li>Name: <code>&gt;</code>
 *       <ul>
 *         <li>SQL semantic: <code>expr1 &gt; expr2 [COLLATE collationIdentifier]</code>
 *         <li>Since version: 3.0.0
 *       </ul>
 *   <li>Name: <code>&gt;=</code>
 *       <ul>
 *         <li>SQL semantic: <code>expr1 &gt;= expr2 [COLLATE collationIdentifier]</code>
 *         <li>Since version: 3.0.0
 *       </ul>
 *   <li>Name: <code>ALWAYS_TRUE</code>
 *       <ul>
 *         <li>SQL semantic: <code>Constant expression whose value is `true`</code>
 *         <li>Since version: 3.0.0
 *       </ul>
 *   <li>Name: <code>ALWAYS_FALSE</code>
 *       <ul>
 *         <li>SQL semantic: <code>Constant expression whose value is `false`</code>
 *         <li>Since version: 3.0.0
 *       </ul>
 *   <li>Name: <code>AND</code>
 *       <ul>
 *         <li>SQL semantic: <code>expr1 AND expr2</code>
 *         <li>Since version: 3.0.0
 *       </ul>
 *   <li>Name: <code>OR</code>
 *       <ul>
 *         <li>SQL semantic: <code>expr1 OR expr2</code>
 *         <li>Since version: 3.0.0
 *       </ul>
 *   <li>Name: <code>NOT</code>
 *       <ul>
 *         <li>SQL semantic: <code>NOT expr</code>
 *         <li>Since version: 3.1.0
 *       </ul>
 *   <li>Name: <code>IS_NOT_NULL</code>
 *       <ul>
 *         <li>SQL semantic: <code>expr IS NOT NULL</code>
 *         <li>Since version: 3.1.0
 *       </ul>
 *   <li>Name: <code>IS_NULL</code>
 *       <ul>
 *         <li>SQL semantic: <code>expr IS NULL</code>
 *         <li>Since version: 3.2.0
 *       </ul>
 *   <li>Name: <code>LIKE</code>
 *       <ul>
 *         <li>SQL semantic: <code>expr LIKE expr</code>
 *         <li>Since version: 3.3.0
 *       </ul>
 *   <li>Name: <code>IS NOT DISTINCT FROM</code>
 *       <ul>
 *         <li>SQL semantic: <code>expr1 IS NOT DISTINCT FROM expr2 [COLLATE collationIdentifier]
 *             </code>
 *         <li>Since version: 3.3.0
 *       </ul>
 *   <li>Name: <code>STARTS_WITH</code>
 *       <ul>
 *         <li>SQL semantic: <code>expr STARTS_WITH expr [COLLATE collationIdentifier]</code>
 *         <li>Since version: 3.4.0
 *       </ul>
 * </ol>
 *
 * @since 3.0.0
 */
@Evolving
public class Predicate extends ScalarExpression {
  /** Optional collation to be used for string comparison in this predicate. */
  protected Optional<CollationIdentifier> collationIdentifier;

  public Predicate(String name, List<Expression> children) {
    super(name, children);
    collationIdentifier = Optional.empty();
  }

  /** Constructor for a unary Predicate expression */
  public Predicate(String name, Expression child) {
    this(name, Arrays.asList(child));
  }

  /** Constructor for a binary Predicate expression */
  public Predicate(String name, Expression left, Expression right) {
    this(name, Arrays.asList(left, right));
  }

  /** Constructor for a binary Predicate expression with collation support. */
  public Predicate(
      String name, Expression left, Expression right, CollationIdentifier collationIdentifier) {
    this(name, Arrays.asList(left, right), collationIdentifier);
  }

  /** Constructor for a binary Predicate expression with collation support. */
  public Predicate(
      String name, List<Expression> children, CollationIdentifier collationIdentifier) {
    this(name, children);
    checkArgument(
        COLLATION_SUPPORTED_OPERATORS.contains(this.name),
        "Collation is not supported for operator %s. Supported operators are %s",
        this.name,
        COLLATION_SUPPORTED_OPERATORS);
    checkArgument(
        this.children.size() == 2,
        "Invalid Predicate: collated predicate '%s' requires exactly 2 children, but found %d.",
        this.name,
        this.children.size());
    this.collationIdentifier = Optional.of(collationIdentifier);
  }

  /** Returns the collation identifier used for this predicate, if specified. */
  public Optional<CollationIdentifier> getCollationIdentifier() {
    return collationIdentifier;
  }

  @Override
  public String toString() {
    if (collationIdentifier.isPresent()) {
      return String.format(
          "(%s %s %s COLLATE %s)",
          children.get(0), name, children.get(1), collationIdentifier.get());
    } else if (BINARY_OPERATORS.contains(name)) {
      return String.format("(%s %s %s)", children.get(0), name, children.get(1));
    }
    return super.toString();
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Predicate)) return false;
    return this.hashCode() == o.hashCode();
  }

  private static final Set<String> BINARY_OPERATORS =
      Stream.of("<", "<=", ">", ">=", "=", "AND", "OR", "IS NOT DISTINCT FROM", "STARTS_WITH")
          .collect(Collectors.toSet());

  /** Operators that support collation-based string comparison. */
  private static final Set<String> COLLATION_SUPPORTED_OPERATORS =
      Stream.of("<", "<=", ">", ">=", "=", "IS NOT DISTINCT FROM", "STARTS_WITH")
          .collect(Collectors.toSet());
}
