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
 *   <li>Name: <code>IN</code>
 *       <ul>
 *         <li>SQL semantic: <code>expr IN (expr1, expr2, ...) [COLLATE collationIdentifier]</code>
 *         <li>Since version: 4.0.0
 *       </ul>
 * </ol>
 *
 * @since 3.0.0
 */
@Evolving
public class Predicate extends ScalarExpression {
  /** Optional collation to be used for string comparison in this predicate. */
  private Optional<CollationIdentifier> collationIdentifier;

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

  /** Constructor for a Predicate expression with collation support. */
  public Predicate(
      String name, Expression left, Expression right, CollationIdentifier collationIdentifier) {
    this(name, Arrays.asList(left, right), collationIdentifier);
  }

  /** Constructor for a Predicate expression with collation support. */
  public Predicate(
      String name, List<Expression> children, CollationIdentifier collationIdentifier) {
    this(name, children);
    checkArgument(
        COLLATION_SUPPORTED_OPERATORS.contains(this.name),
        "Collation is not supported for operator %s. Supported operators are %s",
        this.name,
        COLLATION_SUPPORTED_OPERATORS);

    // For operators with multiple children, we need at least 2 children
    // For other collated expressions, we require exactly 2 children
    if (OPERATORS_WITH_MULTIPLE_CHILDREN.contains(this.name)) {
      checkArgument(
          this.children.size() >= 2,
          "Invalid Predicate: collated predicate '%s' with multiple children requires at least 2 "
              + "children, but found %d.",
          this.name,
          this.children.size());
    } else {
      checkArgument(
          this.children.size() == 2,
          "Invalid Predicate: collated predicate '%s' requires exactly 2 children, but found %d.",
          this.name,
          this.children.size());
    }
    this.collationIdentifier = Optional.of(collationIdentifier);
  }

  /** Returns the collation identifier used for this predicate, if specified. */
  public Optional<CollationIdentifier> getCollationIdentifier() {
    return collationIdentifier;
  }

  /**
   * Returns string representation of the predicate.
   *
   * <p>Format for binary operators: {@code (left OP right)} or {@code (left OP right COLLATE
   * collation)}
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>{@code (col = 5)}
   *   <li>{@code (name = 'John' COLLATE SPARK.UTF8_BINARY)}
   * </ul>
   *
   * <p>Note: Specialized operators like IN override this method to provide their own string
   * representation.
   */
  @Override
  public String toString() {
    String collationSuffix = collationIdentifier.map(c -> " COLLATE " + c).orElse("");
    if (BINARY_OPERATORS.contains(name) || collationIdentifier.isPresent()) {
      return String.format("(%s %s %s%s)", children.get(0), name, children.get(1), collationSuffix);
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

  /** Operators that can have multiple children (more than 2). */
  private static final Set<String> OPERATORS_WITH_MULTIPLE_CHILDREN = Collections.singleton("IN");

  /** Operators that support collation-based string comparison. */
  private static final Set<String> COLLATION_SUPPORTED_OPERATORS =
      Stream.of("<", "<=", ">", ">=", "=", "IS NOT DISTINCT FROM", "STARTS_WITH", "IN")
          .collect(Collectors.toSet());
}
