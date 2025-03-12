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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.types.CollationIdentifier;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Defines collated predicate which is an extension of {@link Predicate} that evaluates to true,
 * false, or null for each input row. <br>
 * <br>
 * Examples:
 *
 * <ol>
 *   <li><code>expr1 = expr2 COLLATE "SPARK.UTF8_LCASE"</code> <br>
 *   <li><code>expr1 <= expr2 COLLATE "ICU.sr_Cyrl_SRB.75.1"</code> <br>
 *   <li><code>expr1 STARTS_WITH expr2 COLLATE "ICU.en_US"</code>
 * </ol>
 */
@Evolving
public class CollatedPredicate extends Predicate {
  private final CollationIdentifier collationIdentifier;

  /** Constructor for a CollatedPredicate expression */
  public CollatedPredicate(
      String name, Expression left, Expression right, CollationIdentifier collationIdentifier) {
    super(name, left, right);
    checkArgument(
        COLLATION_SUPPORTED_OPERATORS.contains(this.name),
        String.format(
            "Collation is not supported for operator %s. Supported operators are %s",
            this.name, COLLATION_SUPPORTED_OPERATORS));
    Objects.requireNonNull(collationIdentifier, "Collation identifier cannot be null");
    this.collationIdentifier = collationIdentifier;
  }

  public CollationIdentifier getCollationIdentifier() {
    return collationIdentifier;
  }

  @Override
  public String toString() {
    return String.format(
        "(%s %s %s COLLATE %s)", children.get(0), name, children.get(1), collationIdentifier);
  }

  /** Supported operators for collation-based comparisons. */
  private static final Set<String> COLLATION_SUPPORTED_OPERATORS =
      Stream.of("<", "<=", ">", ">=", "=", "IS NOT DISTINCT FROM", "STARTS_WITH")
          .collect(Collectors.toSet());
}
