/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import io.delta.kernel.types.CollationIdentifier;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Defines collated predicate which is an extension of {@link Predicate} that
 * is used for comparing string in collated fashion.
 *
 * @since 3.3.0
 */
public class CollatedPredicate extends Predicate {

  private final CollationIdentifier collationIdentifier;

  public CollatedPredicate(String name, List<Expression> children, CollationIdentifier collationIdentifier) {
    super(name, children);
    this.collationIdentifier = collationIdentifier;
  }

  /** Constructor for a binary CollatedPredicate expression */
  public CollatedPredicate(String name, Expression left, Expression right, CollationIdentifier collationIdentifier) {
    super(name, left, right);
    this.collationIdentifier = collationIdentifier;
  }

  /** @return the {@link CollationIdentifier} used for this {@link CollatedPredicate} */
  public CollationIdentifier getCollationIdentifier() {
    return collationIdentifier;
  }

  @Override
  public String toString() {
    if (BINARY_OPERATORS.contains(name)) {
      return String.format("(%s %s %s %s)", children.get(0), name, children.get(1), collationIdentifier);
    }
    return super.toString();
  }

  private static final Set<String> BINARY_OPERATORS =
          Stream.of("<", "<=", ">", ">=", "=", "AND", "OR").collect(Collectors.toSet());
}