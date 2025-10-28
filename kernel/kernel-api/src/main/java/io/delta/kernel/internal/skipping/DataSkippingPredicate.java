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
package io.delta.kernel.internal.skipping;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.CollationIdentifier;
import java.util.*;

/** A {@link Predicate} with a set of columns referenced by the expression. */
public class DataSkippingPredicate extends Predicate {

  /** Set of {@link Column}s referenced by the predicate or any of its child expressions */
  private final Set<Column> referencedCols;

  /**
   * @param name the predicate name
   * @param children list of expressions that are input to this predicate.
   * @param referencedCols set of columns referenced by this predicate or any of its child
   *     expressions
   */
  DataSkippingPredicate(String name, List<Expression> children, Set<Column> referencedCols) {
    super(name, children);
    this.referencedCols = Collections.unmodifiableSet(referencedCols);
  }

  /**
   * @param name the predicate name
   * @param children list of expressions that are input to this predicate.
   * @param collationIdentifier collation identifier used for this predicate
   * @param referencedCols set of columns referenced by this predicate or any of its child
   *     expressions
   */
  DataSkippingPredicate(
      String name,
      List<Expression> children,
      CollationIdentifier collationIdentifier,
      Set<Column> referencedCols) {
    super(name, children, collationIdentifier);
    this.referencedCols = Collections.unmodifiableSet(referencedCols);
  }

  /**
   * Constructor for a binary {@link DataSkippingPredicate} where both children are instances of
   * {@link DataSkippingPredicate}.
   *
   * @param name the predicate name
   * @param left left input to this predicate
   * @param right right input to this predicate
   */
  DataSkippingPredicate(String name, DataSkippingPredicate left, DataSkippingPredicate right) {
    super(name, Arrays.asList(left, right));
    this.referencedCols = immutableUnion(left.referencedCols, right.referencedCols);
  }

  /** @return set of columns referenced by this predicate or any of its child expressions */
  public Set<Column> getReferencedCols() {
    return referencedCols;
  }

  /**
   * @return set of collation identifiers referenced by this predicate or any of its child
   *     expressions
   */
  public Set<Tuple2<CollationIdentifier, Column>> getReferencedCollations() {
    Set<Tuple2<CollationIdentifier, Column>> referencedCollations = new HashSet<>();

    if (this.getCollationIdentifier().isPresent()) {
      CollationIdentifier collationIdentifier = this.getCollationIdentifier().get();
      Expression child = this.getChildren().get(0);
      if (!(child instanceof Column)) {
        throw new IllegalStateException(
            String.format(
                "Expected first child of DataSkippingPredicate with collation to be a Column, but"
                    + " found %s",
                this.getChildren().get(0).getClass().getName()));
      }
      referencedCollations.add(new Tuple2<>(collationIdentifier, (Column) child));
    }

    for (Expression child : children) {
      if (child instanceof DataSkippingPredicate) {
        referencedCollations.addAll(((DataSkippingPredicate) child).getReferencedCollations());
      } else if (child instanceof Predicate) {
        throw new IllegalStateException(
            String.format(
                "Expected child Predicate of DataSkippingPredicate to also be a"
                    + " DataSkippingPredicate, but found %s",
                child.getClass().getName()));
      }
    }
    return Collections.unmodifiableSet(referencedCollations);
  }

  /** @return an unmodifiable set containing all elements from both sets. */
  private <T> Set<T> immutableUnion(Set<T> set1, Set<T> set2) {
    return Collections.unmodifiableSet(
        new HashSet<T>() {
          {
            addAll(set1);
            addAll(set2);
          }
        });
  }
}
