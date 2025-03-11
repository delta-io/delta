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

import io.delta.kernel.expressions.CollatedPredicate;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.CollationIdentifier;

import java.util.*;

/** A {@link Predicate} with a set of columns referenced by the expression. */
public class DataSkippingPredicate extends Predicate implements IDataSkippingPredicate {

  /**
   * Set of {@link Column}s referenced by the predicate or any of its child expressions.
   * {@link Column}s that are referenced just with {@link CollatedPredicate} are not included in this set.
   */
  private final Set<Column> referencedCols;

  /** Map that maps collation to set of {@link Column}s referenced by the {@link CollatedPredicate} or any of its child expressions */
  private final Map<CollationIdentifier, Set<Column>> referencedCollatedCols;

  /**
   * @param name the predicate name
   * @param children list of expressions that are input to this predicate.
   * @param referencedCols set of {@link Column}s referenced by the predicate or any of its child expressions.
   * {@link Column}s that are referenced just with {@link CollatedPredicate} are not included in this set.
   * @param referencedCollatedCols map that maps collation to set of {@link Column}s referenced by the {@link CollatedPredicate} or any of its child expressions
   */
  DataSkippingPredicate(String name, List<Expression> children, Set<Column> referencedCols,
                         Map<CollationIdentifier, Set<Column>> referencedCollatedCols) {
    super(name, children);
    this.referencedCols = Collections.unmodifiableSet(referencedCols);
    this.referencedCollatedCols = Collections.unmodifiableMap(referencedCollatedCols);
  }

  /**
   * Constructor for a binary {@link DataSkippingPredicate} where both children are instances of
   * {@link DataSkippingPredicate}.
   *
   * @param name the predicate name
   * @param left left input to this predicate
   * @param right right input to this predicate
   */
  DataSkippingPredicate(String name, IDataSkippingPredicate left, IDataSkippingPredicate right) {
    this(
        name,
        Arrays.asList(left.asPredicate(), right.asPredicate()),
        new HashSet<Column>() {
          {
            addAll(left.getReferencedCols());
            addAll(right.getReferencedCols());
          }
        },
        new HashMap<CollationIdentifier, Set<Column>>() {
          {
            for (Map.Entry<CollationIdentifier, Set<Column>> entry :
                    left.getReferencedCollatedCols().entrySet()) {
              if (!containsKey(entry.getKey())) {
                put(entry.getKey(), entry.getValue());
              } else {
                get(entry.getKey()).addAll(entry.getValue());
              }
            }
            for (Map.Entry<CollationIdentifier, Set<Column>> entry :
                    right.getReferencedCollatedCols().entrySet()) {
              if (!containsKey(entry.getKey())) {
                put(entry.getKey(), entry.getValue());
              } else {
                get(entry.getKey()).addAll(entry.getValue());
              }
            }
          }
        });
  }

  public Set<Column> getReferencedCols() {
    return referencedCols;
  }

  @Override
  public Map<CollationIdentifier, Set<Column>> getReferencedCollatedCols() {
    return referencedCollatedCols;
  }

  @Override
  public Predicate asPredicate() {
    return this;
  }
}
