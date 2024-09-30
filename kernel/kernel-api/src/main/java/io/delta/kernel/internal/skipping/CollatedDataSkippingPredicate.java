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
package io.delta.kernel.internal.skipping;

import io.delta.kernel.expressions.*;
import io.delta.kernel.types.CollationIdentifier;
import java.util.*;

/** A {@link Predicate} with a set of columns referenced by the expression. */
public class CollatedDataSkippingPredicate extends CollatedPredicate
    implements DataSkippingPredicate {

  /**
   * Set of {@link Column}s referenced by the predicate or any of its child expressions.
   * For a {@link CollatedDataSkippingPredicate}, set of referenced {@link Column}s is same as
   * set of referenced collated {@link Column}s.
   */
  private final Set<Column> referencedCols;

  /** Set of collated {@link Column}s referenced by the predicate or any of its child expressions */
  private final Map<CollationIdentifier, Set<Column>> referencedCollatedCols;

  /**
   *
   * @param name the predicate name
   * @param column the column referenced by this predicate
   * @param literal
   * @param collationIdentifier identifies collation for data skipping
   */
  public CollatedDataSkippingPredicate(
      String name, Column column, Literal literal, CollationIdentifier collationIdentifier) {
    super(name, Arrays.asList(column, literal), collationIdentifier);
    this.referencedCols = Collections.singleton(column);
    this.referencedCollatedCols =
        new HashMap<>(Collections.singletonMap(collationIdentifier, new HashSet<>(Collections.singleton(column))));
  }

  @Override
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
