package io.delta.kernel.internal.skipping;

import io.delta.kernel.expressions.*;
import io.delta.kernel.types.CollationIdentifier;

import java.util.*;

public class CollatedDataSkippingPredicate extends CollatedPredicate implements DataSkippingPredicate {

  private final Set<Column> referencedCols;

  /** Set of collated {@link Column}s referenced by the predicate or any of its child expressions */
  private final Map<CollationIdentifier, Set<Column>> referencedCollatedCols;

  CollatedDataSkippingPredicate(String name, Column column, Literal literal, CollationIdentifier collationIdentifier) {
    super(name, Arrays.asList(column, literal), collationIdentifier);
    this.referencedCols = Collections.singleton(column);
    this.referencedCollatedCols = Collections.singletonMap(collationIdentifier, Collections.singleton(column));
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
