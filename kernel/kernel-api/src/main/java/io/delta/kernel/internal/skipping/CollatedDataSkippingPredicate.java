package io.delta.kernel.internal.skipping;

import io.delta.kernel.expressions.CollatedPredicate;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.CollationIdentifier;

import java.util.*;

public class CollatedDataSkippingPredicate extends CollatedPredicate implements DataSkippingPredicate {

  /** Set of collated {@link Column}s referenced by the predicate or any of its child expressions */
  private final Set<Column> collatedReferencedCols;

  CollatedDataSkippingPredicate(String name, List<Expression> children, Set<Column> collatedReferencedCols, CollationIdentifier collationIdentifier) {
    super(name, children, collationIdentifier);
    this.collatedReferencedCols = Collections.unmodifiableSet(collatedReferencedCols);
  }

  @Override
  public Set<Column> getReferencedCols() {
    return collatedReferencedCols;
  }

  @Override
  public Set<Column> getCollatedReferencedCols() {
    return collatedReferencedCols;
  }

  @Override
  public Predicate asPredicate() {
    return this;
  }
}
