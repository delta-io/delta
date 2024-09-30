package io.delta.kernel.internal.skipping;

import io.delta.kernel.expressions.*;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.CollationIdentifier;

import java.util.*;

public class CollatedDataSkippingPredicate extends CollatedPredicate implements DataSkippingPredicate {

  private final Set<Column> referencedCols;

  /** Set of collated {@link Column}s referenced by the predicate or any of its child expressions */
  private final Set<Tuple2<CollationIdentifier, Column>> collatedReferencedCols;

  CollatedDataSkippingPredicate(String name, Column column, Literal literal, CollationIdentifier collationIdentifier) {
    super(name, Arrays.asList(column, literal), collationIdentifier);
    this.referencedCols = Collections.singleton(column);
    this.collatedReferencedCols = Collections.singleton(new Tuple2<>(collationIdentifier, column));
  }

  @Override
  public Set<Column> getReferencedCols() {
    return referencedCols;
  }

  @Override
  public Set<Tuple2<CollationIdentifier, Column>> getCollatedReferencedCols() {
    return collatedReferencedCols;
  }

  @Override
  public Predicate asPredicate() {
    return this;
  }
}
