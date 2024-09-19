package io.delta.kernel.internal.skipping;

import io.delta.kernel.expressions.*;

import java.util.*;

public class DataSkippingCollatedPredicate extends CollatedPredicate implements DataSkippingPredicate {

  /** Set of {@link Column}s referenced by the predicate or any of its child expressions */
  private final Set<Column> referencedCols;

  /**
   * Constructor for a binary {@link DataSkippingPredicate} where both children are instances of
   * {@link DataSkippingPredicate}.
   *
   * @param name the predicate name
   * @param left left input to this predicate
   * @param right right input to this predicate
   */
  DataSkippingCollatedPredicate(String name, Expression left, Expression right, Set<Column> referencedCols, CollationIdentifier collationIdentifier) {
    super(name, left, right, collationIdentifier);
    this.referencedCols = referencedCols;
  }

  @Override
  public Set<Column> getReferencedCols() {
    return referencedCols;
  }

  @Override
  public Predicate asPredicate() {
    return this;
  }
}
