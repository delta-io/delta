package io.delta.kernel.internal.skipping;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Predicate;

import java.util.Set;

public interface DataSkippingPredicate {
  Set<Column> getReferencedCols();

  Set<Column> getCollatedReferencedCols();

  Predicate asPredicate();
}
