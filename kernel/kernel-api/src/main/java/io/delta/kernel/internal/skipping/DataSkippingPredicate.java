package io.delta.kernel.internal.skipping;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.CollationIdentifier;

import java.util.Map;
import java.util.Set;

public interface DataSkippingPredicate {
  Set<Column> getReferencedCols();

  Map<CollationIdentifier, Set<Column>> getReferencedCollatedCols();

  Predicate asPredicate();
}
