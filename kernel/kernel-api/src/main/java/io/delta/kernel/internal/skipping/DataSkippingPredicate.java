package io.delta.kernel.internal.skipping;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.CollationIdentifier;

import java.util.Set;

public interface DataSkippingPredicate {
  Set<Column> getReferencedCols();

  Set<Tuple2<CollationIdentifier, Column>> getCollatedReferencedCols();

  Predicate asPredicate();
}
