package io.delta.kernel.internal.skipping;

import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.CollationIdentifier;
import java.util.List;

/**
 * A specialized {@link Predicate} used for partition pruning. This class does not add any new
 * functionality beyond {@link Predicate}, but serves as a marker to indicate that the predicate is
 * specifically intended for partition pruning operations.
 */
public class PartitionPredicate extends Predicate {
  PartitionPredicate(String name, List<Expression> children) {
    super(name, children);
  }

  PartitionPredicate(
      String name, List<Expression> children, CollationIdentifier collationIdentifier) {
    super(name, children, collationIdentifier);
  }
}
