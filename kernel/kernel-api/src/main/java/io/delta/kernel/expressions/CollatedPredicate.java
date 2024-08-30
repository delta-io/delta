package io.delta.kernel.expressions;

import java.util.List;

public class CollatedPredicate extends Predicate {
  private final CollationIdentifier collationIdentifier;

  public CollatedPredicate(String name, List<Expression> children, CollationIdentifier collationIdentifier) {
    super(name, children);
    this.collationIdentifier = collationIdentifier;
  }

  public CollationIdentifier getCollationIdentifier() {
    return collationIdentifier;
  }
}
