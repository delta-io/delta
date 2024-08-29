package io.delta.kernel.expressions;

import java.util.List;

public class CollatedPredicate extends Predicate {
  private final Collation collation;

  public CollatedPredicate(String name, List<Expression> children, Collation collation) {
    super(name, children);
    this.collation = collation;
  }

  public Collation getCollation() {
    return collation;
  }
}
