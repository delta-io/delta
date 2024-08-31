package io.delta.kernel.expressions;

public class CollatedPredicate extends Predicate {
  public CollatedPredicate(String name, Expression left, Expression right, CollationIdentifier collationIdentifier) {
    super(name, left, right);
    this.collationIdentifier = collationIdentifier;
  }

  public CollationIdentifier getCollationIdentifier() {
    return collationIdentifier;
  }

  private final CollationIdentifier collationIdentifier;
}
