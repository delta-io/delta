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

  @Override
  public String toString() {
    if (BINARY_OPERATORS.contains(name)) {
      return String.format("(%s %s %s [%s])", children.get(0), name, children.get(1), collationIdentifier);
    }
    return super.toString();
  }
}
