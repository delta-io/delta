package io.delta.kernel.expressions;

import io.delta.kernel.types.CollationIdentifier;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CollatedPredicate extends Predicate {
  public CollatedPredicate(String name, List<Expression> children, CollationIdentifier collationIdentifier) {
    super(name, children);
    this.collationIdentifier = collationIdentifier;
  }

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
      return String.format("(%s %s %s %s)", children.get(0), name, children.get(1), collationIdentifier);
    }
    return super.toString();
  }

  private static final Set<String> BINARY_OPERATORS =
          Stream.of("<", "<=", ">", ">=", "=", "AND", "OR").collect(Collectors.toSet());
}
