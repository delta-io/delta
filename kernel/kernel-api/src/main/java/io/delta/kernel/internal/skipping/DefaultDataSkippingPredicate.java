package io.delta.kernel.internal.skipping;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Predicate;
import java.util.*;

public class DefaultDataSkippingPredicate extends Predicate implements DataSkippingPredicate {

  /** Set of {@link Column}s referenced by the predicate or any of its child expressions */
  private final Set<Column> referencedCols;

  private final Set<Column> referencedCollatedCols;

  /**
   * @param name the predicate name
   * @param children list of expressions that are input to this predicate.
   * @param referencedCols set of columns referenced by this predicate or any of its child
   *     expressions
   */
  DefaultDataSkippingPredicate(String name, List<Expression> children, Set<Column> referencedCols) {
    super(name, children);
    this.referencedCols = Collections.unmodifiableSet(referencedCols);
    this.referencedCollatedCols = new HashSet<>();
    for (Expression child : children) {
      if (child instanceof DataSkippingPredicate) {
        DataSkippingPredicate predicate = (DataSkippingPredicate) child;
        this.referencedCollatedCols.addAll(predicate.getReferencedCollatedCols());
      }
    }
  }

  /**
   * Constructor for a binary {@link DataSkippingPredicate} where both children are instances of
   * {@link DataSkippingPredicate}.
   *
   * @param name the predicate name
   * @param left left input to this predicate
   * @param right right input to this predicate
   */
  DefaultDataSkippingPredicate(
      String name, DataSkippingPredicate left, DataSkippingPredicate right) {
    this(
        name,
        Arrays.asList(left.asPredicate(), right.asPredicate()),
        new HashSet<Column>() {
          {
            addAll(left.getReferencedCols());
            addAll(right.getReferencedCols());
          }
        });
  }

  public Set<Column> getReferencedCols() {
    return referencedCols;
  }

  @Override
  public Set<Column> getReferencedCollatedCols() {
    return referencedCollatedCols;
  }

  @Override
  public Predicate asPredicate() {
    return this;
  }
}
