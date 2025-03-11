package io.delta.kernel.internal.skipping;

import io.delta.kernel.expressions.*;
import io.delta.kernel.types.CollationIdentifier;

import java.util.*;

/** A {@link CollatedPredicate} with a set of columns referenced by the expression. */
public class CollatedDataSkippingPredicate extends CollatedPredicate implements IDataSkippingPredicate {

    /**
     * Set of {@link Column}s referenced by the predicate or any of its child expressions.
     * {@link Column}s that are referenced just with {@link CollatedPredicate} are not included in this set.
     */
    private final Set<Column> referencedCols;

    /** Map that maps collation to set of {@link Column}s referenced by the {@link CollatedPredicate} or any of its child expressions */
    private final Map<CollationIdentifier, Set<Column>> referencedCollatedCols;

    CollatedDataSkippingPredicate(String name, List<Expression> children, CollationIdentifier collationIdentifier, Set<Column> referencedCols,
            Map<CollationIdentifier, Set<Column>> referencedCollatedCols) {
        super(name, children, collationIdentifier);
        this.referencedCols = Collections.unmodifiableSet(referencedCols);
        this.referencedCollatedCols = Collections.unmodifiableMap(referencedCollatedCols);
    }

    /**
     * @param name the predicate name
     * @param column the column referenced by the {@link CollatedPredicate}
     * @param literal the literal value to compare {@link Column} with
     * @param collationIdentifier the collation identifier used for comparison
     */
    public CollatedDataSkippingPredicate(
            String name, Column column, Literal literal, CollationIdentifier collationIdentifier) {
        super(name, column, literal, collationIdentifier);
        this.referencedCols = Collections.singleton(column);
        this.referencedCollatedCols =
                new HashMap<>(
                        Collections.singletonMap(
                                collationIdentifier, new HashSet<>(Collections.singleton(column))));
    }

    @Override
    public Set<Column> getReferencedCols() {
        return referencedCols;
    }

    @Override
    public Map<CollationIdentifier, Set<Column>> getReferencedCollatedCols() {
        return referencedCollatedCols;
    }

    @Override
    public Predicate asPredicate() {
        return this;
    }
}
