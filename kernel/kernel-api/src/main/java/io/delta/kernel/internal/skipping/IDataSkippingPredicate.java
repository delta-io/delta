package io.delta.kernel.internal.skipping;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.expressions.CollatedPredicate;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.CollationIdentifier;

import java.util.Map;
import java.util.Set;

/**
 * Interface for a predicate that can be used for file pruning.
 * There are two implementations of this interface:
 * <br>
 * <ol>
 *     <li>TODO</li>
 *     <li>{@link IDataSkippingPredicate} - TODO </li>
 * </ol>
 */
@Evolving
public interface IDataSkippingPredicate {

    /**
     * @return Set of {@link Column}s referenced by the predicate or any of its child expressions.
     * {@link Column}s that are referenced just with {@link CollatedPredicate} are not included in this set.
     */
    Set<Column> getReferencedCols();

    /** @return Map that maps collation to set of {@link Column}s referenced by the {@link CollatedPredicate} or any of its child expressions */
    Map<CollationIdentifier, Set<Column>> getReferencedCollatedCols();

    /** @return {@link IDataSkippingPredicate} as {@link Predicate} */
    Predicate asPredicate();
}