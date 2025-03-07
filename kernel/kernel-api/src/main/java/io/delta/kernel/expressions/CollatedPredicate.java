package io.delta.kernel.expressions;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.types.CollationIdentifier;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Defines collated predicate which is an extension of {@link Predicate} that
 * evaluates to true, false, or null for each input row.
 *
 * <br><br>
 * Examples:
 * <ol>
 *     <li><code>expr1 = expr2 collate "SPARK.UTF8_LCASE"</code></li>
 *     <br>
 *     <li><code>expr1 <= expr2 collate "sr_Cyrl_SRB.75.1"</code></li>
 *     <br>
 *     <li><code>expr1 STARTS_WITH expr2 collate "ICU.en_US"</code></li>
 * </ol>
 */
@Evolving
public class CollatedPredicate extends Predicate {
    private final CollationIdentifier collationIdentifier;

    /** Constructor for a CollatedPredicate expression */
    public CollatedPredicate(String name, Expression left, Expression right, CollationIdentifier collationIdentifier) {
        super(name, left, right);
        this.collationIdentifier = collationIdentifier;
    }

    @Override
    public String toString() {
        if (BINARY_OPERATORS.contains(name)) {
            return String.format("(%s %s %s [%s])", children.get(0), name, children.get(1), collationIdentifier);
        }
        return super.toString();
    }

    private static final Set<String> BINARY_OPERATORS =
        Stream.of("<", "<=", ">", ">=", "=", "AND", "OR").collect(Collectors.toSet());
}
