// todo: copyright

package io.delta.standalone.expressions;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.internal.expressions.Util;

/**
 * Usage: {@code new In(expr, exprList)} - Returns true if `expr` is equal to any in `exprList`,
 * else false.
 */
public final class In implements Predicate {
    private final Expression value;
    private final List<? extends Expression> elems;
    private final Comparator<Object> comparator;

    public In(Expression value, List<? extends Expression> elems) {
        if (null == value) {
            throw new IllegalArgumentException("'In' expression 'value' cannot be null");
        }
        if (null == elems) {
            throw new IllegalArgumentException("'In' expression 'elems' cannot be null");
        }
        if (elems.isEmpty()) {
            throw new IllegalArgumentException("'In' expression 'elems' cannot be empty");
        }

        boolean allSameDataType = elems
            .stream()
            .allMatch(x -> x.dataType().equals(value.dataType()));

        if (!allSameDataType) {
            throw new IllegalArgumentException(
                "In expression 'elems' and 'value' must all be of the same DataType");
        }

        this.value = value;
        this.elems = elems;
        this.comparator = Util.createComparator(value.dataType());
    }

    /**
     * This implements the {@code IN} expression functionality outlined by the Databricks SQL Null
     * semantics reference guide. The logic is as follows:
     * - TRUE if the non-NULL value is found in the list
     * - FALSE if the non-NULL value is not found in the list and the list does not contain NULL
     *   values
     * - NULL if the value is NULL, or the non-NULL value is not found in the list and the list
     *   contains at least one NULL value
     *
     * @see <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-null-semantics.html#in-and-not-in-subqueries">NULL Semantics</a>
     */
    @Override
    public Boolean eval(RowRecord record) {
        Object origValue = value.eval(record);
        if (null == origValue) return null;

        // null if a null value has been found in list, otherwise false
        Boolean falseOrNullresult = false;
        for (Expression setElem : elems) {
            Object setElemValue = setElem.eval(record);
            if (setElemValue == null) {
                // null value found but element may still be in list
                falseOrNullresult = null;
            } else if (comparator.compare(origValue, setElemValue) == 0) {
                // short circuit and return true; we have found the element in the list
                return true;
            }

        }
        return falseOrNullresult;
    }

    @Override
    public String toString() {
        String elemsStr = elems
            .stream()
            .map(Expression::toString)
            .collect(Collectors.joining(", "));
        return value + " IN (" + elemsStr + ")";
    }

    @Override
    public List<Expression> children() {
        return Stream.concat(Stream.of(value), elems.stream()).collect(Collectors.toList());
    }
}
