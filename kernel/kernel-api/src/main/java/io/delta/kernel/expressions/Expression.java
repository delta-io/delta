package io.delta.kernel.expressions;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.DataType;

/**
 * Generic interface for all Expressions
 */
public interface Expression {

    /**
     * @param row the input row to evaluate.
     * @return the result of evaluating this expression on the given input {@link Row}.
     */
    Object eval(Row row);

    /**
     * @return the {@link DataType} of the result of evaluating this expression.
     */
    DataType dataType();

    /**
     * @return the String representation of this expression.
     */
    String toString();

    /**
     * @return a {@link List} of the immediate children of this node
     */
    List<Expression> children();

    /**
     * @return the names of columns referenced by this expression.
     */
    default Set<String> references() {
        Set<String> result = new HashSet<>();
        children().forEach(child -> result.addAll(child.references()));
        return result;
    }
}
