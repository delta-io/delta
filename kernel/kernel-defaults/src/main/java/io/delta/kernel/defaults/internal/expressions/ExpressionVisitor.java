/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.defaults.internal.expressions;

import java.util.List;
import java.util.Locale;
import static java.util.stream.Collectors.joining;

import io.delta.kernel.expressions.*;
import static io.delta.kernel.expressions.AlwaysFalse.ALWAYS_FALSE;
import static io.delta.kernel.expressions.AlwaysTrue.ALWAYS_TRUE;

/**
 * Interface to allow visiting an expression tree and implementing handling for each
 * specific expression type.
 *
 * @param <R> Return type of result of visit expression methods.
 */
abstract class ExpressionVisitor<R> {

    abstract R visitAnd(And and);

    abstract R visitOr(Or or);

    abstract R visitAlwaysTrue(AlwaysTrue alwaysTrue);

    abstract R visitAlwaysFalse(AlwaysFalse alwaysFalse);

    abstract R visitComparator(Predicate predicate);

    abstract R visitLiteral(Literal literal);

    abstract R visitColumn(Column column);

    abstract R visitCast(Cast cast);

    abstract R visitPartitionValue(PartitionValueExpression partitionValue);

    abstract R visitElementAt(ScalarExpression elementAt);

    abstract R visitNot(Predicate predicate);

    abstract R visitIsNotNull(Predicate predicate);

    abstract R visitIsNull(Predicate predicate);

    abstract R visitCoalesce(ScalarExpression ifNull);

    final R visit(Expression expression) {
        if (expression instanceof PartitionValueExpression) {
            return visitPartitionValue((PartitionValueExpression) expression);
        } else if (expression instanceof Cast) {
            return visitCast((Cast) expression);
        } else if (expression instanceof ScalarExpression) {
            return visitScalarExpression((ScalarExpression) expression);
        } else if (expression instanceof Literal) {
            return visitLiteral((Literal) expression);
        } else if (expression instanceof Column) {
            return visitColumn((Column) expression);
        }

        throw new UnsupportedOperationException(
            String.format("Expression %s is not supported.", expression));
    }

    private R visitScalarExpression(ScalarExpression expression) {
        List<Expression> children = expression.getChildren();
        String name = expression.getName().toUpperCase(Locale.ENGLISH);
        switch (name) {
            case "ALWAYS_TRUE":
                return visitAlwaysTrue(ALWAYS_TRUE);
            case "ALWAYS_FALSE":
                return visitAlwaysFalse(ALWAYS_FALSE);
            case "AND":
                return visitAnd(
                    new And(elemAsPredicate(children, 0), elemAsPredicate(children, 1)));
            case "OR":
                return visitOr(new Or(elemAsPredicate(children, 0), elemAsPredicate(children, 1)));
            case "=":
            case "<":
            case "<=":
            case ">":
            case ">=":
                return visitComparator(new Predicate(name, children));
            case "ELEMENT_AT":
                return visitElementAt(expression);
            case "NOT":
                return visitNot(new Predicate(name, children));
            case "IS_NOT_NULL":
                return visitIsNotNull(new Predicate(name, children));
            case "IS_NULL":
                return visitIsNull(new Predicate(name, children));
            case "COALESCE":
                return visitCoalesce(expression);
            default:
                throw new UnsupportedOperationException(
                    String.format("Scalar expression `%s` is not supported.", name));
        }
    }

    private static Predicate elemAsPredicate(List<Expression> expressions, int index) {
        if (expressions.size() <= index) {
            throw new RuntimeException(
                String.format("Trying to access invalid entry (%d) in list %s", index,
                    expressions.stream().map(Object::toString).collect(joining(","))));
        }
        Expression elemExpression = expressions.get(index);
        if (!(elemExpression instanceof Predicate)) {
            throw new RuntimeException("Expected a predicate, but got " + elemExpression);
        }
        return (Predicate) expressions.get(index);
    }
}
