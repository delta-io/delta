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
package io.delta.kernel.internal.util;

import java.util.List;
import static java.lang.String.format;

import io.delta.kernel.expressions.*;
import static io.delta.kernel.expressions.AlwaysFalse.ALWAYS_FALSE;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

public class ExpressionUtils {
    /**
     * Return an expression cast as a predicate, throw an error if it is not a predicate
     */
    public static Predicate asPredicate(Expression expression) {
        checkArgument(expression instanceof Predicate,
            String.format("Expected predicate but got %s", expression));
        return (Predicate) expression;
    }

    /**
     * Utility method to return the left child of the binary input expression
     */
    public static Expression getLeft(Expression expression) {
        List<Expression> children = expression.getChildren();
        checkArgument(
            children.size() == 2,
            format("%s: expected two inputs, but got %s", expression, children.size()));
        return children.get(0);
    }

    /**
     * Utility method to return the right child of the binary input expression
     */
    public static Expression getRight(Expression expression) {
        List<Expression> children = expression.getChildren();
        checkArgument(
            children.size() == 2,
            format("%s: expected two inputs, but got %s", expression, children.size()));
        return children.get(1);
    }

    /**
     * Utility method to return the single child of the unary input expression
     */
    public static Expression getUnaryChild(Expression expression) {
        List<Expression> children = expression.getChildren();
        checkArgument(
            children.size() == 1,
            format("%s: expected one inputs, but got %s", expression, children.size()));
        return children.get(0);
    }

    /*
     * Utility method to combine the given predicates with AND
     */
    public static Predicate combineWithAndOp(Predicate left, Predicate right) {
        String leftName = left.getName().toUpperCase();
        String rightName = right.getName().toUpperCase();
        if (leftName.equals("ALWAYS_FALSE") || rightName.equals("ALWAYS_FALSE")) {
            return ALWAYS_FALSE;
        }
        if (leftName.equals("ALWAYS_TRUE")) {
            return right;
        }
        if (rightName.equals("ALWAYS_TRUE")) {
            return left;
        }
        return new And(left, right);
    }
}
