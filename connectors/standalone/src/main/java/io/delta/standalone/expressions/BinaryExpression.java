/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.expressions;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import io.delta.standalone.data.RowRecord;

/**
 * An {@link Expression} with two inputs and one output. The output is by default evaluated to null
 * if either input is evaluated to null.
 */
public abstract class BinaryExpression implements Expression {
    protected final Expression left;
    protected final Expression right;

    protected BinaryExpression(Expression left, Expression right) {
        this.left = left;
        this.right = right;
    }

    public Expression getLeft() {
        return left;
    }

    public Expression getRight() {
        return right;
    }

    @Override
    public final Object eval(RowRecord record) {
        Object leftResult = left.eval(record);
        if (null == leftResult) return null;

        Object rightResult = right.eval(record);
        if (null == rightResult) return null;

        return nullSafeEval(leftResult, rightResult);
    }

    protected abstract Object nullSafeEval(Object leftResult, Object rightResult);

    @Override
    public List<Expression> children() {
        return Arrays.asList(left, right);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BinaryExpression that = (BinaryExpression) o;
        return Objects.equals(left, that.left) &&
            Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }
}
