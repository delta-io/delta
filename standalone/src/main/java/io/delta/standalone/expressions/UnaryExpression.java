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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import io.delta.standalone.data.RowRecord;

/**
 * An expression with one input and one output. The output is by default evaluated to null
 * if the input is evaluated to null.
 */
public abstract class UnaryExpression implements Expression {
    protected final Expression child;

    public UnaryExpression(Expression child) {
        this.child = child;
    }

    public Expression getChild() {
        return child;
    }

    @Override
    public Object eval(RowRecord record) {
        Object childResult = child.eval(record);

        if (null == childResult) return null;

        return nullSafeEval(childResult);
    }

    protected Object nullSafeEval(Object childResult) {
        throw new IllegalArgumentException(
            "UnaryExpressions must override either eval or nullSafeEval");
    }

    @Override
    public List<Expression> children() {
        return Collections.singletonList(child);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UnaryExpression that = (UnaryExpression) o;
        return Objects.equals(child, that.child);
    }

    @Override
    public int hashCode() {
        return Objects.hash(child);
    }
}
