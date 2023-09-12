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
package io.delta.kernel.expressions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.annotation.Evolving;

/**
 * Scalar SQL expressions which take zero or more inputs and for each input row generate one
 * output value. A subclass of these expressions are of type {@link Predicate} whose result type is
 * `boolean`. See {@link Predicate} for predicate type scalar expressions. Supported
 * non-predicate type scalar expressions are listed below.
 * TODO: Currently there aren't any. Will be added in future. An example one looks like this:
 * <ol>
 *   <li>Name: <code>+</code>
 *     <ul>
 *       <li>SQL semantic: <code>expr1 + expr2</code></li>
 *       <li>Since version: 3.0.0</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * @since 3.0.0
 */
@Evolving
public class ScalarExpression implements Expression {
    protected final String name;
    protected final List<Expression> children;

    public ScalarExpression(String name, List<Expression> children) {
        this.name = requireNonNull(name, "name is null").toUpperCase(Locale.ENGLISH);
        this.children = Collections.unmodifiableList(new ArrayList<>(children));
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", name,
            children.stream().map(Object::toString).collect(Collectors.joining(", ")));
    }

    public String getName() {
        return name;
    }

    @Override
    public List<Expression> getChildren() {
        return children;
    }
}
