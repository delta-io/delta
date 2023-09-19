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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;

import io.delta.kernel.annotation.Evolving;

/**
 * An expression type that refers to a column (case-sensitive) in the input. The column name is
 * either a single name or array of names (when referring to a nested column).
 *
 * @since 3.0.0
 */
@Evolving
public final class Column implements Expression {
    private final String[] names;

    /**
     * Create a column expression for referring to a column.
     */
    public Column(String name) {
        this.names = new String[] {name};
    }

    /**
     * Create a column expression to refer to a nested column.
     */
    public Column(String[] names) {
        this.names = names;
    }

    /**
     * @return the column names. Each part in the name correspond to one level of nested reference.
     */
    public String[] getNames() {
        return names;
    }

    @Override
    public List<Expression> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public String toString() {

        return "column(" + quoteColumnPath(names)  + ")";
    }

    private static String quoteColumnPath(String names[]) {
        return Arrays.stream(names)
            .map(s -> format("`%s`", s.replace("`", "``")))
            .collect(Collectors.joining("."));
    }
}
