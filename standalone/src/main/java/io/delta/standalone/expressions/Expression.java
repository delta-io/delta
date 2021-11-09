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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.DataType;

/**
 * An expression in Delta Standalone.
 */
public interface Expression {

    /**
     * @param record  the input record to evaluate.
     * @return the result of evaluating this expression on the given input {@link RowRecord}.
     */
    Object eval(RowRecord record);

    /**
     * @return the {@link DataType} of the result of evaluating this expression.
     */
    DataType dataType();

    /**
     * @return the String representation of this expression.
     */
    String toString();

    /**
     * @return the names of columns referenced by this expression.
     */
    default Set<String> references() {
        Set<String> result = new HashSet<>();
        children().forEach(child -> result.addAll(child.references()));
        return result;
    }

    /**
     * @return a {@link List} of the children of this node. Children should not change.
     */
    List<Expression> children();
}
