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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StringType;

import java.util.List;
import java.util.Objects;

import static io.delta.kernel.defaults.internal.DefaultEngineErrors.unsupportedExpressionException;

public class StartsWithExpressionEvaluator {

    /** Validates and transforms the {@code starts_with} expression. */
    static Predicate validateAndTransform(
            Predicate startsWith,
            List<Expression> childrenExpressions,
            List<DataType> childrenOutputTypes) {
        if (childrenExpressions.size() != 2) {
            throw unsupportedExpressionException(
                    startsWith,
                    "Invalid number of inputs to STARTS_WITH expression. "
                            + "Example usage: STARTS_WITH(column, 'test')");
        }
        if (!(StringType.STRING.equivalent(childrenOutputTypes.get(0))
                && StringType.STRING.equivalent(childrenOutputTypes.get(1)))) {
            throw unsupportedExpressionException(
                    startsWith, "'STARTS_WITH' is expects STRING type inputs");
        }
        // TODO: support non literal as the second input of starts with.
        if (!(childrenExpressions.get(1) instanceof Literal)) {
            throw unsupportedExpressionException(
                    startsWith, "'starts with' expects literal as the second input");
        }
        return new Predicate(startsWith.getName(), childrenExpressions);
    }
}