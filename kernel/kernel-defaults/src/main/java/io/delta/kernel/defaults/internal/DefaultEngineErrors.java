/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.internal;

import static java.lang.String.format;

import io.delta.kernel.expressions.Expression;

public class DefaultEngineErrors {

    public static IllegalArgumentException canNotInstantiateLogStore(String logStoreClassName) {
        return new IllegalArgumentException(
                format("Can not instantiate `LogStore` class: %s", logStoreClassName));
    }

    /**
     * Exception for when the default expression evaluator cannot evaluate an expression.
     * @param expression the unsupported expression
     * @param reason reason for why the expression is not supported/cannot be evaluated
     */
    public static UnsupportedOperationException unsupportedExpressionException(
            Expression expression, String reason) {
        String message = format(
            "Default expression evaluator cannot evaluate the expression: %s. Reason: %s",
            expression,
            reason);
        return new UnsupportedOperationException(message);
    }
}
