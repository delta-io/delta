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

import java.util.List;

import io.delta.kernel.annotation.Evolving;

/**
 * Base interface for all Kernel expressions.
 *
 * @since 3.0.0
 */
@Evolving
public interface Expression {
    /**
     * @return a list of expressions that are input to this expression.
     */
    List<Expression> getChildren();
}
