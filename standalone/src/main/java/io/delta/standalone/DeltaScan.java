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

package io.delta.standalone;

import java.util.Optional;

import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.expressions.Expression;

/**
 * Provides access to an iterator over the files in this snapshot.
 * <p>
 * Typically created with a read predicate {@link Expression} to let users filter files.
 */
public interface DeltaScan {

    /**
     * Creates a {@link CloseableIterator} which can iterate over files belonging to this snapshot.
     * <p>
     * It provides no iteration ordering guarantee among files.
     * <p>
     * Files returned are guaranteed to satisfy the predicate, if any, returned by
     * {@link #getPushedPredicate()}.
     *
     * @return a {@link CloseableIterator} to iterate over files.
     */
    CloseableIterator<AddFile> getFiles();

    /**
     * @return the input predicate used to filter files.
     */
    Optional<Expression> getInputPredicate();

    /**
     * @return portion of the input predicate that can be evaluated by Delta Standalone using only
     *         metadata. Files returned by {@link #getFiles()} are guaranteed to satisfy the pushed
     *         predicate, and the caller doesn’t need to apply them again on the returned files.
     */
    Optional<Expression> getPushedPredicate();

    /**
     * @return portion of the input predicate that may not be fully applied. Files returned by
     *         {@link #getFiles()} are not guaranteed to satisfy the residual predicate, and the
     *         caller should still apply them on the returned files.
     */
    Optional<Expression> getResidualPredicate();
}
