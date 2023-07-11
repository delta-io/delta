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

package io.delta.kernel;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.types.StructType;

/**
 * Builder to construct {@link Scan} object.
 */
public interface ScanBuilder
{

    /**
     * Apply the given filter expression to prune any files that do not contain data satisfying
     * the given filter.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @param filter an {@link Expression} which evaluates to boolean.
     * @return A {@link ScanBuilder} with filter applied.
     */
    ScanBuilder withFilter(TableClient tableClient, Expression filter);

    /**
     * Apply the given <i>readSchema</i>. If the builder already has a projection applied, calling
     * this again replaces the existing projection.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @param readSchema Subset of columns to read from the Delta table.
     * @return A {@link ScanBuilder} with projection pruning.
     */
    ScanBuilder withReadSchema(TableClient tableClient, StructType readSchema);

    /**
     * @return Build the {@link Scan instance}
     */
    Scan build();
}
