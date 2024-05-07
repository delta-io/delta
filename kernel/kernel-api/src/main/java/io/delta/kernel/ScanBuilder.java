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

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;

/**
 * Builder to construct {@link Scan} object.
 *
 * @since 3.0.0
 */
@Evolving
public interface ScanBuilder {

    /**
     * Apply the given filter expression to prune any files that do not possibly contain the data
     * that satisfies the given filter.
     * <p>
     * Kernel makes use of the scan file partition values (for partitioned tables) and file-level
     * column statistics (min, max, null count etc.) in the Delta metadata for filtering. Sometimes
     * these metadata is not enough to deterministically say a scan file doesn't contain data that
     * satisfies the filter.
     * <p>
     * E.g. given filter is {@code a = 2}. In file A, column {@code a} has min value as -40
     * and max value as 200. In file B, column {@code a} has min value as 78 and max value as 323.
     * File B can be ruled out as it cannot possibly have rows where `a = 2`, but file A cannot
     * be ruled out as it may contain rows where {@code a = 2}.
     * <p>
     * As filtering is a best effort, the {@link Scan} object may return scan files (through
     * {@link Scan#getScanFiles(Engine)}) that does not satisfy the filter. It is the responsibility
     * of the caller to apply the remaining filter returned by {@link Scan#getRemainingFilter()} to
     * the data read from the scan files (returned by {@link Scan#getScanFiles(Engine)}) to
     * completely filter out the data that doesn't satisfy the filter.```
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @param predicate   a {@link Predicate} to prune the metadata or data.
     * @return A {@link ScanBuilder} with filter applied.
     */
    ScanBuilder withFilter(TableClient tableClient, Predicate predicate);

    /**
     * Apply the given <i>readSchema</i>. If the builder already has a projection applied, calling
     * this again replaces the existing projection.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @param readSchema  Subset of columns to read from the Delta table.
     * @return A {@link ScanBuilder} with projection pruning.
     */
    ScanBuilder withReadSchema(TableClient tableClient, StructType readSchema);

    /**
     * @return Build the {@link Scan instance}
     */
    Scan build();
}
