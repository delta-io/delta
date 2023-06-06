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
import io.delta.kernel.types.StructType;

/**
 * Represents the snapshot of a Delta table.
 */
public interface Snapshot {

    /**
     * Get the version of this snapshot in the table.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @return version of this snapshot in the Delta table
     */
    long getVersion(TableClient tableClient);

    /**
     * Get the schema of the table at this snapshot.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @return Schema of the Delta table at this snapshot.
     */
    StructType getSchema(TableClient tableClient);

    /**
     * Create a scan builder to construct a {@link Scan} to read data from this snapshot.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @return an instance of {@link ScanBuilder}
     */
    ScanBuilder getScanBuilder(TableClient tableClient);
}
