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

import io.delta.kernel.internal.TableImpl;

/**
 * Represents the Delta Lake table for a given path.
 */
public interface Table
{
    /**
     * Instantiate a table object for the Delta Lake table at the given path.
     *
     * @param path location where the Delta table is present. Path needs to be fully qualified.
     * @return an instance of {@link Table} representing the Delta table at given path
     * @throws TableNotFoundException when there is no Delta table at the given path.
     */
    static Table forPath(String path)
        throws TableNotFoundException
    {
        return TableImpl.forPath(path);
    }

    /**
     * Get the latest snapshot of the table.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @return an instance of {@link Snapshot}
     */
    Snapshot getLatestSnapshot(TableClient tableClient)
        throws TableNotFoundException;
}
