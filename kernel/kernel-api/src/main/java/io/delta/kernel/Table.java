package io.delta.kernel;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.internal.TableImpl;

public interface Table {

    /**
     * Instantiate table object for Delta Lake table at the given path.
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
    Snapshot getLatestSnapshot(TableClient tableClient);
}
