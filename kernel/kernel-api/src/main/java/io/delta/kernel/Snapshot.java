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
     * Get the schema of the table according to this snapshot.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @return Schema of the Delta table at this snapshot.
     */
    StructType getSchema(TableClient tableClient);

    /**
     * Create scan builder to allow construction of scans to read data from this snapshot.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @return an instance of {@link ScanBuilder}
     */
    ScanBuilder getScanBuilder(TableClient tableClient);
}
