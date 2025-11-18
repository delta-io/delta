package io.delta.flink.sink;

import java.io.IOException;

import io.delta.flink.utils.DeltaTestUtils;

/**
 * Base class for Delta Sink integration tests.
 * Provides common utility methods for test setup.
 * 
 * <p>Flink 2.0 NOTE: This class was simplified from the original version.
 * The original included failover base classes (FailoverDeltaSinkBase, FailoverDeltaGlobalCommitterBase)
 * that relied on GlobalCommitter (removed in Flink 2.0). These have been removed.</p>
 */
public abstract class DeltaSinkExecutionITCaseBase {

    /**
     * Initializes the source folder for tests with either partitioned or non-partitioned tables.
     *
     * @param isPartitioned whether to create a partitioned table
     * @param deltaTablePath path to the Delta table
     * @return the delta table path
     */
    protected String initSourceFolder(boolean isPartitioned, String deltaTablePath) {
        try {
            if (isPartitioned) {
                DeltaTestUtils.initTestForPartitionedTable(deltaTablePath);
            } else {
                DeltaTestUtils.initTestForNonPartitionedTable(deltaTablePath);
            }

            return deltaTablePath;
        } catch (IOException e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }
}
