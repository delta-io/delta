package io.delta.spark.dsv2.scan;

import io.delta.kernel.internal.SnapshotImpl;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A Spark ScanBuilder implementation that wraps Delta Kernel's ScanBuilder. This allows Spark to
 * use Delta Kernel for reading Delta tables.
 */
public class DeltaKernelScanBuilder implements org.apache.spark.sql.connector.read.ScanBuilder {

    private final SnapshotImpl snapshot;
    private final CaseInsensitiveStringMap scanOptions;

    public DeltaKernelScanBuilder(CaseInsensitiveStringMap scanOptions, SnapshotImpl snapshot) {
        this.snapshot = snapshot;
        this.scanOptions = scanOptions;
    }

    @Override
    public org.apache.spark.sql.connector.read.Scan build() {
        throw new UnsupportedOperationException("batch read is not supported");
    }
}
