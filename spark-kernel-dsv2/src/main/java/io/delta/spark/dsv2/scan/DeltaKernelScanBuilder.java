package io.delta.spark.dsv2.scan;

import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import org.apache.hadoop.conf.Configuration;

/**
 * A Spark ScanBuilder implementation that wraps Delta Kernel's ScanBuilder. This allows Spark to
 * use Delta Kernel for reading Delta tables.
 */
public class DeltaKernelScanBuilder implements org.apache.spark.sql.connector.read.ScanBuilder {

  private final SnapshotImpl snapshot;
  private final Engine engine;

  public DeltaKernelScanBuilder(SnapshotImpl snapshot) {
    this.snapshot = snapshot;
    this.engine = DefaultEngine.create(new Configuration());
  }

  @Override
  public org.apache.spark.sql.connector.read.Scan build() {
    // Build the Delta Kernel scan
    ScanBuilder kernelScanBuilder = snapshot.getScanBuilder();

    Scan kernelScan = kernelScanBuilder.build();

    // Return a Spark Scan that wraps the Delta Kernel scan
    return new DeltaKernelScan(
        kernelScan,
        io.delta.spark.dsv2.utils.SchemaUtils.convertKernelSchemaToSparkSchema(
            snapshot.getSchema()),
        engine);
  }
}
