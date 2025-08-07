package io.delta.spark.dsv2.scan;

import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.types.StructType;
import io.delta.spark.dsv2.utils.SchemaUtils;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType as SparkStructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A Spark ScanBuilder implementation that wraps Delta Kernel's ScanBuilder.
 * This allows Spark to use Delta Kernel for reading Delta tables.
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
    return new DeltaKernelScan(kernelScan, SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema()), engine);
  }

} 