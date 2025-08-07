package io.delta.spark.dsv2.scan;

import io.delta.kernel.Scan;
import io.delta.kernel.engine.Engine;
import io.delta.spark.dsv2.utils.SchemaUtils;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

/**
 * A Spark Scan implementation that wraps Delta Kernel's Scan.
 * This allows Spark to use Delta Kernel for reading Delta tables.
 */
public class DeltaKernelScan implements org.apache.spark.sql.connector.read.Scan {

  private final Scan kernelScan;
  private final Engine engine;
  private final StructType readSchema;

  public DeltaKernelScan(Scan kernelScan, StructType readSchema, Engine engine) {
    this.kernelScan = kernelScan;
    this.engine = engine;
    this.readSchema = readSchema;
  }

  @Override
  public StructType readSchema() {
    // Convert Delta Kernel schema to Spark schema
    return readSchema;
  }

  @Override
  public Batch toBatch() {

  }

} 