package io.delta.spark.internal.v2.read.changelog;

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

public class DeltaChangelogScan implements Scan {
  private final Engine engine;
  private final StructType readSchema;
  private final CommitRange commitRange;
  private final StructType dataSchema;
  private final Snapshot snapshot;
  private final boolean rowTrackingEnabled;
  private final Configuration hadoopConf;

  public DeltaChangelogScan(
      StructType readSchema,
      CommitRange commitRange,
      Engine engine,
      StructType dataSchema,
      Snapshot snapshot,
      boolean rowTrackingEnabled,
      Configuration hadoopConf) {
    this.readSchema = readSchema;
    this.commitRange = commitRange;
    this.engine = engine;
    this.dataSchema = dataSchema;
    this.snapshot = snapshot;
    this.rowTrackingEnabled = rowTrackingEnabled;
    this.hadoopConf = hadoopConf;
  }

  @Override
  public StructType readSchema() {
    return readSchema;
  }

  @Override
  public String description() {
    return "DeltaChangelogScan";
  }

  @Override
  public Batch toBatch() {
    return new DeltaChangelogBatch(
        commitRange, engine, dataSchema, snapshot, rowTrackingEnabled, hadoopConf);
  }
}
