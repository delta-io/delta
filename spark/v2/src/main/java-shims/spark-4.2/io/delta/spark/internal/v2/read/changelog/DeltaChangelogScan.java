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
  private final long startVersion;
  private final long endVersion;
  private final Configuration hadoopConf;

  public DeltaChangelogScan(
      StructType readSchema,
      CommitRange commitRange,
      Engine engine,
      StructType dataSchema,
      Snapshot snapshot,
      long startVersion,
      long endVersion,
      Configuration hadoopConf) {
    this.readSchema = readSchema;
    this.commitRange = commitRange;
    this.engine = engine;
    this.dataSchema = dataSchema;
    this.snapshot = snapshot;
    this.startVersion = startVersion;
    this.endVersion = endVersion;
    this.hadoopConf = hadoopConf;
  }

  @Override
  public StructType readSchema() {
    return readSchema;
  }

  @Override
  public String description() {
    return String.format(
        "DeltaChangelogScan [startVersion=%d, endVersion=%d]", startVersion, endVersion);
  }

  @Override
  public Batch toBatch() {
    return new DeltaChangelogBatch(commitRange, engine, dataSchema, snapshot, hadoopConf);
  }

  // TODO: implement toMicroBatchStream() so spark.readStream...loadChangelog(...) can drive a
  // streaming CDC read through this Changelog. The existing streaming-CDC entrypoint
  // (SparkMicroBatchStream + readChangeFeed=true) provides the wire format today, but it bypasses
  // the catalog-driven Changelog API that SPARK-56687 (PR apache/spark#55637) builds on for
  // streaming netChanges post-processing.
}
