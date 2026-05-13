package io.delta.spark.internal.v2.read.changelog;

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class DeltaChangelogScanBuilder implements ScanBuilder {

  private final DeltaSnapshotManager snapshotManager;
  private final StructType dataSchema;
  private final long startVersion;
  private final long endVersion;
  private final Snapshot startSnapshot;
  private final boolean rowTrackingEnabled;
  private final CaseInsensitiveStringMap options;

  public DeltaChangelogScanBuilder(
      DeltaSnapshotManager snapshotManager,
      StructType dataSchema,
      long startVersion,
      long endVersion,
      Snapshot startSnapshot,
      boolean rowTrackingEnabled,
      CaseInsensitiveStringMap options) {
    this.snapshotManager = snapshotManager;
    this.dataSchema = dataSchema;
    this.startVersion = startVersion;
    this.endVersion = endVersion;
    this.startSnapshot = startSnapshot;
    this.rowTrackingEnabled = rowTrackingEnabled;
    this.options = options;
  }

  @Override
  public Scan build() {
    Configuration hadoopConf =
        Objects.requireNonNull(
            SparkSession.active().sparkContext().hadoopConfiguration(), "hadoopConf is null");
    Engine engine = DefaultEngine.create(hadoopConf);
    CommitRange commitRange =
        snapshotManager.getTableChanges(engine, startVersion, Optional.of(endVersion));

    StructType cdcSchema = dataSchema;
    if (rowTrackingEnabled) {
      cdcSchema =
          cdcSchema.add(DeltaChangelog.METADATA_COLUMN, DeltaChangelog.METADATA_STRUCT, false);
    }
    cdcSchema =
        cdcSchema
            .add("_change_type", DataTypes.StringType, false)
            .add("_commit_version", DataTypes.LongType, false)
            .add("_commit_timestamp", DataTypes.TimestampType, false);
    return new DeltaChangelogScan(
        cdcSchema, commitRange, engine, dataSchema, startSnapshot, rowTrackingEnabled, hadoopConf);
  }
}
