package io.delta.spark.internal.v2.read.changelog;

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.rowtracking.RowTracking;
import io.delta.spark.internal.v2.catalog.SparkTable;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.delta.DeltaErrors;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class DeltaChangelogScanBuilder implements ScanBuilder {

  private final SparkTable sparkTable;
  private final long startVersion;
  private final long endVersion;
  private final CaseInsensitiveStringMap options;

  public DeltaChangelogScanBuilder(
      SparkTable sparkTable, long startVersion, long endVersion, CaseInsensitiveStringMap options) {
    this.sparkTable = sparkTable;
    this.startVersion = startVersion;
    this.endVersion = endVersion;
    this.options = options;
  }

  @Override
  public Scan build() {
    Configuration hadoopConf =
        Objects.requireNonNull(
            SparkSession.active().sparkContext().hadoopConfiguration(), "hadoopConf is null");
    Engine engine = DefaultEngine.create(hadoopConf);
    DeltaSnapshotManager snapshotManager = sparkTable.getSnapshotManager();
    CommitRange commitRange =
        snapshotManager.getTableChanges(engine, startVersion, Optional.of(endVersion));
    Snapshot startSnapshot = snapshotManager.loadSnapshotAt(startVersion);

    // End-version reference: the schema and row-tracking state at endVersion define what
    // DeltaChangelogBatch validates each commit's Metadata action against. Catching schema
    // drift or row-tracking toggles here, at the read entry point, gives the user a clear
    // analysis-time error rather than a corrupted output stream downstream.
    Snapshot endSnapshot = snapshotManager.loadSnapshotAt(endVersion);
    SnapshotImpl endSnapshotImpl = (SnapshotImpl) endSnapshot;
    StructType endSchema = SchemaUtils.convertKernelSchemaToSparkSchema(endSnapshot.getSchema());
    if (!RowTracking.isEnabled(endSnapshotImpl.getProtocol(), endSnapshotImpl.getMetadata())) {
      DeltaErrors.throwChangelogRequiresRowTracking(sparkTable.name());
    }

    StructType cdcSchema =
        endSchema
            .add(DeltaChangelog.METADATA_COLUMN, DeltaChangelog.METADATA_STRUCT, false)
            .add("_change_type", DataTypes.StringType, false)
            .add("_commit_version", DataTypes.LongType, false)
            .add("_commit_timestamp", DataTypes.TimestampType, false);
    return new DeltaChangelogScan(
        cdcSchema,
        commitRange,
        engine,
        endSchema,
        startSnapshot,
        startVersion,
        endVersion,
        hadoopConf);
  }
}
