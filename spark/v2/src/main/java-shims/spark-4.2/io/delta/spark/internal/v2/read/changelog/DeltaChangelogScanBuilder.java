package io.delta.spark.internal.v2.read.changelog;

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.rowtracking.RowTracking;
import io.delta.spark.internal.v2.catalog.DeltaV2Table;
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

  private final DeltaV2Table sparkTable;
  private final long startVersion;
  private final long endVersion;
  private final CaseInsensitiveStringMap options;

  public DeltaChangelogScanBuilder(
      DeltaV2Table sparkTable, long startVersion, long endVersion, CaseInsensitiveStringMap options) {
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
    // Boundary checks: both endpoints must already carry the schema + RT state that
    // DeltaChangelogBatch will validate each in-range Metadata action against. Without these,
    // an RT-disabled boundary with no in-range toggle commit would surface as a raw
    // IllegalStateException "missing baseRowId" downstream.
    //
    // Order matters: check the end snapshot first. If RT is disabled at the latest
    // boundary, the table never had RT (Delta protocol forbids disabling RT once
    // enabled), so emit DELTA_CHANGELOG_REQUIRES_ROW_TRACKING. Only if the end has RT
    // but the start does not, the toggle happened within the range -- emit
    // DELTA_CHANGELOG_ROW_TRACKING_DISABLED_IN_RANGE with the offending start version.
    Snapshot startSnapshot = snapshotManager.loadSnapshotAt(startVersion);
    Snapshot endSnapshot = snapshotManager.loadSnapshotAt(endVersion);
    StructType endSchema = SchemaUtils.convertKernelSchemaToSparkSchema(endSnapshot.getSchema());
    if (!RowTracking.isEnabled(endSnapshot.getProtocol(), endSnapshot.getMetadata())) {
      DeltaErrors.throwChangelogRequiresRowTracking(sparkTable.name());
    }
    if (!RowTracking.isEnabled(startSnapshot.getProtocol(), startSnapshot.getMetadata())) {
      DeltaErrors.throwChangelogRowTrackingDisabledInRange(startVersion);
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
