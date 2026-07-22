package io.delta.spark.internal.v2.read.changelog;

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
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

/**
 * Package-private scan builder for Delta's V2 changelog read path.
 *
 * <p>Package privacy prevents callers from coupling to Delta's internal V2 implementation.
 */
class DeltaV2ChangeLogScanBuilder implements ScanBuilder {

  private final DeltaV2Table deltaV2Table;
  private final long startVersion;
  private final long endVersion;
  private final CaseInsensitiveStringMap options;

  DeltaV2ChangeLogScanBuilder(
      DeltaV2Table deltaV2Table,
      long startVersion,
      long endVersion,
      CaseInsensitiveStringMap options) {
    this.deltaV2Table = deltaV2Table;
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
    DeltaSnapshotManager snapshotManager = deltaV2Table.getSnapshotManager();
    CommitRange commitRange =
        snapshotManager.getTableChanges(engine, startVersion, Optional.of(endVersion));
    // Boundary checks: both endpoints must already carry the schema + RT state that
    // DeltaV2ChangeLogBatch will validate each in-range Metadata action against. Without these,
    // an RT-disabled boundary with no in-range toggle commit would surface as a raw
    // IllegalStateException "missing baseRowId" downstream.
    //
    // Order matters: check the end snapshot first. If RT is disabled at the latest
    // boundary, the table never had RT (Delta protocol forbids disabling RT once
    // enabled), so emit DELTA_CHANGELOG_REQUIRES_ROW_TRACKING. Only if the end has RT
    // but the start does not, the toggle happened within the range -- emit
    // DELTA_CHANGELOG_ROW_TRACKING_DISABLED_IN_RANGE with the offending start version.
    Snapshot startSnapshot = snapshotManager.loadSnapshotAt(startVersion);
    SnapshotImpl startSnapshotImpl = (SnapshotImpl) startSnapshot;
    Snapshot endSnapshot = snapshotManager.loadSnapshotAt(endVersion);
    SnapshotImpl endSnapshotImpl = (SnapshotImpl) endSnapshot;
    StructType endSchema = SchemaUtils.convertKernelSchemaToSparkSchema(endSnapshot.getSchema());
    if (!RowTracking.isEnabled(endSnapshotImpl.getProtocol(), endSnapshotImpl.getMetadata())) {
      DeltaErrors.throwChangelogRequiresRowTracking(deltaV2Table.name());
    }
    if (!RowTracking.isEnabled(startSnapshotImpl.getProtocol(), startSnapshotImpl.getMetadata())) {
      DeltaErrors.throwChangelogRowTrackingDisabledInRange(startVersion);
    }

    StructType cdcSchema =
        endSchema
            .add(DeltaV2ChangeLog.METADATA_COLUMN, DeltaV2ChangeLog.METADATA_STRUCT, false)
            .add("_change_type", DataTypes.StringType, false)
            .add("_commit_version", DataTypes.LongType, false)
            .add("_commit_timestamp", DataTypes.TimestampType, false);
    return new DeltaV2ChangeLogScan(
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
