package io.delta.spark.internal.v2.read.changelog;

import io.delta.kernel.CommitRange;
import org.apache.spark.sql.delta.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import org.apache.spark.sql.delta.RowTracking$;
import io.delta.spark.internal.v2.catalog.DeltaV2Table;
import io.delta.spark.internal.v2.kernel.KernelEngineFactory;
import org.apache.spark.sql.delta.v2.interop.DeltaV2Snapshot$;
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager;
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
class DeltaV2ChangelogScanBuilder implements ScanBuilder {

  private final DeltaV2Table deltaV2Table;
  private final long startVersion;
  private final long endVersion;
  private final CaseInsensitiveStringMap options;

  DeltaV2ChangelogScanBuilder(
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
    Engine engine = KernelEngineFactory.createDefaultEngine(hadoopConf);
    DeltaV2SnapshotManager snapshotManager = deltaV2Table.getSnapshotManager();
    CommitRange commitRange =
        snapshotManager.getTableChanges(engine, startVersion, Optional.of(endVersion));
    // Boundary checks: both endpoints must already carry the schema + RT state that
    // DeltaV2ChangelogBatch will validate each in-range Metadata action against. Without these,
    // an RT-disabled boundary with no in-range toggle commit would surface as a raw
    // IllegalStateException "missing baseRowId" downstream.
    //
    // Order matters: check the end snapshot first. If RT is disabled at the latest
    // boundary, the table never had RT (Delta protocol forbids disabling RT once
    // enabled), so emit DELTA_CHANGELOG_REQUIRES_ROW_TRACKING. Only if the end has RT
    // but the start does not, the toggle happened within the range -- emit
    // DELTA_CHANGELOG_ROW_TRACKING_DISABLED_IN_RANGE with the offending start version.
    Snapshot startSnapshot = snapshotManager.loadSnapshotAt(startVersion);
    SnapshotImpl startSnapshotImpl = DeltaV2Snapshot$.MODULE$.getKernelSnapshot(startSnapshot);
    Snapshot endSnapshot = snapshotManager.loadSnapshotAt(endVersion);
    StructType endSchema = endSnapshot.schema();
    if (!RowTracking$.MODULE$.isEnabled(endSnapshot.protocol(), endSnapshot.metadata())) {
      DeltaErrors.throwChangelogRequiresRowTracking(deltaV2Table.name());
    }
    if (!RowTracking$.MODULE$.isEnabled(startSnapshot.protocol(), startSnapshot.metadata())) {
      DeltaErrors.throwChangelogRowTrackingDisabledInRange(startVersion);
    }

    StructType cdcSchema =
        endSchema
            .add(DeltaV2Changelog.METADATA_COLUMN, DeltaV2Changelog.METADATA_STRUCT, false)
            .add("_change_type", DataTypes.StringType, false)
            .add("_commit_version", DataTypes.LongType, false)
            .add("_commit_timestamp", DataTypes.TimestampType, false);
    return new DeltaV2ChangelogScan(
        cdcSchema,
        commitRange,
        engine,
        endSchema,
        startSnapshotImpl,
        startVersion,
        endVersion,
        hadoopConf);
  }
}
