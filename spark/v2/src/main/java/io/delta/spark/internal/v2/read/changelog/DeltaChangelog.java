package io.delta.spark.internal.v2.read.changelog;

import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import org.apache.spark.sql.connector.catalog.CatalogV2Util;
import org.apache.spark.sql.connector.catalog.Changelog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import io.delta.kernel.Snapshot;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.rowtracking.RowTracking;

public class DeltaChangelog implements Changelog {

  private final String tableName;
  private final StructType dataSchema;
  private final DeltaSnapshotManager snapshotManager;
  private final long startVersion;
  private final long endVersion;
  /** Cached snapshot at {@link #startVersion}, used by downstream Scan/Batch. */
  private final Snapshot startSnapshot;
  /** Whether row tracking was enabled in {@link #startSnapshot}. */
  private final boolean rowTrackingEnabled;

  public static final String METADATA_COLUMN = "_metadata";
  public static final String ROW_ID_FIELD = "row_id";
  public static final String ROW_COMMIT_VERSION_FIELD = "row_commit_version";
  public static final StructType METADATA_STRUCT =
      new StructType()
          .add(ROW_ID_FIELD, DataTypes.LongType, false)
          .add(ROW_COMMIT_VERSION_FIELD, DataTypes.LongType, false);


  public DeltaChangelog(
      String tableName,
      StructType dataSchema,
      DeltaSnapshotManager snapshotManager,
      long startVersion,
      long endVersion) {
    this.tableName = tableName;
    this.dataSchema = dataSchema;
    this.snapshotManager = snapshotManager;
    this.startVersion = startVersion;
    this.endVersion = endVersion;
    // Load the start-version snapshot once and cache the row-tracking flag. Downstream
    // ScanBuilder / Scan / Batch take both as constructor args so the snapshot is loaded
    // and the SnapshotImpl downcast happens exactly once.
    this.startSnapshot = snapshotManager.loadSnapshotAt(startVersion);
    SnapshotImpl snapshotImpl = (SnapshotImpl) startSnapshot;
    this.rowTrackingEnabled =
        RowTracking.isEnabled(snapshotImpl.getProtocol(), snapshotImpl.getMetadata());
  }

  @Override
  public String name() {
    return tableName + " (changes)";
  }

  @Override
  public Column[] columns() {
    StructType cdcSchema = dataSchema;
    if (rowTrackingEnabled) {
      cdcSchema = cdcSchema.add(METADATA_COLUMN, METADATA_STRUCT, false);
    }
    cdcSchema = cdcSchema
      .add("_change_type", DataTypes.StringType, false)
      .add("_commit_version", DataTypes.LongType, false)
      .add("_commit_timestamp", DataTypes.TimestampType, false);

    return CatalogV2Util.structTypeToV2Columns(cdcSchema);
  }

  // TODO: optimise to false when deletion vectors are guaranteed enabled across the entire
  // [startVersion, endVersion] range — DVs enabled over range produces no carry-overs.
  @Override
  public boolean containsCarryoverRows() {
    return true;
  }

  // TODO: optimise to false when the range is a single commit with no UPDATE/MERGE
  // operations. Requires inspecting the commit's operation type, questionable
  @Override
  public boolean containsIntermediateChanges() {
    return true;
  }

  // This V2 path only consumes AddFile/RemoveFile actions, so an UPDATE always
  // surfaces as a DELETE+INSERT pair sharing the same rowId. Spark derives the
  // pre/post-images via update detection.
  @Override
  public boolean representsUpdateAsDeleteAndInsert() {
    return true;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new DeltaChangelogScanBuilder(
        snapshotManager, dataSchema, startVersion, endVersion,
        startSnapshot, rowTrackingEnabled, options);
  }

  @Override
  public NamedReference[] rowId() {
    if (!rowTrackingEnabled) {
      return new NamedReference[0];
    }
    return new NamedReference[] {FieldReference.apply("_metadata.row_id")};
  }

  @Override
  public NamedReference rowVersion() {
    return FieldReference.apply("_metadata." + ROW_COMMIT_VERSION_FIELD);
  }
}
