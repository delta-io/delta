package io.delta.spark.internal.v2.read.changelog;

import io.delta.kernel.Snapshot;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import org.apache.spark.sql.connector.catalog.CatalogV2Util;
import org.apache.spark.sql.connector.catalog.Changelog;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * V2 Changelog implementation for Delta tables.
 *
 * <p>Row tracking must be enabled on the source table at {@code startVersion}; the catalog
 * (DeltaCatalogChangelogSupport) is responsible for validating this before constructing a
 * DeltaChangelog. With that invariant, {@link #rowId()} and {@link #rowVersion()} can always point
 * at the {@code _metadata.row_id} / {@code _metadata.row_commit_version} fields that the SPIP
 * analyzer rule expects for update detection, carry-over removal, and net-change computation.
 */
public class DeltaChangelog implements Changelog {

  private final String tableName;
  private final StructType dataSchema;
  private final DeltaSnapshotManager snapshotManager;
  private final long startVersion;
  private final long endVersion;
  /** Snapshot at {@link #startVersion}, supplied by the catalog after row-tracking validation. */
  private final Snapshot startSnapshot;

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
      Snapshot startSnapshot,
      long startVersion,
      long endVersion) {
    this.tableName = tableName;
    this.dataSchema = dataSchema;
    this.snapshotManager = snapshotManager;
    this.startSnapshot = startSnapshot;
    this.startVersion = startVersion;
    this.endVersion = endVersion;
  }

  @Override
  public String name() {
    return tableName + " (changes)";
  }

  @Override
  public Column[] columns() {
    StructType cdcSchema =
        dataSchema
            .add(METADATA_COLUMN, METADATA_STRUCT, false)
            .add("_change_type", DataTypes.StringType, false)
            .add("_commit_version", DataTypes.LongType, false)
            .add("_commit_timestamp", DataTypes.TimestampType, false);

    return CatalogV2Util.structTypeToV2Columns(cdcSchema);
  }

  // TODO: optimise to false when deletion vectors are guaranteed enabled across the entire
  // [startVersion, endVersion] range. DVs enabled over range produces no carry-overs.
  @Override
  public boolean containsCarryoverRows() {
    return true;
  }

  // TODO: optimise to false when the range is a single commit with no UPDATE/MERGE
  // operations. Requires inspecting the commit's operation type, questionable.
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
        snapshotManager, dataSchema, startVersion, endVersion, startSnapshot, options);
  }

  @Override
  public NamedReference[] rowId() {
    return new NamedReference[] {FieldReference.apply("_metadata.row_id")};
  }

  @Override
  public NamedReference rowVersion() {
    return FieldReference.apply("_metadata." + ROW_COMMIT_VERSION_FIELD);
  }
}
