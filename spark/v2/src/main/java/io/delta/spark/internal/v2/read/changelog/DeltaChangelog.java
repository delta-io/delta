package io.delta.spark.internal.v2.read.changelog;

import io.delta.spark.internal.v2.catalog.SparkTable;
import org.apache.spark.sql.connector.catalog.CatalogV2Util;
import org.apache.spark.sql.connector.catalog.Changelog;
import org.apache.spark.sql.connector.catalog.ChangelogInfo;
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
 * <p>Wraps the {@link SparkTable} resolved by {@code TableCatalog.loadTable(ident)} and the {@link
 * ChangelogInfo} that captures the user's CDC query options (deduplication mode, update detection,
 * the resolved version range). All connector-level work — snapshot loads, row tracking validation,
 * metadata-action inspection across the range — is deferred to the read path inside {@link
 * DeltaChangelogBatch}, so that constructing a DeltaChangelog from the catalog stays cheap and
 * side-effect free.
 *
 * <p>Row tracking is required at the table protocol; without it the SPIP analyzer rule cannot
 * partition by {@code rowId / rowVersion}. Validation is performed by the read path, not here.
 */
public class DeltaChangelog implements Changelog {

  private final String tableName;
  private final SparkTable sparkTable;
  private final ChangelogInfo changelogInfo;
  private final long startVersion;
  private final long endVersion;

  public static final String METADATA_COLUMN = "_metadata";
  public static final String ROW_ID_FIELD = "row_id";
  public static final String ROW_COMMIT_VERSION_FIELD = "row_commit_version";
  public static final StructType METADATA_STRUCT =
      new StructType()
          .add(ROW_ID_FIELD, DataTypes.LongType, false)
          .add(ROW_COMMIT_VERSION_FIELD, DataTypes.LongType, false);

  public DeltaChangelog(
      String tableName,
      SparkTable sparkTable,
      ChangelogInfo changelogInfo,
      long startVersion,
      long endVersion) {
    this.tableName = tableName;
    this.sparkTable = sparkTable;
    this.changelogInfo = changelogInfo;
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
        sparkTable
            .schema()
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
    return new DeltaChangelogScanBuilder(sparkTable, startVersion, endVersion, options);
  }

  @Override
  public NamedReference[] rowId() {
    return new NamedReference[] {FieldReference.apply("_metadata.row_id")};
  }

  @Override
  public NamedReference rowVersion() {
    return FieldReference.apply("_metadata." + ROW_COMMIT_VERSION_FIELD);
  }

  /** Exposes the captured changelog options for downstream readers. */
  public ChangelogInfo getChangelogInfo() {
    return changelogInfo;
  }
}
