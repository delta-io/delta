/*
 * Copyright (2025) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.internal.rowtracking;

import static io.delta.kernel.internal.rowtracking.RowTracking.*;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.ExpressionHandler;
import io.delta.kernel.exceptions.InvalidTableException;
import io.delta.kernel.expressions.*;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.SchemaUtils;
import io.delta.kernel.types.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A collection of helper methods for working with materialized row tracking columns. */
public final class MaterializedRowTrackingColumn {

  /** Static instance for the materialized row ID column. */
  public static final MaterializedRowTrackingColumn MATERIALIZED_ROW_ID =
      new MaterializedRowTrackingColumn(
          TableConfig.MATERIALIZED_ROW_ID_COLUMN_NAME, "_row-id-col-");

  /** Static instance for the materialized row commit version column. */
  public static final MaterializedRowTrackingColumn MATERIALIZED_ROW_COMMIT_VERSION =
      new MaterializedRowTrackingColumn(
          TableConfig.MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME, "_row-commit-version-col-");

  private final TableConfig<String> tableConfig;
  private final String materializedColumnNamePrefix;

  /** Private constructor to enforce the use of static instances. */
  private MaterializedRowTrackingColumn(TableConfig<String> tableConfig, String prefix) {
    this.tableConfig = tableConfig;
    this.materializedColumnNamePrefix = prefix;
  }

  /** Returns the configuration property name associated with this materialized column. */
  public String getMaterializedColumnNameProperty() {
    return tableConfig.getKey();
  }

  /** Returns the prefix to use for generating the materialized column name. */
  public String getMaterializedColumnNamePrefix() {
    return materializedColumnNamePrefix;
  }

  /**
   * Validates that the materialized column names for ROW_ID and ROW_COMMIT_VERSION do not conflict
   * with any existing logical or physical column names in the table's schema.
   *
   * @param metadata The current {@link Metadata} of the table.
   */
  public static void throwIfColumnNamesConflictWithSchema(Metadata metadata) {
    StructType schema = metadata.getSchema();
    Set<String> logicalColNames =
        schema.fields().stream().map(StructField::getName).collect(Collectors.toSet());
    Set<String> physicalColNames =
        schema.fields().stream().map(ColumnMapping::getPhysicalName).collect(Collectors.toSet());

    Stream.of(MATERIALIZED_ROW_ID, MATERIALIZED_ROW_COMMIT_VERSION)
        .map(column -> metadata.getConfiguration().get(column.getMaterializedColumnNameProperty()))
        .filter(Objects::nonNull)
        .forEach(
            columnName -> {
              if (logicalColNames.contains(columnName) || physicalColNames.contains(columnName)) {
                throw DeltaErrors.conflictWithReservedInternalColumnName(columnName);
              }
            });
  }

  /**
   * Validates that materialized column names for ROW_ID and ROW_COMMIT_VERSION are not missing when
   * row tracking is enabled. This should be called for existing tables to ensure that row tracking
   * configs are present when they should be.
   *
   * @param metadata The current {@link Metadata} of the table.
   */
  public static void validateRowTrackingConfigsNotMissing(Metadata metadata, String tablePath) {
    // No validation needed if row tracking is disabled
    if (!TableConfig.ROW_TRACKING_ENABLED.fromMetadata(metadata)) {
      return;
    }

    // Check that both materialized column name configs are present when row tracking is enabled
    Stream.of(MATERIALIZED_ROW_ID, MATERIALIZED_ROW_COMMIT_VERSION)
        .forEach(
            column -> {
              if (!metadata
                  .getConfiguration()
                  .containsKey(column.getMaterializedColumnNameProperty())) {
                throw new InvalidTableException(
                    tablePath,
                    String.format(
                        "Row tracking is enabled but the materialized column name `%s` is missing.",
                        column.getMaterializedColumnNameProperty()));
              }
            });
  }

  /**
   * Assigns materialized column names for ROW_ID and ROW_COMMIT_VERSION if row tracking is enabled
   * and the column names have not been assigned yet.
   *
   * @param metadata The current Metadata of the table.
   * @return An Optional containing updated metadata if any assignments occurred; Optional.empty()
   *     otherwise.
   */
  public static Optional<Metadata> assignMaterializedColumnNamesIfNeeded(Metadata metadata) {
    // No assignment if row tracking is disabled
    if (!TableConfig.ROW_TRACKING_ENABLED.fromMetadata(metadata)) {
      return Optional.empty();
    }

    Map<String, String> configsToAdd = new HashMap<>();

    Stream.of(MATERIALIZED_ROW_ID, MATERIALIZED_ROW_COMMIT_VERSION)
        .filter(
            column ->
                !metadata
                    .getConfiguration()
                    .containsKey(column.getMaterializedColumnNameProperty()))
        .forEach(
            column -> {
              configsToAdd.put(
                  column.getMaterializedColumnNameProperty(),
                  column.generateMaterializedColumnName());
            });

    return configsToAdd.isEmpty()
        ? Optional.empty()
        : Optional.of(metadata.withMergedConfiguration(configsToAdd));
  }

  /**
   * Converts a logical row tracking field to its physical counterpart(s).
   *
   * <p>Since computing the row ID requires the row index, requesting a row tracking column can
   * require adding two columns to the physical schema.
   *
   * <p>Note that we must not mark the physical columns as metadata columns because as far as the
   * parquet reader is concerned, these columns are not metadata columns.
   *
   * @param logicalField The logical field to convert.
   * @param logicalSchema The logical schema containing the field.
   * @param metadata The current metadata of the table.
   * @return A list of {@link StructField}s representing the physical columns corresponding to the
   *     logical field.
   */
  public static List<StructField> convertToPhysicalColumn(
      StructField logicalField, StructType logicalSchema, Metadata metadata) {
    if (!TableConfig.ROW_TRACKING_ENABLED.fromMetadata(metadata)) {
      throw DeltaErrors.missingRowTrackingColumnRequested(logicalField.getName());
    }

    if (logicalField.getMetadataColumnType() == MetadataColumnType.ROW_ID) {
      List<StructField> physicalFields = new ArrayList<>(2);
      physicalFields.add(
          new StructField(
              MATERIALIZED_ROW_ID.getPhysicalColumnName(metadata.getConfiguration()),
              LongType.LONG,
              true /* nullable */));
      if (!logicalSchema.contains(MetadataColumnType.ROW_INDEX)) {
        physicalFields.add(
            SchemaUtils.createInternalColumn(
                StructField.createMetadataColumn(
                    StructField.DEFAULT_ROW_INDEX_COLUMN_NAME, MetadataColumnType.ROW_INDEX)));
      }
      return physicalFields;
    } else if (logicalField.getMetadataColumnType() == MetadataColumnType.ROW_COMMIT_VERSION) {
      return Collections.singletonList(
          new StructField(
              MATERIALIZED_ROW_COMMIT_VERSION.getPhysicalColumnName(metadata.getConfiguration()),
              LongType.LONG,
              true /* nullable */));
    }

    throw new IllegalArgumentException(
        String.format(
            "Logical field `%s` is not a recognized materialized row tracking column.",
            logicalField.getName()));
  }

  /**
   * Computes row IDs and row commit versions based on their materialized values if present in the
   * data returned by the Parquet reader, using the base row ID and default row commit version from
   * the AddFile otherwise.
   *
   * @param dataBatch a batch of physical data read from the table.
   * @param scanFile the {@link Row} representing the scan file metadata.
   * @param logicalSchema the logical schema of the query.
   * @param configuration the configuration map containing table metadata.
   * @param engine the {@link Engine} to use for expression evaluation.
   * @return a new {@link ColumnarBatch} with logical row tracking columns
   */
  public static ColumnarBatch transformPhysicalData(
      ColumnarBatch dataBatch,
      Row scanFile,
      StructType logicalSchema,
      Map<String, String> configuration,
      Engine engine) {
    StructType physicalSchema = dataBatch.getSchema();
    ExpressionHandler exprHandler = engine.getExpressionHandler();

    // NOTE: We assume that each column is requested at most once in the read schema. This is
    // consistent with other parts of the codebase.
    String rowIdColumnName = MATERIALIZED_ROW_ID.getPhysicalColumnName(configuration);
    int rowIdOrdinal = physicalSchema.indexOf(rowIdColumnName);
    if (rowIdOrdinal != -1) {
      dataBatch =
          transformPhysicalRowId(
              dataBatch,
              scanFile,
              rowIdColumnName,
              exprHandler,
              logicalSchema,
              physicalSchema,
              rowIdOrdinal);
    }

    String rowCommitVersionColumnName =
        MATERIALIZED_ROW_COMMIT_VERSION.getPhysicalColumnName(configuration);
    int commitVersionOrdinal = physicalSchema.indexOf(rowCommitVersionColumnName);
    if (commitVersionOrdinal != -1) {
      dataBatch =
          transformPhysicalCommitVersion(
              dataBatch,
              scanFile,
              rowCommitVersionColumnName,
              exprHandler,
              logicalSchema,
              physicalSchema,
              commitVersionOrdinal);
    }

    return dataBatch;
  }

  /** Row IDs are computed as <code>COALESCE(materializedRowId, baseRowId + rowIndex)</code>. */
  private static ColumnarBatch transformPhysicalRowId(
      ColumnarBatch dataBatch,
      Row scanFile,
      String rowIdColumnName,
      ExpressionHandler exprHandler,
      StructType logicalSchema,
      StructType physicalSchema,
      int rowIdOrdinal) {
    long baseRowId =
        InternalScanFileUtils.getBaseRowId(scanFile)
            .orElseThrow(
                () ->
                    DeltaErrors.rowTrackingMetadataMissingInFile(
                        "baseRowId", InternalScanFileUtils.getFilePath(scanFile)));
    Expression rowIdExpr =
        new ScalarExpression(
            "COALESCE",
            Arrays.asList(
                new Column(rowIdColumnName),
                new ScalarExpression(
                    "ADD",
                    Arrays.asList(
                        new Column(
                            dataBatch
                                .getSchema()
                                .at(dataBatch.getSchema().indexOf(MetadataColumnType.ROW_INDEX))
                                .getName()),
                        Literal.ofLong(baseRowId)))));
    ColumnVector rowIdVector =
        exprHandler.getEvaluator(physicalSchema, rowIdExpr, LongType.LONG).eval(dataBatch);

    // Remove the materialized row ID column and replace it with the coalesced vector
    dataBatch =
        dataBatch
            .withDeletedColumnAt(rowIdOrdinal)
            .withNewColumn(
                rowIdOrdinal,
                logicalSchema.at(logicalSchema.indexOf(MetadataColumnType.ROW_ID)),
                rowIdVector);
    return dataBatch;
  }

  /**
   * Row commit versions are computed as <code>COALESCE(materializedRowCommitVersion,
   * defaultRowCommitVersion)</code>.
   */
  private static ColumnarBatch transformPhysicalCommitVersion(
      ColumnarBatch dataBatch,
      Row scanFile,
      String commitVersionColumnName,
      ExpressionHandler exprHandler,
      StructType logicalSchema,
      StructType physicalSchema,
      int commitVersionOrdinal) {
    long defaultRowCommitVersion =
        InternalScanFileUtils.getDefaultRowCommitVersion(scanFile)
            .orElseThrow(
                () ->
                    DeltaErrors.rowTrackingMetadataMissingInFile(
                        "defaultRowCommitVersion", InternalScanFileUtils.getFilePath(scanFile)));
    Expression commitVersionExpr =
        new ScalarExpression(
            "COALESCE",
            Arrays.asList(
                new Column(commitVersionColumnName), Literal.ofLong(defaultRowCommitVersion)));
    ColumnVector commitVersionVector =
        exprHandler.getEvaluator(physicalSchema, commitVersionExpr, LongType.LONG).eval(dataBatch);

    // Remove the materialized row commit version column and replace it with the coalesced vector
    dataBatch =
        dataBatch
            .withDeletedColumnAt(commitVersionOrdinal)
            .withNewColumn(
                commitVersionOrdinal,
                logicalSchema.at(logicalSchema.indexOf(MetadataColumnType.ROW_COMMIT_VERSION)),
                commitVersionVector);
    return dataBatch;
  }

  private String getPhysicalColumnName(Map<String, String> configuration) {
    return Optional.ofNullable(configuration.get(getMaterializedColumnNameProperty()))
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format(
                        "Materialized column name `%s` is missing in the metadata config: %s",
                        getMaterializedColumnNameProperty(), configuration)));
  }

  /** Generates a random name by concatenating the prefix with a random UUID. */
  private String generateMaterializedColumnName() {
    return materializedColumnNamePrefix + UUID.randomUUID().toString();
  }
}
