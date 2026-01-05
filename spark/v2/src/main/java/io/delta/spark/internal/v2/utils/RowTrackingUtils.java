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
package io.delta.spark.internal.v2.utils;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.rowtracking.MaterializedRowTrackingColumn;
import io.delta.kernel.internal.rowtracking.RowTracking;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.catalyst.expressions.FileSourceConstantMetadataStructField;
import org.apache.spark.sql.catalyst.expressions.FileSourceGeneratedMetadataStructField;
import org.apache.spark.sql.delta.DeltaIllegalStateException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;

/**
 * Utility methods for row tracking in Kernel based connector. This class provides row tracking
 * functionality with Spark-specific metadata attributes for marking metadata columns as constant or
 * generated fields.
 */
public class RowTrackingUtils {

  // Field names for row tracking metadata columns (matching Spark V1 definitions)
  private static final String BASE_ROW_ID = "base_row_id";
  private static final String ROW_ID = "row_id";
  private static final String DEFAULT_ROW_COMMIT_VERSION = "default_row_commit_version";
  private static final String ROW_COMMIT_VERSION = "row_commit_version";

  // Metadata keys for row tracking metadata fields
  private static final String BASE_ROW_ID_METADATA_COL_ATTR_KEY = "__base_row_id_metadata_col";
  private static final String DEFAULT_ROW_COMMIT_VERSION_METADATA_COL_ATTR_KEY =
      "__default_row_version_metadata_col";
  private static final String ROW_ID_METADATA_COL_ATTR_KEY = "__row_id_metadata_col";
  private static final String ROW_COMMIT_VERSION_METADATA_COL_ATTR_KEY =
      "__row_commit_version_metadata_col";

  private RowTrackingUtils() {}

  /**
   * Create the row tracking metadata struct fields for reading.
   *
   * <p>The order and presence of fields matches Spark V1 implementation:
   *
   * <ul>
   *   <li>row_id (generated field, always present when row tracking is enabled)
   *   <li>base_row_id (constant field, always present when row tracking is enabled)
   *   <li>default_row_commit_version (constant field, always present when row tracking is enabled)
   *   <li>row_commit_version (generated field, always present when row tracking is enabled)
   * </ul>
   *
   * @param protocol the protocol
   * @param metadata the metadata
   * @param nullableConstantFields whether constant fields should be nullable
   * @param nullableGeneratedFields whether generated fields should be nullable
   * @return list of struct fields for row tracking metadata, or empty list if row tracking is not
   *     enabled
   * @throws DeltaIllegalStateException if row tracking is enabled but materialized column names are
   *     missing
   */
  public static List<StructField> createMetadataStructFields(
      Protocol protocol,
      Metadata metadata,
      boolean nullableConstantFields,
      boolean nullableGeneratedFields) {
    if (!RowTracking.isEnabled(protocol, metadata)) {
      return new ArrayList<>();
    }

    List<StructField> fields = new ArrayList<>();

    // Add row_id (generated field) - will throw if materialized column name is not configured
    String rowIdPhysicalName =
        getPhysicalColumnNameOrThrow(MaterializedRowTrackingColumn.MATERIALIZED_ROW_ID, metadata);
    fields.add(
        new StructField(
            ROW_ID,
            DataTypes.LongType,
            nullableGeneratedFields,
            createGeneratedFieldMetadata(ROW_ID, rowIdPhysicalName, ROW_ID_METADATA_COL_ATTR_KEY)));

    // Add base_row_id (constant field)
    fields.add(
        new StructField(
            BASE_ROW_ID,
            DataTypes.LongType,
            nullableConstantFields,
            createConstantFieldMetadata(BASE_ROW_ID, BASE_ROW_ID_METADATA_COL_ATTR_KEY)));

    // Add default_row_commit_version (constant field)
    fields.add(
        new StructField(
            DEFAULT_ROW_COMMIT_VERSION,
            DataTypes.LongType,
            nullableConstantFields,
            createConstantFieldMetadata(
                DEFAULT_ROW_COMMIT_VERSION, DEFAULT_ROW_COMMIT_VERSION_METADATA_COL_ATTR_KEY)));

    // Add row_commit_version (generated field) - will throw if materialized column name is not
    // configured
    String rowCommitVersionPhysicalName =
        getPhysicalColumnNameOrThrow(
            MaterializedRowTrackingColumn.MATERIALIZED_ROW_COMMIT_VERSION, metadata);
    fields.add(
        new StructField(
            ROW_COMMIT_VERSION,
            DataTypes.LongType,
            nullableGeneratedFields,
            createGeneratedFieldMetadata(
                ROW_COMMIT_VERSION,
                rowCommitVersionPhysicalName,
                ROW_COMMIT_VERSION_METADATA_COL_ATTR_KEY)));

    return fields;
  }

  /**
   * Helper method to get physical column name from MaterializedRowTrackingColumn, converting kernel
   * IllegalArgumentException to Spark DeltaIllegalStateException. This matches the exception thrown
   * by Spark V1's MaterializedRowTrackingColumn.
   *
   * @param column the MaterializedRowTrackingColumn instance
   * @param metadata the table metadata
   * @return the physical column name
   * @throws DeltaIllegalStateException if the materialized column name is missing
   */
  private static String getPhysicalColumnNameOrThrow(
      MaterializedRowTrackingColumn column, Metadata metadata) {
    try {
      return column.getPhysicalColumnName(metadata.getConfiguration());
    } catch (IllegalArgumentException e) {
      // Convert kernel exception to Spark V1-compatible DeltaIllegalStateException
      // Use the same error class as Spark V1: DELTA_MATERIALIZED_ROW_TRACKING_COLUMN_NAME_MISSING
      String rowTrackingColumnType =
          column == MaterializedRowTrackingColumn.MATERIALIZED_ROW_ID
              ? "Row ID"
              : "Row Commit Version";
      throw new DeltaIllegalStateException(
          "DELTA_MATERIALIZED_ROW_TRACKING_COLUMN_NAME_MISSING",
          new String[] {rowTrackingColumnType, metadata.getId()},
          e);
    }
  }

  private static org.apache.spark.sql.types.Metadata createConstantFieldMetadata(
      String columnName, String attrKey) {
    return new MetadataBuilder()
        .withMetadata(FileSourceConstantMetadataStructField.metadata(columnName))
        .putBoolean(attrKey, true)
        .build();
  }

  private static org.apache.spark.sql.types.Metadata createGeneratedFieldMetadata(
      String readColumnName, String writeColumnName, String attrKey) {
    return new MetadataBuilder()
        .withMetadata(
            FileSourceGeneratedMetadataStructField.metadata(readColumnName, writeColumnName))
        .putBoolean(attrKey, true)
        .build();
  }
}
