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
package io.delta.kernel.spark.utils;

import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;

/**
 * Utility methods for row tracking in Kernel-Spark connector.
 *
 * <p>This class provides row tracking functionality with Spark-specific metadata attributes for
 * marking metadata columns as constant or generated fields.
 */
public class RowTrackingUtils {

  // Metadata keys for row tracking fields (Spark-specific implementation)
  private static final String ROW_TRACKING_METADATA_TYPE_KEY = "__metadata_type";
  private static final String METADATA_TYPE_CONSTANT = "constant";
  private static final String METADATA_TYPE_GENERATED = "generated";
  private static final String BASE_ROW_ID_METADATA_COL_ATTR_KEY = "__base_row_id_metadata_col";
  private static final String DEFAULT_ROW_COMMIT_VERSION_METADATA_COL_ATTR_KEY =
      "__default_row_version_metadata_col";
  private static final String ROW_ID_METADATA_COL_ATTR_KEY = "__row_id_metadata_col";
  private static final String ROW_COMMIT_VERSION_METADATA_COL_ATTR_KEY =
      "__row_commit_version_metadata_col";

  /**
   * Check if row tracking is enabled for reading.
   *
   * @param protocol the protocol to check
   * @param metadata the metadata to check
   * @return true if row tracking is enabled
   * @throws IllegalStateException if row tracking is enabled in metadata but not supported by
   *     protocol
   */
  public static boolean isEnabled(Protocol protocol, Metadata metadata) {
    boolean isEnabled = TableConfig.ROW_TRACKING_ENABLED.fromMetadata(metadata);
    if (isEnabled && !TableFeatures.isRowTrackingSupported(protocol)) {
      throw new IllegalStateException(
          "Table property 'delta.enableRowTracking' is set on the table but this table version "
              + "doesn't support the 'rowTracking' table feature.");
    }
    return isEnabled;
  }

  /**
   * Create the row tracking metadata struct fields for reading. This combines all row tracking
   * fields (both constant and generated) with Spark-specific metadata attributes.
   *
   * <p>Returns a list containing four fields:
   *
   * <ul>
   *   <li>base_row_id (constant field)
   *   <li>default_row_commit_version (constant field)
   *   <li>row_id (generated field)
   *   <li>row_commit_version (generated field)
   * </ul>
   *
   * <p>Each field includes Spark-specific metadata attributes marking it as constant or generated.
   *
   * @param protocol the protocol
   * @param metadata the metadata
   * @param nullableConstantFields whether constant fields should be nullable
   * @param nullableGeneratedFields whether generated fields should be nullable
   * @return list of struct fields for row tracking metadata, or empty list if row tracking is not
   *     enabled
   */
  public static List<StructField> createMetadataStructFields(
      Protocol protocol,
      Metadata metadata,
      boolean nullableConstantFields,
      boolean nullableGeneratedFields) {
    if (!isEnabled(protocol, metadata)) {
      return new ArrayList<>();
    }

    List<StructField> fields = new ArrayList<>();

    // Add base_row_id (constant field)
    fields.add(
        new StructField(
            "base_row_id",
            DataTypes.LongType,
            nullableConstantFields,
            createConstantFieldMetadata(BASE_ROW_ID_METADATA_COL_ATTR_KEY)));

    // Add default_row_commit_version (constant field)
    fields.add(
        new StructField(
            "default_row_commit_version",
            DataTypes.LongType,
            nullableConstantFields,
            createConstantFieldMetadata(DEFAULT_ROW_COMMIT_VERSION_METADATA_COL_ATTR_KEY)));

    // Add row_id (generated field)
    fields.add(
        new StructField(
            "row_id",
            DataTypes.LongType,
            nullableGeneratedFields,
            createGeneratedFieldMetadata(ROW_ID_METADATA_COL_ATTR_KEY)));

    // Add row_commit_version (generated field)
    fields.add(
        new StructField(
            "row_commit_version",
            DataTypes.LongType,
            nullableGeneratedFields,
            createGeneratedFieldMetadata(ROW_COMMIT_VERSION_METADATA_COL_ATTR_KEY)));

    return fields;
  }

  /**
   * Create metadata for constant row tracking fields (physically stored in files).
   *
   * @param attrKey the specific attribute key for this field
   * @return Spark Metadata marking this as a constant field
   */
  private static org.apache.spark.sql.types.Metadata createConstantFieldMetadata(String attrKey) {
    return new MetadataBuilder()
        .putString(ROW_TRACKING_METADATA_TYPE_KEY, METADATA_TYPE_CONSTANT)
        .putBoolean(attrKey, true)
        .build();
  }

  /**
   * Create metadata for generated row tracking fields (computed at read time).
   *
   * @param attrKey the specific attribute key for this field
   * @return Spark Metadata marking this as a generated field
   */
  private static org.apache.spark.sql.types.Metadata createGeneratedFieldMetadata(String attrKey) {
    return new MetadataBuilder()
        .putString(ROW_TRACKING_METADATA_TYPE_KEY, METADATA_TYPE_GENERATED)
        .putBoolean(attrKey, true)
        .build();
  }
}
