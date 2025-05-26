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

import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A collection of helper methods for working with materialized row tracking columns. */
public final class MaterializedRowTrackingColumn {

  /** Static instance for the materialized row ID column. */
  public static final MaterializedRowTrackingColumn ROW_ID =
      new MaterializedRowTrackingColumn(
          TableConfig.MATERIALIZED_ROW_ID_COLUMN_NAME, "_row-id-col-");

  /** Static instance for the materialized row commit version column. */
  public static final MaterializedRowTrackingColumn ROW_COMMIT_VERSION =
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

    Stream.of(ROW_ID, ROW_COMMIT_VERSION)
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

    Stream.of(ROW_ID, ROW_COMMIT_VERSION)
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

  /** Generates a random name by concatenating the prefix with a random UUID. */
  private String generateMaterializedColumnName() {
    return materializedColumnNamePrefix + UUID.randomUUID().toString();
  }
}
