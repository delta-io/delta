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

import static java.util.Collections.singletonMap;

import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/** A collection of helper methods for working with materialized row tracking columns. */
public final class MaterializedRowTrackingColumn {

  private final String materializedColumnNameProperty;
  private final String materializedColumnNamePrefix;

  private MaterializedRowTrackingColumn(String property, String prefix) {
    this.materializedColumnNameProperty = property;
    this.materializedColumnNamePrefix = prefix;
  }

  /** Static instance for the materialized row ID column. */
  public static final MaterializedRowTrackingColumn ROW_ID =
      new MaterializedRowTrackingColumn(
          "delta.rowTracking.materializedRowIdColumnName", "_row-id-col-");

  /** Static instance for the materialized row commit version column. */
  public static final MaterializedRowTrackingColumn ROW_COMMIT_VERSION =
      new MaterializedRowTrackingColumn(
          "delta.rowTracking.materializedRowCommitVersionColumnName", "_row-commit-version-col-");

  /** Returns the configuration property name associated with this materialized column. */
  public String getMaterializedColumnNameProperty() {
    return materializedColumnNameProperty;
  }

  /** Returns the prefix to use for generating the materialized column name. */
  public String getMaterializedColumnNamePrefix() {
    return materializedColumnNamePrefix;
  }

  /** Generates a random name by concatenating the prefix with a random UUID. */
  private String generateMaterializedColumnName() {
    return materializedColumnNamePrefix + UUID.randomUUID().toString();
  }

  /**
   * Assigns a materialized row tracking column name to the metadata configuration if row tracking
   * is enabled and the column name has not been assigned yet.
   *
   * @param metadata The current Metadata of the table.
   * @return An Optional containing updated metadata if the column name was assigned;
   *     Optional.empty() otherwise.
   */
  public Optional<Metadata> assignMaterializedColumnNameIfNeeded(Metadata metadata) {
    boolean isRowTrackingEnabled = TableConfig.ROW_TRACKING_ENABLED.fromMetadata(metadata);
    boolean isMaterializedColumnNameAssigned =
        metadata.getConfiguration().containsKey(materializedColumnNameProperty);

    if (isRowTrackingEnabled && !isMaterializedColumnNameAssigned) {
      return Optional.of(
          metadata.withMergedConfiguration(
              singletonMap(materializedColumnNameProperty, generateMaterializedColumnName())));
    } else {
      return Optional.empty();
    }
  }

  /**
   * Validates that the materialized column name does not conflict with any existing column names in
   * the schema.
   *
   * @param metadata The current Metadata of the table.
   */
  public void throwIfColumnNameConflictsWithSchema(Metadata metadata) {
    StructType schema = metadata.getSchema();
    Set<String> logicalColNames =
        schema.fields().stream().map(StructField::getName).collect(Collectors.toSet());
    Set<String> physicalColNames =
        schema.fields().stream().map(ColumnMapping::getPhysicalName).collect(Collectors.toSet());

    Optional.ofNullable(metadata.getConfiguration().get(materializedColumnNameProperty))
        .ifPresent(
            columnName -> {
              if (logicalColNames.contains(columnName) || physicalColNames.contains(columnName)) {
                throw DeltaErrors.conflictWithReservedInternalColumnName(columnName);
              }
            });
  }
}
