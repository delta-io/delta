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
package io.delta.kernel.internal.clustering;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.delta.kernel.internal.metadatadomain.JsonMetadataDomain;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.*;

/** Represents the metadata domain for clustering. */
public final class ClusteringMetadataDomain extends JsonMetadataDomain {

  public static final String DOMAIN_NAME = "delta.clustering";

  /** The physical column names used for clustering */
  private final List<List<String>> clusteringColumns;

  @Override
  public String getDomainName() {
    return DOMAIN_NAME;
  }

  public List<List<String>> getClusteringColumns() {
    return clusteringColumns;
  }

  /**
   * Constructs a ClusteringMetadataDomain with the specified physical clustering columns.
   *
   * @param physicalClusteringColumns the physical columns used for clustering
   */
  @JsonCreator
  public ClusteringMetadataDomain(
      @JsonProperty("clusteringColumns") List<List<String>> physicalClusteringColumns) {
    this.clusteringColumns = physicalClusteringColumns;
  }

  /**
   * Constructs a ClusteringMetadataDomain with the specified logical clustering columns and schema.
   *
   * @param logicalClusteringColumns the logical columns used for clustering (e.g. "a.b.c" for a
   *     nested column "a.b.c")
   * @param schema the schema of the table
   */
  public ClusteringMetadataDomain(List<String> logicalClusteringColumns, StructType schema) {
    List<List<String>> physicalClusteringColumns = new ArrayList<>();
    for (String logicalName : logicalClusteringColumns) {
      physicalClusteringColumns.add(getPhysicalColumnNames(schema, logicalName));
    }
    this.clusteringColumns = physicalClusteringColumns;
  }

  /** Returns the physical column names for logical columns. */
  private List<String> getPhysicalColumnNames(StructType schema, String logicalName) {
    String[] logicalNameParts = logicalName.split("\\.");
    List<String> physicalNameParts = new ArrayList<>();
    DataType currentType = schema;

    // Traverse through each level of the logical name to resolve its corresponding physical name.
    for (String namePart : logicalNameParts) {
      if (!(currentType instanceof StructType)) {
        throw new IllegalArgumentException("Column not found in schema: " + logicalName);
      }

      StructType structType = (StructType) currentType;
      // Find the field in the current structure that matches the given name
      Optional<StructField> fieldOpt =
          structType.fields().stream()
              .filter(
                  field ->
                      field
                          .getName()
                          .toLowerCase(Locale.ROOT)
                          .equals(namePart.toLowerCase(Locale.ROOT)))
              .findFirst();

      if (!fieldOpt.isPresent()) {
        throw new IllegalArgumentException("Column not found in schema: " + logicalName);
      }

      StructField field = fieldOpt.get();
      // If the field itself is a struct, update currentType so we can look inside it in the next
      // iteration.
      currentType = field.getDataType();

      // Convert logical name to physical name using Delta Kernel's column mapping
      physicalNameParts.add(ColumnMapping.getPhysicalName(field));
    }
    return physicalNameParts;
  }
}
