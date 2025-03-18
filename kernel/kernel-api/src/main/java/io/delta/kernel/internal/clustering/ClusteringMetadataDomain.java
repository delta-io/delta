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
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.metadatadomain.JsonMetadataDomain;
import io.delta.kernel.internal.util.ColumnMapping;
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
  public ClusteringMetadataDomain(List<Column> logicalClusteringColumns, StructType schema) {
    List<List<String>> physicalClusteringColumns = new ArrayList<>();
    for (Column logicalName : logicalClusteringColumns) {
      List<String> physicalColumnNames =
          ColumnMapping.convertToPhysicalColumnNames(schema, logicalName);
      physicalClusteringColumns.add(physicalColumnNames);
    }
    this.clusteringColumns = physicalClusteringColumns;
  }
}
