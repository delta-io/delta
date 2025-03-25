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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.metadatadomain.JsonMetadataDomain;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.StructType;
import java.util.*;
import java.util.stream.Collectors;

/** Represents the metadata domain for clustering. */
public final class ClusteringMetadataDomain extends JsonMetadataDomain {

  public static final String DOMAIN_NAME = "delta.clustering";

  /** The physical column names used for clustering */
  private final List<Column> clusteringColumns;

  /**
   * Constructs a ClusteringMetadataDomain with the specified logical clustering columns and schema.
   *
   * @param logicalClusteringColumns the logical columns used for clustering
   * @param schema the schema of the table
   */
  public ClusteringMetadataDomain(List<Column> logicalClusteringColumns, StructType schema) {
    List<Column> physicalClusteringColumns = new ArrayList<>();
    for (Column logicalName : logicalClusteringColumns) {
      Column physicalColumnNames = ColumnMapping.convertToPhysicalColumnNames(schema, logicalName);
      physicalClusteringColumns.add(physicalColumnNames);
    }
    this.clusteringColumns = physicalClusteringColumns;
  }

  @Override
  public String getDomainName() {
    return DOMAIN_NAME;
  }

  /** @return the physical column names used for clustering */
  public List<Column> getClusteringColumns() {
    return clusteringColumns;
  }

  /**
   * Creates an instance of {@link ClusteringMetadataDomain} from a {@link SnapshotImpl}.
   *
   * @param snapshot the snapshot instance
   * @return an {@link Optional} containing the {@link ClusteringMetadataDomain} if present
   */
  public static Optional<ClusteringMetadataDomain> fromSnapshot(SnapshotImpl snapshot) {
    return JsonMetadataDomain.fromSnapshot(snapshot, ClusteringMetadataDomain.class, DOMAIN_NAME);
  }

  @Override
  public String toJsonConfiguration() {
    List<String[]> columnList =
        clusteringColumns.stream().map(Column::getNames).collect(Collectors.toList());

    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put("clusteringColumns", columnList);

    try {
      return OBJECT_MAPPER.writeValueAsString(jsonMap);
    } catch (JsonProcessingException e) {
      throw new KernelException(
          String.format(
              "Could not serialize %s (domain: %s) to JSON",
              this.getClass().getSimpleName(), getDomainName()),
          e);
    }
  }
}
