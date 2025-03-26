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
  private final List<List<String>> clusteringColumns;

  /**
   * Constructs a ClusteringMetadataDomain with the specified logical clustering columns and schema.
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
   * @param logicalClusteringColumns the logical columns used for clustering
   * @param schema the schema of the table
   */
  public ClusteringMetadataDomain(List<Column> logicalClusteringColumns, StructType schema) {
    this(convertToPhysicalClusteringColumns(logicalClusteringColumns, schema));
  }

  @Override
  public String getDomainName() {
    return DOMAIN_NAME;
  }

  public List<List<String>> getClusteringColumns() {
    return clusteringColumns;
  }

  /** @return the physical column names used for clustering. It would be in List[Column] type */
  public List<Column> fetchClusteringColumns() {
    return clusteringColumns.stream()
        .map(list -> new Column(list.toArray(new String[0])))
        .collect(Collectors.toList());
  }

  /**
   * Creates an instance of {@link ClusteringMetadataDomain} from a JSON configuration string.
   *
   * @param json the JSON configuration string
   * @return an instance of {@link ClusteringMetadataDomain}
   */
  public static ClusteringMetadataDomain fromJsonConfiguration(String json) {
    return JsonMetadataDomain.fromJsonConfiguration(json, ClusteringMetadataDomain.class);
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

  private static List<List<String>> convertToPhysicalClusteringColumns(
      List<Column> logicalClusteringColumns, StructType schema) {
    List<List<String>> physicalClusteringColumns = new ArrayList<>();
    for (Column logicalName : logicalClusteringColumns) {
      Column physicalColumn = ColumnMapping.convertToPhysicalColumnNames(schema, logicalName);
      physicalClusteringColumns.add(Arrays.asList(physicalColumn.getNames()));
    }
    return physicalClusteringColumns;
  }
}
