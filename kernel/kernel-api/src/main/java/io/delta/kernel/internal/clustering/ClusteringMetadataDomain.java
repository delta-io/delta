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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.metadatadomain.JsonMetadataDomain;
import java.util.*;
import java.util.stream.Collectors;

/** Represents the metadata domain for clustering. */
public final class ClusteringMetadataDomain extends JsonMetadataDomain {

  public static final String DOMAIN_NAME = "delta.clustering";

  /**
   * Constructs a ClusteringMetadataDomain with the clustering columns.
   *
   * @param clusteringColumns the columns used for clustering. If column mapping is enabled, use the
   *     physical name assigned; otherwise, use the logical column name.
   */
  public static ClusteringMetadataDomain fromClusteringColumns(List<Column> clusteringColumns) {
    return new ClusteringMetadataDomain(
        clusteringColumns.stream()
            .map(column -> Arrays.asList(column.getNames()))
            .collect(Collectors.toList()));
  }

  /**
   * Creates an instance of {@link ClusteringMetadataDomain} from a JSON configuration string.
   *
   * @param json the JSON configuration string
   */
  public static ClusteringMetadataDomain fromJsonConfiguration(String json) {
    return JsonMetadataDomain.fromJsonConfiguration(json, ClusteringMetadataDomain.class);
  }

  /**
   * Creates an optional instance of {@link ClusteringMetadataDomain} from a {@link SnapshotImpl} if
   * present.
   *
   * @param snapshot the snapshot instance
   */
  // TODO: Add the test coverage for this function in the integration test.
  public static Optional<ClusteringMetadataDomain> fromSnapshot(SnapshotImpl snapshot) {
    return JsonMetadataDomain.fromSnapshot(snapshot, ClusteringMetadataDomain.class, DOMAIN_NAME);
  }

  /**
   * The column names used for clustering. If column mapping is enabled, we use physical column
   * names, otherwise we would store its logical column names. Stored as a List of Lists to avoid
   * customized serialization and deserialization logic.
   */
  @JsonProperty("clusteringColumns")
  private final List<List<String>> clusteringColumns;

  @JsonCreator
  private ClusteringMetadataDomain(
      @JsonProperty("clusteringColumns") List<List<String>> physicalClusteringColumns) {
    this.clusteringColumns = physicalClusteringColumns;
  }

  @Override
  public String getDomainName() {
    return DOMAIN_NAME;
  }

  @JsonIgnore
  public List<Column> getClusteringColumns() {
    return clusteringColumns.stream()
        .map(list -> new Column(list.toArray(new String[0])))
        .collect(Collectors.toList());
  }
}
