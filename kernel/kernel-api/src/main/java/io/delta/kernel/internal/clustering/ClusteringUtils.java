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

import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.DomainMetadata;
import java.util.List;
import java.util.Optional;

public class ClusteringUtils {

  private ClusteringUtils() {
    // Empty private constructor to prevent instantiation
  }

  /**
   * Get the domain metadata for the clustering columns. If column mapping is enabled, pass the list
   * of physical names assigned; otherwise, use the logical column names.
   */
  public static DomainMetadata getClusteringDomainMetadata(List<Column> clusteringColumns) {
    ClusteringMetadataDomain clusteringMetadataDomain =
        ClusteringMetadataDomain.fromClusteringColumns(clusteringColumns);
    return clusteringMetadataDomain.toDomainMetadata();
  }

  /**
   * Extract ClusteringColumns from a given snapshot. Return None if the clustering domain metadata
   * is missing.
   */
  public static Optional<List<Column>> getClusteringColumnsOptional(SnapshotImpl snapshot) {
    return ClusteringMetadataDomain.fromSnapshot(snapshot)
        .map(ClusteringMetadataDomain::getClusteringColumns);
  }
}
