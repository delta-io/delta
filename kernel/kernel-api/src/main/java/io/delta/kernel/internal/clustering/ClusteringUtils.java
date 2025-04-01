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
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.utils.DataFileStatus;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ClusteringUtils {

  private ClusteringUtils() {
    // Empty private constructor to prevent instantiation
  }

  /**
   * Get the domain metadata for the clustering columns.
   *
   * @param physicalClusteringColumns The physical clustering columns.
   * @return The domain metadata including the clustering columns.
   */
  public static DomainMetadata getClusteringDomainMetadata(List<Column> physicalClusteringColumns) {
    final ClusteringMetadataDomain clusteringMetadataDomain =
        ClusteringMetadataDomain.fromPhysicalColumns(physicalClusteringColumns);
    return clusteringMetadataDomain.toDomainMetadata();
  }

  /**
   * Extract ClusteringColumns from a given snapshot. Return None if the clustering domain metadata
   * is missing.
   *
   * @param snapshot The snapshot instance
   */
  public static Optional<List<Column>> getClusteringColumnsOptional(SnapshotImpl snapshot) {
    return ClusteringMetadataDomain.fromSnapshot(snapshot)
        .map(ClusteringMetadataDomain::getClusteringColumnsAsColumnList);
  }

  /**
   * Validates the given {@link DataFileStatus} being added as an {@code add} action to the Delta
   * Log. Specifically, it ensures that file-level statistics are present and that statistics exist
   * for all clustering columns.
   *
   * @param clusteringColumns The list of clustering columns that require statistics.
   * @param dataFileStatus The {@link DataFileStatus} to validate.
   */
  public static void validateDataFileStatus(
      List<Column> clusteringColumns, DataFileStatus dataFileStatus) {
    if (!dataFileStatus.getStatistics().isPresent()) {
      throw DeltaErrors.missingFileStatsForClustering(clusteringColumns, dataFileStatus);
    }

    DataFileStatistics dataFileStatistics = dataFileStatus.getStatistics().get();

    Long numRecords = dataFileStatistics.getNumRecords();
    Map<Column, Literal> minValues = dataFileStatistics.getMinValues();
    Map<Column, Literal> maxValues = dataFileStatistics.getMaxValues();
    Map<Column, Long> nullCounts = dataFileStatistics.getNullCounts();

    for (Column column : clusteringColumns) {
      boolean minAndMaxPresent = minValues.containsKey(column) && maxValues.containsKey(column);
      if (!minAndMaxPresent) {
        // if min/max values are missing and nullCount is not equal to numRecords,
        // then we throw the exception
        if (!nullCounts.containsKey(column) || !nullCounts.get(column).equals(numRecords)) {
          throw DeltaErrors.missingColumnStatsForClustering(column, dataFileStatus);
        }
      }
    }
  }
}
