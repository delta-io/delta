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

import static io.delta.kernel.internal.util.VectorUtils.buildArrayValue;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.DataFileStatus;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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

  /**
   * Converts the given list of clustering columns to an {@link ArrayValue} of {@link ArrayValue} of
   * strings.
   *
   * @param clusteringColumns The list of clustering columns to convert.
   * @return An {@link ArrayValue} where each element is an {@link ArrayValue} of strings
   *     representing a column path.
   */
  public static ArrayValue convertToArrays(List<Column> clusteringColumns) {
    return buildArrayValue(
        clusteringColumns.stream()
            .map(
                col ->
                    buildArrayValue(
                        Arrays.stream(col.getNames()).collect(Collectors.toList()),
                        StringType.STRING))
            .collect(Collectors.toList()),
        new ArrayType(StringType.STRING, false /* containsNull */));
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

    long numRecords = dataFileStatistics.getNumRecords();
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
