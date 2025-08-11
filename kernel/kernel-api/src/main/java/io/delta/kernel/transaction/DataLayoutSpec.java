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
package io.delta.kernel.transaction;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.expressions.Column;
import java.util.Collections;
import java.util.List;

/**
 * Specification for the data layout of a Delta table, including partitioning and clustering
 * configurations.
 *
 * <p>This class supports different layout strategies:
 *
 * <ul>
 *   <li><b>No data layout spec:</b> No special data layout
 *   <li><b>Partitioned:</b> Data is partitioned by specified columns.
 *   <li><b>Clustered:</b> Data is clustered by specified columns.
 * </ul>
 *
 * @since 3.4.0
 */
@Evolving
public class DataLayoutSpec {

  /**
   * Creates an unpartitioned data layout specification.
   *
   * @return A new {@link DataLayoutSpec} for an unpartitioned table.
   */
  public static DataLayoutSpec unpartitioned() {
    return new DataLayoutSpec(null, null);
  }

  /**
   * Creates a data layout specification for a partitioned table.
   *
   * @param partitionColumns The columns to partition by. Cannot be null or empty. Only top-level
   *     columns are supported for partitioning.
   * @return A new {@link DataLayoutSpec} for a partitioned table.
   */
  public static DataLayoutSpec partitioned(List<Column> partitionColumns) {
    checkArgument(
        partitionColumns != null && !partitionColumns.isEmpty(),
        "Partition columns cannot be null or empty");
    checkArgument(
        partitionColumns.stream().allMatch(col -> col.getNames().length == 1),
        "Partition columns must be only top-level columns");
    return new DataLayoutSpec(partitionColumns, null);
  }

  /**
   * Creates a data layout specification for a clustered table.
   *
   * @param clusteringColumns The columns to cluster by. Cannot be null, but can be empty to
   *     indicate clustering is enabled without specific column definitions.
   * @return A new {@link DataLayoutSpec} for a clustered table.
   */
  public static DataLayoutSpec clustered(List<Column> clusteringColumns) {
    checkArgument(
        clusteringColumns != null, "Clustering columns cannot be null (but can be empty)");
    return new DataLayoutSpec(null, clusteringColumns);
  }

  private final List<Column> partitionColumns;
  private final List<Column> clusteringColumns;

  private DataLayoutSpec(List<Column> partitionColumns, List<Column> clusteringColumns) {
    if (partitionColumns != null && clusteringColumns != null) {
      throw new IllegalArgumentException("Cannot specify both partition and clustering columns");
    }
    if (partitionColumns != null && partitionColumns.isEmpty()) {
      throw new IllegalArgumentException("Partition columns cannot be empty");
    }
    this.partitionColumns = partitionColumns;
    this.clusteringColumns = clusteringColumns;
  }

  /**
   * Returns true if this layout specification includes partitioning.
   *
   * <p>Partitioning requires non-empty partition columns. An empty list of partition columns is not
   * considered valid partitioning.
   */
  public boolean hasPartitioning() {
    return partitionColumns != null && !partitionColumns.isEmpty();
  }

  /**
   * Returns true if this layout specification includes clustering.
   *
   * <p>Clustering can be enabled even with empty clustering columns, which indicates that
   * clustering is enabled on the table but no specific columns are defined yet.
   */
  public boolean hasClustering() {
    return clusteringColumns != null;
  }

  /**
   * Returns true if this is an unpartitioned layout.
   *
   * <p>An unpartitioned layout has neither partitioning nor clustering enabled.
   */
  public boolean isUnpartitioned() {
    return !hasPartitioning() && !hasClustering();
  }

  /**
   * Returns the partition columns for this layout.
   *
   * @throws IllegalStateException if partitioning is not enabled on this layout. Use {@link
   *     #hasPartitioning()} to check first.
   */
  public List<Column> getPartitionColumns() {
    if (!hasPartitioning()) {
      throw new IllegalStateException(
          "Cannot get partition columns: partitioning is not enabled on this layout");
    }
    return Collections.unmodifiableList(partitionColumns);
  }

  /**
   * Returns the clustering columns for this layout.
   *
   * <p>The returned list may be empty if clustering is enabled but no specific columns are defined.
   *
   * @throws IllegalStateException if clustering is not enabled on this layout. Use {@link
   *     #hasClustering()} to check first.
   */
  public List<Column> getClusteringColumns() {
    if (!hasClustering()) {
      throw new IllegalStateException(
          "Cannot get clustering columns: clustering is not enabled on this layout");
    }
    return Collections.unmodifiableList(clusteringColumns);
  }
}
