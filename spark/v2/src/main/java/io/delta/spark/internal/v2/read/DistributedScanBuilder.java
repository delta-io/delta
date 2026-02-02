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
package io.delta.spark.internal.v2.read;

import static org.apache.spark.sql.functions.*;

import io.delta.kernel.PaginatedScan;
import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.*;

/**
 * Kernel-compatible ScanBuilder that uses distributed DataFrame-based log replay.
 *
 * <p>This implementation satisfies the requirement to use Kernel APIs while leveraging Spark
 * DataFrames for distributed processing.
 */
public class DistributedScanBuilder implements ScanBuilder {
  private final SparkSession spark;
  private final Snapshot snapshot;
  private final int numPartitions;
  private Dataset<org.apache.spark.sql.Row> dataFrame;
  private StructType readSchema;
  private boolean maintainOrdering; // Whether to preserve DataFrame order

  /**
   * Create a new DistributedScanBuilder.
   *
   * @param spark Spark session
   * @param snapshot Delta snapshot
   * @param numPartitions Number of partitions for distributed processing
   */
  public DistributedScanBuilder(SparkSession spark, Snapshot snapshot, int numPartitions) {
    this.spark = spark;
    this.snapshot = snapshot;
    this.numPartitions = numPartitions;

    // Initialize DataFrame with distributed log replay
    this.dataFrame =
        DistributedLogReplayHelper.stateReconstructionV2(spark, snapshot, numPartitions);

    // Start with full schema
    this.readSchema = snapshot.getSchema();
    this.maintainOrdering = false; // Default: no ordering guarantee
  }

  /**
   * Create a new DistributedScanBuilder with a custom DataFrame. Useful for streaming where we need
   * pre-sorted DataFrames.
   *
   * @param spark Spark session
   * @param snapshot Delta snapshot
   * @param numPartitions Number of partitions for distributed processing
   * @param customDataFrame Pre-computed DataFrame with "add" struct
   */
  public DistributedScanBuilder(
      SparkSession spark,
      Snapshot snapshot,
      int numPartitions,
      Dataset<org.apache.spark.sql.Row> customDataFrame) {
    this.spark = spark;
    this.snapshot = snapshot;
    this.numPartitions = numPartitions;
    this.dataFrame = customDataFrame;
    this.readSchema = snapshot.getSchema();
    this.maintainOrdering = false; // Default: no ordering guarantee
  }

  /**
   * Enable ordering preservation for streaming. When called, the scan will maintain the DataFrame's
   * sort order. This is essential for streaming initial snapshot where files must be processed in
   * order.
   *
   * @return this builder for chaining
   */
  public DistributedScanBuilder withSortKey() {
    this.maintainOrdering = true;
    return this;
  }

  @Override
  public ScanBuilder withFilter(Predicate predicate) {
    // Filtering is done via withSparkFilters() using original Spark Filters
    // This method is kept for Kernel API compatibility but doesn't need to do anything
    // since SparkScanBuilder already pushes filters via withSparkFilters()
    return this;
  }

  /**
   * Push Spark Filters to DataFrame for distributed filtering. This is called from
   * SparkScanBuilder.pushFilters() with the original Spark Filter objects.
   *
   * <p>This follows V1's approach: - Partition filters: applied to partitionValues - Data filters:
   * applied to stats (minValues/maxValues/nullCount) after parsing stats JSON
   *
   * @param filters Spark filters to push down
   */
  public void withSparkFilters(List<Filter> filters) {
    if (filters == null || filters.isEmpty()) {
      return;
    }

    // Separate partition filters and data filters
    List<Filter> partitionFilters = new ArrayList<>();
    List<Filter> dataFilters = new ArrayList<>();
    java.util.List<String> partitionColumns = snapshot.getPartitionColumnNames();

    for (Filter filter : filters) {
      if (isPartitionFilter(filter, partitionColumns)) {
        partitionFilters.add(filter);
      } else {
        dataFilters.add(filter);
      }
    }

    // Apply partition filters (no stats parsing needed)
    for (Filter filter : partitionFilters) {
      try {
        Column filterColumn = convertPartitionFilterToColumn(filter);
        if (filterColumn != null) {
          this.dataFrame = this.dataFrame.filter(filterColumn);
        }
      } catch (Exception e) {
        // Skip filters that fail conversion - they will be applied post-scan
      }
    }

    // Apply data filters (requires stats parsing)
    if (!dataFilters.isEmpty()) {
      try {
        // Parse stats JSON field like V1's withStats
        org.apache.spark.sql.types.StructType statsSchema =
            DistributedLogReplayHelper.getStatsSchemaForTable(
                io.delta.spark.internal.v2.utils.SchemaUtils.convertKernelSchemaToSparkSchema(
                    snapshot.getSchema()));
        this.dataFrame = DistributedLogReplayHelper.withStats(this.dataFrame, statsSchema);

        // Apply each data filter
        for (Filter filter : dataFilters) {
          try {
            Column filterColumn = convertDataFilterToColumn(filter);
            if (filterColumn != null) {
              this.dataFrame = this.dataFrame.filter(filterColumn);
            }
          } catch (Exception e) {
            // Skip filters that fail conversion
          }
        }
      } catch (Exception e) {
        // If stats parsing fails, skip data filters (they'll be applied post-scan)
      }
    }
  }

  /**
   * Check if a filter references partition columns only.
   *
   * @param filter Spark filter
   * @param partitionColumns List of partition column names
   * @return true if this filter only references partition columns
   */
  private boolean isPartitionFilter(Filter filter, java.util.List<String> partitionColumns) {
    if (filter instanceof EqualTo) {
      return partitionColumns.contains(((EqualTo) filter).attribute());
    }
    if (filter instanceof EqualNullSafe) {
      return partitionColumns.contains(((EqualNullSafe) filter).attribute());
    }
    if (filter instanceof GreaterThan) {
      return partitionColumns.contains(((GreaterThan) filter).attribute());
    }
    if (filter instanceof GreaterThanOrEqual) {
      return partitionColumns.contains(((GreaterThanOrEqual) filter).attribute());
    }
    if (filter instanceof LessThan) {
      return partitionColumns.contains(((LessThan) filter).attribute());
    }
    if (filter instanceof LessThanOrEqual) {
      return partitionColumns.contains(((LessThanOrEqual) filter).attribute());
    }
    if (filter instanceof In) {
      return partitionColumns.contains(((In) filter).attribute());
    }
    if (filter instanceof IsNull) {
      return partitionColumns.contains(((IsNull) filter).attribute());
    }
    if (filter instanceof IsNotNull) {
      return partitionColumns.contains(((IsNotNull) filter).attribute());
    }
    if (filter instanceof StringStartsWith) {
      return partitionColumns.contains(((StringStartsWith) filter).attribute());
    }
    if (filter instanceof StringEndsWith) {
      return partitionColumns.contains(((StringEndsWith) filter).attribute());
    }
    if (filter instanceof StringContains) {
      return partitionColumns.contains(((StringContains) filter).attribute());
    }
    if (filter instanceof And) {
      And f = (And) filter;
      return isPartitionFilter(f.left(), partitionColumns)
          && isPartitionFilter(f.right(), partitionColumns);
    }
    if (filter instanceof Or) {
      Or f = (Or) filter;
      return isPartitionFilter(f.left(), partitionColumns)
          && isPartitionFilter(f.right(), partitionColumns);
    }
    if (filter instanceof Not) {
      return isPartitionFilter(((Not) filter).child(), partitionColumns);
    }
    return false;
  }

  /**
   * Convert a partition filter to DataFrame Column expression. Partition filters reference
   * add.partitionValues.columnName.
   *
   * @param filter Spark filter
   * @return DataFrame Column expression, or null if conversion fails
   */
  private Column convertPartitionFilterToColumn(Filter filter) {
    if (filter == null) {
      return null;
    }

    // Partition filters reference: add.partitionValues.columnName
    if (filter instanceof EqualTo) {
      EqualTo f = (EqualTo) filter;
      return col("add.partitionValues." + f.attribute()).equalTo(lit(f.value()));
    }

    if (filter instanceof EqualNullSafe) {
      EqualNullSafe f = (EqualNullSafe) filter;
      return col("add.partitionValues." + f.attribute()).eqNullSafe(lit(f.value()));
    }

    if (filter instanceof GreaterThan) {
      GreaterThan f = (GreaterThan) filter;
      return col("add.partitionValues." + f.attribute()).gt(lit(f.value()));
    }

    if (filter instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual f = (GreaterThanOrEqual) filter;
      return col("add.partitionValues." + f.attribute()).geq(lit(f.value()));
    }

    if (filter instanceof LessThan) {
      LessThan f = (LessThan) filter;
      return col("add.partitionValues." + f.attribute()).lt(lit(f.value()));
    }

    if (filter instanceof LessThanOrEqual) {
      LessThanOrEqual f = (LessThanOrEqual) filter;
      return col("add.partitionValues." + f.attribute()).leq(lit(f.value()));
    }

    if (filter instanceof In) {
      In f = (In) filter;
      return col("add.partitionValues." + f.attribute()).isin(f.values());
    }

    if (filter instanceof IsNull) {
      IsNull f = (IsNull) filter;
      return col("add.partitionValues." + f.attribute()).isNull();
    }

    if (filter instanceof IsNotNull) {
      IsNotNull f = (IsNotNull) filter;
      return col("add.partitionValues." + f.attribute()).isNotNull();
    }

    if (filter instanceof And) {
      And f = (And) filter;
      Column left = convertPartitionFilterToColumn(f.left());
      Column right = convertPartitionFilterToColumn(f.right());
      if (left != null && right != null) {
        return left.and(right);
      }
      return left != null ? left : right;
    }

    if (filter instanceof Or) {
      Or f = (Or) filter;
      Column left = convertPartitionFilterToColumn(f.left());
      Column right = convertPartitionFilterToColumn(f.right());
      if (left != null && right != null) {
        return left.or(right);
      }
      return null;
    }

    if (filter instanceof Not) {
      Not f = (Not) filter;
      Column child = convertPartitionFilterToColumn(f.child());
      return child != null ? not(child) : null;
    }

    if (filter instanceof StringStartsWith) {
      StringStartsWith f = (StringStartsWith) filter;
      return col("add.partitionValues." + f.attribute()).startsWith(f.value());
    }

    if (filter instanceof StringEndsWith) {
      StringEndsWith f = (StringEndsWith) filter;
      return col("add.partitionValues." + f.attribute()).endsWith(f.value());
    }

    if (filter instanceof StringContains) {
      StringContains f = (StringContains) filter;
      return col("add.partitionValues." + f.attribute()).contains(f.value());
    }

    return null;
  }

  /**
   * Convert a data filter to DataFrame Column expression. Data filters reference stats:
   * add.stats.minValues.columnName, add.stats.maxValues.columnName, add.stats.nullCount.columnName
   *
   * <p>This follows V1's DataSkippingReader.constructDataFilters logic. For example: - EqualTo(a,
   * 3) → minValues.a <= 3 && maxValues.a >= 3 - GreaterThan(a, 3) → maxValues.a > 3 - IsNotNull(a)
   * → nullCount.a < numRecords
   *
   * @param filter Spark filter
   * @return DataFrame Column expression, or null if conversion fails
   */
  private Column convertDataFilterToColumn(Filter filter) {
    if (filter == null) {
      return null;
    }

    // Data filters reference: add.stats.minValues/maxValues/nullCount
    // Following V1's DataSkippingReader.constructDataFilters logic

    if (filter instanceof EqualTo) {
      EqualTo f = (EqualTo) filter;
      // EqualTo: minValues.a <= value && maxValues.a >= value
      return col("add.stats.minValues." + f.attribute())
          .leq(lit(f.value()))
          .and(col("add.stats.maxValues." + f.attribute()).geq(lit(f.value())));
    }

    if (filter instanceof GreaterThan) {
      GreaterThan f = (GreaterThan) filter;
      // GreaterThan: maxValues.a > value
      return col("add.stats.maxValues." + f.attribute()).gt(lit(f.value()));
    }

    if (filter instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual f = (GreaterThanOrEqual) filter;
      // GreaterThanOrEqual: maxValues.a >= value
      return col("add.stats.maxValues." + f.attribute()).geq(lit(f.value()));
    }

    if (filter instanceof LessThan) {
      LessThan f = (LessThan) filter;
      // LessThan: minValues.a < value
      return col("add.stats.minValues." + f.attribute()).lt(lit(f.value()));
    }

    if (filter instanceof LessThanOrEqual) {
      LessThanOrEqual f = (LessThanOrEqual) filter;
      // LessThanOrEqual: minValues.a <= value
      return col("add.stats.minValues." + f.attribute()).leq(lit(f.value()));
    }

    if (filter instanceof IsNotNull) {
      IsNotNull f = (IsNotNull) filter;
      // IsNotNull: nullCount.a < numRecords (some non-null values exist)
      return col("add.stats.nullCount." + f.attribute()).lt(col("add.stats.numRecords"));
    }

    if (filter instanceof And) {
      And f = (And) filter;
      Column left = convertDataFilterToColumn(f.left());
      Column right = convertDataFilterToColumn(f.right());
      if (left != null && right != null) {
        return left.and(right);
      }
      return left != null ? left : right;
    }

    if (filter instanceof Or) {
      Or f = (Or) filter;
      Column left = convertDataFilterToColumn(f.left());
      Column right = convertDataFilterToColumn(f.right());
      if (left != null && right != null) {
        return left.or(right);
      }
      return null;
    }

    if (filter instanceof Not) {
      Not f = (Not) filter;
      Column child = convertDataFilterToColumn(f.child());
      return child != null ? not(child) : null;
    }

    // For other filter types, return null (will be applied post-scan)
    return null;
  }

  @Override
  public ScanBuilder withReadSchema(StructType readSchema) {
    this.readSchema = readSchema;
    // Schema projection will be handled when converting to PartitionedFiles
    return this;
  }

  @Override
  public Scan build() {
    return new DistributedScan(spark, dataFrame, snapshot, readSchema, maintainOrdering);
  }

  @Override
  public PaginatedScan buildPaginated(long maxFilesPerBatch, Optional<Row> previousPageToken) {
    // Paginated scan not yet supported in distributed mode
    throw new UnsupportedOperationException(
        "Paginated scan is not yet supported with distributed log replay");
  }
}
