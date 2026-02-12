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

import io.delta.kernel.Snapshot;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.stats.DataFiltersBuilderV2;
import org.apache.spark.sql.sources.Filter;
import scala.Option;

/**
 * Java adapter for V2 data skipping using shared V1 logic.
 *
 * <p>Thin Java API that delegates to {@link DataFiltersBuilderV2} (Scala), which in turn delegates
 * ALL shared logic to {@code DataFiltersBuilderUtils} in the V1 spark module.
 *
 * <p><b>CODE REUSE:</b> The entire data skipping pipeline — stats schema construction, JSON
 * parsing, verifyStatsForFilter, filter application — is shared with V1 via
 * DataFiltersBuilderUtils. The only V2-specific code is the StatsProvider creation in
 * DataFiltersBuilderV2.
 */
public class V2DataSkippingAdapter {

  /**
   * Apply data skipping to a flat AddFile DataFrame.
   *
   * <p>This is the main entry point for V2 data skipping. Delegates to the shared pipeline:
   * statsSchema → from_json → verifyStatsForFilter → filter.
   *
   * @param addFilesDF Flat AddFile DataFrame with {path, stats (string), ...}
   * @param filters Spark V2 filters
   * @param snapshot Kernel snapshot
   * @param spark SparkSession
   * @return Filtered DataFrame with same schema as input
   */
  public static Dataset<Row> applyDataSkipping(
      Dataset<Row> addFilesDF, Filter[] filters, Snapshot snapshot, SparkSession spark) {
    return DataFiltersBuilderV2.applyDataSkipping(addFilesDF, filters, snapshot, spark);
  }

  /**
   * Converts Spark V2 Filters to a Column expression for data skipping.
   *
   * @param filters Spark V2 filters
   * @param snapshot Kernel snapshot (provides metadata and schema)
   * @param spark SparkSession
   * @return Column expression for DataFrame filtering, or empty Option if conversion fails
   */
  public static Option<Column> convertFiltersToColumn(
      Filter[] filters, Snapshot snapshot, SparkSession spark) {
    return DataFiltersBuilderV2.convertFiltersToColumn(filters, snapshot, spark);
  }
}
