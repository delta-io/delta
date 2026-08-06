/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.sharing.spark;

import io.delta.sharing.client.util.ConfUtils;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.delta.kernel.engine.Engine;

/**
 * ScanBuilder for the Delta Sharing DSv2 connector.
 *
 * <p>Supports column pruning ({@link SupportsPushDownRequiredColumns}), filter pushdown ({@link
 * SupportsPushDownFilters}), and limit pushdown ({@link SupportsPushDownLimit}). Spark applies
 * these in the order the {@link ScanBuilder} contract defines (filter, then limit, then column
 * pruning), all before {@link #build()} -- so by the time {@link #build()} runs it has everything
 * the {@code getFiles} RPC needs. {@code build()} then calls {@link
 * DeltaSharingDSV2Utils#buildScan}, which issues that RPC with the pushdown hints, builds the
 * synthetic Delta log, opens it as a Delta Kernel snapshot, and returns the Kernel V2 connector's
 * Delta V2 scan over it.
 *
 * <p>Filter and limit pushdown are <b>advisory</b>: the sharing server uses the hints for file
 * skipping but is not required to honor them, so Spark must still apply every filter and re-apply
 * LIMIT after the scan. This builder therefore reports all filters as residual ({@link
 * #pushFilters} returns them unchanged) and leaves {@link SupportsPushDownLimit#isPartiallyPushed}
 * at its default {@code true}. Spark itself only invokes {@link #pushLimit} when no post-scan
 * filter remains (its limit-pushdown rule matches a scan with no residual filter), so a limit hint
 * is never sent alongside a filter hint -- matching the V1 path.
 */
public class DeltaSharingScanBuilder
    implements ScanBuilder,
        SupportsPushDownRequiredColumns,
        SupportsPushDownFilters,
        SupportsPushDownLimit {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaSharingScanBuilder.class);

  private final SparkSession spark;
  private final DeltaSharingV2TableContext ctx;
  // The Delta Kernel engine (a DefaultEngine over the session hadoopConf), built once by the table
  // and reused across scans -- mirroring DeltaV2Table's kernelEngine, not rebuilt per build().
  private final Engine engine;
  // The table's catalog statistics (DeltaSharingV2Table.computeCatalogStats), handed to the scan
  // so estimateStatistics can blend catalog column stats with post-prune file-level estimates.
  // Present only when the shared table's CatalogTable carries stats -- i.e. the mounted share's UC
  // properties include spark.sql.statistics.* (populated when the provider ran ANALYZE and the
  // sharing server propagated it). Empty otherwise (no ANALYZE, freshly mounted share), in which
  // case the scan falls back to file-level estimates alone.
  private final Optional<Statistics> catalogStats;
  // Defaults to the full table schema; narrowed by pruneColumns.
  private StructType requiredSchema;
  // Filters offered by Spark via pushFilters; converted to the server-side json predicate hint in
  // build(). Empty until pushFilters is called.
  private Filter[] pushedFilters = new Filter[0];
  // Limit offered by Spark via pushLimit; forwarded as the getFiles limit hint in build(). Empty
  // unless pushLimit accepted a limit (which requires the limit-pushdown conf on).
  private OptionalLong pushedLimit = OptionalLong.empty();

  public DeltaSharingScanBuilder(
      SparkSession spark,
      DeltaSharingV2TableContext ctx,
      Engine engine,
      Optional<Statistics> catalogStats) {
    this.spark = spark;
    this.ctx = ctx;
    this.engine = engine;
    this.catalogStats = catalogStats;
    this.requiredSchema = ctx.dsMeta().metadata().schema();
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    LOG.info("DSV2-Sharing: pruneColumns -> {}", requiredSchema.catalogString());
    this.requiredSchema = requiredSchema;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    this.pushedFilters = filters;
    LOG.info("DSV2-Sharing: pushFilters received {} filter(s)", filters.length);
    // The json predicate sent to the server is advisory (the server may not apply all of it), so
    // Spark must still evaluate every filter after the scan. Report them all as residual.
    return filters;
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public boolean pushLimit(int limit) {
    if (!ConfUtils.limitPushdownEnabled(spark.sessionState().conf())) {
      LOG.info("DSV2-Sharing: pushLimit({}) declined, limit pushdown disabled", limit);
      return false;
    }
    this.pushedLimit = OptionalLong.of(limit);
    LOG.info("DSV2-Sharing: pushLimit({})", limit);
    // Keep the default isPartiallyPushed() == true: the hint is best-effort, so Spark re-applies
    // LIMIT after the scan.
    return true;
  }

  /**
   * Issues the getFiles RPC (with the pushed-down filter and limit hints) and returns the Delta
   * Kernel V2 connector's Delta V2 scan over the resulting synthetic-log snapshot.
   */
  @Override
  public Scan build() {
    LOG.info("DSV2-Sharing: build() -> DeltaV2Scan, requiredSchema={}, pushedFilters={}, "
            + "pushedLimit={}",
        requiredSchema.catalogString(), pushedFilters.length, pushedLimit);
    return DeltaSharingDSV2Utils.buildScan(
        spark, ctx, engine, requiredSchema, pushedFilters, pushedLimit, catalogStats);
  }
}
