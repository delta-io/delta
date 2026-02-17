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

package org.apache.spark.sql.delta.stats

import java.net.{URI, URISyntaxException}

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaLogFileIndex}
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaStatistics._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._

/**
 * Java-callable helper for V2 distributed scan that orchestrates the V1
 * shared pipeline: state reconstruction -> filter planning -> scan execution.
 *
 * This helper lives in the V1 module so it has full access to V1
 * internals (`Action.logSchema`, `SingleAction`, `InMemoryLogReplay`, etc.),
 * while exposing a clean Java-friendly API to V2's `DistributedScanBuilder`.
 *
 * Returns a **DataFrame** (not collected files) so that `DistributedScan`
 * can use zero-copy Spark Row -> Kernel Row adapters.
 */
private[delta] object DistributedScanHelper {

  /**
   * Executes the full V1 data skipping pipeline for V2 batch scans and
   * returns a **DataFrame** with schema `{add: struct<...>}`.
   *
   * The DataFrame is NOT collected — it is returned lazily so that
   * `DistributedScan` can stream rows via `toLocalIterator()` through
   * zero-copy adapters.
   *
   * @param spark          SparkSession
   * @param loadActions    Supplier for the union of checkpoint + delta log actions
   * @param numPartitions  Number of partitions for state reconstruction
   * @param tableSchema    Spark table schema (all columns, physical names)
   * @param partitionColumns Partition column names
   * @param partitionSchema  Partition schema (Spark StructType)
   * @param filters        Spark V2 pushdown filters (already classified)
   * @return DataFrame with schema `{add: struct<path, partitionValues, size, ...>}`
   */
  def executeFilteredScanAsDF(
      spark: SparkSession,
      loadActions: () => DataFrame,
      numPartitions: Int,
      tableSchema: StructType,
      partitionColumns: java.util.List[String],
      partitionSchema: StructType,
      filters: Array[Filter]): DataFrame = {

    val partCols = scala.collection.JavaConverters.asScalaBufferConverter(
      partitionColumns).asScala.toSeq

    // 1. Compute stats schema from table schema
    val statsSchema = buildStatsSchema(tableSchema)

    // 2. Create DefaultStateProvider (full pipeline: load -> reconstruct -> extract -> parse stats)
    val stateProvider = new DefaultStateProvider(
      loadActions = loadActions,
      numPartitions = numPartitions,
      canonicalizeUdf = c => callUDF("canonicalizePath", c),
      minFileRetentionTimestamp = None, // V2: keep all
      minSetTransactionRetentionTimestamp = None, // V2: no expiry
      stateCacheFactory = None, // V2: no caching
      addStatsColName = DefaultStateProvider.AddStatsToUseColName,
      parsedStatsConfig = Some(DefaultStateProvider.ParsedStatsConfig(
        statsSchema = statsSchema,
        cacheFactory = identity[DataFrame] // V2: no caching
      ))
    )

    // 3. Convert filters to resolved Catalyst expressions
    val filterExprs: Seq[Expression] = if (filters.nonEmpty) {
      FilterConversionUtils.filtersToExpressions(filters, tableSchema)
    } else {
      Seq.empty
    }

    // 4. Get DataFrames for the V1 pipeline
    val withStats = stateProvider.allAddFilesWithParsedStats
    val allFilesDF = stateProvider.allAddFiles

    // Get the base stats column (from the parsed "stats" struct)
    val baseStatsColumn = col("stats")

    // Create stats column resolver
    val statsColumnResolver = new StatsColumnResolver(
      baseStatsColumn = baseStatsColumn,
      statsSchema = statsSchema,
      tableSchema = tableSchema
    )

    val getStatsColumnOpt: StatsColumn => Option[Column] =
      (stat: StatsColumn) => statsColumnResolver.getStatsColumnOpt(stat)

    val useStats = spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_STATS_SKIPPING)

    // 5. If no filters, return all files wrapped as {add: struct<...>}
    if (filterExprs.isEmpty) {
      return wrapAsAddStruct(allFilesDF)
    }

    // 6. Create filter planner
    val filterPlanner = new DefaultDataSkippingFilterPlanner(
      spark = spark,
      dataSkippingType = DeltaDataSkippingType.dataSkippingAndPartitionFilteringV1,
      getStatsColumnOpt = getStatsColumnOpt,
      constructNotNullFilter = constructNotNullFilter(getStatsColumnOpt),
      partitionColumns = partCols,
      partitionSchema = partitionSchema,
      useStats = useStats,
      clusteringColumns = Nil, // V2: no clustering info yet
      protocol = None, // V2: no V1 protocol
      numOfFilesIfKnown = None)

    val plan = filterPlanner.plan(filterExprs)

    // 7. Apply filters as DataFrame operations (no collection!)
    val filteredDF: DataFrame = if (plan.dataFilters.isEmpty) {
      // Partition-only path: filter allFiles by partition predicates
      if (plan.partitionFilters.nonEmpty) {
        PartitionFilterUtils.filterFileList(partitionSchema, allFilesDF, plan.partitionFilters)
      } else {
        allFilesDF
      }
    } else {
      // Data skipping path: apply partition + data skipping filters to withStats
      val partitionFilterCol =
        filterPlanner.constructPartitionFilters(plan.partitionFilters)
      withStats.where(partitionFilterCol && plan.verifiedSkippingExpr)
    }

    // 8. Wrap as {add: struct<...>} and return DataFrame (NOT collected)
    wrapAsAddStruct(filteredDF)
  }

  /**
   * Wraps a flat AddFile DataFrame into `{add: struct<...>}` schema.
   * Serializes any parsed stats back to JSON string first.
   */
  private def wrapAsAddStruct(df: DataFrame): DataFrame = {
    // Serialize parsed stats struct back to JSON string for Kernel compatibility
    val withSerializedStats = if (df.schema.fieldNames.contains("stats") &&
        df.schema("stats").dataType.isInstanceOf[StructType]) {
      df.withColumn("stats", to_json(col("stats")))
    } else {
      df
    }
    withSerializedStats.select(struct(col("*")).as("add"))
  }

  /**
   * Extracts a version number from a delta file path.
   * E.g. ".../_delta_log/00000000000000000005.json" -> 5
   */
  def extractDeltaVersion(path: String): Long = {
    val fileName = path.substring(path.lastIndexOf('/') + 1)
    val versionStr = fileName.replace(".json", "")
    try { versionStr.toLong } catch { case _: NumberFormatException => -1L }
  }

  /**
   * Loads raw log actions (checkpoint + delta files) from a Kernel LogSegment
   * as a Spark DataFrame suitable for [[DefaultStateProvider]].
   *
   * The returned DataFrame has the SingleAction schema plus `version` and
   * `add_stats_to_use` columns (same as V1's `Snapshot.loadActions`).
   *
   * @param spark SparkSession
   * @param checkpointPaths Checkpoint file paths (parquet)
   * @param checkpointVersion Checkpoint version (or -1 if no checkpoint)
   * @param deltaPaths Delta file paths (json) with their versions
   * @return DataFrame ready for DefaultStateProvider
   */
  def loadActionsFromPaths(
      spark: SparkSession,
      checkpointPaths: Array[String],
      checkpointVersion: Long,
      deltaPaths: Array[String],
      deltaVersions: Array[Long]): DataFrame = {

    val logSchema = Action.logSchema
    val versionCol = DeltaLogFileIndex.COMMIT_VERSION_COLUMN
    val statsCol = DefaultStateProvider.AddStatsToUseColName

    // Load checkpoint files as parquet
    val checkpointDF = if (checkpointPaths.nonEmpty) {
      spark.read.format("parquet").schema(logSchema).load(checkpointPaths: _*)
        .withColumn(versionCol, lit(checkpointVersion))
        .withColumn(statsCol, col("add.stats"))
    } else {
      spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], logSchema)
        .withColumn(versionCol, lit(0L))
        .withColumn(statsCol, lit(null).cast(StringType))
    }

    // Load delta files as JSON
    val deltaDF = if (deltaPaths.nonEmpty) {
      // Read all delta files
      val rawDelta = spark.read.format("json").schema(logSchema).load(deltaPaths: _*)
        .withColumn("_input_file", input_file_name())

      // Build a mapping from file path to version
      val pathVersionMap = deltaPaths.zip(deltaVersions).toMap

      // Register UDF to extract version from input file path
      val extractVersion = udf((path: String) => {
        // Try exact match first, then try suffix match
        pathVersionMap.find { case (p, _) => path.contains(p) || p.contains(path) }
          .map(_._2)
          .getOrElse {
            // Fallback: extract from filename pattern 00000000000000NNNNN.json
            val fileName = path.substring(path.lastIndexOf('/') + 1)
            val versionStr = fileName.replace(".json", "")
            try { versionStr.toLong } catch { case _: NumberFormatException => -1L }
          }
      })

      rawDelta
        .withColumn(versionCol, extractVersion(col("_input_file")))
        .withColumn(statsCol, col("add.stats"))
        .drop("_input_file")
    } else {
      spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], logSchema)
        .withColumn(versionCol, lit(0L))
        .withColumn(statsCol, lit(null).cast(StringType))
    }

    if (checkpointPaths.nonEmpty && deltaPaths.nonEmpty) {
      checkpointDF.unionAll(deltaDF)
    } else if (checkpointPaths.nonEmpty) {
      checkpointDF
    } else {
      deltaDF
    }
  }

  /**
   * Registers the canonicalizePath UDF for V2 path normalization.
   */
  def registerCanonicalizeUdf(spark: SparkSession): Unit = {
    spark.udf.register("canonicalizePath", (path: String) => {
      if (path == null) null
      else {
        try {
          new URI(path).normalize().toString
        } catch {
          case _: URISyntaxException => path.trim
        }
      }
    })
  }

  // ========================
  // Private helpers
  // ========================

  /**
   * Builds the stats schema from a table schema.
   * Simplified version of StatisticsCollection.statsSchema.
   */
  private def buildStatsSchema(tableSchema: StructType): StructType = {
    def getMinMaxSchema(schema: StructType): Option[StructType] = {
      val fields = schema.fields.flatMap {
        case f @ StructField(_, dt: StructType, _, _) =>
          getMinMaxSchema(dt).map(newDt =>
            StructField(DeltaColumnMapping.getPhysicalName(f), newDt))
        case f @ StructField(_, SkippingEligibleDataType(dt), _, _) =>
          Some(StructField(DeltaColumnMapping.getPhysicalName(f), dt))
        case _ => None
      }
      if (fields.nonEmpty) Some(StructType(fields)) else None
    }

    def getNullCountSchema(schema: StructType): Option[StructType] = {
      val fields = schema.fields.flatMap {
        case f @ StructField(_, dt: StructType, _, _) =>
          getNullCountSchema(dt).map(newDt =>
            StructField(DeltaColumnMapping.getPhysicalName(f), newDt))
        case f: StructField =>
          Some(StructField(DeltaColumnMapping.getPhysicalName(f), LongType))
      }
      if (fields.nonEmpty) Some(StructType(fields)) else None
    }

    val minMaxOpt = getMinMaxSchema(tableSchema)
    val nullCountOpt = getNullCountSchema(tableSchema)

    val fields: Array[(String, DataType)] =
      Array(NUM_RECORDS -> LongType) ++
        minMaxOpt.map(MIN -> _) ++
        minMaxOpt.map(MAX -> _) ++
        nullCountOpt.map(NULL_COUNT -> _)

    StructType(fields.map { case (name, dt) => StructField(name, dt) })
  }

  /**
   * Creates the constructNotNullFilter function used by the filter planner.
   */
  private def constructNotNullFilter(
      getStatsColumnOpt: StatsColumn => Option[Column]
  ): (StatsProvider, Seq[String]) => Option[DataSkippingPredicate] = {
    (statsProvider: StatsProvider, pathToColumn: Seq[String]) => {
      val nullCountCol = StatsColumn(NULL_COUNT, pathToColumn, LongType)
      val numRecordsCol = StatsColumn(NUM_RECORDS, pathToColumn = Nil, LongType)
      statsProvider.getPredicateWithStatsColumnsIfExists(nullCountCol, numRecordsCol) {
        (nullCount, numRecords) => nullCount < numRecords
      }
    }
  }
}
