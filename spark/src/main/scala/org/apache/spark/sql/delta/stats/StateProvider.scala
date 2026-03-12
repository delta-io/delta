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

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.DeltaLogFileIndex
import org.apache.spark.sql.delta.actions.{Action, AddFile, InMemoryLogReplay, SingleAction}
import org.apache.spark.sql.delta.expressions.DecodeNestedZ85EncodedVariant
import org.apache.spark.sql.delta.implicits._
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}

private[delta] object DefaultStateProvider {
  /** Column in loadActions output containing selected AddFile stats JSON. */
  val AddStatsToUseColName = "add_stats_to_use"

  /** Configuration for parsing and optionally caching AddFile stats. */
  case class ParsedStatsConfig(
      statsSchema: StructType,
      cacheFactory: DataFrame => DataFrame = identity[DataFrame])

  // ===========================================================================
  // Shared utilities for loading raw log actions from file paths.
  // V1 (Snapshot.loadActions) uses DeltaLogFileIndex; V2 uses these helpers
  // to load from raw paths provided by Kernel's LogSegment.
  // ===========================================================================

  /**
   * Loads raw log actions (checkpoint + delta files) from file paths
   * as a DataFrame suitable for [[DefaultStateProvider]].
   *
   * The returned DataFrame has the SingleAction schema plus
   * [[DeltaLogFileIndex.COMMIT_VERSION_COLUMN]] and [[AddStatsToUseColName]]
   * columns, matching the contract expected by [[DefaultStateProvider]].
   *
   * @param spark SparkSession
   * @param checkpointPaths Checkpoint file paths (parquet). May be empty.
   * @param checkpointVersion Checkpoint version (ignored when no checkpoint paths)
   * @param deltaPaths Delta file paths (JSON). May be empty.
   * @param deltaVersions Version for each delta path (parallel array with deltaPaths)
   * @return DataFrame ready for DefaultStateProvider's loadActions parameter
   */
  def loadActionsFromPaths(
      spark: SparkSession,
      checkpointPaths: Array[String],
      checkpointVersion: Long,
      deltaPaths: Array[String],
      deltaVersions: Array[Long]): DataFrame = {

    val logSchema = Action.logSchema
    val versionCol = DeltaLogFileIndex.COMMIT_VERSION_COLUMN
    val statsCol = AddStatsToUseColName

    val checkpointDF = if (checkpointPaths.nonEmpty) {
      spark.read.format("parquet").schema(logSchema).load(checkpointPaths: _*)
        .withColumn(versionCol, lit(checkpointVersion))
        .withColumn(statsCol, col("add.stats"))
    } else {
      emptyLogDF(spark, logSchema)
    }

    val deltaDF = if (deltaPaths.nonEmpty) {
      val rawDelta = spark.read.format("json").schema(logSchema).load(deltaPaths: _*)
        .withColumn("_input_file", input_file_name())

      val pathVersionMap = deltaPaths.zip(deltaVersions).toMap

      val extractVersion = udf((path: String) => {
        pathVersionMap.find { case (p, _) => path.contains(p) || p.contains(path) }
          .map(_._2)
          .getOrElse(extractDeltaVersion(path))
      })

      rawDelta
        .withColumn(versionCol, extractVersion(col("_input_file")))
        .withColumn(statsCol, col("add.stats"))
        .drop("_input_file")
    } else {
      emptyLogDF(spark, logSchema)
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
   * Extracts a version number from a delta log file path.
   * E.g. ".../_delta_log/00000000000000000005.json" returns 5.
   */
  def extractDeltaVersion(path: String): Long = {
    val fileName = path.substring(path.lastIndexOf('/') + 1)
    val versionStr = fileName.replace(".json", "")
    try { versionStr.toLong } catch { case _: NumberFormatException => -1L }
  }

  /**
   * Returns a canonicalize-path UDF based on URI normalization.
   *
   * V1 uses [[org.apache.spark.sql.delta.DeltaLog.getCanonicalPathUdf]]
   * which leverages Hadoop FileSystem. V2 (Kernel-based) does not have
   * a DeltaLog, so this provides a standalone alternative.
   */
  def defaultCanonicalizeColumn(spark: SparkSession): Column => Column = {
    spark.udf.register("deltaDefaultCanonicalizePath", (path: String) => {
      if (path == null) null
      else {
        try {
          new URI(path).normalize().toString
        } catch {
          case _: URISyntaxException => path.trim
        }
      }
    })
    (c: Column) => callUDF("deltaDefaultCanonicalizePath", c)
  }

  /**
   * Convenience factory for V2: creates a fully-configured [[DefaultStateProvider]]
   * from raw file paths, handling loading and canonicalization internally.
   *
   * Callers provide only the log file paths and a stats configuration;
   * the factory wires up [[loadActionsFromPaths]], [[defaultCanonicalizeColumn]],
   * and the V2-specific defaults (no retention, no cache) automatically.
   *
   * @param spark             SparkSession
   * @param checkpointPaths   Checkpoint file paths (parquet). May be empty.
   * @param checkpointVersion Checkpoint version (or -1 if no checkpoint)
   * @param deltaPaths        Delta file paths (JSON). May be empty.
   * @param deltaVersions     Version for each delta path (parallel array)
   * @param numPartitions     Number of Spark partitions for state reconstruction
   * @param parsedStatsConfig Optional stats schema + cache config.
   *                          V2 passes the stats schema here; V1 does not use this factory.
   */
  def fromPaths(
      spark: SparkSession,
      checkpointPaths: Array[String],
      checkpointVersion: Long,
      deltaPaths: Array[String],
      deltaVersions: Array[Long],
      numPartitions: Int,
      parsedStatsConfig: Option[ParsedStatsConfig] = None): DefaultStateProvider = {
    val canonicalizeUdf = defaultCanonicalizeColumn(spark)
    new DefaultStateProvider(
      loadActions = () => loadActionsFromPaths(
        spark, checkpointPaths, checkpointVersion, deltaPaths, deltaVersions),
      numPartitions = numPartitions,
      canonicalizeUdf = canonicalizeUdf,
      parsedStatsConfig = parsedStatsConfig
    )
  }

  /**
   * Java-friendly overload: takes a [[StructType]] stats schema directly
   * (no cache) instead of an `Option[ParsedStatsConfig]`.
   */
  def fromPaths(
      spark: SparkSession,
      checkpointPaths: Array[String],
      checkpointVersion: Long,
      deltaPaths: Array[String],
      deltaVersions: Array[Long],
      numPartitions: Int,
      statsSchema: StructType): DefaultStateProvider = {
    fromPaths(spark, checkpointPaths, checkpointVersion, deltaPaths, deltaVersions,
      numPartitions, Some(ParsedStatsConfig(statsSchema)))
  }

  /** Creates an empty DataFrame with the log schema plus version and stats columns. */
  private def emptyLogDF(spark: SparkSession, logSchema: StructType): DataFrame = {
    spark.createDataFrame(
      spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], logSchema)
      .withColumn(DeltaLogFileIndex.COMMIT_VERSION_COLUMN, lit(0L))
      .withColumn(AddStatsToUseColName, lit(null).cast(StringType))
  }
}

/**
 * Provides raw and reconstructed state DataFrames for scan planning.
 *
 * Implementations own the full pipeline:
 *   raw source -> state reconstruction -> extract add files
 *
 * [[DefaultStateProvider]] is the shared implementation used by both V1 and V2.
 */
private[delta] trait DeltaStateProvider {

  /** Full reconstructed state as a typed Dataset (all action types). */
  def stateDS: Dataset[SingleAction]

  /** Full reconstructed state as a plain DataFrame. */
  def stateDF: DataFrame

  /** Flat AddFile rows extracted from the reconstructed state. */
  def allAddFiles: DataFrame

  /** AddFile rows with the `stats` JSON column parsed as a struct. */
  def allAddFilesWithParsedStats: DataFrame
}

/**
 * Shared [[DeltaStateProvider]] implementation used by both V1 and V2.
 *
 * Owns the full state reconstruction pipeline from raw log actions to
 * deduplicated state:
 *
 *  1. Load actions (checkpoint + deltas) via [[loadActions]]
 *  2. Add canonical path columns for add/remove actions
 *  3. Repartition by canonical path, sort by commitVersion
 *  4. Reconstruct add/remove structs with canonical paths
 *  5. Deduplicate via [[InMemoryLogReplay]] per partition
 *  6. Optional cache of full state
 *
 * A simple window function (e.g., row_number) is NOT sufficient for
 * step 5 because it only keeps "latest row per path" and misses
 * DV-aware dedup, add/remove cancellation, and tombstone expiry.
 *
 * V1/V2 differences are captured through constructor parameters:
 *
 *  - V1: passes `Snapshot.loadActions`, `getCanonicalPathUdf`,
 *    retention timestamps, and cache factory from [[org.apache.spark.sql.delta.util.StateCache]].
 *  - V2: passes `DistributedLogReplayHelper.loadActions`,
 *    `callUDF("canonicalizePath", _)`, no retention, no caching.
 *
 * V1's `Snapshot` owns a `DefaultStateProvider` instance and delegates
 * `stateDS`, `stateDF`, `allFiles` to it. This eliminates the
 * separate `stateReconstruction` + `cachedState` chain in `Snapshot`.
 *
 * @param loadActions            Supplier for the union of checkpoint + delta
 *                               log files as a DataFrame. Must include
 *                               `commitVersion` and `add_stats_to_use` columns.
 * @param numPartitions          Number of Spark partitions for state
 *                               reconstruction (V1: config, V2: passed in)
 * @param canonicalizeUdf        UDF to canonicalize file paths
 *                               (V1: deltaLog.getCanonicalPathUdf,
 *                                V2: callUDF("canonicalizePath", _))
 * @param minFileRetentionTimestamp
 *                               Tombstone expiry threshold (V1: from config,
 *                               V2: None -- keep all)
 * @param minSetTransactionRetentionTimestamp
 *                               Transaction expiry threshold (V1: from config,
 *                               V2: None)
 * @param stateCacheFactory      Optional factory to cache the full state
 *                               Dataset[SingleAction]. V1 passes
 *                               `ds => cacheDS(ds, name).getDS`;
 *                               V2 passes None.
 * @param addStatsColName        Name of the pre-selected AddFile stats column in [[loadActions]].
 * @param parsedStatsConfig      Optional parsed-stats configuration.
 */
private[delta] class DefaultStateProvider(
    loadActions: () => DataFrame,
    numPartitions: Int,
    canonicalizeUdf: Column => Column,
    minFileRetentionTimestamp: Option[Long] = None,
    minSetTransactionRetentionTimestamp: Option[Long] = None,
    stateCacheFactory: Option[Dataset[SingleAction] => Dataset[SingleAction]] = None,
    addStatsColName: String = DefaultStateProvider.AddStatsToUseColName,
    parsedStatsConfig: Option[DefaultStateProvider.ParsedStatsConfig] = None
) extends DeltaStateProvider {

  private val ADD_PATH_CANONICAL_COL_NAME = "add_path_canonical"
  private val REMOVE_PATH_CANONICAL_COL_NAME = "remove_path_canonical"

  /**
   * Full state after log replay, optionally cached.
   *
   * Pipeline: loadActions() -> canonicalize paths -> repartition by path
   *   -> sort by version -> InMemoryLogReplay per partition -> optional cache
   *
   * For V1 this replaces `Snapshot.stateReconstruction` + `cachedState`.
   * For V2 this replaces the V2 distributed log replay pipeline.
   */
  override lazy val stateDS: Dataset[SingleAction] = {
    // Steps 2-4: canonicalize paths, repartition, sort, reconstruct structs
    val reconstructed = loadActions()
      .withColumn(ADD_PATH_CANONICAL_COL_NAME, when(
        col("add.path").isNotNull, canonicalizeUdf(col("add.path"))))
      .withColumn(REMOVE_PATH_CANONICAL_COL_NAME, when(
        col("remove.path").isNotNull, canonicalizeUdf(col("remove.path"))))
      .repartition(
        numPartitions,
        coalesce(col(ADD_PATH_CANONICAL_COL_NAME), col(REMOVE_PATH_CANONICAL_COL_NAME)))
      .sortWithinPartitions(DeltaLogFileIndex.COMMIT_VERSION_COLUMN)
      .withColumn("add", when(
        col("add.path").isNotNull,
        struct(
          col(ADD_PATH_CANONICAL_COL_NAME).as("path"),
          col("add.partitionValues"),
          col("add.size"),
          col("add.modificationTime"),
          col("add.dataChange"),
          col(addStatsColName).as("stats"),
          col("add.tags"),
          col("add.deletionVector"),
          col("add.baseRowId"),
          col("add.defaultRowCommitVersion"),
          col("add.clusteringProvider")
        )))
      .withColumn("remove", when(
        col("remove.path").isNotNull,
        col("remove").withField("path", col(REMOVE_PATH_CANONICAL_COL_NAME))))

    // Step 5: deduplicate via InMemoryLogReplay per partition
    val replayed = reconstructed
      .as[SingleAction]
      .mapPartitions { iter =>
        val state = new InMemoryLogReplay(
          minFileRetentionTimestamp,
          minSetTransactionRetentionTimestamp)
        state.append(0, iter.map(_.unwrap))
        state.checkpoint.map(_.wrap)
      }

    // Step 6: optional cache
    stateCacheFactory.map(_(replayed)).getOrElse(replayed)
  }

  /** DataFrame view of the full state. */
  override lazy val stateDF: DataFrame = stateDS.toDF()

  /**
   * Extract flat AddFile rows from the reconstructed state.
   * Equivalent to V1's `Snapshot.allFilesViaStateReconstruction`.
   */
  override lazy val allAddFiles: DataFrame =
    stateDS.where("add IS NOT NULL")
      .select(col("add").as[AddFile])
      .toDF

  override lazy val allAddFilesWithParsedStats: DataFrame = parsedStatsConfig match {
    case Some(DefaultStateProvider.ParsedStatsConfig(statsSchema, cacheFactory)) =>
      val parsedStats = from_json(col("stats"), statsSchema)
      // Decode Variant fields only when present to avoid unnecessary overhead.
      val decodedStats = if (SchemaUtils.checkForVariantTypeColumnsRecursively(statsSchema)) {
        Column(DecodeNestedZ85EncodedVariant(parsedStats.expr))
      } else {
        parsedStats
      }
      cacheFactory(allAddFiles.withColumn("stats", decodedStats))
    case None =>
      throw new IllegalStateException(
        "allAddFilesWithParsedStats requires parsedStatsConfig to be defined.")
  }
}
