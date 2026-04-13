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
package io.delta.spark.internal.v2.write

import scala.collection.JavaConverters._

import io.delta.kernel.internal.SnapshotImpl

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.delta.{DataFrameUtils, DeltaColumnMappingMode, NoMapping, TableFeature}
import org.apache.spark.sql.delta.files.DeltaFileFormatWriter
import org.apache.spark.sql.delta.actions.{Metadata => V1Metadata, Protocol => V1Protocol}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.{
  DeltaJobStatisticsTracker,
  StatisticsCollection,
  StatsCollectionUtils
}
import org.apache.spark.sql.execution.datasources.{WriteJobStatsTracker, WriteTaskResult}
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.types.StructType

/**
 * Bridges Kernel's Snapshot metadata to V1's [[DeltaJobStatisticsTracker]] for collecting
 * per-file column statistics (min/max/nullCount) during writes.
 *
 * This helper exists because [[StatisticsCollection]] is a Scala trait with dependencies on
 * V1 [[V1Metadata]] / [[V1Protocol]] and Catalyst expressions, which are awkward to use
 * from Java.
 */
object DeltaStatsTrackerHelper {

  /**
   * Creates a [[DeltaJobStatisticsTracker]] from a Kernel snapshot, reusing the V1
   * [[StatisticsCollection]] trait.
   *
   * @return Some(tracker) if stats collection is enabled, None otherwise.
   */
  def createStatsTracker(
      sparkSession: SparkSession,
      snapshot: io.delta.kernel.Snapshot,
      hadoopConf: Configuration,
      outputPath: Path,
      dataSchema: StructType): Option[DeltaJobStatisticsTracker] = {
    if (!sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_COLLECT_STATS)) {
      return None
    }

    val snapshotImpl = snapshot.asInstanceOf[SnapshotImpl]
    val v1Metadata = buildV1Metadata(snapshotImpl)
    val v1Protocol = buildV1Protocol(snapshotImpl)

    val dataAttributes = toAttributes(dataSchema)
    val tableAttributes = toAttributes(v1Metadata.schema)

    val partitionColNames = v1Metadata.partitionColumns.toSet
    val outputStatsSchema = dataAttributes.filterNot(c => partitionColNames.contains(c.name))
    val tableStatsSchema = {
      val mapped = if (v1Metadata.columnMappingMode == NoMapping) {
        tableAttributes
      } else {
        org.apache.spark.sql.delta.DeltaColumnMapping
          .createPhysicalAttributes(
            tableAttributes, v1Metadata.schema, v1Metadata.columnMappingMode)
      }
      mapped.filterNot(c => partitionColNames.contains(c.name))
    }

    // Capture sparkSession in a local val to avoid Scala initialization-order issues
    // inside the anonymous StatisticsCollection (its abstract `spark` field would shadow
    // the enclosing method parameter).
    val capturedSpark = sparkSession

    val statsCollection = new StatisticsCollection {
      override val columnMappingMode: DeltaColumnMappingMode = v1Metadata.columnMappingMode
      override def tableSchema: StructType = v1Metadata.schema
      override def outputTableStatsSchema: StructType = {
        if (capturedSpark.sessionState.conf
            .getConf(DeltaSQLConf.DELTA_COLLECT_STATS_USING_TABLE_SCHEMA)) {
          tableStatsSchema.toStructType
        } else {
          outputStatsSchema.toStructType
        }
      }
      override def outputAttributeSchema: StructType = outputStatsSchema.toStructType
      override val spark: SparkSession = capturedSpark
      override val statsColumnSpec =
        StatisticsCollection.configuredDeltaStatsColumnSpec(v1Metadata)
      override val protocol: V1Protocol = v1Protocol
      override def getDataSkippingStringPrefixLength: Int =
        StatsCollectionUtils.getDataSkippingStringPrefixLength(capturedSpark, v1Metadata)
    }

    val (statsColExpr, newStatsDataSchema) = getStatsColExpr(
      sparkSession, outputStatsSchema, statsCollection)
    Some(new DeltaJobStatisticsTracker(
      hadoopConf, outputPath, newStatsDataSchema, statsColExpr))
  }

  /**
   * Processes task-level stats through each tracker, populating recordedStats.
   * Delegates to [[DeltaFileFormatWriter.processStats]].
   */
  def processStats(
      statsTrackers: Seq[WriteJobStatsTracker],
      taskResults: Array[WriteTaskResult]): Unit = {
    if (taskResults.isEmpty || statsTrackers.isEmpty) return
    DeltaFileFormatWriter.processStats(
      statsTrackers, taskResults.map(_.summary.stats).toSeq)
  }

  /**
   * Retrieves the per-file stats JSON map from a [[DeltaJobStatisticsTracker]].
   *
   * @return Map from file basename to JSON stats string.
   */
  def getRecordedStats(tracker: DeltaJobStatisticsTracker): java.util.Map[String, String] = {
    if (tracker.recordedStats == null) {
      java.util.Collections.emptyMap()
    } else {
      tracker.recordedStats.asJava
    }
  }

  // ---- Private helpers ----

  private def getStatsColExpr(
      spark: SparkSession,
      statsDataSchema: Seq[Attribute],
      statsCollection: StatisticsCollection): (Expression, Seq[Attribute]) = {
    val resolvedPlan = DataFrameUtils.ofRows(spark, LocalRelation(statsDataSchema))
      .select(to_json(statsCollection.statsCollector))
      .queryExecution.analyzed
    val newStatsDataSchema = resolvedPlan.children.head.output
    (resolvedPlan.expressions.head, newStatsDataSchema)
  }

  /**
   * Builds a V1 [[V1Metadata]] from a Kernel snapshot for [[StatisticsCollection]].
   *
   * [[StatisticsCollection]] reads the following fields:
   *  - `schema` (derived from `schemaString`) — for stats column resolution
   *  - `partitionColumns` — to exclude partition columns from stats schema
   *  - `configuration` — for `delta.dataSkippingNumIndexedCols` and stats column spec
   *  - `columnMappingMode` (derived from `configuration`) — for physical name mapping
   *
   * The remaining case class fields (`name`, `description`, `format`, `createdTime`)
   * use their defaults and are not accessed by the stats collection code path.
   */
  private def buildV1Metadata(snapshotImpl: SnapshotImpl): V1Metadata = {
    val kernelMeta = snapshotImpl.getMetadata
    val kernelConfig = kernelMeta.getConfiguration.asScala.toMap
    val partitionCols = snapshotImpl.getPartitionColumnNames.asScala.toSeq
    val sparkSchema = io.delta.spark.internal.v2.utils.SchemaUtils
      .convertKernelSchemaToSparkSchema(kernelMeta.getSchema)

    V1Metadata(
      id = kernelMeta.getId,
      schemaString = sparkSchema.json,
      partitionColumns = partitionCols,
      configuration = kernelConfig
    )
  }

  /**
   * Builds a V1 [[V1Protocol]] from a Kernel snapshot for [[StatisticsCollection]].
   *
   * [[StatisticsCollection]] checks `protocol` to determine whether certain table features
   * (e.g. column mapping) affect stats column resolution.
   */
  private def buildV1Protocol(snapshotImpl: SnapshotImpl): V1Protocol = {
    val kernelProto = snapshotImpl.getProtocol
    val base = V1Protocol(kernelProto.getMinReaderVersion, kernelProto.getMinWriterVersion)
    val featureNames = kernelProto.getReaderFeatures.asScala ++
      kernelProto.getWriterFeatures.asScala
    val features = featureNames.flatMap(TableFeature.allSupportedFeaturesMap.get)
    if (features.nonEmpty) base.withFeatures(features) else base
  }
}
