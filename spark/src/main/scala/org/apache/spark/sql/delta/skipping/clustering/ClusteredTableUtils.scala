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

package org.apache.spark.sql.delta.skipping.clustering

import org.apache.spark.sql.delta.skipping.clustering.temp.ClusterBySpec
import org.apache.spark.sql.delta.{ClusteringTableFeature, DeltaColumnMappingMode, DeltaErrors, DeltaLog, OptimisticTransaction, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, DomainMetadata, Metadata, Protocol, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.clustering.ClusteringMetadataDomain
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.{DeltaStatistics, StatisticsCollection}
import org.apache.spark.sql.delta.util.{Utils => DeltaUtils}

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}

case class MatchingMetadataDomain(
    clusteringDomainOpt: Option[DomainMetadata]
)

/**
 * Clustered table utility functions.
 */
trait ClusteredTableUtilsBase extends DeltaLogging {
  // Clustering columns property key. The column names are logical and separated by comma.
  // This will be removed when we integrate with OSS Spark and use
  // [[CatalogTable.PROP_CLUSTERING_COLUMNS]] directly.
  val PROP_CLUSTERING_COLUMNS: String = "clusteringColumns"

 /**
  * Returns whether the protocol version supports the Liquid table feature.
  */
  def isSupported(protocol: Protocol): Boolean = protocol.isFeatureSupported(ClusteringTableFeature)

  /** The clustering implementation name for [[AddFile.clusteringProvider]] */
  def clusteringProvider: String = "liquid"

  /**
   * Returns an optional [[ClusterBySpec]] from the given CatalogTable.
   */
  def getClusterBySpecOptional(table: CatalogTable): Option[ClusterBySpec] = {
    table.properties.get(PROP_CLUSTERING_COLUMNS).map(ClusterBySpec.fromProperty)
  }

  /**
   * Returns an optional [[ClusterBySpec]] from the given Snapshot.
   */
  def getClusterBySpecOptional(snapshot: Snapshot): Option[ClusterBySpec] = {
    if (isSupported(snapshot.protocol)) {
      val clusteringColumns = ClusteringColumnInfo.extractLogicalNames(snapshot)
      Some(ClusterBySpec.fromColumnNames(clusteringColumns))
    } else {
      None
    }
  }

  /**
   * Extract clustering columns from ClusterBySpec.
   *
   * @param maybeClusterBySpec optional ClusterBySpec. If it's empty, will return the
   *                             original properties.
   * @return an optional pair with clustering columns.
   */
  def getClusteringColumnsAsProperty(
      maybeClusterBySpec: Option[ClusterBySpec]): Option[(String, String)] = {
    maybeClusterBySpec.map(ClusterBySpec.toProperty)
  }

  /**
   * Extract clustering columns from a given snapshot.
   */
  def getClusteringColumnsAsProperty(snapshot: Snapshot): Option[(String, String)] = {
    val clusterBySpec = getClusterBySpecOptional(snapshot)
    getClusteringColumnsAsProperty(clusterBySpec)
  }

  /**
   * Returns table feature properties that's required to create a clustered table.
   *
   * @param existingProperties Table properties set by the user when creating a clustered table.
   */
  def getTableFeatureProperties(existingProperties: Map[String, String]): Map[String, String] = {
    val properties = collection.mutable.Map.empty[String, String]
    properties += TableFeatureProtocolUtils.propertyKey(ClusteringTableFeature) ->
      TableFeatureProtocolUtils.FEATURE_PROP_SUPPORTED

    properties.toMap
  }

  /**
   * Verify user didn't set clustering table feature in table properties.
   *
   * @param existingProperties Table properties set by the user when creating a clustered table.
   */
  def validateExistingTableFeatureProperties(existingProperties: Map[String, String]): Unit = {
    if (existingProperties.contains(
        TableFeatureProtocolUtils.propertyKey(ClusteringTableFeature))) {
      throw DeltaErrors.createTableSetClusteringTableFeatureException(ClusteringTableFeature.name)
    }
  }

  /**
   * Validate the number of clustering columns doesn't exceed the limit.
   *
   * @param clusteringColumns clustering columns for the table.
   * @param deltaLogOpt optional delta log. If present, will be used to record a delta event.
   */
  def validateNumClusteringColumns(
      clusteringColumns: Seq[Seq[String]],
      deltaLogOpt: Option[DeltaLog] = None): Unit = {
    val numColumnsLimit =
      SQLConf.get.getConf(DeltaSQLConf.DELTA_NUM_CLUSTERING_COLUMNS_LIMIT)
    val actualNumColumns = clusteringColumns.size
    if (actualNumColumns > numColumnsLimit) {
      deltaLogOpt.foreach { deltaLog =>
        recordDeltaEvent(
          deltaLog,
          opType = "delta.clusteredTable.invalidNumClusteringColumns",
          data = Map(
            "numCols" -> clusteringColumns.size,
            "numColsLimit" -> numColumnsLimit))
      }
      throw DeltaErrors.clusterByInvalidNumColumnsException(numColumnsLimit, actualNumColumns)
    }
  }

  /**
   * Remove clustered table internal table properties. These properties are never stored into
   * [[Metadata.configuration]] such as table features.
   */
  def removeInternalTableProperties(
      props: scala.collection.Map[String, String]): Map[String, String] = {
    props.toMap --
      // Clustering table feature and dependent table features
      Seq(ClusteringTableFeature).flatMap { feature =>
        (feature +: feature.requiredFeatures.toSeq).map(TableFeatureProtocolUtils.propertyKey)
      }
  }

  /**
   * Remove PROP_CLUSTERING_COLUMNS from metadata action.
   * Clustering columns should only exist in:
   * 1. CatalogTable.properties(PROP_CLUSTERING_COLUMNS)
   * 2. Clustering metadata domain.
   * @param configuration original configuration.
   * @return new configuration without clustering columns property
   */
  def removeClusteringColumnsProperty(configuration: Map[String, String]): Map[String, String] = {
    configuration - PROP_CLUSTERING_COLUMNS
  }

  /**
   * Returns [[DomainMetadata]] action to store clustering columns.
   * If clusterBySpecOpt is not empty (clustering columns are specified by CLUSTER BY), it creates
   * the domain metadata based on the clustering columns.
   * Otherwise (CLUSTER BY is not specified for REPLACE TABLE), it creates the domain metadata
   * with empty clustering columns if a clustering domain exists.
   *
   * This is used for CREATE TABLE and REPLACE TABLE.
   */
  def getDomainMetadataFromTransaction(
      clusterBySpecOpt: Option[ClusterBySpec],
      txn: OptimisticTransaction): Seq[DomainMetadata] = {
    clusterBySpecOpt.map { clusterBy =>
      ClusteredTableUtils.validateClusteringColumnsInStatsSchema(
        txn.protocol, txn.metadata, clusterBy)
      val clusteringColumns =
        clusterBy.columnNames.map(_.toString).map(ClusteringColumn(txn.metadata.schema, _))
      Seq(createDomainMetadata(clusteringColumns))
    }.getOrElse {
      getMatchingMetadataDomain(
        clusteringColumns = Seq.empty,
        txn.snapshot.domainMetadata).clusteringDomainOpt.toSeq
    }
  }

  /**
   * Returns a sequence of [[DomainMetadata]] actions to update the existing domain metadata with
   * the given clustering columns.
   *
   * This is mainly used for REPLACE TABLE and RESTORE TABLE.
   */
  def getMatchingMetadataDomain(
      clusteringColumns: Seq[ClusteringColumn],
      existingDomainMetadata: Seq[DomainMetadata]): MatchingMetadataDomain = {
    val clusteringMetadataDomainOpt =
      if (existingDomainMetadata.exists(_.domain == ClusteringMetadataDomain.domainName)) {
        Some(ClusteringMetadataDomain.fromClusteringColumns(clusteringColumns).toDomainMetadata)
      } else {
        None
      }

    MatchingMetadataDomain(
      clusteringMetadataDomainOpt
    )
  }

  /**
   * Create a [[DomainMetadata]] action to store clustering columns.
   */
  def createDomainMetadata(clusteringColumns: Seq[ClusteringColumn]): DomainMetadata = {
    ClusteringMetadataDomain.fromClusteringColumns(clusteringColumns).toDomainMetadata
  }

  /**
   * Extract [[ClusteringColumn]]s from a given snapshot. Return None if the clustering domain
   * metadata is missing.
   */
  def getClusteringColumnsOptional(snapshot: Snapshot): Option[Seq[ClusteringColumn]] = {
    ClusteringMetadataDomain
      .fromSnapshot(snapshot)
      .map(_.clusteringColumns.map(ClusteringColumn.apply))
  }

  /**
   * Extract [[DomainMetadata]] for storing clustering columns from a given snapshot.
   * It returns clustering domain metadata if exists.
   * Return empty if the clustering domain metadata is missing.
   */
  def getClusteringDomainMetadata(snapshot: Snapshot): Seq[DomainMetadata] = {
    ClusteringMetadataDomain.fromSnapshot(snapshot).map(_.toDomainMetadata).toSeq
  }

  /**
   * Create new clustering [[DomainMetadata]] actions given updated column names for
   * 'ALTER TABLE ... CLUSTER BY'.
   */
  def getClusteringDomainMetadataForAlterTableClusterBy(
      newLogicalClusteringColumns: Seq[String],
      txn: OptimisticTransaction): Seq[DomainMetadata] = {
    val newClusteringColumns =
      newLogicalClusteringColumns.map(ClusteringColumn(txn.metadata.schema, _))
    val clusteringMetadataDomainOpt =
      Some(ClusteringMetadataDomain.fromClusteringColumns(newClusteringColumns).toDomainMetadata)
    clusteringMetadataDomainOpt.toSeq
  }

  /**
   * Extract the logical clustering column names from the to-be committed domain metadata action.
   *
   * @param txn the transaction being used to commit the actions.
   * @param actionsToCommit the actions to be committed.
   * @return optional logical clustering column names.
   */
  def getLogicalClusteringColumnNames(
      txn: OptimisticTransaction,
      actionsToCommit: Seq[Action]): Option[Seq[String]] = {
    def getLogicalColumnNames(clusteringColumns: Seq[ClusteringColumn]): Seq[String] = {
      clusteringColumns.map(ClusteringColumnInfo(txn.metadata.schema, _).logicalName)
    }

    actionsToCommit.collectFirst {
      // Only consider clustering domain metadata actions that are getting added
      // (removed = false).
      case ClusteringMetadataDomain(domain, removed) if !removed =>
        getLogicalColumnNames(domain.clusteringColumns.map(ClusteringColumn.apply))
    }
  }

  /**
   * Validate stats will be collected for all clustering columns.
   */
  def validateClusteringColumnsInStatsSchema(
      snapshot: Snapshot,
      logicalClusteringColumns: Seq[String]): Unit = {
    validateClusteringColumnsInStatsSchema(
      snapshot,
      logicalClusteringColumns.map { name =>
        ClusteringColumnInfo(snapshot.schema, ClusteringColumn(snapshot.schema, name))
      })
  }

  /**
   * Returns true if stats will be collected for all clustering columns.
   */
  def areClusteringColumnsInStatsSchema(
      snapshot: Snapshot,
      logicalClusteringColumns: Seq[String]): Boolean = {
    getClusteringColumnsNotInStatsSchema(
      snapshot,
      logicalClusteringColumns.map { name =>
        ClusteringColumnInfo(snapshot.schema, ClusteringColumn(snapshot.schema, name))
      }).isEmpty
  }

  /**
   * Validate stats will be collected for all clustering columns.
   *
   * This version is used when [[Snapshot]] doesn't have latest stats column information such as
   * `CREATE TABLE...` where the initial snapshot doesn't have updated metadata / protocol yet.
   */
  def validateClusteringColumnsInStatsSchema(
      protocol: Protocol,
      metadata: Metadata,
      clusterBy: ClusterBySpec): Unit = {
    validateClusteringColumnsInStatsSchema(
      statisticsCollectionFromMetadata(protocol, metadata),
      clusterBy.columnNames.map { column =>
        ClusteringColumnInfo(metadata.schema, ClusteringColumn(metadata.schema, column.toString))
      })
  }

  /**
   * Build a [[StatisticsCollection]] with minimal requirements that can be used to find stats
   * columns.
   *
   * We can not use [[Snapshot]] as in a normal case during table creation such as `CREATE TABLE`
   * because the initial snapshot doesn't have the updated metadata / protocol to find latest stats
   * columns.
   */
  private def statisticsCollectionFromMetadata(
      p: Protocol,
      metadata: Metadata): StatisticsCollection = {
    new StatisticsCollection {
      override val tableSchema: StructType = metadata.schema
      override val outputAttributeSchema: StructType = tableSchema
      // [[outputTableStatsSchema]] is the candidate schema to find statistics columns.
      override val outputTableStatsSchema: StructType = tableSchema
      override val statsColumnSpec = StatisticsCollection.configuredDeltaStatsColumnSpec(metadata)
      override val columnMappingMode: DeltaColumnMappingMode = metadata.columnMappingMode
      override val protocol: Protocol = p

      override def spark: SparkSession = {
        throw new Exception("Method not used in statisticsCollectionFromMetadata")
      }
    }
  }

  /**
   * Validate physical clustering columns can be found in the latest stats columns.
   *
   * @param statsCollection Provides latest stats columns.
   * @param clusteringColumnInfos Clustering columns in physical names.
   *
   * A [[AnalysisException]] is thrown if the clustering column can not be found in the latest
   * stats columns. The error message contains logical names only for better user experience.
   */
  private def validateClusteringColumnsInStatsSchema(
      statsCollection: StatisticsCollection,
      clusteringColumnInfos: Seq[ClusteringColumnInfo]): Unit = {
    val missingColumn = getClusteringColumnsNotInStatsSchema(statsCollection, clusteringColumnInfos)
    if (missingColumn.nonEmpty) {
      // Convert back to logical names.
      throw DeltaErrors.clusteringColumnMissingStats(
        missingColumn.mkString(", "),
        statsCollection.statCollectionLogicalSchema.treeString)
    }
  }

  /**
   * Validate that the given clusterBySpec matches the existing table's in the given snapshot.
   * This is used for append mode and replaceWhere.
   */
  def validateClusteringColumnsInSnapshot(
      snapshot: Snapshot,
      clusterBySpec: ClusterBySpec): Unit = {
    // This uses physical column names to compare.
    val providedClusteringColumns =
      Some(clusterBySpec.columnNames.map(col => ClusteringColumn(snapshot.schema, col.toString)))
    val existingClusteringColumns = ClusteredTableUtils.getClusteringColumnsOptional(snapshot)
    if (providedClusteringColumns != existingClusteringColumns) {
      throw DeltaErrors.clusteringColumnsMismatchException(
        clusterBySpec.columnNames.map(_.toString).mkString(","),
        existingClusteringColumns.map(_.map(
          ClusteringColumnInfo(snapshot.schema, _).logicalName).mkString(",")).getOrElse("")
      )
    }
  }

  /**
   * Returns empty if all physical clustering columns can be found in the latest stats columns.
   * Otherwise, returns the logical names of the all clustering columns that are not found.
   *
   * [[StatisticsCollection.statsSchema]] has converted field's name to physical name and also it
   * filters out any columns that are NOT qualified as a stats data type
   * through [[SkippingEligibleDataType]].
   *
   * @param statsCollection       Provides latest stats columns.
   * @param clusteringColumnInfos Clustering columns in physical names.
   */
  private def getClusteringColumnsNotInStatsSchema(
      statsCollection: StatisticsCollection,
      clusteringColumnInfos: Seq[ClusteringColumnInfo]): Seq[String] = {
    clusteringColumnInfos.flatMap { info =>
      val path = DeltaStatistics.MIN +: info.physicalName
      SchemaUtils.findNestedFieldIgnoreCase(statsCollection.statsSchema, path) match {
        // Validate that the column exists in the stats schema and is not a struct
        // in the stats schema (to catch CLUSTER BY an entire struct).
        case None | Some(StructField(_, _: StructType, _, _)) =>
          Some(info.logicalName)
        case _ => None
      }
    }
  }
}

object ClusteredTableUtils extends ClusteredTableUtilsBase
