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

package org.apache.spark.sql.delta.sources

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

// scalastyle:off import.ordering.noEmptyLine
import com.databricks.spark.util.DatabricksLogging
import org.apache.spark.internal.MDC
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.{PartitionUtils, Utils}
import org.apache.hadoop.fs.Path
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DataType, VariantShims}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap


/** A DataSource V1 for integrating Delta into Spark SQL batch and Streaming APIs. */
class DeltaDataSource
  extends RelationProvider
  with StreamSourceProvider
  with StreamSinkProvider
  with CreatableRelationProviderShim
  with DataSourceRegister
  with TableProvider
  with DeltaLogging {

  /**
   * WARNING: This field has complex initialization timing.
   *
   * This field is not initialized in the constructor because the DataSource V1 API does not allow
   * for passing a catalog table. As a work around, we set this field immediately after the
   * `DeltaDataSource` is constructed in `DataSource::providingInstance()`.
   */
  private var catalogTableOpt: Option[CatalogTable] = None

  /**
   * Internal method used only by `DataSource.providingInstance()` right after `DeltaDataSource`
   * construction to plumb the catalog table. This is intended to be set once per instance;
   * subsequent sets are ignored by a guard.
   */
  def setCatalogTableOpt(newCatalogTableOpt: Option[CatalogTable]): Unit = {
    if (catalogTableOpt.isEmpty) {
      catalogTableOpt = newCatalogTableOpt
    }
  }

  /**
   * Construct a snapshot from either the catalog table or a path.
   *
   * If catalogTableOpt is defined, use it to construct the snapshot; otherwise, fall back to use
   * path-based snapshot construction.
   */
  private def getSnapshotFromTableOrPath(sparkSession: SparkSession, path: Path): Snapshot = {
    catalogTableOpt
      .map(catalogTable => DeltaLog.forTableWithSnapshot(
        sparkSession, catalogTable, options = Map.empty[String, String]))
      .getOrElse(DeltaLog.forTableWithSnapshot(sparkSession, path))._2
  }

  def inferSchema: StructType = new StructType() // empty

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = inferSchema

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val path = options.get("path")
    if (path == null) throw DeltaErrors.pathNotSpecifiedException
    DeltaTableV2(SparkSession.active, new Path(path), options = options.asScala.toMap)
  }

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    val path = parameters.getOrElse("path", {
      throw DeltaErrors.pathNotSpecifiedException
    })

    val (_, maybeTimeTravel) = DeltaTableUtils.extractIfPathContainsTimeTravel(
      sqlContext.sparkSession, path, Map.empty)
    if (maybeTimeTravel.isDefined) throw DeltaErrors.timeTravelNotSupportedException
    if (DeltaDataSource.getTimeTravelVersion(parameters).isDefined) {
      throw DeltaErrors.timeTravelNotSupportedException
    }

    val snapshot =
      getSnapshotFromTableOrPath(sqlContext.sparkSession, new Path(path))
    // This is the analyzed schema for Delta streaming
    val readSchema = {
      // Check if we would like to merge consecutive schema changes, this would allow customers
      // to write queries based on their latest changes instead of an arbitrary schema in the past.
      val shouldMergeConsecutiveSchemas = sqlContext.sparkSession.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_STREAMING_ENABLE_SCHEMA_TRACKING_MERGE_CONSECUTIVE_CHANGES
      )
      // This method is invoked during the analysis phase and would determine the schema for the
      // streaming dataframe. We only need to merge consecutive schema changes here because the
      // process would create a new entry in the schema log such that when the schema log is
      // looked up again in the execution phase, we would use the correct schema.
      DeltaDataSource.getMetadataTrackingLogForDeltaSource(
          sqlContext.sparkSession, snapshot, catalogTableOpt, parameters,
          mergeConsecutiveSchemaChanges = shouldMergeConsecutiveSchemas)
        .flatMap(_.getCurrentTrackedMetadata.map(_.dataSchema))
        .getOrElse(snapshot.schema)
    }

    if (schema.nonEmpty && schema.get.nonEmpty &&
      !DataType.equalsIgnoreCompatibleNullability(readSchema, schema.get)) {
      throw DeltaErrors.specifySchemaAtReadTimeException
    }

    val schemaToUse = DeltaTableUtils.removeInternalDeltaMetadata(
      sqlContext.sparkSession,
      DeltaTableUtils.removeInternalWriterMetadata(sqlContext.sparkSession, readSchema)
    )
    if (schemaToUse.isEmpty) {
      throw DeltaErrors.schemaNotSetException
    }
    val options = new CaseInsensitiveStringMap(parameters.asJava)
    if (CDCReader.isCDCRead(options)) {
      (shortName(), CDCReader.cdcReadSchema(schemaToUse))
    } else {
      (shortName(), schemaToUse)
    }
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    if (schema.nonEmpty && schema.get.nonEmpty) {
      throw DeltaErrors.specifySchemaAtReadTimeException
    }
    val path = parameters.getOrElse("path", {
      throw DeltaErrors.pathNotSpecifiedException
    })
    val options = new DeltaOptions(parameters, sqlContext.sparkSession.sessionState.conf)
    val snapshot =
      getSnapshotFromTableOrPath(sqlContext.sparkSession, new Path(path))
    val schemaTrackingLogOpt =
      DeltaDataSource.getMetadataTrackingLogForDeltaSource(
        sqlContext.sparkSession, snapshot, catalogTableOpt, parameters,
        // Pass in the metadata path opt so we can use it for validation
        sourceMetadataPathOpt = Some(metadataPath))

    val readSchema = schemaTrackingLogOpt.flatMap(_.getCurrentTrackedMetadata).map { metadata =>
      logInfo(log"Delta source schema fetched from tracking log version " +
        log"${MDC(DeltaLogKeys.VERSION2, schemaTrackingLogOpt.get.getCurrentTrackedSeqNum)}" +
        log" with Delta commit version " +
        log"${MDC(DeltaLogKeys.VERSION2, metadata.deltaCommitVersion)}")
      metadata.dataSchema
    }.getOrElse {
      logInfo(log"Delta source schema fetched from Delta snapshot version " +
        log"${MDC(DeltaLogKeys.VERSION2, snapshot.version)}")
      snapshot.schema
    }

    if (readSchema.isEmpty) {
      throw DeltaErrors.schemaNotSetException
    }
    DeltaSource(
      sqlContext.sparkSession,
      snapshot.deltaLog,
      catalogTableOpt,
      options,
      snapshot,
      metadataPath,
      schemaTrackingLogOpt
    )
  }

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    val path = parameters.getOrElse("path", {
      throw DeltaErrors.pathNotSpecifiedException
    })
    if (outputMode != OutputMode.Append && outputMode != OutputMode.Complete) {
      throw DeltaErrors.outputModeNotSupportedException(getClass.getName, outputMode.toString)
    }
    val deltaOptions = new DeltaOptions(parameters, sqlContext.sparkSession.sessionState.conf)
    // NOTE: Spark API doesn't give access to the CatalogTable here, but DeltaAnalysis will pick
    // that info out of the containing WriteToStream (if present), and update the sink there.
    new DeltaSink(sqlContext, new Path(path), partitionColumns, outputMode, deltaOptions)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val path = parameters.getOrElse("path", {
      throw DeltaErrors.pathNotSpecifiedException
    })
    val partitionColumns = parameters.get(DeltaSourceUtils.PARTITIONING_COLUMNS_KEY)
      .map(DeltaDataSource.decodePartitioningColumns)
      .getOrElse(Nil)

    val deltaLog = Utils.getDeltaLogFromTableOrPath(
      sqlContext.sparkSession, catalogTableOpt, new Path(path), parameters)
    WriteIntoDelta(
      deltaLog = deltaLog,
      mode = mode,
      new DeltaOptions(parameters, sqlContext.sparkSession.sessionState.conf),
      partitionColumns = partitionColumns,
      configuration = DeltaConfigs.validateConfigurations(
        parameters.filterKeys(_.startsWith("delta.")).toMap),
      data = data,
      // empty catalogTable is acceptable as the code path is only for path based writes
      // (df.write.save("path")) which does not need to use/update catalog
      catalogTableOpt = None
      ).run(sqlContext.sparkSession)

    deltaLog.createRelation(catalogTableOpt = catalogTableOpt)
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    recordFrameProfile("Delta", "DeltaDataSource.createRelation") {
      val maybePath = parameters.getOrElse("path", {
        throw DeltaErrors.pathNotSpecifiedException
      })

      // Log any invalid options that are being passed in
      DeltaOptions.verifyOptions(CaseInsensitiveMap(parameters))

      val timeTravelByParams = DeltaDataSource.getTimeTravelVersion(parameters)
      var cdcOptions: mutable.Map[String, String] = mutable.Map.empty
      val caseInsensitiveParams = new CaseInsensitiveStringMap(parameters.asJava)
      if (CDCReader.isCDCRead(caseInsensitiveParams)) {
        cdcOptions = mutable.Map[String, String](DeltaDataSource.CDC_ENABLED_KEY -> "true")
        if (caseInsensitiveParams.containsKey(DeltaDataSource.CDC_START_VERSION_KEY)) {
          cdcOptions(DeltaDataSource.CDC_START_VERSION_KEY) = caseInsensitiveParams.get(
            DeltaDataSource.CDC_START_VERSION_KEY)
        }
        if (caseInsensitiveParams.containsKey(DeltaDataSource.CDC_START_TIMESTAMP_KEY)) {
          cdcOptions(DeltaDataSource.CDC_START_TIMESTAMP_KEY) = caseInsensitiveParams.get(
            DeltaDataSource.CDC_START_TIMESTAMP_KEY)
        }
        if (caseInsensitiveParams.containsKey(DeltaDataSource.CDC_END_VERSION_KEY)) {
          cdcOptions(DeltaDataSource.CDC_END_VERSION_KEY) = caseInsensitiveParams.get(
            DeltaDataSource.CDC_END_VERSION_KEY)
        }
        if (caseInsensitiveParams.containsKey(DeltaDataSource.CDC_END_TIMESTAMP_KEY)) {
          cdcOptions(DeltaDataSource.CDC_END_TIMESTAMP_KEY) = caseInsensitiveParams.get(
            DeltaDataSource.CDC_END_TIMESTAMP_KEY)
        }
      }
      val dfOptions: Map[String, String] =
        if (sqlContext.sparkSession.sessionState.conf.getConf(
            DeltaSQLConf.LOAD_FILE_SYSTEM_CONFIGS_FROM_DATAFRAME_OPTIONS)) {
          parameters ++ cdcOptions
        } else {
          cdcOptions.toMap
        }
      DeltaTableV2(
        sqlContext.sparkSession,
        new Path(maybePath),
        timeTravelOpt = timeTravelByParams,
        options = dfOptions
      ).toBaseRelation
    }
  }

  /**
   * Extend the default `supportsDataType` to allow VariantType.
   * Implemented by `CreatableRelationProviderShim`.
   */
  override def supportsDataType(dt: DataType): Boolean = {
    VariantShims.isVariantType(dt) || super.supportsDataType(dt)
  }

  override def shortName(): String = {
    DeltaSourceUtils.ALT_NAME
  }

}

object DeltaDataSource extends DatabricksLogging {
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  final val TIME_TRAVEL_SOURCE_KEY = "__time_travel_source__"

  /**
   * The option key for time traveling using a timestamp. The timestamp should be a valid
   * timestamp string which can be cast to a timestamp type.
   */
  final val TIME_TRAVEL_TIMESTAMP_KEY = "timestampAsOf"

  /**
   * The option key for time traveling using a version of a table. This value should be
   * castable to a long.
   */
  final val TIME_TRAVEL_VERSION_KEY = "versionAsOf"

  final val CDC_START_VERSION_KEY = "startingVersion"

  final val CDC_START_TIMESTAMP_KEY = "startingTimestamp"

  final val CDC_END_VERSION_KEY = "endingVersion"

  final val CDC_END_TIMESTAMP_KEY = "endingTimestamp"

  final val CDC_ENABLED_KEY = "readChangeFeed"

  final val CDC_ENABLED_KEY_LEGACY = "readChangeData"

  def encodePartitioningColumns(columns: Seq[String]): String = {
    Serialization.write(columns)
  }

  def decodePartitioningColumns(str: String): Seq[String] = {
    Serialization.read[Seq[String]](str)
  }

  /**
   * For Delta, we allow certain magic to be performed through the paths that are provided by users.
   * Normally, a user specified path should point to the root of a Delta table. However, some users
   * are used to providing specific partition values through the path, because of how expensive it
   * was to perform partition discovery before. We treat these partition values as logical partition
   * filters, if a table does not exist at the provided path.
   *
   * In addition, we allow users to provide time travel specifications through the path. This is
   * provided after an `@` symbol after a path followed by a time specification in
   * `yyyyMMddHHmmssSSS` format, or a version number preceded by a `v`.
   *
   * This method parses these specifications and returns these modifiers only if a path does not
   * really exist at the provided path. We first parse out the time travel specification, and then
   * the partition filters. For example, a path specified as:
   *      /some/path/partition=1@v1234
   * will be parsed into `/some/path` with filters `partition=1` and a time travel spec of version
   * 1234.
   *
   * @return A tuple of the root path of the Delta table, partition filters, and time travel options
   */
  def parsePathIdentifier(
      spark: SparkSession,
      userPath: String,
      options: Map[String, String]): (Path, Seq[(String, String)], Option[DeltaTimeTravelSpec]) = {
    // Handle time travel
    val (path, timeTravelByPath) =
      DeltaTableUtils.extractIfPathContainsTimeTravel(spark, userPath, options)

    val hadoopPath = new Path(path)
    val rootPath =
      DeltaTableUtils.findDeltaTableRoot(spark, hadoopPath, options).getOrElse(hadoopPath)

    val partitionFilters = if (rootPath != hadoopPath) {
      logConsole(
        """
          |WARNING: loading partitions directly with delta is not recommended.
          |If you are trying to read a specific partition, use a where predicate.
          |
          |CORRECT: spark.read.format("delta").load("/data").where("part=1")
          |INCORRECT: spark.read.format("delta").load("/data/part=1")
        """.stripMargin)

      val fragment = hadoopPath.toString.substring(rootPath.toString.length() + 1)
      try {
        PartitionUtils.parsePathFragmentAsSeq(fragment)
      } catch {
        case _: ArrayIndexOutOfBoundsException =>
          throw DeltaErrors.partitionPathParseException(fragment)
      }
    } else {
      Nil
    }

    (rootPath, partitionFilters, timeTravelByPath)
  }

  /**
   * Verifies that the provided partition filters are valid and returns the corresponding
   * expressions.
   */
  def verifyAndCreatePartitionFilters(
      userPath: String,
      snapshot: Snapshot,
      partitionFilters: Seq[(String, String)]): Seq[Expression] = {
    if (partitionFilters.nonEmpty) {
      val metadata = snapshot.metadata

      val badColumns = partitionFilters.map(_._1).filterNot(metadata.partitionColumns.contains)
      if (badColumns.nonEmpty) {
        val fragment = partitionFilters.map(f => s"${f._1}=${f._2}").mkString("/")
        throw DeltaErrors.partitionPathInvolvesNonPartitionColumnException(badColumns, fragment)
      }

      val filters = partitionFilters.map { case (key, value) =>
        // Nested fields cannot be partitions, so we pass the key as a identifier
        EqualTo(UnresolvedAttribute(Seq(key)), Literal(value))
      }
      val files = DeltaLog.filterFileList(
        metadata.partitionSchema, snapshot.allFiles.toDF(), filters)
      if (files.count() == 0) {
        throw DeltaErrors.pathNotExistsException(userPath)
      }
      filters
    } else {
      Nil
    }
  }

  /** Extracts whether users provided the option to time travel a relation. */
  def getTimeTravelVersion(parameters: Map[String, String]): Option[DeltaTimeTravelSpec] = {
    val caseInsensitive = CaseInsensitiveMap[String](parameters)
    val tsOpt = caseInsensitive.get(DeltaDataSource.TIME_TRAVEL_TIMESTAMP_KEY)
    val versionOpt = caseInsensitive.get(DeltaDataSource.TIME_TRAVEL_VERSION_KEY)
    val sourceOpt = caseInsensitive.get(DeltaDataSource.TIME_TRAVEL_SOURCE_KEY)

    if (tsOpt.isDefined && versionOpt.isDefined) {
      throw DeltaErrors.provideOneOfInTimeTravel
    } else if (tsOpt.isDefined) {
      Some(DeltaTimeTravelSpec(Some(Literal(tsOpt.get)), None, sourceOpt.orElse(Some("dfReader"))))
    } else if (versionOpt.isDefined) {
      val version = Try(versionOpt.get.toLong) match {
        case Success(v) => v
        case Failure(t) =>
          throw DeltaErrors.timeTravelInvalidBeginValue(DeltaDataSource.TIME_TRAVEL_VERSION_KEY, t)
      }
      Some(DeltaTimeTravelSpec(None, Some(version), sourceOpt.orElse(Some("dfReader"))))
    } else {
      None
    }
  }

  /**
   * Extract the schema tracking location from options.
   */
  def extractSchemaTrackingLocationConfig(
      spark: SparkSession, parameters: Map[String, String]): Option[String] = {
    val options = new CaseInsensitiveStringMap(parameters.asJava)

    Option(options.get(DeltaOptions.SCHEMA_TRACKING_LOCATION))
      .orElse(Option(options.get(DeltaOptions.SCHEMA_TRACKING_LOCATION_ALIAS)))
  }

  /**
   * Create a schema log for Delta streaming source if possible
   */
  def getMetadataTrackingLogForDeltaSource(
      spark: SparkSession,
      sourceSnapshot: SnapshotDescriptor,
      catalogTableOpt: Option[CatalogTable],
      parameters: Map[String, String],
      sourceMetadataPathOpt: Option[String] = None,
      mergeConsecutiveSchemaChanges: Boolean = false): Option[DeltaSourceMetadataTrackingLog] = {

    DeltaDataSource.extractSchemaTrackingLocationConfig(spark, parameters)
      .map { schemaTrackingLocation =>
        if (!spark.sessionState.conf.getConf(
          DeltaSQLConf.DELTA_STREAMING_ENABLE_SCHEMA_TRACKING)) {
          throw new UnsupportedOperationException(
            "Schema tracking location is not supported for Delta streaming source")
        }

        DeltaSourceMetadataTrackingLog.create(
          spark,
          schemaTrackingLocation,
          sourceSnapshot,
          catalogTableOpt,
          parameters,
          sourceMetadataPathOpt,
          mergeConsecutiveSchemaChanges
        )
      }
  }
}
