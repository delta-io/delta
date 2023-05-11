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
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.PartitionUtils
import org.apache.hadoop.fs.Path
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** A DataSource V1 for integrating Delta into Spark SQL batch and Streaming APIs. */
class DeltaDataSource
  extends RelationProvider
  with StreamSourceProvider
  with StreamSinkProvider
  with CreatableRelationProvider
  with DataSourceRegister
  with TableProvider
  with DeltaLogging {

  def inferSchema: StructType = new StructType() // empty

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = inferSchema

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val path = options.get("path")
    if (path == null) throw DeltaErrors.pathNotSpecifiedException
    DeltaTableV2(SparkSession.active, new Path(path))
  }

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    if (schema.nonEmpty && schema.get.nonEmpty) {
      throw DeltaErrors.specifySchemaAtReadTimeException
    }
    val path = parameters.getOrElse("path", {
      throw DeltaErrors.pathNotSpecifiedException
    })

    val (_, maybeTimeTravel) = DeltaTableUtils.extractIfPathContainsTimeTravel(
      sqlContext.sparkSession, path, Map.empty)
    if (maybeTimeTravel.isDefined) throw DeltaErrors.timeTravelNotSupportedException
    if (DeltaDataSource.getTimeTravelVersion(parameters).isDefined) {
      throw DeltaErrors.timeTravelNotSupportedException
    }

    val (_, snapshot) = DeltaLog.forTableWithSnapshot(sqlContext.sparkSession, path)
    val readSchema = {
      getSchemaTrackingLogForDeltaSource(sqlContext.sparkSession, snapshot, parameters)
        .flatMap(_.getCurrentTrackedSchema.map(_.dataSchema))
        .getOrElse(snapshot.schema)
    }

    val schemaToUse = DeltaTableUtils.removeInternalMetadata(sqlContext.sparkSession, readSchema)
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
    val (deltaLog, snapshot) = DeltaLog.forTableWithSnapshot(sqlContext.sparkSession, path)

    val schemaTrackingLogOpt =
      getSchemaTrackingLogForDeltaSource(sqlContext.sparkSession, snapshot, parameters)

    val readSchema = schemaTrackingLogOpt
      .flatMap(_.getCurrentTrackedSchema.map(_.dataSchema))
      .getOrElse(snapshot.schema)

    if (readSchema.isEmpty) {
      throw DeltaErrors.schemaNotSetException
    }
    DeltaSource(
      sqlContext.sparkSession,
      deltaLog,
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

    val deltaLog = DeltaLog.forTable(sqlContext.sparkSession, path, parameters)
    WriteIntoDelta(
      deltaLog = deltaLog,
      mode = mode,
      new DeltaOptions(parameters, sqlContext.sparkSession.sessionState.conf),
      partitionColumns = partitionColumns,
      configuration = DeltaConfigs.validateConfigurations(
        parameters.filterKeys(_.startsWith("delta.")).toMap),
      data = data).run(sqlContext.sparkSession)

    deltaLog.createRelation()
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
          parameters
        } else {
          Map.empty
        }
      DeltaTableV2(
        sqlContext.sparkSession,
        new Path(maybePath),
        timeTravelOpt = timeTravelByParams,
        options = dfOptions,
        cdcOptions = new CaseInsensitiveStringMap(cdcOptions.asJava)
      ).toBaseRelation
    }
  }

  override def shortName(): String = {
    DeltaSourceUtils.ALT_NAME
  }

  /**
   * Create a schema log for Delta streaming source if possible
   */
  private def getSchemaTrackingLogForDeltaSource(
      spark: SparkSession,
      sourceSnapshot: Snapshot,
      parameters: Map[String, String]): Option[DeltaSourceSchemaTrackingLog] = {
    val options = new CaseInsensitiveStringMap(parameters.asJava)

    Option(options.get(DeltaOptions.SCHEMA_TRACKING_LOCATION))
      .orElse(Option(options.get(DeltaOptions.SCHEMA_TRACKING_LOCATION_ALIAS)))
      .map { schemaTrackingLocation =>
        if (!spark.sessionState.conf.getConf(
          DeltaSQLConf.DELTA_STREAMING_ENABLE_NON_ADDITIVE_SCHEMA_EVOLUTION)) {
          // TODO: remove once non-additive schema evolution is released
          throw new UnsupportedOperationException(
            "Schema tracking location is not supported for Delta streaming source")
        }
        DeltaSourceSchemaTrackingLog.create(
          spark, schemaTrackingLocation, sourceSnapshot,
          Option(options.get(DeltaOptions.STREAMING_SOURCE_TRACKING_ID))
        )
      }
  }

}

object DeltaDataSource extends DatabricksLogging {
  private implicit val formats = Serialization.formats(NoTypeHints)

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
   * Extract the Delta path if `dataset` is created to load a Delta table. Otherwise returns `None`.
   * Table UI in universe will call this.
   */
  def extractDeltaPath(dataset: Dataset[_]): Option[String] = {
    if (dataset.isStreaming) {
      dataset.queryExecution.logical match {
        case logical: org.apache.spark.sql.execution.streaming.StreamingRelation =>
          if (logical.dataSource.providingClass == classOf[DeltaDataSource]) {
            CaseInsensitiveMap(logical.dataSource.options).get("path")
          } else {
            None
          }
        case _ => None
      }
    } else {
      dataset.queryExecution.analyzed match {
        case DeltaTable(tahoeFileIndex) =>
          Some(tahoeFileIndex.path.toString)
        case SubqueryAlias(_, DeltaTable(tahoeFileIndex)) =>
          Some(tahoeFileIndex.path.toString)
        case _ => None
      }
    }
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
}
