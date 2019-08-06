/*
 * Copyright 2019 Databricks, Inc.
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

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.PartitionUtils
import org.apache.hadoop.fs.Path
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/** A DataSource V1 for integrating Delta into Spark SQL batch and Streaming APIs. */
class DeltaDataSource
  extends RelationProvider
  with StreamSourceProvider
  with StreamSinkProvider
  with CreatableRelationProvider
  with DataSourceRegister
  with DeltaLogging {

  SparkSession.getActiveSession.foreach { spark =>
    // Enable "passPartitionByAsOptions" to support "write.partitionBy(...)"
    // TODO Remove this when upgrading to Spark 3.0.0
    spark.conf.set("spark.sql.legacy.sources.write.passPartitionByAsOptions", "true")
  }

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    if (schema.nonEmpty) {
      throw DeltaErrors.specifySchemaAtReadTimeException
    }
    val path = parameters.getOrElse("path", {
      throw DeltaErrors.pathNotSpecifiedException
    })

    val maybeTimeTravel = DeltaTableUtils.extractIfPathContainsTimeTravel(
      sqlContext.sparkSession, path)
    if (maybeTimeTravel.isDefined) throw DeltaErrors.timeTravelNotSupportedException

    val deltaLog = DeltaLog.forTable(sqlContext.sparkSession, path)
    if (deltaLog.snapshot.schema.isEmpty) {
      throw DeltaErrors.schemaNotSetException
    }
    (shortName(), deltaLog.snapshot.schema)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    if (schema.nonEmpty) {
      throw DeltaErrors.specifySchemaAtReadTimeException
    }
    val path = parameters.getOrElse("path", {
      throw DeltaErrors.pathNotSpecifiedException
    })
    val deltaLog = DeltaLog.forTable(sqlContext.sparkSession, path)
    if (deltaLog.snapshot.schema.isEmpty) {
      throw DeltaErrors.schemaNotSetException
    }
    val options = new DeltaOptions(parameters, sqlContext.sparkSession.sessionState.conf)
    new DeltaSource(sqlContext.sparkSession, deltaLog, options)
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
      throw DeltaErrors.outputModeNotSupportedException(getClass.getName, outputMode)
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

    val deltaLog = DeltaLog.forTable(sqlContext.sparkSession, path)
    WriteIntoDelta(
      deltaLog = deltaLog,
      mode = mode,
      new DeltaOptions(parameters, sqlContext.sparkSession.sessionState.conf),
      partitionColumns = partitionColumns,
      configuration = Map.empty,
      data = data).run(sqlContext.sparkSession)

    deltaLog.createRelation()
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val maybePath = parameters.getOrElse("path", {
      throw DeltaErrors.pathNotSpecifiedException
    })

    // Handle time travel
    val (path, timeTravelOpt) =
      DeltaTableUtils.getTimeTravel(sqlContext.sparkSession, maybePath, parameters)

    val hadoopPath = new Path(path)
    val rootPath = DeltaTableUtils.findDeltaTableRoot(sqlContext.sparkSession, Seq(hadoopPath))

    val deltaLog = DeltaLog.forTable(sqlContext.sparkSession, rootPath)

    val partitionFilters = if (rootPath != hadoopPath) {
      logConsole(
        """
          |WARNING: loading partitions directly with delta is not recommended.
          |If you are trying to read a specific partition, use a where predicate.
          |
          |CORRECT: spark.read.format("delta").load("/data").where("part=1")
          |INCORRECT: spark.read.format("delta").load("/data/part=1")
        """.stripMargin)

      val fragment = hadoopPath.toString().substring(rootPath.toString().length() + 1)

      DeltaTableUtils.resolvePathFilters(deltaLog, Seq(fragment))
    } else {
      Nil
    }

    deltaLog.createRelation(partitionFilters, timeTravelOpt)
  }

  override def shortName(): String = {
    DeltaSourceUtils.ALT_NAME
  }
}

object DeltaDataSource {
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
}
