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

package io.delta.sharing.spark

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.{
  DeltaColumnMapping,
  DeltaErrors,
  DeltaTableUtils => TahoeDeltaTableUtils
}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSQLConf}
import io.delta.sharing.client.{DeltaSharingClient, DeltaSharingRestClient}
import io.delta.sharing.client.model.{Table => DeltaSharingTable}
import io.delta.sharing.client.util.{ConfUtils, JsonUtils}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkEnv
import org.apache.spark.delta.sharing.PreSignedUrlCache
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{
  BaseRelation,
  DataSourceRegister,
  RelationProvider,
  StreamSourceProvider
}
import org.apache.spark.sql.types.StructType

/**
 * A DataSource for Delta Sharing, used to support all types of queries on a delta sharing table:
 * batch, cdf, streaming, time travel, filters, etc.
 */
private[sharing] class DeltaSharingDataSource
    extends RelationProvider
    with StreamSourceProvider
    with DataSourceRegister
    with DeltaLogging {

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    DeltaSharingDataSource.setupFileSystem(sqlContext)
    if (schema.nonEmpty && schema.get.nonEmpty) {
      throw DeltaErrors.specifySchemaAtReadTimeException
    }
    val options = new DeltaSharingOptions(parameters)
    if (options.isTimeTravel) {
      throw DeltaErrors.timeTravelNotSupportedException
    }
    val path = options.options.getOrElse("path", throw DeltaSharingErrors.pathNotSpecifiedException)

    if (options.responseFormat == DeltaSharingOptions.RESPONSE_FORMAT_PARQUET) {
      logInfo(s"sourceSchema with parquet format for table path:$path, parameters:$parameters")
      val deltaLog = RemoteDeltaLog(
        path,
        forStreaming = true,
        responseFormat = options.responseFormat
      )
      val schemaToUse = deltaLog.snapshot().schema
      if (schemaToUse.isEmpty) {
        throw DeltaSharingErrors.schemaNotSetException
      }

      if (options.readChangeFeed) {
        (shortName(), DeltaTableUtils.addCdcSchema(schemaToUse))
      } else {
        (shortName(), schemaToUse)
      }
    } else if (options.responseFormat == DeltaSharingOptions.RESPONSE_FORMAT_DELTA) {
      logInfo(s"sourceSchema with delta format for table path:$path, parameters:$parameters")
      if (options.readChangeFeed) {
        throw new UnsupportedOperationException(
          s"Delta sharing cdc streaming is not supported when responseforma=delta."
        )
      }
      //  1. create delta sharing client
      val parsedPath = DeltaSharingRestClient.parsePath(path)
      val client = DeltaSharingRestClient(
        profileFile = parsedPath.profileFile,
        forStreaming = true,
        responseFormat = options.responseFormat,
        // comma separated delta reader features, used to tell delta sharing server what delta
        // reader features the client is able to process.
        readerFeatures = DeltaSharingUtils.STREAMING_SUPPORTED_READER_FEATURES.mkString(",")
      )
      val dsTable = DeltaSharingTable(
        share = parsedPath.share,
        schema = parsedPath.schema,
        name = parsedPath.table
      )

      //  2. getMetadata for schema to be used in the file index.
      val deltaSharingTableMetadata = DeltaSharingUtils.getDeltaSharingTableMetadata(
        client = client,
        table = dsTable
      )
      val customTablePathWithUUIDSuffix = DeltaSharingUtils.getTablePathWithIdSuffix(
        client.getProfileProvider.getCustomTablePath(path),
        DeltaSharingUtils.getFormattedTimestampWithUUID()
      )
      val deltaLogPath =
        s"${DeltaSharingLogFileSystem.encode(customTablePathWithUUIDSuffix).toString}/_delta_log"
      val (_, snapshotDescriptor) = DeltaSharingUtils.getDeltaLogAndSnapshotDescriptor(
        sqlContext.sparkSession,
        deltaSharingTableMetadata,
        customTablePathWithUUIDSuffix
      )

      // This is the analyzed schema for Delta streaming
      val readSchema = {
        // Check if we would like to merge consecutive schema changes, this would allow customers
        // to write queries based on their latest changes instead of an arbitrary schema in the
        // past.
        val shouldMergeConsecutiveSchemas = sqlContext.sparkSession.sessionState.conf.getConf(
          DeltaSQLConf.DELTA_STREAMING_ENABLE_SCHEMA_TRACKING_MERGE_CONSECUTIVE_CHANGES
        )

        // This method is invoked during the analysis phase and would determine the schema for the
        // streaming dataframe. We only need to merge consecutive schema changes here because the
        // process would create a new entry in the schema log such that when the schema log is
        // looked up again in the execution phase, we would use the correct schema.
        DeltaDataSource
          .getMetadataTrackingLogForDeltaSource(
            sqlContext.sparkSession,
            snapshotDescriptor,
            parameters,
            mergeConsecutiveSchemaChanges = shouldMergeConsecutiveSchemas
          )
          .flatMap(_.getCurrentTrackedMetadata.map(_.dataSchema))
          .getOrElse(snapshotDescriptor.schema)
      }

      val schemaToUse = TahoeDeltaTableUtils.removeInternalWriterMetadata(
        sqlContext.sparkSession,
        readSchema
      )
      if (schemaToUse.isEmpty) {
        throw DeltaErrors.schemaNotSetException
      }

      DeltaSharingLogFileSystem.tryToCleanUpDeltaLog(deltaLogPath)
      (shortName(), schemaToUse)
    } else {
      throw new UnsupportedOperationException(
        s"responseformat(${options.responseFormat}) is not " +
        s"supported in delta sharing."
      )
    }
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    DeltaSharingDataSource.setupFileSystem(sqlContext)
    if (schema.nonEmpty && schema.get.nonEmpty) {
      throw DeltaSharingErrors.specifySchemaAtReadTimeException
    }
    val options = new DeltaSharingOptions(parameters)
    val path = options.options.getOrElse("path", throw DeltaSharingErrors.pathNotSpecifiedException)

    if (options.responseFormat == DeltaSharingOptions.RESPONSE_FORMAT_PARQUET) {
      logInfo(s"createSource with parquet format for table path:$path, parameters:$parameters")
      val deltaLog = RemoteDeltaLog(path, forStreaming = true, options.responseFormat)
      DeltaSharingSource(SparkSession.active, deltaLog, options)
    } else if (options.responseFormat == DeltaSharingOptions.RESPONSE_FORMAT_DELTA) {
      logInfo(s"createSource with delta format for table path:$path, parameters:$parameters")
      if (options.readChangeFeed) {
        throw new UnsupportedOperationException(
          s"Delta sharing cdc streaming is not supported when responseforma=delta."
        )
      }
      //  1. create delta sharing client
      val parsedPath = DeltaSharingRestClient.parsePath(path)
      val client = DeltaSharingRestClient(
        profileFile = parsedPath.profileFile,
        forStreaming = true,
        responseFormat = options.responseFormat,
        // comma separated delta reader features, used to tell delta sharing server what delta
        // reader features the client is able to process.
        readerFeatures = DeltaSharingUtils.STREAMING_SUPPORTED_READER_FEATURES.mkString(",")
      )
      val dsTable = DeltaSharingTable(
        share = parsedPath.share,
        schema = parsedPath.schema,
        name = parsedPath.table
      )

      DeltaFormatSharingSource(
        spark = sqlContext.sparkSession,
        client = client,
        table = dsTable,
        options = options,
        parameters = parameters,
        sqlConf = sqlContext.sparkSession.sessionState.conf,
        metadataPath = metadataPath
      )
    } else {
      throw new UnsupportedOperationException(
        s"responseformat(${options.responseFormat}) is not " +
        s"supported in delta sharing."
      )
    }
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    DeltaSharingDataSource.setupFileSystem(sqlContext)
    val options = new DeltaSharingOptions(parameters)

    val userInputResponseFormat = options.options.get(DeltaSharingOptions.RESPONSE_FORMAT)
    if (userInputResponseFormat.isEmpty && !options.readChangeFeed) {
      return autoResolveBaseRelationForSnapshotQuery(options)
    }

    val path = options.options.getOrElse("path", throw DeltaSharingErrors.pathNotSpecifiedException)
    if (options.responseFormat == DeltaSharingOptions.RESPONSE_FORMAT_PARQUET) {
      // When user explicitly set responseFormat=parquet, to query shared tables without advanced
      // delta features.
      logInfo(s"createRelation with parquet format for table path:$path, parameters:$parameters")
      val deltaLog = RemoteDeltaLog(
        path,
        forStreaming = false,
        responseFormat = options.responseFormat
      )
      deltaLog.createRelation(
        options.versionAsOf,
        options.timestampAsOf,
        options.cdfOptions
      )
    } else if (options.responseFormat == DeltaSharingOptions.RESPONSE_FORMAT_DELTA) {
      // When user explicitly set responseFormat=delta, to query shared tables with advanced
      // delta features.
      logInfo(s"createRelation with delta format for table path:$path, parameters:$parameters")
      //  1. create delta sharing client
      val parsedPath = DeltaSharingRestClient.parsePath(path)
      val client = DeltaSharingRestClient(
        profileFile = parsedPath.profileFile,
        forStreaming = false,
        responseFormat = options.responseFormat,
        // comma separated delta reader features, used to tell delta sharing server what delta
        // reader features the client is able to process.
        readerFeatures = DeltaSharingUtils.SUPPORTED_READER_FEATURES.mkString(",")
      )
      val dsTable = DeltaSharingTable(
        share = parsedPath.share,
        schema = parsedPath.schema,
        name = parsedPath.table
      )

      if (options.readChangeFeed) {
        return DeltaSharingCDFUtils.prepareCDFRelation(sqlContext, options, dsTable, client)
      }
      //  2. getMetadata for schema to be used in the file index.
      val deltaTableMetadata = DeltaSharingUtils.queryDeltaTableMetadata(
        client = client,
        table = dsTable,
        versionAsOf = options.versionAsOf,
        timestampAsOf = options.timestampAsOf
      )
      val deltaSharingTableMetadata = DeltaSharingUtils.getDeltaSharingTableMetadata(
        table = dsTable,
        deltaTableMetadata = deltaTableMetadata
      )

      //  3. Prepare HadoopFsRelation
      getHadoopFsRelationForDeltaSnapshotQuery(
        path = path,
        options = options,
        dsTable = dsTable,
        client = client,
        deltaSharingTableMetadata = deltaSharingTableMetadata
      )
    } else {
      throw new UnsupportedOperationException(
        s"responseformat(${options.responseFormat}) is not supported in delta sharing."
      )
    }
  }

  /**
   * "parquet format sharing" leverages the existing set of remote classes to directly handle the
   * list of presigned urls and read data.
   * "delta format sharing" instead constructs a local delta log and leverages the delta library to
   * read data.
   * Firstly we sends a getMetadata call to the delta sharing server the suggested response format
   * of the shared table by the server (based on whether there are advanced delta features in the
   * shared table), and then decide the code path on the client side.
   */
  private def autoResolveBaseRelationForSnapshotQuery(
      options: DeltaSharingOptions): BaseRelation = {
    val path = options.options.getOrElse("path", throw DeltaSharingErrors.pathNotSpecifiedException)
    val parsedPath = DeltaSharingRestClient.parsePath(path)

    val client = DeltaSharingRestClient(
      profileFile = parsedPath.profileFile,
      forStreaming = false,
      // Indicating that the client is able to process response format in both parquet and delta.
      responseFormat = s"${DeltaSharingOptions.RESPONSE_FORMAT_PARQUET}," +
        s"${DeltaSharingOptions.RESPONSE_FORMAT_DELTA}",
      // comma separated delta reader features, used to tell delta sharing server what delta
      // reader features the client is able to process.
      readerFeatures = DeltaSharingUtils.SUPPORTED_READER_FEATURES.mkString(",")
    )
    val dsTable = DeltaSharingTable(
      name = parsedPath.table,
      schema = parsedPath.schema,
      share = parsedPath.share
    )

    val deltaTableMetadata = DeltaSharingUtils.queryDeltaTableMetadata(
      client = client,
      table = dsTable,
      versionAsOf = options.versionAsOf,
      timestampAsOf = options.timestampAsOf
    )

    if (deltaTableMetadata.respondedFormat == DeltaSharingOptions.RESPONSE_FORMAT_PARQUET) {
      val deltaLog = RemoteDeltaLog(
        path = path,
        forStreaming = false,
        responseFormat = DeltaSharingOptions.RESPONSE_FORMAT_PARQUET,
        initDeltaTableMetadata = Some(deltaTableMetadata)
      )
      deltaLog.createRelation(options.versionAsOf, options.timestampAsOf, options.cdfOptions)
    } else if (deltaTableMetadata.respondedFormat == DeltaSharingOptions.RESPONSE_FORMAT_DELTA) {
      val deltaSharingTableMetadata = DeltaSharingUtils.getDeltaSharingTableMetadata(
        table = dsTable,
        deltaTableMetadata = deltaTableMetadata
      )
      val deltaOnlyClient = DeltaSharingRestClient(
        profileFile = parsedPath.profileFile,
        forStreaming = false,
        // Indicating that the client request delta format in response.
        responseFormat = DeltaSharingOptions.RESPONSE_FORMAT_DELTA,
        // comma separated delta reader features, used to tell delta sharing server what delta
        // reader features the client is able to process.
        readerFeatures = DeltaSharingUtils.SUPPORTED_READER_FEATURES.mkString(",")
      )
      getHadoopFsRelationForDeltaSnapshotQuery(
        path = path,
        options = options,
        dsTable = dsTable,
        client = deltaOnlyClient,
        deltaSharingTableMetadata = deltaSharingTableMetadata
      )
    } else {
      throw new UnsupportedOperationException(
        s"Unexpected respondedFormat for getMetadata rpc:${deltaTableMetadata.respondedFormat}."
      )
    }
  }

  /**
   * Prepare a HadoopFsRelation for the snapshot query on a delta sharing table. It will contain a
   * DeltaSharingFileIndex which is used to handle delta sharing rpc, and construct the local delta
   * log, and then build a TahoeFileIndex on top of the delta log.
   */
  private def getHadoopFsRelationForDeltaSnapshotQuery(
      path: String,
      options: DeltaSharingOptions,
      dsTable: DeltaSharingTable,
      client: DeltaSharingClient,
      deltaSharingTableMetadata: DeltaSharingUtils.DeltaSharingTableMetadata): BaseRelation = {
    // Prepare DeltaSharingFileIndex
    val spark = SparkSession.active
    val params = new DeltaSharingFileIndexParams(
      new Path(path),
      spark,
      deltaSharingTableMetadata,
      options
    )
    if (ConfUtils.limitPushdownEnabled(spark.sessionState.conf)) {
      DeltaFormatSharingLimitPushDown.setup(spark)
    }
    // limitHint is always None here and will be overridden in DeltaFormatSharingLimitPushDown.
    val fileIndex = DeltaSharingFileIndex(
      params = params,
      table = dsTable,
      client = client,
      limitHint = None
    )

    //  return HadoopFsRelation with the DeltaSharingFileIndex.
    HadoopFsRelation(
      location = fileIndex,
      // This is copied from DeltaLog.buildHadoopFsRelationWithFileIndex.
      // Dropping column mapping metadata because it is not relevant for partition schema.
      partitionSchema = DeltaColumnMapping.dropColumnMappingMetadata(fileIndex.partitionSchema),
      // This is copied from DeltaLog.buildHadoopFsRelationWithFileIndex, original comment:
      // We pass all table columns as `dataSchema` so that Spark will preserve the partition
      // column locations. Otherwise, for any partition columns not in `dataSchema`, Spark would
      // just append them to the end of `dataSchema`.
      dataSchema = DeltaColumnMapping.dropColumnMappingMetadata(
        TahoeDeltaTableUtils.removeInternalWriterMetadata(
          spark,
          SchemaUtils.dropNullTypeColumns(deltaSharingTableMetadata.metadata.schema)
        )
      ),
      bucketSpec = None,
      // Handle column mapping metadata in schema.
      fileFormat = fileIndex.fileFormat(
        deltaSharingTableMetadata.protocol.deltaProtocol,
        deltaSharingTableMetadata.metadata.deltaMetadata
      ),
      options = Map.empty
    )(spark)
  }

  override def shortName(): String = "deltaSharing"
}

private[sharing] object DeltaSharingDataSource {
  def setupFileSystem(sqlContext: SQLContext): Unit = {
    sqlContext.sparkContext.hadoopConfiguration
      .setIfUnset("fs.delta-sharing.impl", "io.delta.sharing.client.DeltaSharingFileSystem")
    sqlContext.sparkContext.hadoopConfiguration
      .setIfUnset(
        "fs.delta-sharing-log.impl",
        "io.delta.sharing.spark.DeltaSharingLogFileSystem"
      )
    PreSignedUrlCache.registerIfNeeded(SparkEnv.get)
  }
}
