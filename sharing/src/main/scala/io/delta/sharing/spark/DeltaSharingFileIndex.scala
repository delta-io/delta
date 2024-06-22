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

import java.lang.ref.WeakReference

import org.apache.spark.sql.delta.{DeltaFileFormat, DeltaLog}
import org.apache.spark.sql.delta.files.{SupportsRowIndexFilters, TahoeLogFileIndex}
import io.delta.sharing.client.DeltaSharingClient
import io.delta.sharing.client.model.{Table => DeltaSharingTable}
import io.delta.sharing.client.util.{ConfUtils, JsonUtils}
import io.delta.sharing.filters.{AndOp, BaseOp, OpConverter}
import org.apache.hadoop.fs.Path

import org.apache.spark.delta.sharing.CachedTableManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitionDirectory}
import org.apache.spark.sql.types.StructType

private[sharing] case class DeltaSharingFileIndexParams(
    path: Path,
    spark: SparkSession,
    deltaSharingTableMetadata: DeltaSharingUtils.DeltaSharingTableMetadata,
    options: DeltaSharingOptions)

/**
 * A file index for delta sharing batch queries, that wraps a delta sharing table and client, which
 * is used to issue rpcs to delta sharing server to fetch pre-signed urls, then a local delta log is
 * constructed, and a TahoeFileIndex can be built on top of it.
 */
case class DeltaSharingFileIndex(
    params: DeltaSharingFileIndexParams,
    table: DeltaSharingTable,
    client: DeltaSharingClient,
    limitHint: Option[Long])
    extends FileIndex
    with SupportsRowIndexFilters
    with DeltaFileFormat
    with Logging {
  private lazy val queryCustomTablePath = client.getProfileProvider.getCustomTablePath(
    params.path.toString
  )

  override def spark: SparkSession = params.spark

  override def refresh(): Unit = {}

  override def sizeInBytes: Long =
    Option(params.deltaSharingTableMetadata.metadata.size).getOrElse {
      // Throw error if metadata.size is not returned, to urge the server to respond a table size.
      throw new IllegalStateException(
        "size is null in the metadata returned from the delta " +
        s"sharing server: ${params.deltaSharingTableMetadata.metadata}."
      )
    }

  override def partitionSchema: StructType =
    params.deltaSharingTableMetadata.metadata.partitionSchema

  // Returns the partition columns of the shared delta table based on the returned metadata.
  def partitionColumns: Seq[String] =
    params.deltaSharingTableMetadata.metadata.deltaMetadata.partitionColumns

  override def rootPaths: Seq[Path] = params.path :: Nil

  override def inputFiles: Array[String] = {
    throw new UnsupportedOperationException("DeltaSharingFileIndex.inputFiles")
  }

  // A map that from queriedTableQueryId that we've issued delta sharing rpc, to the deltaLog
  // constructed with the response.
  // It is because this function will be called twice or more in a spark query, with this set, we
  // can avoid doing duplicated work of making expensive rpc and constructing the delta log.
  private val queriedTableQueryIdToDeltaLog = scala.collection.mutable.Map[String, DeltaLog]()

  def fetchFilesAndConstructDeltaLog(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression],
      overrideLimit: Option[Long]): DeltaLog = {
    val jsonPredicateHints = convertToJsonPredicate(partitionFilters, dataFilters)
    val queryParamsHashId = DeltaSharingUtils.getQueryParamsHashId(
      params.options,
      // Using .sql instead of toString because it doesn't include class pointer, which
      // keeps the string the same for the same filters.
      partitionFilters.map(_.sql).mkString(";"),
      dataFilters.map(_.sql).mkString(";"),
      jsonPredicateHints.getOrElse(""),
      params.deltaSharingTableMetadata.version
    )
    val tablePathWithHashIdSuffix = DeltaSharingUtils.getTablePathWithIdSuffix(
      queryCustomTablePath,
      queryParamsHashId
    )
    // listFiles will be called twice or more in a spark query, with this check we can avoid
    // duplicated work of making expensive rpc and constructing the delta log.
    queriedTableQueryIdToDeltaLog.get(tablePathWithHashIdSuffix) match {
      case Some(deltaLog) => deltaLog
      case None =>
        createDeltaLog(
          jsonPredicateHints,
          queryParamsHashId,
          tablePathWithHashIdSuffix,
          overrideLimit
        )
    }
  }

  private def createDeltaLog(
      jsonPredicateHints: Option[String],
      queryParamsHashId: String,
      tablePathWithHashIdSuffix: String,
      overrideLimit: Option[Long]): DeltaLog = {
    //  1. Call client.getFiles.
    val startTime = System.currentTimeMillis()
    val deltaTableFiles = client.getFiles(
      table = table,
      predicates = Nil,
      limit = overrideLimit.orElse(limitHint),
      versionAsOf = params.options.versionAsOf,
      timestampAsOf = params.options.timestampAsOf,
      jsonPredicateHints = jsonPredicateHints,
      refreshToken = None
    )
    logInfo(
      s"Fetched ${deltaTableFiles.lines.size} lines for table $table with version " +
      s"${deltaTableFiles.version} from delta sharing server, took " +
      s"${(System.currentTimeMillis() - startTime) / 1000.0}s."
    )

    // 2. Prepare a DeltaLog.
    val deltaLogMetadata =
      DeltaSharingLogFileSystem.constructLocalDeltaLogAtVersionZero(
        deltaTableFiles.lines,
        tablePathWithHashIdSuffix
      )

    // 3. Register parquet file id to url mapping
    CachedTableManager.INSTANCE.register(
      // Using params.path instead of queryCustomTablePath because it will be customized
      // within CachedTableManager.
      tablePath = DeltaSharingUtils.getTablePathWithIdSuffix(
        params.path.toString,
        queryParamsHashId
      ),
      idToUrl = deltaLogMetadata.idToUrl,
      refs = Seq(new WeakReference(this)),
      profileProvider = client.getProfileProvider,
      refresher = DeltaSharingUtils.getRefresherForGetFiles(
        client = client,
        table = table,
        predicates = Nil,
        limit = overrideLimit.orElse(limitHint),
        versionAsOf = params.options.versionAsOf,
        timestampAsOf = params.options.timestampAsOf,
        jsonPredicateHints = jsonPredicateHints
      ),
      expirationTimestamp =
        if (CachedTableManager.INSTANCE
            .isValidUrlExpirationTime(deltaLogMetadata.minUrlExpirationTimestamp)) {
          deltaLogMetadata.minUrlExpirationTimestamp.get
        } else {
          System.currentTimeMillis() + CachedTableManager.INSTANCE.preSignedUrlExpirationMs
        },
      refreshToken = deltaTableFiles.refreshToken
    )

    // 4. Create a local file index and call listFiles of this class.
    val deltaLog = DeltaLog.forTable(
      params.spark,
      DeltaSharingLogFileSystem.encode(tablePathWithHashIdSuffix)
    )

    // In theory there should only be one entry in this set since each query creates its own
    // FileIndex class. This is purged together with the FileIndex class when the query
    // finishes.
    queriedTableQueryIdToDeltaLog.put(tablePathWithHashIdSuffix, deltaLog)

    deltaLog
  }

  def asTahoeFileIndex(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): TahoeLogFileIndex = {
    val deltaLog = fetchFilesAndConstructDeltaLog(partitionFilters, dataFilters, None)
    new TahoeLogFileIndex(
      params.spark,
      deltaLog,
      deltaLog.dataPath,
      deltaLog.unsafeVolatileSnapshot
    )
  }

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    // NOTE: The server is not required to apply all filters, so we apply them client-side as well.
    asTahoeFileIndex(partitionFilters, dataFilters).listFiles(partitionFilters, dataFilters)
  }

  // Converts the specified SQL expressions to a json predicate.
  //
  // If jsonPredicatesV2 are enabled, converts both partition and data filters
  // and combines them using an AND.
  //
  // If the conversion fails, returns a None, which will imply that we will
  // not perform json predicate based filtering.
  private def convertToJsonPredicate(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Option[String] = {
    if (!ConfUtils.jsonPredicatesEnabled(params.spark.sessionState.conf)) {
      return None
    }

    // Convert the partition filters.
    val partitionOp = try {
      OpConverter.convert(partitionFilters)
    } catch {
      case e: Exception =>
        log.error("Error while converting partition filters: " + e)
        None
    }

    // If V2 predicates are enabled, also convert the data filters.
    val dataOp = try {
      if (ConfUtils.jsonPredicatesV2Enabled(params.spark.sessionState.conf)) {
        log.info("Converting data filters")
        OpConverter.convert(dataFilters)
      } else {
        None
      }
    } catch {
      case e: Exception =>
        log.error("Error while converting data filters: " + e)
        None
    }

    // Combine partition and data filters using an AND operation.
    val combinedOp = if (partitionOp.isDefined && dataOp.isDefined) {
      Some(AndOp(Seq(partitionOp.get, dataOp.get)))
    } else if (partitionOp.isDefined) {
      partitionOp
    } else {
      dataOp
    }
    log.info("Using combined predicate: " + combinedOp)

    if (combinedOp.isDefined) {
      Some(JsonUtils.toJson[BaseOp](combinedOp.get))
    } else {
      None
    }
  }
}
