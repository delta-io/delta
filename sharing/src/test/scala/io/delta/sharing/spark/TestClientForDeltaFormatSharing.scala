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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.util.JsonUtils
import io.delta.sharing.client.{
  DeltaSharingClient,
  DeltaSharingProfileProvider,
  DeltaSharingRestClient
}
import io.delta.sharing.client.model.{
  AddFile => ClientAddFile,
  DeltaTableFiles,
  DeltaTableMetadata,
  SingleAction,
  Table
}

import org.apache.spark.SparkEnv
import org.apache.spark.storage.BlockId

/**
 * A mocked delta sharing client for DeltaFormatSharing.
 * The test suite need to prepare the mocked delta sharing rpc response and store them in
 * BlockManager. Then this client will just load the response of return upon rpc call.
 */
private[spark] class TestClientForDeltaFormatSharing(
    profileProvider: DeltaSharingProfileProvider,
    timeoutInSeconds: Int = 120,
    numRetries: Int = 10,
    maxRetryDuration: Long = Long.MaxValue,
    sslTrustAll: Boolean = false,
    forStreaming: Boolean = false,
    responseFormat: String = DeltaSharingRestClient.RESPONSE_FORMAT_DELTA,
    readerFeatures: String = "",
    queryTablePaginationEnabled: Boolean = false,
    maxFilesPerReq: Int = 100000,
    enableAsyncQuery: Boolean = false,
    asyncQueryPollIntervalMillis: Long = 10000L,
    asyncQueryMaxDuration: Long = 600000L,
    tokenExchangeMaxRetries: Int = 5,
    tokenExchangeMaxRetryDurationInSeconds: Int = 60,
    tokenRenewalThresholdInSeconds: Int = 600)
    extends DeltaSharingClient {

  assert(
    responseFormat == DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET ||
    (
      readerFeatures.contains("deletionVectors") &&
      readerFeatures.contains("columnMapping") &&
      readerFeatures.contains("timestampNtz")
    ),
    "deletionVectors, columnMapping, timestampNtz should be supported in all types of queries."
  )

  import TestClientForDeltaFormatSharing._

  override def listAllTables(): Seq[Table] = throw new UnsupportedOperationException("not needed")

  override def getMetadata(
      table: Table,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None): DeltaTableMetadata = {
    val iterator = SparkEnv.get.blockManager
      .get[String](getBlockId(table.name, "getMetadata", versionAsOf, timestampAsOf))
      .map(_.data.asInstanceOf[Iterator[String]])
      .getOrElse {
        throw new IllegalStateException(
          s"getMetadata is missing for: ${table.name}, versionAsOf:$versionAsOf, " +
          s"timestampAsOf:$timestampAsOf. This shouldn't happen in the unit test."
        )
      }
    // iterator.toSeq doesn't trigger CompletionIterator in BlockManager which releases the reader
    // lock on the underlying block. iterator hasNext does trigger it.
    val linesBuilder = Seq.newBuilder[String]
    while (iterator.hasNext) {
      linesBuilder += iterator.next()
    }
    if (table.name.contains("shared_parquet_table")) {
      val lines = linesBuilder.result()
      val protocol = JsonUtils.fromJson[SingleAction](lines(0)).protocol
      val metadata = JsonUtils.fromJson[SingleAction](lines(1)).metaData
      DeltaTableMetadata(
        version = versionAsOf.getOrElse(getTableVersion(table)),
        protocol = protocol,
        metadata = metadata,
        respondedFormat = DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET
      )
    } else {
      DeltaTableMetadata(
        version = versionAsOf.getOrElse(getTableVersion(table)),
        lines = linesBuilder.result(),
        respondedFormat = DeltaSharingRestClient.RESPONSE_FORMAT_DELTA
      )
    }
  }

  override def getTableVersion(table: Table, startingTimestamp: Option[String] = None): Long = {
    val versionOpt = SparkEnv.get.blockManager.getSingle[Long](
      getBlockId(table.name, "getTableVersion")
    )
    val version = versionOpt.getOrElse {
      throw new IllegalStateException(
        s"getTableVersion is missing for: ${table.name}. This shouldn't happen in the unit test."
      )
    }
    SparkEnv.get.blockManager.releaseLock(getBlockId(table.name, "getTableVersion"))
    version
  }

  override def getFiles(
      table: Table,
      predicates: Seq[String],
      limit: Option[Long],
      versionAsOf: Option[Long],
      timestampAsOf: Option[String],
      jsonPredicateHints: Option[String],
      refreshToken: Option[String]
  ): DeltaTableFiles = {
    val tableFullName = s"${table.share}.${table.schema}.${table.name}"
    limit.foreach(lim => TestClientForDeltaFormatSharing.limits.put(tableFullName, lim))
    TestClientForDeltaFormatSharing.requestedFormat.put(tableFullName, responseFormat)
    jsonPredicateHints.foreach(p =>
      TestClientForDeltaFormatSharing.jsonPredicateHints.put(tableFullName, p))

    val iterator = SparkEnv.get.blockManager
      .get[String](getBlockId(
        table.name,
        "getFiles",
        versionAsOf = versionAsOf,
        timestampAsOf = timestampAsOf,
        limit = limit)
      )
      .map(_.data.asInstanceOf[Iterator[String]])
      .getOrElse {
        throw new IllegalStateException(
          s"getFiles is missing for: ${table.name} versionAsOf:$versionAsOf, " +
          s"timestampAsOf:$timestampAsOf, limit: $limit. This shouldn't happen in the unit test."
        )
      }
    // iterator.toSeq doesn't trigger CompletionIterator in BlockManager which releases the reader
    // lock on the underlying block. iterator hasNext does trigger it.
    val linesBuilder = Seq.newBuilder[String]
    while (iterator.hasNext) {
      linesBuilder += iterator.next()
    }
    if (table.name.contains("shared_parquet_table")) {
      val lines = linesBuilder.result()
      val protocol = JsonUtils.fromJson[SingleAction](lines(0)).protocol
      val metadata = JsonUtils.fromJson[SingleAction](lines(1)).metaData
      val files = ArrayBuffer[ClientAddFile]()
      lines.drop(2).foreach { line =>
        val action = JsonUtils.fromJson[SingleAction](line)
        if (action.file != null) {
          files.append(action.file)
        } else {
          throw new IllegalStateException(s"Unexpected Line:${line}")
        }
      }
      DeltaTableFiles(
        versionAsOf.getOrElse(getTableVersion(table)),
        protocol,
        metadata,
        files.toSeq,
        respondedFormat = DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET
      )
    } else {
      DeltaTableFiles(
        version = versionAsOf.getOrElse(getTableVersion(table)),
        lines = linesBuilder.result(),
        respondedFormat = DeltaSharingRestClient.RESPONSE_FORMAT_DELTA
      )
    }
  }

  override def getFiles(
      table: Table,
      startingVersion: Long,
      endingVersion: Option[Long]
  ): DeltaTableFiles = {
    assert(
      endingVersion.isDefined,
      "endingVersion is not defined. This shouldn't happen in unit test."
    )
    val iterator = SparkEnv.get.blockManager
      .get[String](getBlockId(table.name, s"getFiles_${startingVersion}_${endingVersion.get}"))
      .map(_.data.asInstanceOf[Iterator[String]])
      .getOrElse {
        throw new IllegalStateException(
          s"getFiles is missing for: ${table.name} with [${startingVersion}, " +
          s"${endingVersion.get}]. This shouldn't happen in the unit test."
        )
      }
    // iterator.toSeq doesn't trigger CompletionIterator in BlockManager which releases the reader
    // lock on the underlying block. iterator hasNext does trigger it.
    val linesBuilder = Seq.newBuilder[String]
    while (iterator.hasNext) {
      linesBuilder += iterator.next()
    }
    DeltaTableFiles(
      version = getTableVersion(table),
      lines = linesBuilder.result(),
      respondedFormat = DeltaSharingRestClient.RESPONSE_FORMAT_DELTA
    )
  }

  override def getCDFFiles(
      table: Table,
      cdfOptions: Map[String, String],
      includeHistoricalMetadata: Boolean
  ): DeltaTableFiles = {
    val suffix = cdfOptions
      .get(DeltaSharingOptions.CDF_START_VERSION)
      .getOrElse(
        cdfOptions.get(DeltaSharingOptions.CDF_START_TIMESTAMP).get
      )
    val iterator = SparkEnv.get.blockManager
      .get[String](
        getBlockId(
          table.name,
          s"getCDFFiles_$suffix"
        )
      )
      .map(
        _.data.asInstanceOf[Iterator[String]]
      )
      .getOrElse {
        throw new IllegalStateException(
          s"getCDFFiles is missing for: ${table.name}. This shouldn't happen in the unit test."
        )
      }
    // iterator.toSeq doesn't trigger CompletionIterator in BlockManager which releases the reader
    // lock on the underlying block. iterator hasNext does trigger it.
    val linesBuilder = Seq.newBuilder[String]
    while (iterator.hasNext) {
      linesBuilder += iterator.next()
    }
    DeltaTableFiles(
      version = getTableVersion(table),
      lines = linesBuilder.result(),
      respondedFormat = DeltaSharingRestClient.RESPONSE_FORMAT_DELTA
    )
  }

  override def getForStreaming(): Boolean = forStreaming

  override def getProfileProvider: DeltaSharingProfileProvider = profileProvider
}

object TestClientForDeltaFormatSharing {
  def getBlockId(
      sharedTableName: String,
      queryType: String,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None,
      limit: Option[Long] = None): BlockId = {
    assert(!(versionAsOf.isDefined && timestampAsOf.isDefined))
    val suffix = if (versionAsOf.isDefined) {
      s"_v${versionAsOf.get}"
    } else if (timestampAsOf.isDefined) {
      s"_t${timestampAsOf.get}"
    } else {
      ""
    }
    val limitSuffix = limit.map{ l => s"_l${l}"}.getOrElse("")
    BlockId(
      s"${DeltaSharingUtils.DELTA_SHARING_BLOCK_ID_PREFIX}" +
      s"_${sharedTableName}_$queryType$suffix$limitSuffix"
    )
  }

  val limits = scala.collection.mutable.Map[String, Long]()
  val requestedFormat = scala.collection.mutable.Map[String, String]()
  val jsonPredicateHints = scala.collection.mutable.Map[String, String]()
}
