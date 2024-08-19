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

import java.nio.charset.StandardCharsets.UTF_8
import java.text.SimpleDateFormat
import java.util.{TimeZone, UUID}

import scala.reflect.ClassTag

import org.apache.spark.sql.delta.{
  ColumnMappingTableFeature,
  DeletionVectorsTableFeature,
  DeltaLog,
  DeltaParquetFileFormat,
  SnapshotDescriptor,
  TimestampNTZTableFeature
}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import com.google.common.hash.Hashing
import io.delta.sharing.client.{DeltaSharingClient, DeltaSharingRestClient}
import io.delta.sharing.client.model.{DeltaTableFiles, DeltaTableMetadata, Table}
import io.delta.sharing.client.util.JsonUtils

import org.apache.spark.SparkEnv
import org.apache.spark.delta.sharing.TableRefreshResult
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.storage.{BlockId, StorageLevel}

object DeltaSharingUtils extends Logging {

  val STREAMING_SUPPORTED_READER_FEATURES: Seq[String] =
    Seq(
      DeletionVectorsTableFeature.name,
      ColumnMappingTableFeature.name,
      TimestampNTZTableFeature.name
    )
  val SUPPORTED_READER_FEATURES: Seq[String] =
    Seq(
      DeletionVectorsTableFeature.name,
      ColumnMappingTableFeature.name,
      TimestampNTZTableFeature.name
    )

  // The prefix will be used for block ids of all blocks that store the delta log in BlockManager.
  // It's used to ensure delta sharing queries don't mess up with blocks with other applications.
  val DELTA_SHARING_BLOCK_ID_PREFIX = "test_delta-sharing"

  // Refresher function for CachedTableManager to use.
  // It takes refreshToken: Option[String] as a parameter and return TableRefreshResult.
  type RefresherFunction = Option[String] => TableRefreshResult

  case class DeltaSharingTableMetadata(
      version: Long,
      protocol: model.DeltaSharingProtocol,
      metadata: model.DeltaSharingMetadata
  )

  // A wrapper function for streaming query to get the latest version/protocol/metadata of the
  // shared table.
  def getDeltaSharingTableMetadata(
      client: DeltaSharingClient,
      table: Table): DeltaSharingTableMetadata = {
    val deltaTableMetadata = client.getMetadata(table)
    getDeltaSharingTableMetadata(table, deltaTableMetadata)
  }

  def queryDeltaTableMetadata(
      client: DeltaSharingClient,
      table: Table,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None): DeltaTableMetadata = {
    val deltaTableMetadata = client.getMetadata(table, versionAsOf, timestampAsOf)
    logInfo(
      s"getMetadata returned in ${deltaTableMetadata.respondedFormat} format for table " +
      s"$table with v_${versionAsOf.map(_.toString).getOrElse("None")} " +
      s"t_${timestampAsOf.getOrElse("None")} from delta sharing server."
    )
    deltaTableMetadata
  }

  /**
   * parse the protocol and metadata from rpc response for getMetadata.
   */
  def getDeltaSharingTableMetadata(
      table: Table,
      deltaTableMetadata: DeltaTableMetadata): DeltaSharingTableMetadata = {

    var metadataOption: Option[model.DeltaSharingMetadata] = None
    var protocolOption: Option[model.DeltaSharingProtocol] = None

    deltaTableMetadata.lines
      .map(
        JsonUtils.fromJson[model.DeltaSharingSingleAction](_).unwrap
      )
      .foreach {
        case m: model.DeltaSharingMetadata => metadataOption = Some(m)
        case p: model.DeltaSharingProtocol => protocolOption = Some(p)
        case _ => // ignore other lines
      }

    DeltaSharingTableMetadata(
      version = deltaTableMetadata.version,
      protocol = protocolOption.getOrElse {
        throw new IllegalStateException(
          s"Failed to get Protocol for ${table.toString}, " +
          s"response from server:${deltaTableMetadata.lines}."
        )
      },
      metadata = metadataOption.getOrElse {
        throw new IllegalStateException(
          s"Failed to get Metadata for ${table.toString}, " +
          s"response from server:${deltaTableMetadata.lines}."
        )
      }
    )
  }

  private def getTableRefreshResult(tableFiles: DeltaTableFiles): TableRefreshResult = {
    var minUrlExpiration: Option[Long] = None
    val idToUrl = tableFiles.lines
      .map(
        JsonUtils.fromJson[model.DeltaSharingSingleAction](_).unwrap
      )
      .collect {
        case fileAction: model.DeltaSharingFileAction =>
          if (fileAction.expirationTimestamp != null) {
            minUrlExpiration = minUrlExpiration
              .filter(_ < fileAction.expirationTimestamp)
              .orElse(Some(fileAction.expirationTimestamp))
          }
          fileAction.id -> fileAction.path
      }
      .toMap

    TableRefreshResult(idToUrl, minUrlExpiration, tableFiles.refreshToken)
  }

  /**
   * Get the refresher function for a delta sharing table who calls client.getFiles with the
   * provided parameters.
   *
   * @return A refresher function used by the CachedTableManager to refresh urls.
   */
  def getRefresherForGetFiles(
      client: DeltaSharingClient,
      table: Table,
      predicates: Seq[String],
      limit: Option[Long],
      versionAsOf: Option[Long],
      timestampAsOf: Option[String],
      jsonPredicateHints: Option[String]): RefresherFunction = { refreshTokenOpt =>
    {
      val tableFiles = client
        .getFiles(
          table = table,
          predicates = predicates,
          limit = limit,
          versionAsOf = versionAsOf,
          timestampAsOf = timestampAsOf,
          jsonPredicateHints = jsonPredicateHints,
          refreshToken = refreshTokenOpt
        )
      getTableRefreshResult(tableFiles)
    }
  }

  /**
   * Get the refresher function for a delta sharing table who calls client.getCDFFiles with the
   * provided parameters.
   *
   * @return A refresher function used by the CachedTableManager to refresh urls.
   */
  def getRefresherForGetCDFFiles(
      client: DeltaSharingClient,
      table: Table,
      cdfOptions: Map[String, String]): RefresherFunction = { (_: Option[String]) =>
    {
      val tableFiles = client.getCDFFiles(
        table = table,
        cdfOptions = cdfOptions,
        includeHistoricalMetadata = true
      )
      getTableRefreshResult(tableFiles)
    }
  }

  /**
   * Get the refresher function for a delta sharing table who calls client.getFiles with the
   * provided parameters.
   *
   * @return A refresher function used by the CachedTableManager to refresh urls.
   */
  def getRefresherForGetFilesWithStartingVersion(
      client: DeltaSharingClient,
      table: Table,
      startingVersion: Long,
      endingVersion: Option[Long]): RefresherFunction = { (_: Option[String]) =>
    {
      val tableFiles = client
        .getFiles(table = table, startingVersion = startingVersion, endingVersion = endingVersion)
      getTableRefreshResult(tableFiles)
    }
  }

  def overrideSingleBlock[T: ClassTag](blockId: BlockId, value: T): Unit = {
    assert(
      blockId.name.startsWith(DELTA_SHARING_BLOCK_ID_PREFIX),
      s"invalid delta sharing log block id: $blockId"
    )
    removeBlockForJsonLogIfExists(blockId)
    SparkEnv.get.blockManager.putSingle[T](
      blockId = blockId,
      value = value,
      level = StorageLevel.MEMORY_AND_DISK_SER,
      tellMaster = true
    )
  }

  def overrideIteratorBlock[T: ClassTag](blockId: BlockId, values: Iterator[T]): Unit = {
    assert(
      blockId.name.startsWith(DELTA_SHARING_BLOCK_ID_PREFIX),
      s"invalid delta sharing log block id: $blockId"
    )
    removeBlockForJsonLogIfExists(blockId)
    SparkEnv.get.blockManager.putIterator[T](
      blockId = blockId,
      values = values,
      level = StorageLevel.MEMORY_AND_DISK_SER,
      tellMaster = true
    )
  }

  // A helper function used by DeltaSharingSource and DeltaSharingDataSource to get
  // SnapshotDescriptor used for delta sharing streaming.
  def getDeltaLogAndSnapshotDescriptor(
      spark: SparkSession,
      deltaSharingTableMetadata: DeltaSharingTableMetadata,
      customTablePathWithUUIDSuffix: String): (DeltaLog, SnapshotDescriptor) = {
    // Create a delta log with metadata at version 0.
    // Used by DeltaSharingSource to initialize a DeltaLog class, which is then used to initialize
    // a DeltaSource class, also the metadata id will be used for schemaTrackingLocation.
    DeltaSharingLogFileSystem.constructDeltaLogWithMetadataAtVersionZero(
      customTablePathWithUUIDSuffix,
      deltaSharingTableMetadata
    )
    val tablePath = DeltaSharingLogFileSystem.encode(customTablePathWithUUIDSuffix).toString
    val localDeltaLog = DeltaLog.forTable(spark, tablePath)
    (
      localDeltaLog,
      new SnapshotDescriptor {
        val deltaLog: DeltaLog = localDeltaLog
        val metadata: Metadata = deltaSharingTableMetadata.metadata.deltaMetadata
        val protocol: Protocol = deltaSharingTableMetadata.protocol.deltaProtocol
        val version = deltaSharingTableMetadata.version
        val numOfFilesIfKnown = None
        val sizeInBytesIfKnown = None
      }
    )
  }

  // Get a query hash id based on the query parameters: time travel options and filters.
  // The id concatenated with table name and used in local DeltaLog and CachedTableManager.
  // This is to uniquely identify the delta sharing table used twice in the same query but with
  // different query parameters, so we can differentiate their delta log and entries in the
  // CachedTableManager.
  private[sharing] def getQueryParamsHashId(
      options: DeltaSharingOptions,
      partitionFiltersString: String,
      dataFiltersString: String,
      jsonPredicateHints: String,
      limitHint: String,
      version: Long): String = {
    val fullQueryString = s"${options.versionAsOf}_${options.timestampAsOf}_" +
      s"${partitionFiltersString}_${dataFiltersString}_${jsonPredicateHints}_${limitHint}_" +
      s"${version}"
    Hashing.sha256().hashString(fullQueryString, UTF_8).toString
  }

  // Get a query hash id based on the query parameters: cdfOptions.
  // The id concatenated with table name and used in local DeltaLoc and CachedTableManager.
  // This is to uniquely identify the delta sharing table used twice in the same query but with
  // different query parameters, so we can differentiate their delta log and entries in the
  // CachedTableManager.
  private[sharing] def getQueryParamsHashId(cdfOptions: Map[String, String]): String = {
    Hashing.sha256().hashString(cdfOptions.toString, UTF_8).toString
  }

  // Concatenate table path with an id as a suffix, to uniquely identify a delta sharing table and
  // its corresponding delta log in a query.
  private[sharing] def getTablePathWithIdSuffix(customTablePath: String, id: String): String = {
    s"${customTablePath}_${id}"
  }

  // Get a unique string composed of a formatted timestamp and an uuid.
  // Used as a suffix for the table name and its delta log path of a delta sharing table in a
  // streaming job, to avoid overwriting the delta log from multiple references of the same delta
  // sharing table in one streaming job.
  private[sharing] def getFormattedTimestampWithUUID(): String = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss")
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    val formattedDateTime = dateFormat.format(System.currentTimeMillis())
    val uuid = UUID.randomUUID().toString().split('-').head
    s"${formattedDateTime}_${uuid}"
  }

  private def removeBlockForJsonLogIfExists(blockId: BlockId): Unit = {
    val blockManager = SparkEnv.get.blockManager
    blockManager.getMatchingBlockIds(_.name == blockId.name).foreach { b =>
      logWarning(s"Found and removing existing block for $blockId.")
      blockManager.removeBlock(b)
    }
  }

  // This is a base64 encoded string of the content of an empty delta checkpoint file.
  // Will be used to fake a checkpoint file in the locally constructed delta log for cdf and
  // streaming queries.
  val FAKE_CHECKPOINT_FILE_BASE64_ENCODED_STRING =
    """
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
BhUIAAAHGAMAAAADAAAVABUOFRIV+tzH6QMcFQQVABUGFQgAAAcYAwAAAAMAABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYE
ABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYE
ABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYE
ABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYE
ABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYE
ABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYE
ABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYE
ABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRAhkYBnRlc3RJZBkY
BnRlc3RJZBUCGRYCABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRAhkYB3BhcnF1ZXQZGAdwYXJxdWV0FQIZFgIAGREB
GRgAGRgAFQIZFgQAGREBGRgAGRgAFQIZFgQAGREBGRgAGRgAFQIZFgQAGREBGRgAGRgAFQIZFgQAGREBGRgAGRgAFQIZFgQAGREB
GRgAGRgAFQIZFgQAGREBGRgAGRgAFQIZFgQAGRECGRgEAQAAABkYBAEAAAAVAhkWAgAZEQIZGAQCAAAAGRgEAgAAABUCGRYCABkR
ARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkRARkYABkYABUCGRYEABkc
FggVQBYAAAAZHBZIFUAWAAAAGRwWiAEVQBYAAAAZHBbIARVAFgAAABkcFogCFUwWAAAAGRwW1AIVThYAAAAZHBaiAxVAFgAAABkc
FuIDFUAWAAAAGRwWogQVQBYAAAAZHBbiBBVMFgAAABkcFq4FFU4WAAAAGRwW/AUVQBYAAAAZHBa8BhVAFgAAABkcFvwGFUAWAAAA
GRwWvAcVQBYAAAAZHBb8BxVAFgAAABkcFrwIFUAWAAAAGRwW/AgVQBYAAAAZHBa8CRVAFgAAABkcFvwJFUAWAAAAGRwWvAoVQBYA
AAAZHBb8ChVAFgAAABkcFrwLFUAWAAAAGRwW/AsVQBYAAAAZHBa8DBVAFgAAABkcFvwMFUwWAAAAGRwWyA0VThYAAAAZHBaWDhVA
FgAAABkcFtYOFUAWAAAAGRwWlg8VQBYAAAAZHBbWDxVAFgAAABkcFpYQFUAWAAAAGRwW1hAVQBYAAAAZHBaWERVAFgAAABkcFtYR
FUAWAAAAGRwWlhIVQBYAAAAZHBbWEhVUFgAAABkcFqoTFUAWAAAAGRwW6hMVQBYAAAAZHBaqFBVWFgAAABkcFoAVFUwWAAAAGRwW
zBUVTBYAAAAZHBaYFhVAFgAAABkcFtgWFU4WAAAAGRwWphcVTBYAAAAZHBbyFxVOFgAAABkcFsAYFUAWAAAAGRwWgBkVSBYAAAAZ
HBbIGRVIFgAAABkcFpAaFU4WAAAAGRwW3hoVThYAAAAZHBasGxVAFgAAABkcFuwbFUAWAAAAGRwWrBwVQBYAAAAVAhn8UUgMc3Bh
cmtfc2NoZW1hFQwANQIYA3R4bhUGABUMJQIYBWFwcElkJQBMHAAAABUEJQIYB3ZlcnNpb24AFQQlAhgLbGFzdFVwZGF0ZWQANQIY
A2FkZBUWABUMJQIYBHBhdGglAEwcAAAANQIYD3BhcnRpdGlvblZhbHVlcxUCFQJMLAAAADUEGAlrZXlfdmFsdWUVBAAVDCUAGANr
ZXklAEwcAAAAFQwlAhgFdmFsdWUlAEwcAAAAFQQlAhgEc2l6ZQAVBCUCGBBtb2RpZmljYXRpb25UaW1lABUAJQIYCmRhdGFDaGFu
Z2UANQIYBHRhZ3MVAhUCTCwAAAA1BBgJa2V5X3ZhbHVlFQQAFQwlABgDa2V5JQBMHAAAABUMJQIYBXZhbHVlJQBMHAAAADUCGA5k
ZWxldGlvblZlY3RvchUMABUMJQIYC3N0b3JhZ2VUeXBlJQBMHAAAABUMJQIYDnBhdGhPcklubGluZUR2JQBMHAAAABUCJQIYBm9m
ZnNldAAVAiUCGAtzaXplSW5CeXRlcwAVBCUCGAtjYXJkaW5hbGl0eQAVBCUCGAttYXhSb3dJbmRleAAVBCUCGAliYXNlUm93SWQA
FQQlAhgXZGVmYXVsdFJvd0NvbW1pdFZlcnNpb24AFQwlAhgFc3RhdHMlAEwcAAAANQIYDHN0YXRzX3BhcnNlZBUCABUEJQIYCm51
bVJlY29yZHMANQIYBnJlbW92ZRUSABUMJQIYBHBhdGglAEwcAAAAFQQlAhgRZGVsZXRpb25UaW1lc3RhbXAAFQAlAhgKZGF0YUNo
YW5nZQAVACUCGBRleHRlbmRlZEZpbGVNZXRhZGF0YQA1AhgPcGFydGl0aW9uVmFsdWVzFQIVAkwsAAAANQQYCWtleV92YWx1ZRUE
ABUMJQAYA2tleSUATBwAAAAVDCUCGAV2YWx1ZSUATBwAAAAVBCUCGARzaXplADUCGA5kZWxldGlvblZlY3RvchUMABUMJQIYC3N0
b3JhZ2VUeXBlJQBMHAAAABUMJQIYDnBhdGhPcklubGluZUR2JQBMHAAAABUCJQIYBm9mZnNldAAVAiUCGAtzaXplSW5CeXRlcwAV
BCUCGAtjYXJkaW5hbGl0eQAVBCUCGAttYXhSb3dJbmRleAAVBCUCGAliYXNlUm93SWQAFQQlAhgXZGVmYXVsdFJvd0NvbW1pdFZl
cnNpb24ANQIYCG1ldGFEYXRhFRAAFQwlAhgCaWQlAEwcAAAAFQwlAhgEbmFtZSUATBwAAAAVDCUCGAtkZXNjcmlwdGlvbiUATBwA
AAA1AhgGZm9ybWF0FQQAFQwlAhgIcHJvdmlkZXIlAEwcAAAANQIYB29wdGlvbnMVAhUCTCwAAAA1BBgJa2V5X3ZhbHVlFQQAFQwl
ABgDa2V5JQBMHAAAABUMJQIYBXZhbHVlJQBMHAAAABUMJQIYDHNjaGVtYVN0cmluZyUATBwAAAA1AhgQcGFydGl0aW9uQ29sdW1u
cxUCFQZMPAAAADUEGARsaXN0FQIAFQwlAhgHZWxlbWVudCUATBwAAAA1AhgNY29uZmlndXJhdGlvbhUCFQJMLAAAADUEGAlrZXlf
dmFsdWUVBAAVDCUAGANrZXklAEwcAAAAFQwlAhgFdmFsdWUlAEwcAAAAFQQlAhgLY3JlYXRlZFRpbWUANQIYCHByb3RvY29sFQgA
FQIlAhgQbWluUmVhZGVyVmVyc2lvbgAVAiUCGBBtaW5Xcml0ZXJWZXJzaW9uADUCGA5yZWFkZXJGZWF0dXJlcxUCFQZMPAAAADUE
GARsaXN0FQIAFQwlAhgHZWxlbWVudCUATBwAAAA1AhgOd3JpdGVyRmVhdHVyZXMVAhUGTDwAAAA1BBgEbGlzdBUCABUMJQIYB2Vs
ZW1lbnQlAEwcAAAANQIYDmRvbWFpbk1ldGFkYXRhFQYAFQwlAhgGZG9tYWluJQBMHAAAABUMJQIYDWNvbmZpZ3VyYXRpb24lAEwc
AAAAFQAlAhgHcmVtb3ZlZAAWBBkcGfw2JggcFQwZNQAGCBkoA3R4bgVhcHBJZBUCFgQWPBZAJgg8NgQAGRwVABUAFQIAABaUKhUU
FuwcFR4AJkgcFQQZNQAGCBkoA3R4bgd2ZXJzaW9uFQIWBBY8FkAmSDw2BAAZHBUAFQAVAgAAFqgqFRQWih0VHgAmiAEcFQQZNQAG
CBkoA3R4bgtsYXN0VXBkYXRlZBUCFgQWPBZAJogBPDYEABkcFQAVABUCAAAWvCoVFhaoHRUeACbIARwVDBk1AAYIGSgDYWRkBHBh
dGgVAhYEFjwWQCbIATw2BAAZHBUAFQAVAgAAFtIqFRYWxh0VHgAmiAIcFQwZJQAGGUgDYWRkD3BhcnRpdGlvblZhbHVlcwlrZXlf
dmFsdWUDa2V5FQIWBBZIFkwmiAI8NgQAGRwVABUAFQIAABboKhUWFuQdFR4AJtQCHBUMGSUABhlIA2FkZA9wYXJ0aXRpb25WYWx1
ZXMJa2V5X3ZhbHVlBXZhbHVlFQIWBBZKFk4m1AI8NgQAGRwVABUAFQIAABb+KhUWFoIeFR4AJqIDHBUEGTUABggZKANhZGQEc2l6
ZRUCFgQWPBZAJqIDPDYEABkcFQAVABUCAAAWlCsVFhagHhUeACbiAxwVBBk1AAYIGSgDYWRkEG1vZGlmaWNhdGlvblRpbWUVAhYE
FjwWQCbiAzw2BAAZHBUAFQAVAgAAFqorFRYWvh4VHgAmogQcFQAZNQAGCBkoA2FkZApkYXRhQ2hhbmdlFQIWBBY8FkAmogQ8NgQA
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
c3BhcmsudGltZVpvbmUYE0FtZXJpY2EvTG9zX0FuZ2VsZXMAGBxvcmcuYXBhY2hlLnNwYXJrLmxlZ2FjeUlOVDk2GAAAGBhvcmcu
YXBhY2hlLnNwYXJrLnZlcnNpb24YBTQuMC4wABgpb3JnLmFwYWNoZS5zcGFyay5zcWwucGFycXVldC5yb3cubWV0YWRhdGEYiyV7
InR5cGUiOiJzdHJ1Y3QiLCJmaWVsZHMiOlt7Im5hbWUiOiJ0eG4iLCJ0eXBlIjp7InR5cGUiOiJzdHJ1Y3QiLCJmaWVsZHMiOlt7
Im5hbWUiOiJhcHBJZCIsInR5cGUiOiJzdHJpbmciLCJudWxsYWJsZSI6dHJ1ZSwibWV0YWRhdGEiOnt9fSx7Im5hbWUiOiJ2ZXJz
aW9uIiwidHlwZSI6ImxvbmciLCJudWxsYWJsZSI6dHJ1ZSwibWV0YWRhdGEiOnt9fSx7Im5hbWUiOiJsYXN0VXBkYXRlZCIsInR5
cGUiOiJsb25nIiwibnVsbGFibGUiOnRydWUsIm1ldGFkYXRhIjp7fX1dfSwibnVsbGFibGUiOnRydWUsIm1ldGFkYXRhIjp7fX0s
eyJuYW1lIjoiYWRkIiwidHlwZSI6eyJ0eXBlIjoic3RydWN0IiwiZmllbGRzIjpbeyJuYW1lIjoicGF0aCIsInR5cGUiOiJzdHJp
bmciLCJudWxsYWJsZSI6dHJ1ZSwibWV0YWRhdGEiOnt9fSx7Im5hbWUiOiJwYXJ0aXRpb25WYWx1ZXMiLCJ0eXBlIjp7InR5cGUi
OiJtYXAiLCJrZXlUeXBlIjoic3RyaW5nIiwidmFsdWVUeXBlIjoic3RyaW5nIiwidmFsdWVDb250YWluc051bGwiOnRydWV9LCJu
dWxsYWJsZSI6dHJ1ZSwibWV0YWRhdGEiOnt9fSx7Im5hbWUiOiJzaXplIiwidHlwZSI6ImxvbmciLCJudWxsYWJsZSI6dHJ1ZSwi
bWV0YWRhdGEiOnt9fSx7Im5hbWUiOiJtb2RpZmljYXRpb25UaW1lIiwidHlwZSI6ImxvbmciLCJudWxsYWJsZSI6dHJ1ZSwibWV0
YWRhdGEiOnt9fSx7Im5hbWUiOiJkYXRhQ2hhbmdlIiwidHlwZSI6ImJvb2xlYW4iLCJudWxsYWJsZSI6dHJ1ZSwibWV0YWRhdGEi
Ont9fSx7Im5hbWUiOiJ0YWdzIiwidHlwZSI6eyJ0eXBlIjoibWFwIiwia2V5VHlwZSI6InN0cmluZyIsInZhbHVlVHlwZSI6InN0
cmluZyIsInZhbHVlQ29udGFpbnNOdWxsIjp0cnVlfSwibnVsbGFibGUiOnRydWUsIm1ldGFkYXRhIjp7fX0seyJuYW1lIjoiZGVs
ZXRpb25WZWN0b3IiLCJ0eXBlIjp7InR5cGUiOiJzdHJ1Y3QiLCJmaWVsZHMiOlt7Im5hbWUiOiJzdG9yYWdlVHlwZSIsInR5cGUi
OiJzdHJpbmciLCJudWxsYWJsZSI6dHJ1ZSwibWV0YWRhdGEiOnt9fSx7Im5hbWUiOiJwYXRoT3JJbmxpbmVEdiIsInR5cGUiOiJz
dHJpbmciLCJudWxsYWJsZSI6dHJ1ZSwibWV0YWRhdGEiOnt9fSx7Im5hbWUiOiJvZmZzZXQiLCJ0eXBlIjoiaW50ZWdlciIsIm51
bGxhYmxlIjp0cnVlLCJtZXRhZGF0YSI6e319LHsibmFtZSI6InNpemVJbkJ5dGVzIiwidHlwZSI6ImludGVnZXIiLCJudWxsYWJs
ZSI6dHJ1ZSwibWV0YWRhdGEiOnt9fSx7Im5hbWUiOiJjYXJkaW5hbGl0eSIsInR5cGUiOiJsb25nIiwibnVsbGFibGUiOnRydWUs
Im1ldGFkYXRhIjp7fX0seyJuYW1lIjoibWF4Um93SW5kZXgiLCJ0eXBlIjoibG9uZyIsIm51bGxhYmxlIjp0cnVlLCJtZXRhZGF0
YSI6e319XX0sIm51bGxhYmxlIjp0cnVlLCJtZXRhZGF0YSI6e319LHsibmFtZSI6ImJhc2VSb3dJZCIsInR5cGUiOiJsb25nIiwi
bnVsbGFibGUiOnRydWUsIm1ldGFkYXRhIjp7fX0seyJuYW1lIjoiZGVmYXVsdFJvd0NvbW1pdFZlcnNpb24iLCJ0eXBlIjoibG9u
ZyIsIm51bGxhYmxlIjp0cnVlLCJtZXRhZGF0YSI6e319LHsibmFtZSI6InN0YXRzIiwidHlwZSI6InN0cmluZyIsIm51bGxhYmxl
Ijp0cnVlLCJtZXRhZGF0YSI6e319LHsibmFtZSI6InN0YXRzX3BhcnNlZCIsInR5cGUiOnsidHlwZSI6InN0cnVjdCIsImZpZWxk
cyI6W3sibmFtZSI6Im51bVJlY29yZHMiLCJ0eXBlIjoibG9uZyIsIm51bGxhYmxlIjp0cnVlLCJtZXRhZGF0YSI6e319XX0sIm51
bGxhYmxlIjp0cnVlLCJtZXRhZGF0YSI6e319XX0sIm51bGxhYmxlIjp0cnVlLCJtZXRhZGF0YSI6e319LHsibmFtZSI6InJlbW92
ZSIsInR5cGUiOnsidHlwZSI6InN0cnVjdCIsImZpZWxkcyI6W3sibmFtZSI6InBhdGgiLCJ0eXBlIjoic3RyaW5nIiwibnVsbGFi
bGUiOnRydWUsIm1ldGFkYXRhIjp7fX0seyJuYW1lIjoiZGVsZXRpb25UaW1lc3RhbXAiLCJ0eXBlIjoibG9uZyIsIm51bGxhYmxl
Ijp0cnVlLCJtZXRhZGF0YSI6e319LHsibmFtZSI6ImRhdGFDaGFuZ2UiLCJ0eXBlIjoiYm9vbGVhbiIsIm51bGxhYmxlIjp0cnVl
LCJtZXRhZGF0YSI6e319LHsibmFtZSI6ImV4dGVuZGVkRmlsZU1ldGFkYXRhIiwidHlwZSI6ImJvb2xlYW4iLCJudWxsYWJsZSI6
dHJ1ZSwibWV0YWRhdGEiOnt9fSx7Im5hbWUiOiJwYXJ0aXRpb25WYWx1ZXMiLCJ0eXBlIjp7InR5cGUiOiJtYXAiLCJrZXlUeXBl
Ijoic3RyaW5nIiwidmFsdWVUeXBlIjoic3RyaW5nIiwidmFsdWVDb250YWluc051bGwiOnRydWV9LCJudWxsYWJsZSI6dHJ1ZSwi
bWV0YWRhdGEiOnt9fSx7Im5hbWUiOiJzaXplIiwidHlwZSI6ImxvbmciLCJudWxsYWJsZSI6dHJ1ZSwibWV0YWRhdGEiOnt9fSx7
Im5hbWUiOiJkZWxldGlvblZlY3RvciIsInR5cGUiOnsidHlwZSI6InN0cnVjdCIsImZpZWxkcyI6W3sibmFtZSI6InN0b3JhZ2VU
eXBlIiwidHlwZSI6InN0cmluZyIsIm51bGxhYmxlIjp0cnVlLCJtZXRhZGF0YSI6e319LHsibmFtZSI6InBhdGhPcklubGluZUR2
IiwidHlwZSI6InN0cmluZyIsIm51bGxhYmxlIjp0cnVlLCJtZXRhZGF0YSI6e319LHsibmFtZSI6Im9mZnNldCIsInR5cGUiOiJp
bnRlZ2VyIiwibnVsbGFibGUiOnRydWUsIm1ldGFkYXRhIjp7fX0seyJuYW1lIjoic2l6ZUluQnl0ZXMiLCJ0eXBlIjoiaW50ZWdl
ciIsIm51bGxhYmxlIjp0cnVlLCJtZXRhZGF0YSI6e319LHsibmFtZSI6ImNhcmRpbmFsaXR5IiwidHlwZSI6ImxvbmciLCJudWxs
YWJsZSI6dHJ1ZSwibWV0YWRhdGEiOnt9fSx7Im5hbWUiOiJtYXhSb3dJbmRleCIsInR5cGUiOiJsb25nIiwibnVsbGFibGUiOnRy
dWUsIm1ldGFkYXRhIjp7fX1dfSwibnVsbGFibGUiOnRydWUsIm1ldGFkYXRhIjp7fX0seyJuYW1lIjoiYmFzZVJvd0lkIiwidHlw
ZSI6ImxvbmciLCJudWxsYWJsZSI6dHJ1ZSwibWV0YWRhdGEiOnt9fSx7Im5hbWUiOiJkZWZhdWx0Um93Q29tbWl0VmVyc2lvbiIs
InR5cGUiOiJsb25nIiwibnVsbGFibGUiOnRydWUsIm1ldGFkYXRhIjp7fX1dfSwibnVsbGFibGUiOnRydWUsIm1ldGFkYXRhIjp7
fX0seyJuYW1lIjoibWV0YURhdGEiLCJ0eXBlIjp7InR5cGUiOiJzdHJ1Y3QiLCJmaWVsZHMiOlt7Im5hbWUiOiJpZCIsInR5cGUi
OiJzdHJpbmciLCJudWxsYWJsZSI6dHJ1ZSwibWV0YWRhdGEiOnt9fSx7Im5hbWUiOiJuYW1lIiwidHlwZSI6InN0cmluZyIsIm51
bGxhYmxlIjp0cnVlLCJtZXRhZGF0YSI6e319LHsibmFtZSI6ImRlc2NyaXB0aW9uIiwidHlwZSI6InN0cmluZyIsIm51bGxhYmxl
Ijp0cnVlLCJtZXRhZGF0YSI6e319LHsibmFtZSI6ImZvcm1hdCIsInR5cGUiOnsidHlwZSI6InN0cnVjdCIsImZpZWxkcyI6W3si
bmFtZSI6InByb3ZpZGVyIiwidHlwZSI6InN0cmluZyIsIm51bGxhYmxlIjp0cnVlLCJtZXRhZGF0YSI6e319LHsibmFtZSI6Im9w
dGlvbnMiLCJ0eXBlIjp7InR5cGUiOiJtYXAiLCJrZXlUeXBlIjoic3RyaW5nIiwidmFsdWVUeXBlIjoic3RyaW5nIiwidmFsdWVD
b250YWluc051bGwiOnRydWV9LCJudWxsYWJsZSI6dHJ1ZSwibWV0YWRhdGEiOnt9fV19LCJudWxsYWJsZSI6dHJ1ZSwibWV0YWRh
dGEiOnt9fSx7Im5hbWUiOiJzY2hlbWFTdHJpbmciLCJ0eXBlIjoic3RyaW5nIiwibnVsbGFibGUiOnRydWUsIm1ldGFkYXRhIjp7
fX0seyJuYW1lIjoicGFydGl0aW9uQ29sdW1ucyIsInR5cGUiOnsidHlwZSI6ImFycmF5IiwiZWxlbWVudFR5cGUiOiJzdHJpbmci
LCJjb250YWluc051bGwiOnRydWV9LCJudWxsYWJsZSI6dHJ1ZSwibWV0YWRhdGEiOnt9fSx7Im5hbWUiOiJjb25maWd1cmF0aW9u
IiwidHlwZSI6eyJ0eXBlIjoibWFwIiwia2V5VHlwZSI6InN0cmluZyIsInZhbHVlVHlwZSI6InN0cmluZyIsInZhbHVlQ29udGFp
bnNOdWxsIjp0cnVlfSwibnVsbGFibGUiOnRydWUsIm1ldGFkYXRhIjp7fX0seyJuYW1lIjoiY3JlYXRlZFRpbWUiLCJ0eXBlIjoi
bG9uZyIsIm51bGxhYmxlIjp0cnVlLCJtZXRhZGF0YSI6e319XX0sIm51bGxhYmxlIjp0cnVlLCJtZXRhZGF0YSI6e319LHsibmFt
ZSI6InByb3RvY29sIiwidHlwZSI6eyJ0eXBlIjoic3RydWN0IiwiZmllbGRzIjpbeyJuYW1lIjoibWluUmVhZGVyVmVyc2lvbiIs
InR5cGUiOiJpbnRlZ2VyIiwibnVsbGFibGUiOnRydWUsIm1ldGFkYXRhIjp7fX0seyJuYW1lIjoibWluV3JpdGVyVmVyc2lvbiIs
InR5cGUiOiJpbnRlZ2VyIiwibnVsbGFibGUiOnRydWUsIm1ldGFkYXRhIjp7fX0seyJuYW1lIjoicmVhZGVyRmVhdHVyZXMiLCJ0
eXBlIjp7InR5cGUiOiJhcnJheSIsImVsZW1lbnRUeXBlIjoic3RyaW5nIiwiY29udGFpbnNOdWxsIjp0cnVlfSwibnVsbGFibGUi
OnRydWUsIm1ldGFkYXRhIjp7fX0seyJuYW1lIjoid3JpdGVyRmVhdHVyZXMiLCJ0eXBlIjp7InR5cGUiOiJhcnJheSIsImVsZW1l
bnRUeXBlIjoic3RyaW5nIiwiY29udGFpbnNOdWxsIjp0cnVlfSwibnVsbGFibGUiOnRydWUsIm1ldGFkYXRhIjp7fX1dfSwibnVs
bGFibGUiOnRydWUsIm1ldGFkYXRhIjp7fX0seyJuYW1lIjoiZG9tYWluTWV0YWRhdGEiLCJ0eXBlIjp7InR5cGUiOiJzdHJ1Y3Qi
LCJmaWVsZHMiOlt7Im5hbWUiOiJkb21haW4iLCJ0eXBlIjoic3RyaW5nIiwibnVsbGFibGUiOnRydWUsIm1ldGFkYXRhIjp7fX0s
eyJuYW1lIjoiY29uZmlndXJhdGlvbiIsInR5cGUiOiJzdHJpbmciLCJudWxsYWJsZSI6dHJ1ZSwibWV0YWRhdGEiOnt9fSx7Im5h
bWUiOiJyZW1vdmVkIiwidHlwZSI6ImJvb2xlYW4iLCJudWxsYWJsZSI6dHJ1ZSwibWV0YWRhdGEiOnt9fV19LCJudWxsYWJsZSI6
dHJ1ZSwibWV0YWRhdGEiOnt9fV19ABgfb3JnLmFwYWNoZS5zcGFyay5sZWdhY3lEYXRlVGltZRgAABhacGFycXVldC1tciB2ZXJz
aW9uIDEuMTIuMy1kYXRhYnJpY2tzLTAwMDIgKGJ1aWxkIDI0ODRhOTVkYmUxNmEwMDIzZTNlYjI5YzIwMWY5OWZmOWVhNzcxZWUp
Gfw2HAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAA
HAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAA
HAAAHAAAHAAAHAAAHAAAAJUqAABQQVIx""".stripMargin.replaceAll("\n", "")

  // Pre-prepare the byte array for (minVersion-1).checkpoint.parquet.
  val FAKE_CHECKPOINT_BYTE_ARRAY = {
    java.util.Base64.getDecoder.decode(FAKE_CHECKPOINT_FILE_BASE64_ENCODED_STRING)
  }
}
