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

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.actions.{
  Action,
  AddCDCFile,
  AddFile,
  DeletionVectorDescriptor,
  Metadata,
  RemoveFile
}
import org.apache.spark.sql.delta.deletionvectors.{
  RoaringBitmapArray,
  RoaringBitmapArrayFormat
}
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import com.google.common.hash.Hashing
import io.delta.sharing.client.model.{
  AddCDCFile => ClientAddCDCFile,
  AddFile => ClientAddFile,
  AddFileForCDF => ClientAddFileForCDF,
  Metadata => ClientMetadata,
  Protocol => ClientProtocol,
  RemoveFile => ClientRemoveFile
}
import io.delta.sharing.spark.model.{
  DeltaSharingFileAction,
  DeltaSharingMetadata,
  DeltaSharingProtocol
}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

trait DeltaSharingDataSourceDeltaTestUtils extends SharedSparkSession {

  override def beforeAll(): Unit = {
    super.beforeAll()
    // close DeltaSharingFileSystem to avoid impact from other unit tests.
    FileSystem.closeAll()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.delta.sharing.preSignedUrl.expirationMs", "30000")
      .set("spark.delta.sharing.driver.refreshCheckIntervalMs", "3000")
      .set("spark.delta.sharing.driver.refreshThresholdMs", "10000")
      .set("spark.delta.sharing.driver.accessThresholdToExpireMs", "300000")
  }

  private[spark] def removePartitionPrefix(filePath: String): String = {
    filePath.split("/").last
  }

  private def getResponseDVAndId(
      sharedTable: String,
      deletionVector: DeletionVectorDescriptor): (DeletionVectorDescriptor, String) = {
    if (deletionVector != null) {
      if (deletionVector.storageType == DeletionVectorDescriptor.INLINE_DV_MARKER) {
        (deletionVector, Hashing.sha256().hashString(deletionVector.uniqueId, UTF_8).toString)
      } else {
        val dvPath = deletionVector.absolutePath(new Path("not-used"))
        (
          deletionVector.copy(
            pathOrInlineDv = TestDeltaSharingFileSystem.encode(sharedTable,
              SparkPath.fromPathString(dvPath.getName).urlEncoded),
            storageType = DeletionVectorDescriptor.PATH_DV_MARKER
          ),
          Hashing.sha256().hashString(deletionVector.uniqueId, UTF_8).toString
        )
      }
    } else {
      (null, null)
    }
  }

  private def isDataFile(filePath: String): Boolean = {
    filePath.endsWith(".parquet") || filePath.endsWith(".bin")
  }

  // Convert from delta AddFile to DeltaSharingFileAction to serialize to json.
  private def getDeltaSharingFileActionForAddFile(
      addFile: AddFile,
      sharedTable: String,
      version: Long,
      timestamp: Long): DeltaSharingFileAction = {
    val parquetFile = removePartitionPrefix(addFile.path)

    val (responseDV, dvFileId) = getResponseDVAndId(sharedTable, addFile.deletionVector)

    DeltaSharingFileAction(
      id = Hashing.sha256().hashString(parquetFile, UTF_8).toString,
      version = version,
      timestamp = timestamp,
      deletionVectorFileId = dvFileId,
      deltaSingleAction = addFile
        .copy(
          path = TestDeltaSharingFileSystem.encode(sharedTable, parquetFile),
          deletionVector = responseDV
        )
        .wrap
    )
  }

  // Convert from delta RemoveFile to DeltaSharingFileAction to serialize to json.
  // scalastyle:off removeFile
  private def getDeltaSharingFileActionForRemoveFile(
      removeFile: RemoveFile,
      sharedTable: String,
      version: Long,
      timestamp: Long): DeltaSharingFileAction = {
    val parquetFile = removePartitionPrefix(removeFile.path)

    val (responseDV, dvFileId) = getResponseDVAndId(sharedTable, removeFile.deletionVector)

    DeltaSharingFileAction(
      id = Hashing.sha256().hashString(parquetFile, UTF_8).toString,
      version = version,
      timestamp = timestamp,
      deletionVectorFileId = dvFileId,
      deltaSingleAction = removeFile
        .copy(
          path = TestDeltaSharingFileSystem.encode(sharedTable, parquetFile),
          deletionVector = responseDV
        )
        .wrap
    )
    // scalastyle:on removeFile
  }

  // Reset the result for client.GetTableVersion for the sharedTable based on the latest table
  // version of the deltaTable, use BlockManager to store the result.
  private[spark] def prepareMockedClientGetTableVersion(
      deltaTable: String,
      sharedTable: String,
      inputVersion: Option[Long] = None): Unit = {
    DeltaSharingUtils.overrideSingleBlock[Long](
      blockId = TestClientForDeltaFormatSharing.getBlockId(sharedTable, "getTableVersion"),
      value = inputVersion.getOrElse(getSnapshotToUse(deltaTable, None).version)
    )
  }

  def getTimeStampForVersion(deltaTable: String, version: Long): Long = {
    val snapshotToUse = getSnapshotToUse(deltaTable, None)
    FileUtils
      .listFiles(new File(snapshotToUse.deltaLog.logPath.toUri()), null, true)
      .asScala
      .foreach { f =>
        if (FileNames.isDeltaFile(new Path(f.getName))) {
          if (FileNames.getFileVersion(new Path(f.getName)) == version) {
            return f.lastModified
          }
        }
      }
    0
  }

  // Prepare the result(Protocol and Metadata) for client.GetMetadata for the sharedTable based on
  // the latest table info of the deltaTable, store them in BlockManager.
  private[spark] def prepareMockedClientMetadata(
      deltaTable: String,
      sharedTable: String,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None): Unit = {
    val snapshotToUse = getSnapshotToUse(deltaTable, versionAsOf)
    val dsProtocol: DeltaSharingProtocol = DeltaSharingProtocol(snapshotToUse.protocol)
    val dsMetadata: DeltaSharingMetadata = DeltaSharingMetadata(
      deltaMetadata = snapshotToUse.metadata
    )

    // Put the metadata in blockManager for DeltaSharingClient to return for getMetadata. The block
    // is keyed by versionAsOf/timestampAsOf so callers can mock the getMetadata pinned to a
    // specific boundary (e.g. the end of a CDF range).
    DeltaSharingUtils.overrideIteratorBlock[String](
      blockId = TestClientForDeltaFormatSharing.getBlockId(
        sharedTable, "getMetadata", versionAsOf = versionAsOf, timestampAsOf = timestampAsOf),
      values = Seq(dsProtocol.json, dsMetadata.json).toIterator
    )
  }

  private def updateAddFileWithInlineDV(
      addFile: AddFile,
      inlineDvFormat: RoaringBitmapArrayFormat.Value,
      bitmap: RoaringBitmapArray): AddFile = {
    val dv = DeletionVectorDescriptor.inlineInLog(
      bitmap.serializeAsByteArray(inlineDvFormat),
      bitmap.cardinality
    )
    addFile
      .removeRows(
        deletionVector = dv,
        updateStats = true
      )
      ._1
  }

  private def updateDvPathToCount(
      addFile: AddFile,
      pathToCount: scala.collection.mutable.Map[String, Int]): Unit = {
    if (addFile.deletionVector != null &&
      addFile.deletionVector.storageType != DeletionVectorDescriptor.INLINE_DV_MARKER) {
      val dvPath = addFile.deletionVector.pathOrInlineDv
      pathToCount.put(dvPath, pathToCount.getOrElse(dvPath, 0) + 1)
    }
  }

  // Sort by id in decreasing order.
  private def deltaSharingFileActionDecreaseOrderFunc(
      f1: model.DeltaSharingFileAction,
      f2: model.DeltaSharingFileAction): Boolean = {
    f1.id > f2.id
  }

  // Sort by id in increasing order.
  private def deltaSharingFileActionIncreaseOrderFunc(
      f1: model.DeltaSharingFileAction,
      f2: model.DeltaSharingFileAction): Boolean = {
    f1.id < f2.id
  }

  private def getSnapshotToUse(deltaTable: String, versionAsOf: Option[Long]): Snapshot = {
    val deltaLog = DeltaLog.forTable(spark, new TableIdentifier(deltaTable))
    if (versionAsOf.isDefined) {
      deltaLog.getSnapshotAt(versionAsOf.get)
    } else {
      deltaLog.update()
    }
  }

  // This function does 2 jobs:
  // 1. Prepare the result for functions of delta sharing rest client, i.e., (Protocol, Metadata)
  // for getMetadata, (Protocol, Metadata, and list of lines from delta actions) for getFiles, use
  // BlockManager to store the data to make them available across different classes. All the lines
  // are for responseFormat=parquet.
  // 2. Put the parquet file in blockManager for DeltaSharingFileSystem to load bytes out of it.
  private[spark] def prepareMockedClientAndFileSystemResultForParquet(
      deltaTable: String,
      sharedTable: String,
      versionAsOf: Option[Long] = None,
      limitHint: Option[Long] = None): Unit = {
    val lines = Seq.newBuilder[String]
    var totalSize = 0L
    val clientAddFilesArrayBuffer = ArrayBuffer[ClientAddFile]()

    // To prepare faked delta sharing responses with needed files for DeltaSharingClient.
    val snapshotToUse = getSnapshotToUse(deltaTable, versionAsOf)

    snapshotToUse.allFiles.collect().foreach { addFile =>
      val parquetFile = removePartitionPrefix(addFile.path)
      val clientAddFile = ClientAddFile(
        url = TestDeltaSharingFileSystem.encode(sharedTable, parquetFile),
        id = Hashing.md5().hashString(parquetFile, UTF_8).toString,
        partitionValues = addFile.partitionValues,
        size = addFile.size,
        stats = null,
        version = snapshotToUse.version,
        timestamp = snapshotToUse.timestamp
      )
      totalSize = totalSize + addFile.size
      clientAddFilesArrayBuffer += clientAddFile
    }

    // Scan through the parquet files of the local delta table, and prepare the data of parquet file
    // reading in DeltaSharingFileSystem.
    val files =
      FileUtils.listFiles(new File(snapshotToUse.deltaLog.dataPath.toUri()), null, true).asScala
    files.foreach { f =>
      val filePath = f.getCanonicalPath
      val fileName = SparkPath.fromPathString(f.getName).urlEncoded
      if (isDataFile(filePath)) {
        // Put the parquet file in blockManager for DeltaSharingFileSystem to load bytes out of it.
        DeltaSharingUtils.overrideIteratorBlock[Byte](
          blockId = TestDeltaSharingFileSystem.getBlockId(sharedTable, fileName),
          values = FileUtils.readFileToByteArray(f).toIterator
        )
      }
    }

    val clientProtocol = ClientProtocol(minReaderVersion = 1)
    // This is specifically to set the size of the metadata.
    val deltaMetadata = snapshotToUse.metadata
    val clientMetadata = ClientMetadata(
      id = deltaMetadata.id,
      name = deltaMetadata.name,
      description = deltaMetadata.description,
      schemaString = deltaMetadata.schemaString,
      configuration = deltaMetadata.configuration,
      partitionColumns = deltaMetadata.partitionColumns,
      size = totalSize
    )
    lines += JsonUtils.toJson(clientProtocol.wrap)
    lines += JsonUtils.toJson(clientMetadata.wrap)
    clientAddFilesArrayBuffer.toSeq.foreach { clientAddFile =>
      lines += JsonUtils.toJson(clientAddFile.wrap)
    }

    // Put the metadata in blockManager for DeltaSharingClient to return metadata when being asked.
    DeltaSharingUtils.overrideIteratorBlock[String](
      blockId = TestClientForDeltaFormatSharing.getBlockId(
        sharedTableName = sharedTable,
        queryType = "getMetadata",
        versionAsOf = versionAsOf
      ),
      values = Seq(
        JsonUtils.toJson(clientProtocol.wrap),
        JsonUtils.toJson(clientMetadata.wrap)
      ).toIterator
    )

    // Put the delta log (list of actions) in blockManager for DeltaSharingClient to return as the
    // http response when getFiles is called.
    DeltaSharingUtils.overrideIteratorBlock[String](
      blockId = TestClientForDeltaFormatSharing.getBlockId(
        sharedTableName = sharedTable,
        queryType = "getFiles",
        versionAsOf = versionAsOf,
        limit = limitHint
      ),
      values = lines.result().toIterator
    )
  }

  // This function does 2 jobs:
  // 1. Prepare the result for functions of delta sharing rest client, i.e., (Protocol, Metadata)
  // for getMetadata, (Protocol, Metadata, and list of lines from delta actions) for getFiles, use
  // BlockManager to store the data to make them available across different classes.
  // 2. Put the parquet file in blockManager for DeltaSharingFileSystem to load bytes out of it.
  private[spark] def prepareMockedClientAndFileSystemResult(
      deltaTable: String,
      sharedTable: String,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None,
      inlineDvFormat: Option[RoaringBitmapArrayFormat.Value] = None,
      assertMultipleDvsInOneFile: Boolean = false,
      reverseFileOrder: Boolean = false,
      limitHint: Option[Long] = None): Unit = {
    val lines = Seq.newBuilder[String]
    var totalSize = 0L

    // To prepare faked delta sharing responses with needed files for DeltaSharingClient.
    val snapshotToUse = getSnapshotToUse(deltaTable, versionAsOf)
    val fileActionsArrayBuffer = ArrayBuffer[model.DeltaSharingFileAction]()
    val dvPathToCount = scala.collection.mutable.Map[String, Int]()
    var numRecords = 0L
    snapshotToUse.allFiles.collect().foreach { addFile =>
      if (assertMultipleDvsInOneFile) {
        updateDvPathToCount(addFile, dvPathToCount)
      }

      val updatedAdd = if (inlineDvFormat.isDefined) {
        // Remove row 0 and 2 in the AddFile.
        updateAddFileWithInlineDV(addFile, inlineDvFormat.get, RoaringBitmapArray(0L, 2L))
      } else {
        addFile
      }

      if (limitHint.isEmpty || limitHint.map(_ > numRecords).getOrElse(true)) {
        val dsAddFile = getDeltaSharingFileActionForAddFile(
          updatedAdd,
          sharedTable,
          snapshotToUse.version,
          snapshotToUse.timestamp
        )
        numRecords += addFile.numLogicalRecords.getOrElse(0L)
        totalSize = totalSize + addFile.size
        fileActionsArrayBuffer += dsAddFile
      }
    }
    val fileActionSeq = if (reverseFileOrder) {
      fileActionsArrayBuffer.toSeq.sortWith(deltaSharingFileActionDecreaseOrderFunc)
    } else {
      fileActionsArrayBuffer.toSeq.sortWith(deltaSharingFileActionIncreaseOrderFunc)
    }
    var previousIdOpt: Option[String] = None
    fileActionSeq.foreach { fileAction =>
      if (reverseFileOrder) {
        assert(
          // Using < instead of <= because there can be a removeFile and addFile pointing to the
          // same parquet file which result in the same file id, since id is a hash of file path.
          // This is ok because eventually it can read data out of the correct parquet file.
          !previousIdOpt.exists(_ < fileAction.id),
          s"fileActions must be in decreasing order by id: ${previousIdOpt} is not smaller than" +
          s" ${fileAction.id}."
        )
        previousIdOpt = Some(fileAction.id)
      }
      lines += fileAction.json
    }
    if (assertMultipleDvsInOneFile) {
      assert(dvPathToCount.max._2 > 1)
    }

    // Scan through the parquet files of the local delta table, and prepare the data of parquet file
    // reading in DeltaSharingFileSystem.
    val files =
      FileUtils.listFiles(new File(snapshotToUse.deltaLog.dataPath.toUri()), null, true).asScala
    files.foreach { f =>
      val filePath = f.getCanonicalPath
      val fileName = SparkPath.fromPathString(f.getName).urlEncoded
      if (isDataFile(filePath)) {
        // Put the parquet file in blockManager for DeltaSharingFileSystem to load bytes out of it.
        DeltaSharingUtils.overrideIteratorBlock[Byte](
          blockId = TestDeltaSharingFileSystem.getBlockId(sharedTable, fileName),
          values = FileUtils.readFileToByteArray(f).toIterator
        )
      }
    }

    // This is specifically to set the size of the metadata.
    val dsMetadata = DeltaSharingMetadata(
      deltaMetadata = snapshotToUse.metadata,
      size = totalSize
    )
    val dsProtocol = DeltaSharingProtocol(deltaProtocol = snapshotToUse.protocol)
    // Put the metadata in blockManager for DeltaSharingClient to return metadata when being asked.
    DeltaSharingUtils.overrideIteratorBlock[String](
      blockId = TestClientForDeltaFormatSharing.getBlockId(
        sharedTableName = sharedTable,
        queryType = "getMetadata",
        versionAsOf = versionAsOf,
        timestampAsOf = timestampAsOf
      ),
      values = Seq(dsProtocol.json, dsMetadata.json).toIterator
    )

    lines += dsProtocol.json
    lines += dsMetadata.json
    // Put the delta log (list of actions) in blockManager for DeltaSharingClient to return as the
    // http response when getFiles is called.
    DeltaSharingUtils.overrideIteratorBlock[String](
      blockId = TestClientForDeltaFormatSharing.getBlockId(
        sharedTableName = sharedTable,
        queryType = "getFiles",
        versionAsOf = versionAsOf,
        timestampAsOf = timestampAsOf,
        limit = limitHint
      ),
      values = lines.result().toIterator
    )
  }

  private[spark] def prepareMockedClientAndFileSystemResultForStreaming(
      deltaTable: String,
      sharedTable: String,
      startingVersion: Long,
      endingVersion: Long,
      assertDVExists: Boolean = false): Unit = {
    val actionLines = Seq.newBuilder[String]

    var maxVersion = -1L
    var totalSize = 0L

    val deltaLog = DeltaLog.forTable(spark, new TableIdentifier(deltaTable))
    val startingSnapshot = deltaLog.getSnapshotAt(startingVersion)
    actionLines += DeltaSharingProtocol(deltaProtocol = startingSnapshot.protocol).json
    actionLines += DeltaSharingMetadata(
      deltaMetadata = startingSnapshot.metadata,
      version = startingVersion
    ).json

    val logFiles =
      FileUtils.listFiles(new File(deltaLog.logPath.toUri()), null, true).asScala
    var dvExists = false
    logFiles.foreach { f =>
      if (FileNames.isDeltaFile(new Path(f.getName))) {
        val version = FileNames.getFileVersion(new Path(f.getName))
        if (version >= startingVersion && version <= endingVersion) {
          // protocol/metadata are processed from startingSnapshot, only process versions greater
          // than startingVersion for real actions and possible metadata changes.
          maxVersion = maxVersion.max(version)
          val timestamp = f.lastModified

          FileUtils.readLines(f).asScala.foreach { l =>
            val action = Action.fromJson(l)
            action match {
              case m: Metadata =>
                actionLines += DeltaSharingMetadata(
                  deltaMetadata = m,
                  version = version
                ).json
              case addFile: AddFile if addFile.dataChange =>
                // Convert from delta AddFile to DeltaSharingAddFile to serialize to json.
                val dsAddFile =
                  getDeltaSharingFileActionForAddFile(addFile, sharedTable, version, timestamp)
                dvExists = dvExists || (dsAddFile.deletionVectorFileId != null)
                totalSize = totalSize + addFile.size
                actionLines += dsAddFile.json
              case removeFile: RemoveFile if removeFile.dataChange =>
                // scalastyle:off removeFile
                val dsRemoveFile = getDeltaSharingFileActionForRemoveFile(
                  removeFile,
                  sharedTable,
                  version,
                  timestamp
                )
                // scalastyle:on removeFile
                dvExists = dvExists || (dsRemoveFile.deletionVectorFileId != null)
                totalSize = totalSize + removeFile.size.getOrElse(0L)
                actionLines += dsRemoveFile.json
              case _ => // ignore all other actions such as CommitInfo.
            }
          }
        }
      }
    }
    val dataFiles =
      FileUtils.listFiles(new File(deltaLog.dataPath.toUri()), null, true).asScala
    dataFiles.foreach { f =>
      val fileName = SparkPath.fromPathString(f.getName).urlEncoded
      if (isDataFile(f.getCanonicalPath)) {
        DeltaSharingUtils.overrideIteratorBlock[Byte](
          blockId = TestDeltaSharingFileSystem.getBlockId(sharedTable, fileName),
          values = FileUtils.readFileToByteArray(f).toIterator
        )
      }
    }

    if (assertDVExists) {
      assert(dvExists, "There should be DV in the files returned from server.")
    }

    DeltaSharingUtils.overrideIteratorBlock[String](
      blockId = TestClientForDeltaFormatSharing.getBlockId(
        sharedTable,
        s"getFiles_${startingVersion}_$endingVersion"
      ),
      values = actionLines.result().toIterator
    )
  }

  // Convert a delta Metadata to the parquet-wire client Metadata used in parquet-format responses.
  private def getClientMetadataForParquet(
      deltaMetadata: Metadata,
      size: java.lang.Long = null): ClientMetadata = {
    ClientMetadata(
      id = deltaMetadata.id,
      name = deltaMetadata.name,
      description = deltaMetadata.description,
      schemaString = deltaMetadata.schemaString,
      configuration = deltaMetadata.configuration,
      partitionColumns = deltaMetadata.partitionColumns,
      size = size
    )
  }

  private[spark] def prepareMockedClientAndFileSystemResultForCdf(
      deltaTable: String,
      sharedTable: String,
      startingVersion: Long,
      startingTimestamp: Option[String] = None,
      inlineDvFormat: Option[RoaringBitmapArrayFormat.Value] = None,
      assertMultipleDvsInOneFile: Boolean = false,
      endingVersion: Option[Long] = None,
      responseFormat: String = DeltaSharingOptions.RESPONSE_FORMAT_DELTA): Unit = {
    val parquetFormat = responseFormat == DeltaSharingOptions.RESPONSE_FORMAT_PARQUET
    // File action lines (plus any mid-range metadata changes), in commit order. The protocol and
    // metadata header is prepended after the walk because the parquet metadata carries the
    // accumulated table size, which is only known once all file actions are seen.
    val actionLines = Seq.newBuilder[String]

    var maxVersion = -1L
    var totalSize = 0L

    val deltaLog = DeltaLog.forTable(spark, new TableIdentifier(deltaTable))
    val startingSnapshot = deltaLog.getSnapshotAt(startingVersion)

    val dvPathToCount = scala.collection.mutable.Map[String, Int]()
    val files =
      FileUtils.listFiles(new File(deltaLog.logPath.toUri()), null, true).asScala
    files.foreach { f =>
      if (FileNames.isDeltaFile(new Path(f.getName))) {
        val version = FileNames.getFileVersion(new Path(f.getName))
        if (version >= startingVersion && endingVersion.forall(version <= _)) {
          // protocol/metadata are processed from startingSnapshot, only process versions greater
          // than startingVersion for real actions and possible metadata changes. When endingVersion
          // is set, skip versions past it so the mocked response is bounded like a server that
          // honors the endingVersion/endingTimestamp request bound.
          maxVersion = maxVersion.max(version)
          val timestamp = f.lastModified
          val versionActions = FileUtils.readLines(f).asScala.toSeq.map(Action.fromJson)
          // A commit that produced CDC files (_change_data) is authoritative for CDF via those
          // files; delta's CDF reader then ignores the data-rewrite add/remove of that commit (e.g.
          // a partition-changing UPDATE rewrites files AND writes update_pre/postimage CDC). The
          // delta-format response carries everything and lets the delta reader sort it out, but the
          // parquet CDF reader has no such logic, so for parquet we skip those add/remove files to
          // avoid double-counting them as insert/delete.
          val versionHasCdc = versionActions.exists(_.isInstanceOf[AddCDCFile])
          versionActions.foreach { action =>
            action match {
              case m: Metadata =>
                if (parquetFormat) {
                  actionLines += JsonUtils.toJson(getClientMetadataForParquet(m).wrap)
                } else {
                  actionLines += DeltaSharingMetadata(
                    deltaMetadata = m,
                    version = version
                  ).json
                }
              case addFile: AddFile if addFile.dataChange =>
                if (assertMultipleDvsInOneFile) {
                  updateDvPathToCount(addFile, dvPathToCount)
                }
                val updatedAdd = if (inlineDvFormat.isDefined) {
                  // Remove row 0 and 1 in the AddFile.
                  updateAddFileWithInlineDV(addFile, inlineDvFormat.get, RoaringBitmapArray(0L, 1L))
                } else {
                  addFile
                }
                totalSize = totalSize + updatedAdd.size
                if (parquetFormat) {
                  // Skip data-rewrite adds when the commit's change is captured by CDC files.
                  if (!versionHasCdc) {
                    val parquetFile = removePartitionPrefix(updatedAdd.path)
                    actionLines += JsonUtils.toJson(
                      ClientAddFileForCDF(
                        url = TestDeltaSharingFileSystem.encode(sharedTable, parquetFile),
                        id = Hashing.md5().hashString(parquetFile, UTF_8).toString,
                        partitionValues = updatedAdd.partitionValues,
                        size = updatedAdd.size,
                        version = version,
                        timestamp = timestamp
                      ).wrap
                    )
                  }
                } else {
                  val dsAddFile =
                    getDeltaSharingFileActionForAddFile(updatedAdd, sharedTable, version, timestamp)
                  actionLines += dsAddFile.json
                }
              case removeFile: RemoveFile if removeFile.dataChange =>
                // scalastyle:off removeFile
                totalSize = totalSize + removeFile.size.getOrElse(0L)
                if (parquetFormat) {
                  // Skip data-rewrite removes when the commit's change is captured by CDC files.
                  if (!versionHasCdc) {
                    val parquetFile = removePartitionPrefix(removeFile.path)
                    actionLines += JsonUtils.toJson(
                      ClientRemoveFile(
                        url = TestDeltaSharingFileSystem.encode(sharedTable, parquetFile),
                        id = Hashing.md5().hashString(parquetFile, UTF_8).toString,
                        partitionValues =
                          Option(removeFile.partitionValues).getOrElse(Map.empty[String, String]),
                        size = removeFile.size.getOrElse(0L),
                        version = version,
                        timestamp = timestamp
                      ).wrap
                    )
                  }
                } else {
                  val dsRemoveFile = getDeltaSharingFileActionForRemoveFile(
                    removeFile,
                    sharedTable,
                    version,
                    timestamp
                  )
                  actionLines += dsRemoveFile.json
                }
                // scalastyle:on removeFile
              case cdcFile: AddCDCFile =>
                val parquetFile = removePartitionPrefix(cdcFile.path)
                totalSize = totalSize + cdcFile.size
                if (parquetFormat) {
                  actionLines += JsonUtils.toJson(
                    ClientAddCDCFile(
                      url = TestDeltaSharingFileSystem.encode(sharedTable, parquetFile),
                      id = Hashing.md5().hashString(parquetFile, UTF_8).toString,
                      partitionValues = cdcFile.partitionValues,
                      size = cdcFile.size,
                      version = version,
                      timestamp = timestamp
                    ).wrap
                  )
                } else {
                  // Convert from delta AddCDCFile to DeltaSharingFileAction to serialize to json.
                  val dsCDCFile = DeltaSharingFileAction(
                    id = Hashing.sha256().hashString(parquetFile, UTF_8).toString,
                    version = version,
                    timestamp = timestamp,
                    deltaSingleAction = cdcFile
                      .copy(
                        path = TestDeltaSharingFileSystem.encode(sharedTable, parquetFile)
                      )
                      .wrap
                  )
                  actionLines += dsCDCFile.json
                }
              case _ => // ignore other lines
            }
          }
        }
      }
    }
    val dataFiles =
      FileUtils.listFiles(new File(deltaLog.dataPath.toUri()), null, true).asScala
    dataFiles.foreach { f =>
      val filePath = f.getCanonicalPath
      val fileName = SparkPath.fromPathString(f.getName).urlEncoded
      if (isDataFile(filePath)) {
        DeltaSharingUtils.overrideIteratorBlock[Byte](
          blockId = TestDeltaSharingFileSystem.getBlockId(sharedTable, fileName),
          values = FileUtils.readFileToByteArray(f).toIterator
        )
      }
    }

    if (assertMultipleDvsInOneFile) {
      assert(dvPathToCount.max._2 > 1)
    }

    // Prepend the protocol/metadata header. For parquet the metadata carries the table size, which
    // is only known after walking the file actions above; for delta it mirrors the prior behavior.
    val headerLines = if (parquetFormat) {
      Seq(
        JsonUtils.toJson(ClientProtocol(minReaderVersion = 1).wrap),
        JsonUtils.toJson(getClientMetadataForParquet(startingSnapshot.metadata, totalSize).wrap)
      )
    } else {
      Seq(
        DeltaSharingProtocol(deltaProtocol = startingSnapshot.protocol).json,
        DeltaSharingMetadata(
          deltaMetadata = startingSnapshot.metadata,
          version = startingVersion
        ).json
      )
    }
    val resultLines = headerLines ++ actionLines.result()

    DeltaSharingUtils.overrideIteratorBlock[String](
      blockId =
        TestClientForDeltaFormatSharing.getBlockId(sharedTable, s"getCDFFiles_$startingVersion"),
      values = resultLines.toIterator
    )
    if (startingTimestamp.isDefined) {
      DeltaSharingUtils.overrideIteratorBlock[String](
        blockId = TestClientForDeltaFormatSharing.getBlockId(
          sharedTable,
          s"getCDFFiles_${startingTimestamp.get}"
        ),
        values = resultLines.toIterator
      )
    }
  }

  protected def getDeltaSharingClassesSQLConf: Map[String, String] = {
    Map(
      "fs.delta-sharing.impl" -> classOf[TestDeltaSharingFileSystem].getName,
      "spark.delta.sharing.client.class" ->
      classOf[TestClientForDeltaFormatSharing].getName,
      "spark.delta.sharing.profile.provider.class" ->
      "io.delta.sharing.client.DeltaSharingFileProfileProvider"
    )
  }

  /** Assert the response format recorded by the test client for the given table. */
  protected def assertRequestedFormat(tableName: String, expectedFormat: Seq[String]): Unit = {
    assert(
      expectedFormat ==
        TestClientForDeltaFormatSharing.requestedFormat.filter(_._1.contains(tableName)).map(_._2))
  }
}
