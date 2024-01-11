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
  AddFile => ClientAddFile,
  Metadata => ClientMetadata,
  Protocol => ClientProtocol
}
import io.delta.sharing.spark.model.{
  DeltaSharingFileAction,
  DeltaSharingMetadata,
  DeltaSharingProtocol
}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
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
            pathOrInlineDv = TestDeltaSharingFileSystem.encode(sharedTable, dvPath.getName),
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
      sharedTable: String): Unit = {
    val snapshotToUse = getSnapshotToUse(deltaTable, None)
    DeltaSharingUtils.overrideSingleBlock[Long](
      blockId = TestClientForDeltaFormatSharing.getBlockId(sharedTable, "getTableVersion"),
      value = snapshotToUse.version
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
  private[spark] def prepareMockedClientMetadata(deltaTable: String, sharedTable: String): Unit = {
    val snapshotToUse = getSnapshotToUse(deltaTable, None)
    val dsProtocol: DeltaSharingProtocol = DeltaSharingProtocol(snapshotToUse.protocol)
    val dsMetadata: DeltaSharingMetadata = DeltaSharingMetadata(
      deltaMetadata = snapshotToUse.metadata
    )

    // Put the metadata in blockManager for DeltaSharingClient to return for getMetadata.
    DeltaSharingUtils.overrideIteratorBlock[String](
      blockId = TestClientForDeltaFormatSharing.getBlockId(sharedTable, "getMetadata"),
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
      versionAsOf: Option[Long] = None): Unit = {
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
      if (isDataFile(filePath)) {
        // Put the parquet file in blockManager for DeltaSharingFileSystem to load bytes out of it.
        DeltaSharingUtils.overrideIteratorBlock[Byte](
          blockId = TestDeltaSharingFileSystem.getBlockId(sharedTable, f.getName),
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
        versionAsOf = versionAsOf
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
      reverseFileOrder: Boolean = false): Unit = {
    val lines = Seq.newBuilder[String]
    var totalSize = 0L

    // To prepare faked delta sharing responses with needed files for DeltaSharingClient.
    val snapshotToUse = getSnapshotToUse(deltaTable, versionAsOf)
    val fileActionsArrayBuffer = ArrayBuffer[model.DeltaSharingFileAction]()
    val dvPathToCount = scala.collection.mutable.Map[String, Int]()
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

      val dsAddFile = getDeltaSharingFileActionForAddFile(
        updatedAdd,
        sharedTable,
        snapshotToUse.version,
        snapshotToUse.timestamp
      )
      totalSize = totalSize + addFile.size
      fileActionsArrayBuffer += dsAddFile
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
      if (isDataFile(filePath)) {
        // Put the parquet file in blockManager for DeltaSharingFileSystem to load bytes out of it.
        DeltaSharingUtils.overrideIteratorBlock[Byte](
          blockId = TestDeltaSharingFileSystem.getBlockId(sharedTable, f.getName),
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
        timestampAsOf = timestampAsOf
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
      if (isDataFile(f.getCanonicalPath)) {
        DeltaSharingUtils.overrideIteratorBlock[Byte](
          blockId = TestDeltaSharingFileSystem.getBlockId(sharedTable, f.getName),
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

  private[spark] def prepareMockedClientAndFileSystemResultForCdf(
      deltaTable: String,
      sharedTable: String,
      startingVersion: Long,
      startingTimestamp: Option[String] = None,
      inlineDvFormat: Option[RoaringBitmapArrayFormat.Value] = None,
      assertMultipleDvsInOneFile: Boolean = false): Unit = {
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

    val dvPathToCount = scala.collection.mutable.Map[String, Int]()
    val files =
      FileUtils.listFiles(new File(deltaLog.logPath.toUri()), null, true).asScala
    files.foreach { f =>
      if (FileNames.isDeltaFile(new Path(f.getName))) {
        val version = FileNames.getFileVersion(new Path(f.getName))
        if (version >= startingVersion) {
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
                if (assertMultipleDvsInOneFile) {
                  updateDvPathToCount(addFile, dvPathToCount)
                }
                val updatedAdd = if (inlineDvFormat.isDefined) {
                  // Remove row 0 and 1 in the AddFile.
                  updateAddFileWithInlineDV(addFile, inlineDvFormat.get, RoaringBitmapArray(0L, 1L))
                } else {
                  addFile
                }
                val dsAddFile =
                  getDeltaSharingFileActionForAddFile(updatedAdd, sharedTable, version, timestamp)
                totalSize = totalSize + updatedAdd.size
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
                totalSize = totalSize + removeFile.size.getOrElse(0L)
                actionLines += dsRemoveFile.json
              case cdcFile: AddCDCFile =>
                val parquetFile = removePartitionPrefix(cdcFile.path)

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
                totalSize = totalSize + cdcFile.size
                actionLines += dsCDCFile.json
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
      if (isDataFile(filePath)) {
        DeltaSharingUtils.overrideIteratorBlock[Byte](
          blockId = TestDeltaSharingFileSystem.getBlockId(sharedTable, f.getName),
          values = FileUtils.readFileToByteArray(f).toIterator
        )
      }
    }

    if (assertMultipleDvsInOneFile) {
      assert(dvPathToCount.max._2 > 1)
    }

    DeltaSharingUtils.overrideIteratorBlock[String](
      blockId =
        TestClientForDeltaFormatSharing.getBlockId(sharedTable, s"getCDFFiles_$startingVersion"),
      values = actionLines.result().toIterator
    )
    if (startingTimestamp.isDefined) {
      DeltaSharingUtils.overrideIteratorBlock[String](
        blockId = TestClientForDeltaFormatSharing.getBlockId(
          sharedTable,
          s"getCDFFiles_${startingTimestamp.get}"
        ),
        values = actionLines.result().toIterator
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
}
