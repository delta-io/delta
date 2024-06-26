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

package org.apache.spark.sql.delta.hudi

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.client.WriteStatus
import org.apache.hudi.common.model.{HoodieAvroPayload, HoodieTableType, HoodieTimelineTimeZone, HoodieDeltaWriteStat}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.ExternalFilePathUtil
import org.apache.hudi.exception.TableNotFoundException
import org.apache.hudi.storage.StorageConfiguration
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.metering.DeltaLogging

object HudiTransactionUtils extends DeltaLogging {

  /////////////////
  // Public APIs //
  /////////////////
  def convertAddFile(addFile: AddFile,
                     tablePath: Path,
                     commitTime: String): WriteStatus = {

    val writeStatus = new WriteStatus
    val path = addFile.toPath
    val partitionPath = getPartitionPath(tablePath, path)
    val fileName = path.getName
    val fileId = fileName
    val filePath = if (partitionPath.isEmpty) fileName else partitionPath + "/" + fileName
    writeStatus.setFileId(fileId)
    writeStatus.setPartitionPath(partitionPath)
    val writeStat = new HoodieDeltaWriteStat
    writeStat.setFileId(fileId)
    writeStat.setPath(
      ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(filePath, commitTime))
    writeStat.setPartitionPath(partitionPath)
    writeStat.setNumWrites(addFile.numLogicalRecords.getOrElse(0L))
    writeStat.setTotalWriteBytes(addFile.getFileSize)
    writeStat.setFileSizeInBytes(addFile.getFileSize)
    writeStatus.setStat(writeStat)

    writeStatus
  }

  def getPartitionPath(tableBasePath: Path, filePath: Path): String = {
    val fileName = filePath.getName
    val pathStr = filePath.toUri.getPath
    val tableBasePathStr = tableBasePath.toUri.getPath
    if (pathStr.contains(tableBasePathStr)) {
      // input file path is absolute
      val startIndex = tableBasePath.toUri.getPath.length + 1
      val endIndex = pathStr.length - fileName.length - 1
      if (endIndex <= startIndex) ""
      else pathStr.substring(startIndex, endIndex)
    } else {
      val lastSlash = pathStr.lastIndexOf("/")
      if (lastSlash <= 0) ""
      else pathStr.substring(0, pathStr.lastIndexOf("/"))
    }
  }

  /**
   * Loads the meta client for the table at the base path if it exists.
   * If it does not exist, initializes the Hudi table and returns the meta client.
   *
   * @param tableDataPath the path for the table
   * @param tableName the name of the table
   * @param partitionFields the fields used for partitioning
   * @param conf the hadoop configuration
   * @return {@link HoodieTableMetaClient} for the existing table or that was created
   */
  def loadTableMetaClient(tableDataPath: String,
                          tableName: Option[String],
                          partitionFields: Seq[String],
                          conf: StorageConfiguration[_]): HoodieTableMetaClient = {
    try HoodieTableMetaClient.builder
      .setBasePath(tableDataPath).setConf(conf)
      .setLoadActiveTimelineOnLoad(false)
      .build
    catch {
      case ex: TableNotFoundException =>
        log.debug("Hudi table does not exist, creating now.")
        if (tableName.isEmpty) {
          log.warn("No name is specified for the table. "
            + "Creating a new Hudi table with a default name: 'table'.")
        }
        initializeHudiTable(tableDataPath, tableName.getOrElse("table"), partitionFields, conf)
    }
  }

    /**
     * Initializes a Hudi table with the provided properties
     *
     * @param tableDataPath the base path for the data files in the table
     * @param tableName the name of the table
     * @param partitionFields the fields used for partitioning
     * @param conf the hadoop configuration
     * @return {@link HoodieTableMetaClient} for the table that was created
     */
    private def initializeHudiTable(tableDataPath: String,
                                    tableName: String,
                                    partitionFields: Seq[String],
                                    conf: StorageConfiguration[_]): HoodieTableMetaClient = {
      val keyGeneratorClass = getKeyGeneratorClass(partitionFields)
      HoodieTableMetaClient
        .withPropertyBuilder
        .setCommitTimezone(HoodieTimelineTimeZone.UTC)
        .setHiveStylePartitioningEnable(true)
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(tableName)
        .setPayloadClass(classOf[HoodieAvroPayload])
        .setKeyGeneratorClassProp(keyGeneratorClass)
        .setPopulateMetaFields(false)
        .setPartitionFields(partitionFields.mkString(","))
        .initTable(conf, tableDataPath)
    }

    private def getKeyGeneratorClass(partitionFields: Seq[String]): String = {
      if (partitionFields.isEmpty) {
        "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
      } else if (partitionFields.size > 1) {
        "org.apache.hudi.keygen.CustomKeyGenerator"
      } else {
        "org.apache.hudi.keygen.SimpleKeyGenerator"
      }
    }
}
