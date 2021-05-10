/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.FileNames.{checkpointVersion, deltaVersion, isCheckpointFile, isChecksumFile, isDeltaFile, numCheckpointParts}
import org.apache.spark.util.Utils

import scala.util.Try

sealed case class DeltaFileType(value: String)

object DeltaFileType {
  object DELTA extends DeltaFileType("DELTA")
  object CHECKPOINT extends DeltaFileType("CHECKPOINT")
  object CHECKSUM extends DeltaFileType("CHECKSUM")
  object UNKNOWN extends DeltaFileType("UNKNOWN")

  val values = Seq(DELTA, CHECKPOINT, CHECKSUM, UNKNOWN)

  def getFileType(path: Path): DeltaFileType = {
    path match {
      case f if isCheckpointFile(f) => DeltaFileType.CHECKPOINT
      case f if isDeltaFile(f) => DeltaFileType.DELTA
      case f if isChecksumFile(f) => DeltaFileType.CHECKSUM
      case _ => DeltaFileType.UNKNOWN
    }
  }
}

case class LogFileMeta(fileStatus: FileStatus,
                      version: Long,
                      fileType: DeltaFileType,
                      numParts: Option[Int]) {

  def asCheckpointInstance(): CheckpointInstance = {
    CheckpointInstance(version, numParts)
  }
}

object LogFileMeta {
  def isCheckpointFile(logFileMeta: LogFileMeta): Boolean = {
    logFileMeta.fileType == DeltaFileType.CHECKPOINT
  }

  def isDeltaFile(logFileMeta: LogFileMeta): Boolean = {
    logFileMeta.fileType == DeltaFileType.DELTA
  }
}


class LogFileMetaParser(logStore: LogStore) {

  def listFilesFrom(logPath: Path): Iterator[LogFileMeta] = {

    logStore.listFrom(logPath).map(fs => {
      LogFileMeta(fs,
        Try(deltaVersion(fs.getPath)).getOrElse(Try(checkpointVersion(fs.getPath)).getOrElse(-1L)),
        DeltaFileType.getFileType(fs.getPath),
        numCheckpointParts(fs.getPath))
    })
  }

}

object LogFileMetaParser extends LogFileMetaProvider
  with Logging {

  def apply(sc: SparkContext, logStore: LogStore): LogFileMetaParser = {
    apply(sc.getConf, sc.hadoopConfiguration, logStore)
  }

  def apply(sparkConf: SparkConf,
            hadoopConf: Configuration,
            logStore: LogStore): LogFileMetaParser = {
    createLogFileMetaParser(sparkConf, hadoopConf, logStore)
  }
}

trait LogFileMetaProvider {

  def createLogFileMetaParser(spark: SparkSession, logStore: LogStore): LogFileMetaParser = {
    val sc = spark.sparkContext
    createLogFileMetaParser(sc.getConf, sc.hadoopConfiguration, logStore)
  }

  def createLogFileMetaParser(sparkConf: SparkConf,
                              hadoopConf: Configuration,
                              logStore: LogStore): LogFileMetaParser = {
    val logStoreClassName = sparkConf.get("spark.delta.logFileHandler.class",
      classOf[LogFileMetaParser].getName)
    val logStoreClass = Utils.classForName(logStoreClassName)
    logStoreClass.getConstructor(classOf[LogStore])
      .newInstance(logStore).asInstanceOf[LogFileMetaParser]
  }
}
