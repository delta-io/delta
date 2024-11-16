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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.storage.ClosableIterator
import org.apache.spark.sql.delta.util.FileNames.DeltaFile
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JsonToStructs
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

object DeltaFileProviderUtils {

  protected def readThreadPool = SnapshotManagement.deltaLogAsyncUpdateThreadPool

  /** Put any future parsing options here. */
  val jsonStatsParseOption = Map.empty[String, String]

  private[delta] def createJsonStatsParser(schemaToUse: StructType): String => InternalRow = {
    val parser = JsonToStructs(
      schema = schemaToUse,
      options = jsonStatsParseOption,
      child = null,
      timeZoneId = Some(SQLConf.get.sessionLocalTimeZone)
    )
    (json: String) => {
      val utf8json = UTF8String.fromString(json)
      parser.nullSafeEval(utf8json).asInstanceOf[InternalRow]
    }
  }

  /**
   * Get the Delta json files present in the delta log in the range [startVersion, endVersion].
   * Returns the files in sorted order, and throws if any in the range are missing.
   */
  def getDeltaFilesInVersionRange(
      spark: SparkSession,
      deltaLog: DeltaLog,
      startVersion: Long,
      endVersion: Long): Seq[FileStatus] = {
    // Pass `failOnDataLoss = false` as we are doing an explicit validation on the result ourselves
    // to identify that there are no gaps.
    val result =
      deltaLog
        .getChangeLogFiles(startVersion, endVersion, failOnDataLoss = false)
        .map(_._2)
        .collect { case DeltaFile(fs, v) => (fs, v) }
        .toSeq
    // Verify that we got the entire range requested
    if (result.size.toLong != endVersion - startVersion + 1) {
      throw DeltaErrors.deltaVersionsNotContiguousException(spark, result.map(_._2))
    }
    result.map(_._1)
  }

  /** Helper method to read and parse the delta files parallelly into [[Action]]s. */
  def parallelReadAndParseDeltaFilesAsIterator(
      deltaLog: DeltaLog,
      spark: SparkSession,
      files: Seq[FileStatus]): Seq[ClosableIterator[String]] = {
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    parallelReadDeltaFilesBase(spark, files, hadoopConf, { file: FileStatus =>
      deltaLog.store.readAsIterator(file, hadoopConf)
    })
  }

  protected def parallelReadDeltaFilesBase[A](
      spark: SparkSession,
      files: Seq[FileStatus],
      hadoopConf: Configuration,
      f: FileStatus => A): Seq[A] = {
    readThreadPool.parallelMap(spark, files) { file =>
      f(file)
    }.toSeq
  }
}
