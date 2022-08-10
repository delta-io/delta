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

package org.apache.spark.sql.delta.files

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.actions.SingleAction.addFileEncoder
import org.apache.spark.sql.delta.actions.{AddCDCFile, AddFile, FileAction, RemoveFile}
import org.apache.spark.sql.delta.commands.cdc.CDCReader.{CDCDataSpec, CDC_COMMIT_TIMESTAMP, CDC_COMMIT_VERSION}
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, Snapshot}
import org.apache.spark.sql.types.StructType

/**
 * A base [[TahoeFileIndex]] for all CDC file indexes
 */
abstract class TahoeCDCBaseFileIndex[T <: FileAction](spark: SparkSession,
    val filesByVersion: Seq[CDCDataSpec[T]],
    deltaLog: DeltaLog,
    path: Path,
    snapshot: Snapshot)
  extends TahoeFileIndex(spark, deltaLog, path) {

    case class ActionParameters(
        partitionValues: Map[String, String],
        size: Long,
        modificationTime: Long,
        dataChange: Boolean,
        tags: Map[String, String])

    private def extractActionParameters(action: T):
    ActionParameters = {
        action match {
            case AddCDCFile(_, partitionValues, size, tags) =>
                ActionParameters(partitionValues, size, 0, false, tags)
            case r@RemoveFile(_, _, dataChange, extendedFileMetadata, partitionValues, size, tags) =>
                if (!extendedFileMetadata.getOrElse(false)) {
                    // This shouldn't happen in user queries - the CDC flag was added at the same time as
                    // extended metadata, so all removes in a table with CDC enabled should have it. (The
                    // only exception is FSCK removes, which we screen out separately because they have
                    // dataChange set to false.)
                    throw DeltaErrors.removeFileCDCMissingExtendedMetadata(r.toString)
                }
                ActionParameters(partitionValues, size.getOrElse(0L), 0, dataChange, tags)
            case AddFile(_, partitionValues, size, modificationTime, dataChange, _, tags) =>
                ActionParameters(partitionValues, size, modificationTime, dataChange, tags)
        }
    }

    override def tableVersion: Long = snapshot.version

    override def matchingFiles(
        partitionFilters: Seq[Expression],
        dataFilters: Seq[Expression]): Seq[AddFile] = {
        val addFiles = filesByVersion
          .flatMap {
              case CDCDataSpec(version, timestamp, actions) =>
                  actions.map { action =>
                      val ActionParameters(
                      partitionValues,
                      size,
                      modificationTime,
                      dataChange,
                      tags) = extractActionParameters(action)

                      val newPartitionValues =
                          partitionValues ++
                            (Map(
                                CDC_COMMIT_VERSION -> version.toString,
                                CDC_COMMIT_TIMESTAMP -> Option(timestamp).map(_.toString).orNull)
                              ++ cdcPartitionValues())

                      AddFile(
                          action.path,
                          newPartitionValues,
                          size,
                          modificationTime,
                          dataChange,
                          tags = tags)
                  }
          }
        DeltaLog.filterFileList(
            partitionSchema,
            spark.createDataset(addFiles)(addFileEncoder).toDF(),
            partitionFilters)
          .as[AddFile](addFileEncoder)
          .collect()
    }

    def cdcPartitionValues(): Map[String, String]

    override def inputFiles: Array[String] = {
        filesByVersion.flatMap(_.actions).map(f => absolutePath(f.path).toString).toArray
    }

    override def refresh(): Unit = {}

    override def sizeInBytes: Long =
        filesByVersion
          .map(_.actions.map {
              case change: AddCDCFile => change.size
              case add: AddFile => add.size
              case remove: RemoveFile => remove.size.getOrElse(0L)
          }.sum)
          .sum

}
