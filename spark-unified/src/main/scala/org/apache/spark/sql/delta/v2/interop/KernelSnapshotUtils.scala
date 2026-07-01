/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.v2.interop

import scala.collection.mutable.ArrayBuffer
import scala.jdk.OptionConverters._

import org.apache.spark.sql.delta.actions.{
  AddFile,
  DeletionVectorDescriptor => V1DeletionVectorDescriptor
}
import org.apache.spark.sql.delta.implicits._
import io.delta.kernel.data.MapValue
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.{ScanImpl, SnapshotImpl}
import io.delta.kernel.internal.actions.{AddFile => KernelAddFile}

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Helpers for code that has a Kernel snapshot but still needs V1 file actions.
 * An AddFile action is the V1 record for one data file in a Delta table.
 *
 * This object does not create a SparkSession or Kernel Engine. The caller passes them in so the
 * caller controls which Spark context and Hadoop configuration are used.
 */
private[delta] object KernelSnapshotUtils {

  /**
   * Reads the active files in the Kernel snapshot and returns them as V1 [[AddFile]] rows.
   * Active files are the data files that belong to this snapshot and have not been removed.
   *
   * This builds the Kernel-backed version of V1 Snapshot.allFiles. Spark builds the Dataset.
   * The Engine reads the file metadata from Kernel.
   */
  def buildAllFiles(
      kernelSnapshot: SnapshotImpl,
      spark: SparkSession,
      engine: Engine): Dataset[AddFile] = {
    spark.createDataset(collectV1AddFiles(kernelSnapshot, engine))
  }

  /**
   * Reads the active file rows from Kernel and returns them as a Scala sequence.
   *
   * The scan asks Kernel for stats because V1 allFiles includes stats JSON on AddFile when stats
   * are available. Kernel returns the files in batches, and this method collects them.
   */
  private def collectV1AddFiles(
      kernelSnapshot: SnapshotImpl,
      engine: Engine): Seq[AddFile] = {
    val scan = kernelSnapshot.getScanBuilder.build().asInstanceOf[ScanImpl]
    val scanFileBatches = scan.getScanFiles(engine, true /* includeStats */)
    try {
      val files = ArrayBuffer.empty[AddFile]
      while (scanFileBatches.hasNext) {
        val rows = scanFileBatches.next().getRows
        try {
          while (rows.hasNext) {
            val kernelAddFile = new KernelAddFile(
              rows.next().getStruct(0 /* addFileColumnOrdinal */))
            files += toV1AddFile(kernelAddFile)
          }
        } finally {
          rows.close()
        }
      }
      files.toSeq
    } finally {
      scanFileBatches.close()
    }
  }

  /**
   * Builds one V1 [[AddFile]] from one Kernel AddFile.
   *
   * The returned AddFile keeps the path, partition values, size, modification time, stats,
   * deletion vector, and row tracking fields from Kernel. The row tracking fields are baseRowId
   * and defaultRowCommitVersion.
   */
  private def toV1AddFile(kernelAddFile: KernelAddFile): AddFile = {
    AddFile(
      path = kernelAddFile.getPath,
      partitionValues = toV1PartitionValues(kernelAddFile.getPartitionValues),
      size = kernelAddFile.getSize,
      modificationTime = kernelAddFile.getModificationTime,
      // V1 Snapshot.allFiles normalizes active files to dataChange = false.
      dataChange = false,
      stats = kernelAddFile.getStatsJson.toScala.orNull,
      deletionVector = toV1DeletionVectorDescriptor(kernelAddFile),
      baseRowId = kernelAddFile.getBaseRowId.toScala.map(_.longValue()),
      defaultRowCommitVersion =
        kernelAddFile.getDefaultRowCommitVersion.toScala.map(_.longValue()))
  }

  /**
   * Builds the V1 deletion vector descriptor for a Kernel AddFile.
   *
   * A deletion vector records which rows in the file are deleted without removing the whole file.
   * The descriptor stores where that deletion vector lives and how many rows it marks as deleted.
   * V1 AddFile uses null when a file has no deletion vector, so this method does the same.
   */
  private def toV1DeletionVectorDescriptor(
      kernelAddFile: KernelAddFile): V1DeletionVectorDescriptor = {
    val deletionVector = kernelAddFile.getDeletionVector
    if (deletionVector.isPresent) {
      val kernelDeletionVector = deletionVector.get
      V1DeletionVectorDescriptor(
        storageType = kernelDeletionVector.getStorageType,
        pathOrInlineDv = kernelDeletionVector.getPathOrInlineDv,
        offset = kernelDeletionVector.getOffset.toScala.map(_.intValue()),
        sizeInBytes = kernelDeletionVector.getSizeInBytes,
        cardinality = kernelDeletionVector.getCardinality)
    } else {
      null
    }
  }

  /**
   * Returns the partition column values for one AddFile as a Scala map.
   *
   * For a table partitioned by date and country, one file may have values like
   * `Map("date" -> "2026-06-08", "country" -> "US")`. V1 stores these values in AddFile
   * as `Map[String, String]`, with null when a partition value is null.
   */
  private def toV1PartitionValues(partitionValues: MapValue): Map[String, String] = {
    if (partitionValues != null) {
      val keys = partitionValues.getKeys
      val values = partitionValues.getValues
      (0 until partitionValues.getSize).map { index =>
        val value = if (values.isNullAt(index)) null else values.getString(index)
        keys.getString(index) -> value
      }.toMap
    } else {
      Map.empty
    }
  }
}
