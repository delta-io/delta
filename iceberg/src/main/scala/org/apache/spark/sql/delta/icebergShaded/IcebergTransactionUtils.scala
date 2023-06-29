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

package org.apache.spark.sql.delta.icebergShaded

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaConfigs, DeltaLog}
import org.apache.spark.sql.delta.actions.{AddFile, FileAction, RemoveFile}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.hadoop.fs.Path
import shadedForDelta.org.apache.iceberg.{DataFile, DataFiles, FileFormat, PartitionSpec, Schema => IcebergSchema}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

object IcebergTransactionUtils
    extends DeltaLogging
  {

  /////////////////
  // Public APIs //
  /////////////////

  def createPartitionSpec(
      icebergSchema: IcebergSchema,
      partitionColumns: Seq[String]): PartitionSpec = {
    if (partitionColumns.isEmpty) {
      PartitionSpec.unpartitioned
    } else {
      val builder = PartitionSpec.builderFor(icebergSchema)
      for (partitionName <- partitionColumns) {
        builder.identity(partitionName)
      }
      builder.build()
    }
  }

  def convertDeltaAddFileToIcebergDataFile(
      add: AddFile,
      tablePath: Path,
      partitionSpec: PartitionSpec,
      logicalToPhysicalPartitionNames: Map[String, String],
      statsSchema: StructType,
      statsParser: String => InternalRow,
      deltaLog: DeltaLog): DataFile = {
    if (add.deletionVector != null) {
      throw new UnsupportedOperationException("No support yet for DVs")
    }

    var dataFileBuilder =
      convertFileAction(add, tablePath, partitionSpec, logicalToPhysicalPartitionNames)
        // Attempt to attach the number of records metric regardless of whether the Delta stats
        // string is null/empty or not because this metric is required by Iceberg. If the number
        // of records is both unavailable here and unavailable in the Delta stats, Iceberg will
        // throw an exception when building the data file.
        .withRecordCount(add.numLogicalRecords.getOrElse(-1L))


    dataFileBuilder.build()
  }

  /**
   * Note that APIs like [[shadedForDelta.org.apache.iceberg.OverwriteFiles#deleteFile]] take
   * a DataFile, and not a DeleteFile as you might have expected.
   */
  def convertDeltaRemoveFileToIcebergDataFile(
      remove: RemoveFile,
      tablePath: Path,
      partitionSpec: PartitionSpec,
      logicalToPhysicalPartitionNames: Map[String, String]): DataFile = {
    convertFileAction(remove, tablePath, partitionSpec, logicalToPhysicalPartitionNames)
      .withRecordCount(remove.numLogicalRecords.getOrElse(0L))
      .build()
  }

  /**
   * We expose this as a public API since APIs like
   * [[shadedForDelta.org.apache.iceberg.DeleteFiles#deleteFile]] actually only need to take in
   * a file path String, thus we don't need to actually convert a [[RemoveFile]] into a [[DataFile]]
   * in this case.
   */
  def canonicalizeFilePath(f: FileAction, tablePath: Path): String = {
    // Recall that FileActions can have either relative paths or absolute paths (i.e. from shallow-
    // cloned files).
    // Iceberg spec requires path be fully qualified path, suitable for constructing a Hadoop Path
    if (f.pathAsUri.isAbsolute) f.path else new Path(tablePath, f.path).toString
  }

  /** Returns the (deletions, additions) iceberg table property changes. */
  def detectPropertiesChange(
      newProperties: Map[String, String],
      prevPropertiesOpt: Map[String, String]): (Set[String], Map[String, String]) = {
    val newPropertiesIcebergOnly = getIcebergPropertiesFromDeltaProperties(newProperties)
    val prevPropertiesOptIcebergOnly = getIcebergPropertiesFromDeltaProperties(prevPropertiesOpt)

    if (prevPropertiesOptIcebergOnly == newPropertiesIcebergOnly) return (Set.empty, Map.empty)

    (
      prevPropertiesOptIcebergOnly.keySet.diff(newPropertiesIcebergOnly.keySet),
      newPropertiesIcebergOnly
    )
  }

  /**
   * Only keep properties whose key starts with "delta.universalformat.config.iceberg"
   * and strips the prefix from the key; Note the key is already normalized to lower case.
   */
  def getIcebergPropertiesFromDeltaProperties(
      properties: Map[String, String]): Map[String, String] = {
    val prefix = DeltaConfigs.DELTA_UNIVERSAL_FORMAT_ICEBERG_CONFIG_PREFIX
    properties.filterKeys(_.startsWith(prefix)).map(kv => (kv._1.stripPrefix(prefix), kv._2)).toMap
  }

  /** Returns the mapping of logicalPartitionColName -> physicalPartitionColName */
  def getPartitionPhysicalNameMapping(partitionSchema: StructType): Map[String, String] = {
    partitionSchema.fields.map(f => f.name -> DeltaColumnMapping.getPhysicalName(f)).toMap
  }

  ////////////////////
  // Helper Methods //
  ////////////////////

  /** Visible for testing. */
  private[delta] def convertFileAction(
      f: FileAction,
      tablePath: Path,
      partitionSpec: PartitionSpec,
      logicalToPhysicalPartitionNames: Map[String, String]): DataFiles.Builder = {
    val absPath = canonicalizeFilePath(f, tablePath)

    var builder = DataFiles
      .builder(partitionSpec)
      .withPath(absPath)
      .withFileSizeInBytes(f.getFileSize)
      .withFormat(FileFormat.PARQUET)

    if (partitionSpec.isPartitioned) {
      val partitionPath = partitionSpec
        .fields()
        .asScala
        .map(_.name)
        .map { logicalPartCol =>
          // The Iceberg Schema and PartitionSpec all use the column logical names.
          // Delta FileAction::partitionValues, however, uses physical names.
          val physicalPartKey = logicalToPhysicalPartitionNames(logicalPartCol)
          s"$logicalPartCol=${f.partitionValues(physicalPartKey)}"
        }
        .mkString("/")

      builder = builder.withPartitionPath(partitionPath)
    }

    builder
  }
}
