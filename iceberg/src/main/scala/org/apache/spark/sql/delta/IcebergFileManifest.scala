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

package org.apache.spark.sql.delta.commands.convert

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.{DeltaColumnMapping, SerializableFileStatus}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path
import org.apache.iceberg.{BaseTable, DataFile, DataFiles, FileContent, FileFormat, ManifestContent, ManifestFile, ManifestFiles, PartitionData, PartitionSpec, RowLevelOperationMode, Schema, StructLike, Table, TableProperties}

import org.apache.spark.internal.{LoggingShims, MDC}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType

class IcebergFileManifest(
    spark: SparkSession,
    table: Table,
    partitionSchema: StructType,
    convertStats: Boolean = true) extends ConvertTargetFileManifest with LoggingShims {

  // scalastyle:off sparkimplicits
  import spark.implicits._
  // scalastyle:on sparkimplicits

  private var fileSparkResults: Option[Dataset[ConvertTargetFile]] = None

  private var _numFiles: Option[Long] = None

  private var _sizeInBytes: Option[Long] = None

  val basePath = table.location()

  override def numFiles: Long = {
    if (_numFiles.isEmpty) _numFiles = Some(allFiles.count())
    _numFiles.get
  }

  override def sizeInBytes: Long = {
    if (_sizeInBytes.isEmpty) {
      _sizeInBytes =
        Some(if (allFiles.isEmpty) 0L else allFiles.map(_.fileStatus.length).reduce(_ + _))
    }
    _sizeInBytes.get
  }

  def allFiles: Dataset[ConvertTargetFile] = {
    if (fileSparkResults.isEmpty) fileSparkResults = Some(getFileSparkResults())
    fileSparkResults.get
  }

  private def getFileSparkResults(): Dataset[ConvertTargetFile] = {
    val format = table
      .properties()
      .getOrDefault(
        TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT)

    if (format.toLowerCase() != "parquet") {
      throw new UnsupportedOperationException(
        s"Cannot convert Iceberg tables with file format $format. Only parquet is supported.")
    }

    if (table.currentSnapshot() == null) {
      return spark.emptyDataset[ConvertTargetFile]
    }

    // We do not support Iceberg Merge-On-Read using delete files and will by default
    // throw errors when encountering them. This flag will bypass the check and ignore the delete
    // files, and in other words, generating a CORRUPTED table. We keep this flag only for
    // backward compatibility. No new use cases of this flag should be allowed.
    val unsafeConvertMorTable =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CONVERT_ICEBERG_UNSAFE_MOR_TABLE_ENABLE)
    val hasMergeOnReadDeletionFiles = table.currentSnapshot().deleteManifests(table.io()).size() > 0
    if (hasMergeOnReadDeletionFiles && !unsafeConvertMorTable) {
      throw new UnsupportedOperationException(
        s"Cannot support convert Iceberg table with row-level deletes." +
          s"Please trigger an Iceberg compaction and retry the command.")
    }

    // Localize variables so we don't need to serialize the File Manifest class
    val localTable = table
    // We use the latest snapshot timestamp for all generated Delta AddFiles due to the fact that
    // retrieving timestamp for each DataFile is non-trivial time-consuming. This can be improved
    // in the future.
    val snapshotTs = table.currentSnapshot().timestampMillis

    val shouldConvertPartition = spark.sessionState.conf
      .getConf(DeltaSQLConf.DELTA_CONVERT_ICEBERG_USE_NATIVE_PARTITION_VALUES)
    val convertPartition = if (shouldConvertPartition) {
      new IcebergPartitionConverter(localTable, partitionSchema)
    } else {
      null
    }

    val shouldConvertStats = convertStats

    val manifestFiles = localTable
      .currentSnapshot()
      .dataManifests(localTable.io())
      .asScala
      .map(new ManifestFileWrapper(_))
      .toSeq
    spark
      .createDataset(manifestFiles)
      .flatMap(ManifestFiles.read(_, localTable.io()).asScala.map(new DataFileWrapper(_)))
      .map { dataFile: DataFileWrapper =>
        ConvertTargetFile(
          SerializableFileStatus(
            path = dataFile.path,
            length = dataFile.fileSizeInBytes,
            isDir = false,
            modificationTime = snapshotTs
          ),
          partitionValues = if (shouldConvertPartition) {
            Some(convertPartition.toDelta(dataFile.partition()))
          } else None,
          stats = if (shouldConvertStats) {
            IcebergStatsUtils.icebergStatsToDelta(localTable.schema, dataFile)
          } else None
        )
      }
      .cache()
  }


  override def close(): Unit = {
    fileSparkResults.map(_.unpersist())
    fileSparkResults = None
  }
}
