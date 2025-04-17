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
import scala.collection.mutable

import org.apache.spark.sql.delta.{DeltaColumnMapping, SerializableFileStatus}
import org.apache.spark.sql.delta.DeltaErrors.cloneFromIcebergSourceWithPartitionEvolution
import org.apache.spark.sql.delta.commands.convert.IcebergTable.ERR_MULTIPLE_PARTITION_SPECS
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path
import org.apache.iceberg.{BaseTable, DataFile, DataFiles, FileContent, FileFormat, ManifestContent, ManifestFile, ManifestFiles, PartitionData, PartitionSpec, RowLevelOperationMode, Schema, StructLike, Table, TableProperties}
import org.apache.iceberg.transforms.IcebergPartitionUtil
import org.apache.iceberg.types.Type.TypeID

import org.apache.spark.SparkThrowable
import org.apache.spark.internal.{LoggingShims, MDC}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType

class IcebergFileManifest(
    spark: SparkSession,
    table: IcebergTableLike,
    partitionSchema: StructType,
    convertStats: Boolean = true) extends ConvertTargetFileManifest with LoggingShims {

  // scalastyle:off sparkimplicits
  import spark.implicits._
  // scalastyle:on sparkimplicits

  private var fileSparkResults: Option[Dataset[ConvertTargetFile]] = None

  private var _numFiles: Option[Long] = None

  private var _sizeInBytes: Option[Long] = None

  private val specIdsToIfSpecHasNonBucketPartition =
    table.specs().asScala.map { case (specId, spec) =>
      specId.toInt -> IcebergPartitionUtil.hasNonBucketPartition(spec)
  }

  private val partitionEvolutionEnabled =
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CONVERT_ICEBERG_PARTITION_EVOLUTION_ENABLED)

  private val statsAllowTypes: Set[TypeID] = IcebergStatsUtils.typesAllowStatsConversion(spark)
  private val allowPartialStatsConverted: Boolean =
    spark.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_CLONE_ICEBERG_ALLOW_PARTIAL_STATS
    )

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
    // Some contexts: Spark needs all variables in closure to be serializable
    // while class members carry the entire class, so they require serialization of the class
    // As IcebergFileManifest is not serializable,
    // we localize member variables to avoid serialization of the class
    val localTable = table
    // We use the latest snapshot timestamp for all generated Delta AddFiles due to the fact that
    // retrieving timestamp for each DataFile is non-trivial time-consuming. This can be improved
    // in the future.
    val snapshotTs = table.currentSnapshot().timestampMillis

    val shouldConvertPartition = spark.sessionState.conf
      .getConf(DeltaSQLConf.DELTA_CONVERT_ICEBERG_USE_NATIVE_PARTITION_VALUES)
    val convertPartition = if (shouldConvertPartition) {
      new IcebergPartitionConverter(localTable, partitionSchema, partitionEvolutionEnabled)
    } else {
      null
    }

    val shouldConvertStats = convertStats
    val partialStatsConvertedEnabled = allowPartialStatsConverted
    val statsAllowTypesSet = statsAllowTypes

    val shouldCheckPartitionEvolution = !partitionEvolutionEnabled
    val specIdsToIfSpecHasNonBucketPartitionMap = specIdsToIfSpecHasNonBucketPartition
    val tableSpecsSize = table.specs().size()

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
        if (shouldCheckPartitionEvolution) {
          IcebergFileManifest.validateLimitedPartitionEvolution(
            dataFile.specId,
            tableSpecsSize,
            specIdsToIfSpecHasNonBucketPartitionMap
          )
        }
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
            IcebergStatsUtils.icebergStatsToDelta(
              localTable.schema,
              dataFile,
              statsAllowTypesSet,
              shouldSkipForFile = (df: DataFile) => {
                !partialStatsConvertedEnabled && IcebergStatsUtils.hasPartialStats(df)
              }
            )
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

object IcebergFileManifest {
  // scalastyle:off
  /**
   * Validates on partition evolution for proposed partitionSpecId
   * We don't support the conversion of tables with partition evolution
   *
   * However, we allow one special case where
   *  all data files have either no-partition or bucket-partition
   *  regardless of multiple partition spec present in the table
   */
  // scalastyle:on
  private def validateLimitedPartitionEvolution(
      partitionSpecId: Int,
      tableSpecsSize: Int,
      specIdsToIfSpecHasNonBucketPartition: mutable.Map[Int, Boolean]): Unit = {
    if (hasPartitionEvolved(
      partitionSpecId, tableSpecsSize, specIdsToIfSpecHasNonBucketPartition)
    ) {
      throw cloneFromIcebergSourceWithPartitionEvolution()
    }
  }

  private def hasPartitionEvolved(
      partitionSpecID: Int,
      tableSpecsSize: Int,
      specIdsToIfSpecHasNonBucketPartition: mutable.Map[Int, Boolean]): Boolean = {
    val isSpecPartitioned = specIdsToIfSpecHasNonBucketPartition(partitionSpecID)
    isSpecPartitioned && tableSpecsSize > 1
  }
}
