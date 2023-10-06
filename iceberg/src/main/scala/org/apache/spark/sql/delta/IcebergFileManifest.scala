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
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{DateFormatter, TimestampFormatter}
import org.apache.hadoop.fs.Path
import org.apache.iceberg.{RowLevelOperationMode, Table, TableProperties}
import org.apache.iceberg.transforms.IcebergPartitionUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

class IcebergFileManifest(
    spark: SparkSession,
    table: Table,
    partitionSchema: StructType) extends ConvertTargetFileManifest with Logging {

  // scalastyle:off sparkimplicits
  import spark.implicits._
  // scalastyle:on sparkimplicits

  final val VOID_TRANSFORM = "void"

  private var fileSparkResults: Option[Dataset[ConvertTargetFile]] = None

  private var _numFiles: Option[Long] = None

  val basePath = table.location()

  override def numFiles: Long = {
    if (_numFiles.isEmpty) getFileSparkResults()
    _numFiles.get
  }

  def allFiles: Dataset[ConvertTargetFile] = {
    if (fileSparkResults.isEmpty) getFileSparkResults()
    fileSparkResults.get
  }

  private def getFileSparkResults(): Unit = {
    // scalastyle:off deltahadoopconfiguration
    val hadoopConf = spark.sessionState.newHadoopConf()
    // scalastyle:on deltahadoopconfiguration
    val serializableConfiguration = new SerializableConfiguration(hadoopConf)
    val conf = spark.sparkContext.broadcast(serializableConfiguration)
    val format = table
      .properties()
      .getOrDefault(
        TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT)

    if (format != "parquet") {
      throw new UnsupportedOperationException(
        s"Cannot convert Iceberg tables with file format $format. Only parquet is supported.")
    }

    val schemaBatchSize =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_IMPORT_BATCH_SIZE_SCHEMA_INFERENCE)

    val partFields = table.spec().fields().asScala
    val icebergSchema = table.schema()
    // Prune removed partition fields.
    val physicalNameToFieldIndex = partFields.zipWithIndex.collect {
      case (field, index) if field.transform().toString != VOID_TRANSFORM =>
        DeltaColumnMapping.getPhysicalName(partitionSchema(field.name)) -> index
    }.toMap

    val dateFormatter = DateFormatter()
    val timestampFormatter = TimestampFormatter(ConvertUtils.timestampPartitionPattern,
      java.util.TimeZone.getDefault)

    // This flag is strongly not recommended to turn on, but we still provide a flag for regression
    // purpose.
    val unsafeConvertMorTable =
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CONVERT_ICEBERG_UNSAFE_MOR_TABLE_ENABLE)
    val properties = CaseInsensitiveMap(table.properties().asScala.toMap)
    val isMergeOnReadTable = Seq(
      TableProperties.DELETE_MODE,
      TableProperties.UPDATE_MODE,
      TableProperties.MERGE_MODE
    ).exists { propKey =>
      properties.get(propKey)
        .exists(RowLevelOperationMode.fromName(_) == RowLevelOperationMode.MERGE_ON_READ)
    }

    var numFiles = 0L
    val res = table.newScan().planFiles().iterator().asScala.grouped(schemaBatchSize).map { batch =>
      logInfo(s"Getting file statuses for a batch of ${batch.size} of files; " +
        s"finished $numFiles files so far")
      numFiles += batch.length
      val filePathWithPartValues = batch.map { fileScanTask =>
        val filePath = fileScanTask.file().path().toString
        // If an Iceberg table has merge on read enabled AND it has deletion file associated with
        // the data file, we could not convert directly.
        val hasMergeOnReadDeletionFiles = isMergeOnReadTable && fileScanTask.deletes().size() > 0
        if (hasMergeOnReadDeletionFiles && !unsafeConvertMorTable) {
          throw new UnsupportedOperationException(
            s"Cannot convert Iceberg merge-on-read table with delete files. " +
              s"Please trigger an Iceberg compaction and retry the command.")
        }
        val partitionValues = if (spark.sessionState.conf.getConf(
          DeltaSQLConf.DELTA_CONVERT_ICEBERG_USE_NATIVE_PARTITION_VALUES)) {
          val icebergPartitionValues = fileScanTask.file().partition()
          val physicalNameToPartValueMap = physicalNameToFieldIndex
            .map { case (physicalName, fieldIndex) =>
              val partValue = icebergPartitionValues.get(fieldIndex, classOf[java.lang.Object])
              val partValueAsString = IcebergPartitionUtil.partitionValueToString(
                partFields(fieldIndex), partValue, icebergSchema, dateFormatter, timestampFormatter)
              (physicalName, partValueAsString)
            }
          Some(physicalNameToPartValueMap)
        } else None
        (filePath, partitionValues)
      }
      val numParallelism = Math.min(Math.max(filePathWithPartValues.size, 1),
        spark.sparkContext.defaultParallelism)

      val rdd = spark.sparkContext.parallelize(filePathWithPartValues, numParallelism)
        .mapPartitions { iterator =>
          iterator.map { case (filePath, partValues) =>
            val path = new Path(filePath)
            val fs = path.getFileSystem(conf.value.value)
            val fileStatus = fs.getFileStatus(path)
            ConvertTargetFile(SerializableFileStatus.fromStatus(fileStatus), partValues)
          }
        }
      spark.createDataset(rdd)
    }.reduceOption(_.union(_)).getOrElse(spark.emptyDataset[ConvertTargetFile])

    fileSparkResults = Some(res.cache())
    _numFiles = Some(numFiles)
  }

  override def close(): Unit = fileSparkResults.map(_.unpersist())
}
