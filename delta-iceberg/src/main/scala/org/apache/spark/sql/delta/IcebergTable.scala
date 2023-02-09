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

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.DeltaColumnMapping
import org.apache.spark.sql.delta.SerializableFileStatus
import org.apache.spark.sql.delta.commands.{ConvertTargetFile, ConvertTargetFileManifest, ConvertTargetTable, ConvertToDeltaCommand}
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{DateFormatter, TimestampFormatter}
import org.apache.hadoop.fs.Path
import org.apache.iceberg.{RowLevelOperationMode, Table, TableProperties}
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.transforms.IcebergPartitionUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * A target Iceberg table for conversion to a Delta table.
 *
 * @param icebergTable the Iceberg table underneath.
 * @param existingSchema schema used for incremental update, none for initial conversion.
 */
class IcebergTable(
  spark: SparkSession,
  icebergTable: Table,
  existingSchema: Option[StructType]) extends ConvertTargetTable {

  def this(spark: SparkSession, basePath: String, existingSchema: Option[StructType]) =
    // scalastyle:off deltahadoopconfiguration
    this(spark, new HadoopTables(spark.sessionState.newHadoopConf).load(basePath), existingSchema)
    // scalastyle:on deltahadoopconfiguration

  private val partitionEvolutionEnabled =
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CONVERT_ICEBERG_PARTITION_EVOLUTION_ENABLED)

  private val fieldPathToPhysicalName =
    existingSchema.map {
      SchemaMergingUtils.explode(_).collect {
        case (path, field) if DeltaColumnMapping.hasPhysicalName(field) =>
          path.map(_.toLowerCase(Locale.ROOT)) -> DeltaColumnMapping.getPhysicalName(field)
      }.toMap
    }.getOrElse(Map.empty[Seq[String], String])

  private val convertedSchema = {
    // Reuse physical names of existing columns.
    val mergedSchema = DeltaColumnMapping.setPhysicalNames(
      IcebergSchemaUtils.convertIcebergSchemaToSpark(icebergTable.schema()),
      fieldPathToPhysicalName)

    // Assign physical names to new columns.
    DeltaColumnMapping.assignPhysicalNames(mergedSchema)
  }

  override val requiredColumnMappingMode: DeltaColumnMappingMode = IdMapping

  override val properties: Map[String, String] = {
    icebergTable.properties().asScala.toMap + (DeltaConfigs.COLUMN_MAPPING_MODE.key -> "id")
  }

  override val partitionSchema: StructType = {
    // Reuse physical names of existing columns.
    val mergedPartitionSchema = DeltaColumnMapping.setPhysicalNames(
      StructType(
        IcebergPartitionUtil.getPartitionFields(icebergTable.spec(), icebergTable.schema())),
      fieldPathToPhysicalName)

    // Assign physical names to new partition columns.
    DeltaColumnMapping.assignPhysicalNames(mergedPartitionSchema)
  }

  val tableSchema: StructType = PartitioningUtils.mergeDataAndPartitionSchema(
    convertedSchema,
    partitionSchema,
    spark.sessionState.conf.caseSensitiveAnalysis)._1

  checkConvertible()

  val fileManifest = new IcebergFileManifest(spark, icebergTable, partitionSchema)

  lazy val numFiles: Long = fileManifest.numFiles

  override val format: String = "iceberg"

  def checkConvertible(): Unit = {
    /**
     * Having multiple partition specs implies that the Iceberg table has experienced
     * partition evolution. (https://iceberg.apache.org/evolution/#partition-evolution)
     * We don't support the conversion of such tables right now.
     *
     * Note that this simple check won't consider the underlying data, so there might be cases
     * s.t. the data itself is partitioned using a single spec despite multiple specs created
     * in the past. we do not account for that atm due to the complexity of data introspection
     */

    if (!partitionEvolutionEnabled && icebergTable.specs().size() > 1) {
      throw new UnsupportedOperationException(IcebergTable.ERR_MULTIPLE_PARTITION_SPECS)
    }

    /**
     * Existing Iceberg Table that has data imported from table without field ids will need
     * to add a custom property to enable the mapping for Iceberg.
     * Therefore, we can simply check for the existence of this property to see if there was
     * a custom mapping within Iceberg.
     *
     * Ref: https://www.mail-archive.com/dev@iceberg.apache.org/msg01638.html
     */
    if (icebergTable.properties().containsKey(TableProperties.DEFAULT_NAME_MAPPING)) {
      throw new UnsupportedOperationException(IcebergTable.ERR_CUSTOM_NAME_MAPPING)
    }

    /**
     * Delta does not support case sensitive columns while Iceberg does. We should check for
     * this here to throw a better message tailored to converting to Delta than the default
     * AnalysisException
     */
     try {
       SchemaMergingUtils.checkColumnNameDuplication(tableSchema, "during convert to Delta")
     } catch {
       case e: AnalysisException if e.getMessage.contains("during convert to Delta") =>
         throw new UnsupportedOperationException(
           IcebergTable.caseSensitiveConversionExceptionMsg(e.getMessage))
     }
  }
}

object IcebergTable {
  /** Error message constants */
  val ERR_MULTIPLE_PARTITION_SPECS =
    s"""This Iceberg table has undergone partition evolution. Iceberg tables that had partition
      | columns removed can be converted without data loss by setting the SQL configuration
      | '${DeltaSQLConf.DELTA_CONVERT_ICEBERG_PARTITION_EVOLUTION_ENABLED.key}' to true. Tables that
      | had data columns converted to partition columns will not be able to read the pre-partition
      | column values.""".stripMargin
  val ERR_CUSTOM_NAME_MAPPING = "Cannot convert Iceberg tables with column name mapping"

  def caseSensitiveConversionExceptionMsg(conflictingColumns: String): String =
    s"""Cannot convert table to Delta as the table contains column names that only differ by case.
       |$conflictingColumns. Delta does not support case sensitive column names.
       |Please rename these columns before converting to Delta.
       """.stripMargin
}

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

  def numFiles: Long = {
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
    val timestampFormatter = TimestampFormatter(ConvertToDeltaCommand.timestampPartitionPattern,
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
