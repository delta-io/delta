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


package org.apache.spark.sql.delta.amt

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.RowIndexFilterProvider
import org.apache.spark.sql.delta.{DeltaLogFileIndex, DeltaParquetWriteSupport, RowIndexFilterMetadataColumnUtils}
import org.apache.spark.sql.delta.RowIndexFilterMetadataColumnUtils.ColumnMetadata
import org.apache.spark.sql.delta.deletionvectors.KeepAllRowsFilter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.util.ContextUtil

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{ByteType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

/**
 * Parquet file format for AMT manifests (root and leaves).
 *
 * Write side: AMT manifests are Iceberg-V4 manifests, so [[prepareWrite]] always writes them with
 * the Iceberg-compatible Parquet settings.
 *
 * Read side: exposes Manifest-DV membership without dropping rows. AMT must compute row-ID
 * inheritance over every physical entry before applying the Manifest DV. Consequently this format
 * keeps files unsplit, disables pushed filters, and materializes a marker that the AMT checkpoint
 * provider filters only after computing the inheritance prefix.
 */
private[delta] class AMTParquetFileFormat extends ParquetFileFormat {
  import AMTParquetFileFormat._

  // Set isSplitable to false so `first_row_id` inheritance uses the complete `record_count`
  // prefix sum of preceding entries with null `first_row_id` in the same leaf.
  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = false

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val factory = super.prepareWrite(sparkSession, job, options, dataSchema)
    // AMT manifests are Iceberg-V4 manifests. Iceberg requires timestamps as int64
    // TIMESTAMP(MICROS); Spark's default is INT96.
    ContextUtil.getConfiguration(job).set(
      SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key,
      SQLConf.ParquetOutputTimestampType.TIMESTAMP_MICROS.toString)
    // Write list-element / map key-value field ids (carried on the schema via
    // `parquet.field.nested.ids`), which the stock `ParquetWriteSupport` omits.
    ParquetOutputFormat.setWriteSupportClass(job, classOf[DeltaParquetWriteSupport])
    factory
  }


  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val parquetDataReader = super.buildReaderWithPartitionValues(
      sparkSession,
      dataSchema,
      partitionSchema,
      requiredSchema,
      // Filtering before row-ID inheritance would change the prefix assigned to later entries.
      filters = Seq.empty,
      options = options,
      hadoopConf = hadoopConf)

    val schemaWithIndices = requiredSchema.fields.zipWithIndex
    def findColumn(name: String): Option[ColumnMetadata] = {
      val matches = schemaWithIndices.filter(_._1.name == name)
      require(matches.length <= 1,
        s"More than one column with name '$name' was requested from the AMT reader")
      matches.headOption.map { case (field, index) => ColumnMetadata(index, field) }
    }

    val markerColumnOpt = findColumn(IS_ROW_DELETED_COLUMN_NAME)
    if (markerColumnOpt.isEmpty) return parquetDataReader

    val rowIndexColumn = findColumn(ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME).getOrElse {
      throw SparkException.internalError(
        "AMT Manifest-DV reads require the Parquet row-index metadata column")
    }
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    val useOffHeapBuffers = sparkSession.sessionState.conf.offHeapColumnVectorEnabled

    (partitionedFile: PartitionedFile) => {
      val parquetIterator = parquetDataReader(partitionedFile)
      try {
        val rowIndexFilter = partitionedFile.otherConstantMetadataColumnValues
          .get(DeltaLogFileIndex.ROW_INDEX_FILTER_PROVIDER_METADATA_KEY)
          .map(_.asInstanceOf[RowIndexFilterProvider].retrieve(serializableHadoopConf.value))
          .getOrElse(KeepAllRowsFilter)
        RowIndexFilterMetadataColumnUtils.iteratorWithAdditionalMetadataColumns(
          parquetIterator,
          Some(rowIndexFilter),
          markerColumnOpt,
          Some(rowIndexColumn),
          useOffHeapBuffers,
          useMetadataRowIndex = true).asInstanceOf[Iterator[InternalRow]]
      } catch {
        case NonFatal(e) =>
          parquetIterator match {
            case resource: AutoCloseable =>
              RowIndexFilterMetadataColumnUtils.closeQuietly(resource)
            case _ => // Nothing to close.
          }
          throw e
      }
    }
  }
}

private[delta] object AMTParquetFileFormat {
  val IS_ROW_DELETED_COLUMN_NAME = "__delta_internal_is_row_deleted"
  val IS_ROW_DELETED_STRUCT_FIELD = StructField(IS_ROW_DELETED_COLUMN_NAME, ByteType)


}
