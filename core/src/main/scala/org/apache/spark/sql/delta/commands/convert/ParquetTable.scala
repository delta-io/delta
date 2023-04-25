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

import org.apache.spark.sql.delta.{DeltaErrors, SerializableFileStatus}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetToSparkSchemaConverter}
import org.apache.spark.sql.execution.streaming.FileStreamSink
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * A target Parquet table for conversion to a Delta table.
 *
 * @param spark: spark session to use.
 * @param basePath: the root directory of the Parquet table.
 * @param catalogTable: optional catalog table (if exists) of the Parquet table.
 * @param userPartitionSchema: user provided partition schema of the Parquet table.
 */
class ParquetTable(
    val spark: SparkSession,
    val basePath: String,
    val catalogTable: Option[CatalogTable],
    val userPartitionSchema: Option[StructType]) extends ConvertTargetTable with DeltaLogging {

  // Validate user provided partition schema if catalogTable is available.
  if (catalogTable.isDefined && userPartitionSchema.isDefined
    && !catalogTable.get.partitionSchema.equals(userPartitionSchema.get)) {
    throw DeltaErrors.unexpectedPartitionSchemaFromUserException(
      catalogTable.get.partitionSchema, userPartitionSchema.get)
  }

  private var _numFiles: Option[Long] = None

  private var _tableSchema: Option[StructType] = {
    if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CONVERT_USE_CATALOG_SCHEMA)) {
      catalogTable.map(_.schema)
    } else {
      None
    }
  }

  protected lazy val serializableConf: SerializableConfiguration = {
    // scalastyle:off deltahadoopconfiguration
    new SerializableConfiguration(spark.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
  }

  override val partitionSchema: StructType = {
    userPartitionSchema.orElse(catalogTable.map(_.partitionSchema)).getOrElse(new StructType())
  }

  def numFiles: Long = {
    if (_numFiles.isEmpty) {
      inferSchema()
    }
    _numFiles.get
  }

  def tableSchema: StructType = {
    if (_tableSchema.isEmpty) {
      inferSchema()
    }
    _tableSchema.get
  }

  override val format: String = "parquet"

  /**
   * This method is forked from [[ParquetFileFormat]]. The only change here is that we use
   * our SchemaMergingUtils.mergeSchemas() instead of StructType.merge(),
   * where we allow upcast between ByteType, ShortType and IntegerType.
   *
   * Figures out a merged Parquet schema with a distributed Spark job.
   *
   * Note that locality is not taken into consideration here because:
   *
   *  1. For a single Parquet part-file, in most cases the footer only resides in the last block of
   *     that file.  Thus we only need to retrieve the location of the last block.  However, Hadoop
   *     `FileSystem` only provides API to retrieve locations of all blocks, which can be
   *     potentially expensive.
   *
   *  2. This optimization is mainly useful for S3, where file metadata operations can be pretty
   *     slow.  And basically locality is not available when using S3 (you can't run computation on
   *     S3 nodes).
   */
  protected def mergeSchemasInParallel(
      sparkSession: SparkSession,
      filesToTouch: Seq[FileStatus],
      serializedConf: SerializableConfiguration): Option[StructType] = {
    val assumeBinaryIsString = sparkSession.sessionState.conf.isParquetBinaryAsString
    val assumeInt96IsTimestamp = sparkSession.sessionState.conf.isParquetINT96AsTimestamp

    // !! HACK ALERT !!
    //
    // Parquet requires `FileStatus`es to read footers.  Here we try to send cached `FileStatus`es
    // to executor side to avoid fetching them again.  However, `FileStatus` is not `Serializable`
    // but only `Writable`.  What makes it worse, for some reason, `FileStatus` doesn't play well
    // with `SerializableWritable[T]` and always causes a weird `IllegalStateException`.  These
    // facts virtually prevents us to serialize `FileStatus`es.
    //
    // Since Parquet only relies on path and length information of those `FileStatus`es to read
    // footers, here we just extract them (which can be easily serialized), send them to executor
    // side, and resemble fake `FileStatus`es there.
    val partialFileStatusInfo = filesToTouch.map(f => (f.getPath.toString, f.getLen))

    // Set the number of partitions to prevent following schema reads from generating many tasks
    // in case of a small number of parquet files.
    val numParallelism = Math.min(Math.max(partialFileStatusInfo.size, 1),
      sparkSession.sparkContext.defaultParallelism)

    val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles

    // Issues a Spark job to read Parquet schema in parallel.
    val partiallyMergedSchemas =
      sparkSession
        .sparkContext
        .parallelize(partialFileStatusInfo, numParallelism)
        .mapPartitions { iterator =>
          // Resembles fake `FileStatus`es with serialized path and length information.
          val fakeFileStatuses = iterator.map { case (path, length) =>
            new FileStatus(length, false, 0, 0, 0, 0, null, null, null, new Path(path))
          }.toSeq

          // Reads footers in multi-threaded manner within each task
          val footers =
            DeltaFileOperations.readParquetFootersInParallel(
              serializedConf.value, fakeFileStatuses, ignoreCorruptFiles)

          // Converter used to convert Parquet `MessageType` to Spark SQL `StructType`
          val converter = new ParquetToSparkSchemaConverter(
            assumeBinaryIsString = assumeBinaryIsString,
            assumeInt96IsTimestamp = assumeInt96IsTimestamp)
          if (footers.isEmpty) {
            Iterator.empty
          } else {
            var mergedSchema = ParquetFileFormat.readSchemaFromFooter(footers.head, converter)
            footers.tail.foreach { footer =>
              val schema = ParquetFileFormat.readSchemaFromFooter(footer, converter)
              try {
                mergedSchema = SchemaMergingUtils.mergeSchemas(mergedSchema, schema)
              } catch { case cause: AnalysisException =>
                throw DeltaErrors.failedMergeSchemaFile(
                  footer.getFile.toString, schema.treeString, cause)
              }
            }
            Iterator.single(mergedSchema)
          }
        }.collect()

    if (partiallyMergedSchemas.isEmpty) {
      None
    } else {
      var finalSchema = partiallyMergedSchemas.head
      partiallyMergedSchemas.tail.foreach { schema =>
        finalSchema = SchemaMergingUtils.mergeSchemas(finalSchema, schema)
      }
      Some(finalSchema)
    }
  }

  protected def getSchemaForBatch(
      spark: SparkSession,
      batch: Seq[SerializableFileStatus],
      serializedConf: SerializableConfiguration): StructType = {
    mergeSchemasInParallel(spark, batch.map(_.toFileStatus), serializedConf).getOrElse(
      throw DeltaErrors.failedInferSchema)
  }

  /** Infers _tableSchema from Parquet schema of data files */
  private def inferSchema(): Unit = {
    val initialList = fileManifest.getFiles.map(_.fileStatus)
    val schemaBatchSize =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_IMPORT_BATCH_SIZE_SCHEMA_INFERENCE)
    var numFiles = 0L
    var dataSchema: StructType = StructType(Seq())
    recordDeltaOperationForTablePath(basePath, "delta.convert.schemaInference") {
      initialList.grouped(schemaBatchSize).foreach { batch =>
        numFiles += batch.size
        // Obtain a union schema from all files.
        // Here we explicitly mark the inferred schema nullable. This also means we don't
        // currently support specifying non-nullable columns after the table conversion.
        val batchSchema =
        getSchemaForBatch(spark, batch, serializableConf).asNullable
        dataSchema = SchemaMergingUtils.mergeSchemas(dataSchema, batchSchema)
      }
    }

    val partitionFields = partitionSchema.fields.toSeq

    _numFiles = Some(numFiles)
    _tableSchema = Some(PartitioningUtils.mergeDataAndPartitionSchema(
      dataSchema,
      StructType(partitionFields),
      spark.sessionState.conf.caseSensitiveAnalysis)._1)
  }

  val fileManifest: ConvertTargetFileManifest = {
    if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CONVERT_USE_METADATA_LOG) &&
      FileStreamSink.hasMetadata(Seq(basePath), serializableConf.value, spark.sessionState.conf)) {
      new MetadataLogFileManifest(spark, basePath)
    } else if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CONVERT_USE_CATALOG_PARTITIONS) &&
      catalogTable.isDefined) {
      new CatalogFileManifest(spark, basePath, catalogTable.get, serializableConf)
    } else {
      new ManualListingFileManifest(spark, basePath, serializableConf)
    }
  }
}
