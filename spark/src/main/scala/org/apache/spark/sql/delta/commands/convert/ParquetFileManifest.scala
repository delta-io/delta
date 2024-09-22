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
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{DeltaFileOperations, PartitionUtils}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetToSparkSchemaConverter}
import org.apache.spark.sql.execution.streaming.MetadataLogFileIndex
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/** A file manifest generated through recursively listing a base path. */
class ManualListingFileManifest(
    spark: SparkSession,
    override val basePath: String,
    partitionSchema: StructType,
    parquetSchemaFetchConfig: ParquetSchemaFetchConfig,
    serializableConf: SerializableConfiguration)
  extends ConvertTargetFileManifest with DeltaLogging {

  protected def doList(): Dataset[SerializableFileStatus] = {
    val conf = spark.sparkContext.broadcast(serializableConf)
    DeltaFileOperations
      .recursiveListDirs(spark, Seq(basePath), conf, ConvertUtils.dirNameFilter)
      .where("!isDir")
  }

  override lazy val allFiles: Dataset[ConvertTargetFile] = {
    import org.apache.spark.sql.delta.implicits._

    val conf = spark.sparkContext.broadcast(serializableConf)
    val fetchConfig = parquetSchemaFetchConfig
    val files = doList().mapPartitions { iter =>
      val fileStatuses = iter.toSeq
      val pathToStatusMapping = fileStatuses.map { fileStatus =>
        fileStatus.path -> fileStatus
      }.toMap
      val footerSeq = DeltaFileOperations.readParquetFootersInParallel(
        conf.value.value, fileStatuses.map(_.toFileStatus), fetchConfig.ignoreCorruptFiles)
      val schemaConverter = new ParquetToSparkSchemaConverter(
        assumeBinaryIsString = fetchConfig.assumeBinaryIsString,
        assumeInt96IsTimestamp = fetchConfig.assumeInt96IsTimestamp
      )
      footerSeq.map { footer =>
        val fileStatus = pathToStatusMapping(footer.getFile.toString)
        val schema = ParquetFileFormat.readSchemaFromFooter(footer, schemaConverter)
        ConvertTargetFile(fileStatus, None, Some(schema.toDDL))
      }.toIterator
    }
    files.cache()
    files
  }

  override lazy val parquetSchema: Option[StructType] = {
    recordDeltaOperationForTablePath(basePath, "delta.convert.schemaInference") {
      Some(ConvertUtils.mergeSchemasInParallel(spark, partitionSchema, allFiles))
    }
  }

  override def close(): Unit = allFiles.unpersist()
}

/** A file manifest generated through listing partition paths from Metastore catalog. */
class CatalogFileManifest(
    spark: SparkSession,
    override val basePath: String,
    catalogTable: CatalogTable,
    partitionSchema: StructType,
    parquetSchemaFetchConfig: ParquetSchemaFetchConfig,
    serializableConf: SerializableConfiguration)
  extends ConvertTargetFileManifest with DeltaLogging {

  private val useCatalogSchema =
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CONVERT_USE_CATALOG_SCHEMA)

  // List of partition directories and corresponding partition values.
  private lazy val partitionList = {
    if (catalogTable.partitionSchema.isEmpty) {
      // Not a partitioned table.
      Seq(basePath -> Map.empty[String, String])
    } else {
      val partitions = spark.sessionState.catalog.listPartitions(catalogTable.identifier)
      partitions.map { partition =>
        // Convert URI into Path first to decode special characters.
        val partitionDir = partition.storage.locationUri.map(new Path(_).toString())
          .getOrElse {
            val partitionDir =
              PartitionUtils.getPathFragment(partition.spec, catalogTable.partitionSchema)
            basePath.stripSuffix("/") + "/" + partitionDir
          }
        partitionDir -> partition.spec
      }
    }
  }

  protected def doList(): Dataset[SerializableFileStatus] = {
    if (partitionList.isEmpty) {
      throw DeltaErrors.convertToDeltaNoPartitionFound(catalogTable.identifier.unquotedString)
    }

    ConvertUtils.listDirsInParallel(spark, basePath, partitionList.map(_._1), serializableConf)
  }

  override lazy val allFiles: Dataset[ConvertTargetFile] = {
    import org.apache.spark.sql.delta.implicits._

    // Avoid the serialization of this CatalogFileManifest during distributed execution.
    val conf = spark.sparkContext.broadcast(serializableConf)
    val useParquetSchema = !useCatalogSchema
    val dirToPartitionSpec = partitionList.toMap
    val fetchConfig = parquetSchemaFetchConfig

    val files = doList().mapPartitions { iter =>
      val fileStatuses = iter.toSeq
      if (useParquetSchema) {
        val pathToFile = fileStatuses.map { fileStatus => fileStatus.path -> fileStatus }.toMap
        val footerSeq = DeltaFileOperations.readParquetFootersInParallel(
          conf.value.value,
          fileStatuses.map(_.toFileStatus),
          fetchConfig.ignoreCorruptFiles)
        val schemaConverter = new ParquetToSparkSchemaConverter(
          assumeBinaryIsString = fetchConfig.assumeBinaryIsString,
          assumeInt96IsTimestamp = fetchConfig.assumeInt96IsTimestamp
        )
        footerSeq.map { footer =>
          val schema = ParquetFileFormat.readSchemaFromFooter(footer, schemaConverter)
          val fileStatus = pathToFile(footer.getFile.toString)
          ConvertTargetFile(
            fileStatus,
            dirToPartitionSpec.get(footer.getFile.getParent.toString),
            Some(schema.toDDL))
        }.toIterator
      } else {
        // TODO: Currently "spark.sql.files.ignoreCorruptFiles" is not respected for
        //  CatalogFileManifest when catalog schema is used to avoid performance regression.
        fileStatuses.map { fileStatus =>
            ConvertTargetFile(
              fileStatus,
              dirToPartitionSpec.get(fileStatus.getHadoopPath.getParent.toString),
              None)
        }.toIterator
      }
    }
    files.cache()
    files
  }

  override lazy val parquetSchema: Option[StructType] = {
    if (useCatalogSchema) {
      Some(catalogTable.schema)
    } else {
      recordDeltaOperationForTablePath(basePath, "delta.convert.schemaInference") {
        Some(ConvertUtils.mergeSchemasInParallel(spark, partitionSchema, allFiles))
      }
    }
  }

  override def close(): Unit = allFiles.unpersist()
}

/** A file manifest generated from pre-existing parquet MetadataLog. */
class MetadataLogFileManifest(
    spark: SparkSession,
    override val basePath: String,
    partitionSchema: StructType,
    parquetSchemaFetchConfig: ParquetSchemaFetchConfig,
    serializableConf: SerializableConfiguration)
  extends ConvertTargetFileManifest with DeltaLogging {

  val index = new MetadataLogFileIndex(spark, new Path(basePath), Map.empty, None)

  protected def doList(): Dataset[SerializableFileStatus] = {
    import org.apache.spark.sql.delta.implicits._

    val rdd = spark.sparkContext.parallelize(index.allFiles()).mapPartitions { _
        .map(SerializableFileStatus.fromStatus)
    }
    spark.createDataset(rdd)
  }

  override lazy val allFiles: Dataset[ConvertTargetFile] = {
    import org.apache.spark.sql.delta.implicits._

    val conf = spark.sparkContext.broadcast(serializableConf)
    val fetchConfig = parquetSchemaFetchConfig

    val files = doList().mapPartitions { iter =>
      val fileStatuses = iter.toSeq
      val pathToStatusMapping = fileStatuses.map { fileStatus =>
        fileStatus.path -> fileStatus
      }.toMap
      val footerSeq = DeltaFileOperations.readParquetFootersInParallel(
        conf.value.value, fileStatuses.map(_.toFileStatus), fetchConfig.ignoreCorruptFiles)
      val schemaConverter = new ParquetToSparkSchemaConverter(
        assumeBinaryIsString = fetchConfig.assumeBinaryIsString,
        assumeInt96IsTimestamp = fetchConfig.assumeInt96IsTimestamp
      )
      footerSeq.map { footer =>
        val fileStatus = pathToStatusMapping(footer.getFile.toString)
        val schema = ParquetFileFormat.readSchemaFromFooter(footer, schemaConverter)
        ConvertTargetFile(fileStatus, None, Some(schema.toDDL))
      }.toIterator
    }
    files.cache()
    files
  }

  override lazy val parquetSchema: Option[StructType] = {
    recordDeltaOperationForTablePath(basePath, "delta.convert.schemaInference") {
      Some(ConvertUtils.mergeSchemasInParallel(spark, partitionSchema, allFiles))
    }
  }

  override def close(): Unit = allFiles.unpersist()
}
