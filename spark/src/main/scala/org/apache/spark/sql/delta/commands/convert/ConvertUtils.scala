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

import java.lang.reflect.InvocationTargetException

import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaErrors}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{DateFormatter, DeltaFileOperations, PartitionUtils, TimestampFormatter}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.{AnalysisException, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.Utils

object ConvertUtils extends ConvertUtilsBase

trait ConvertUtilsBase extends DeltaLogging {

  val timestampPartitionPattern = "yyyy-MM-dd HH:mm:ss[.S]"

  var icebergSparkTableClassPath =
    "org.apache.spark.sql.delta.commands.convert.IcebergTable"
  var icebergLibTableClassPath = "org.apache.iceberg.Table"

  /**
   * Creates a source Parquet table for conversion.
   *
   * @param spark: the spark session to use.
   * @param targetDir: the target directory of the Parquet table.
   * @param catalogTable: the optional catalog table of the Parquet table.
   * @param partitionSchema: the user provided partition schema (if exists) of the Parquet table.
   * @return a target Parquet table.
   */
  def getParquetTable(
      spark: SparkSession,
      targetDir: String,
      catalogTable: Option[CatalogTable],
      partitionSchema: Option[StructType]): ConvertTargetTable = {
    val qualifiedDir = getQualifiedPath(spark, new Path(targetDir)).toString
    new ParquetTable(spark, qualifiedDir, catalogTable, partitionSchema)
  }

  /**
   * Creates a source Iceberg table for conversion.
   *
   * @param spark: the spark session to use.
   * @param targetDir: the target directory of the Iceberg table.
   * @param sparkTable: the optional V2 table interface of the Iceberg table.
   * @param tableSchema: the existing converted Delta table schema (if exists) of the Iceberg table.
   * @return a target Iceberg table.
   */
  def getIcebergTable(
      spark: SparkSession,
      targetDir: String,
      sparkTable: Option[Table],
      tableSchema: Option[StructType]): ConvertTargetTable = {
    try {
      val clazz = Utils.classForName(icebergSparkTableClassPath)
      if (sparkTable.isDefined) {
        val constFromTable = clazz.getConstructor(
          classOf[SparkSession],
          Utils.classForName(icebergLibTableClassPath),
          classOf[Option[StructType]])
        val method = sparkTable.get.getClass.getMethod("table")
        constFromTable.newInstance(spark, method.invoke(sparkTable.get), tableSchema)
      } else {
        val baseDir = getQualifiedPath(spark, new Path(targetDir)).toString
        val constFromPath = clazz.getConstructor(
          classOf[SparkSession], classOf[String], classOf[Option[StructType]])
        constFromPath.newInstance(spark, baseDir, tableSchema)
      }
    } catch {
      case e: ClassNotFoundException =>
        logError(s"Failed to find Iceberg class", e)
        throw DeltaErrors.icebergClassMissing(spark.sparkContext.getConf, e)
      case e: InvocationTargetException =>
        logError(s"Got error when creating an Iceberg Converter", e)
        // The better error is within the cause
        throw ExceptionUtils.getRootCause(e)
    }
  }

  /**
   * Generates a qualified Hadoop path from a given path.
   *
   * @param spark: the spark session to use
   * @param path: the raw path used to generate the qualified path.
   * @return the qualified path of the provided raw path.
   */
  def getQualifiedPath(spark: SparkSession, path: Path): Path = {
    // scalastyle:off deltahadoopconfiguration
    val sessionHadoopConf = spark.sessionState.newHadoopConf()
    // scalastyle:on deltahadoopconfiguration
    val fs = path.getFileSystem(sessionHadoopConf)
    val qualifiedPath = fs.makeQualified(path)
    if (!fs.exists(qualifiedPath)) {
      throw DeltaErrors.directoryNotFoundException(qualifiedPath.toString)
    }
    qualifiedPath
  }

  /**
   * Generates AddFile from ConvertTargetFile for conversion.
   *
   * @param targetFile: the target file to convert.
   * @param basePath: the table directory of the target file.
   * @param fs: the file system to access the target file.
   * @param conf: the SQL configures use to convert.
   * @param partitionSchema: the partition schema of the target file if exists.
   * @param useAbsolutePath: whether to use absolute path instead of relative path in the AddFile.
   * @return an AddFile corresponding to the provided ConvertTargetFile.
   */
  def createAddFile(
      targetFile: ConvertTargetFile,
      basePath: Path,
      fs: FileSystem,
      conf: SQLConf,
      partitionSchema: Option[StructType],
      useAbsolutePath: Boolean = false): AddFile = {
    val partitionFields = partitionSchema.map(_.fields.toSeq).getOrElse(Nil)
    val partitionColNames = partitionSchema.map(_.fieldNames.toSeq).getOrElse(Nil)
    val physicalPartitionColNames = partitionSchema.map(_.map { f =>
      DeltaColumnMapping.getPhysicalName(f)
    }).getOrElse(Nil)
    val file = targetFile.fileStatus
    val path = file.getHadoopPath
    val partition = targetFile.partitionValues.getOrElse {
      // partition values are not provided by the source table format, so infer from the file path
      val pathStr = file.getHadoopPath.toUri.toString
      val dateFormatter = DateFormatter()
      val timestampFormatter =
        TimestampFormatter(timestampPartitionPattern, java.util.TimeZone.getDefault)
      val resolver = conf.resolver
      val dir = if (file.isDir) file.getHadoopPath else file.getHadoopPath.getParent
      val (partitionOpt, _) = PartitionUtils.parsePartition(
        dir,
        typeInference = false,
        basePaths = Set(basePath),
        userSpecifiedDataTypes = Map.empty,
        validatePartitionColumns = false,
        java.util.TimeZone.getDefault,
        dateFormatter,
        timestampFormatter)

      partitionOpt.map { partValues =>
        if (partitionColNames.size != partValues.columnNames.size) {
          throw DeltaErrors.unexpectedNumPartitionColumnsFromFileNameException(
            pathStr, partValues.columnNames, partitionColNames)
        }

        val tz = Option(conf.sessionLocalTimeZone)
        // Check if the partition value can be casted to the provided type
        if (!conf.getConf(DeltaSQLConf.DELTA_CONVERT_PARTITION_VALUES_IGNORE_CAST_FAILURE)) {
          partValues.literals.zip(partitionFields).foreach { case (literal, field) =>
            if (literal.eval() != null &&
              Cast(literal, field.dataType, tz, ansiEnabled = false).eval() == null) {
              val partitionValue = Cast(literal, StringType, tz, ansiEnabled = false).eval()
              val partitionValueStr = Option(partitionValue).map(_.toString).orNull
              throw DeltaErrors.castPartitionValueException(partitionValueStr, field.dataType)
            }
          }
        }

        val values = partValues
          .literals
          .map(l => Cast(l, StringType, tz, ansiEnabled = false).eval())
          .map(Option(_).map(_.toString).orNull)

        partitionColNames.zip(partValues.columnNames).foreach { case (expected, parsed) =>
          if (!resolver(expected, parsed)) {
            throw DeltaErrors.unexpectedPartitionColumnFromFileNameException(
              pathStr, parsed, expected)
          }
        }
        physicalPartitionColNames.zip(values).toMap
      }.getOrElse {
        if (partitionColNames.nonEmpty) {
          throw DeltaErrors.unexpectedNumPartitionColumnsFromFileNameException(
            pathStr, Seq.empty, partitionColNames)
        }
        Map[String, String]()
      }
    }

    val pathStrForAddFile = if (!useAbsolutePath) {
      val relativePath = DeltaFileOperations.tryRelativizePath(fs, basePath, path)
      assert(!relativePath.isAbsolute,
        s"Fail to relativize path $path against base path $basePath.")
      relativePath.toUri.toString
    } else {
      path.toUri.toString
    }

    AddFile(pathStrForAddFile, partition, file.length, file.modificationTime, dataChange = true)
  }

  /**
   * A helper function to check whether a file should be included during conversion.
   *
   * @param fileName: the file name to check.
   * @return true if file should be included for conversion, otherwise false.
   */
  def hiddenDirNameFilter(fileName: String): Boolean = {
    // Allow partition column name starting with underscore and dot
    DeltaFileOperations.defaultHiddenFileFilter(fileName) && !fileName.contains("=")
  }

  /**
   * Merges the schemas of the ConvertTargetFiles.
   *
   * @param spark: the SparkSession used for schema merging.
   * @param partitionSchema: the partition schema to be merged with the data schema.
   * @param convertTargetFiles: the Dataset of ConvertTargetFiles to be merged.
   * @return the merged StructType representing the combined schema of the Parquet files.
   * @throws DeltaErrors.failedInferSchema If no schemas are found for merging.
   */
  def mergeSchemasInParallel(
      spark: SparkSession,
      partitionSchema: StructType,
      convertTargetFiles: Dataset[ConvertTargetFile]): StructType = {
    import org.apache.spark.sql.delta.implicits._
    val partiallyMergedSchemas = convertTargetFiles.mapPartitions { iterator =>
      var dataSchema: StructType = StructType(Seq())
      iterator.foreach { file =>
        try {
          dataSchema = SchemaMergingUtils.mergeSchemas(dataSchema,
            StructType.fromDDL(file.parquetSchemaDDL.get).asNullable)
        } catch {
          case cause: AnalysisException =>
            throw DeltaErrors.failedMergeSchemaFile(
              file.fileStatus.path, StructType.fromDDL(file.parquetSchemaDDL.get).treeString, cause)
        }
      }
      Iterator.single(dataSchema.toDDL)
    }.collect().filter(_.nonEmpty)

    if (partiallyMergedSchemas.isEmpty) {
      throw DeltaErrors.failedInferSchema
    }
    var mergedSchema: StructType = StructType(Seq())
    partiallyMergedSchemas.foreach { schema =>
      mergedSchema = SchemaMergingUtils.mergeSchemas(mergedSchema, StructType.fromDDL(schema))
    }
    PartitioningUtils.mergeDataAndPartitionSchema(
      mergedSchema,
      StructType(partitionSchema.fields.toSeq),
      spark.sessionState.conf.caseSensitiveAnalysis)._1
  }
}

/**
 * Configuration for fetching Parquet schema.
 *
 * @param assumeBinaryIsString: whether unannotated BINARY fields should be assumed to be Spark
 *                              SQL [[StringType]] fields.
 * @param assumeInt96IsTimestamp: whether unannotated INT96 fields should be assumed to be Spark
 *                                SQL [[TimestampType]] fields.
 * @param ignoreCorruptFiles: a boolean indicating whether corrupt files should be ignored during
 *                            schema retrieval.
 */
case class ParquetSchemaFetchConfig(
  assumeBinaryIsString: Boolean,
  assumeInt96IsTimestamp: Boolean,
  ignoreCorruptFiles: Boolean)

