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

package org.apache.spark.sql.delta.commands

import org.apache.spark.sql.delta.{MaterializedRowCommitVersion, MaterializedRowId, Snapshot}
import org.apache.spark.sql.delta.actions.{AddFile, Metadata, Protocol}
import org.apache.spark.sql.delta.commands.VacuumCommand.generateCandidateFileMap
import org.apache.spark.sql.delta.schema.{SchemaMergingUtils, SchemaUtils}
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetToSparkSchemaConverter}
import org.apache.spark.sql.types.{AtomicType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

trait ReorgTableHelper extends Serializable {
  /**
   * Determine whether `fileSchema` has any column that has a type that differs from
   * `tablePhysicalSchema`.
   *
   * @param fileSchema the current parquet schema to be checked.
   * @param tablePhysicalSchema the current table schema.
   * @return whether the file has any column that has a different type from table column.
   */
  protected def fileHasDifferentTypes(
      fileSchema: StructType,
      tablePhysicalSchema: StructType): Boolean = {
    SchemaMergingUtils.transformColumns(fileSchema, tablePhysicalSchema) {
      case (_, StructField(_, fileType: AtomicType, _, _),
        Some(StructField(_, tableType: AtomicType, _, _)), _) if fileType != tableType =>
        return true
      case (_, field, _, _) => field
    }
    false
  }

  /**
   * Determine whether `fileSchema` has any column that does not exist in the
   * `tablePhysicalSchema`, this is possible by running ALTER TABLE commands,
   * e.g., ALTER TABLE DROP COLUMN.
   *
   * @param fileSchema the current parquet schema to be checked.
   * @param tablePhysicalSchema the current table schema.
   * @param protocol the protocol used to check `row_id` and `row_commit_version`.
   * @param metadata the metadata used to check `row_id` and `row_commit_version`.
   * @return whether the file has any dropped column.
   */
  protected def fileHasExtraColumns(
      fileSchema: StructType,
      tablePhysicalSchema: StructType,
      protocol: Protocol,
      metadata: Metadata): Boolean = {
    // 0. get the materialized names for `row_id` and `row_commit_version`.
    val materializedRowIdColumnNameOpt =
      MaterializedRowId.getMaterializedColumnName(protocol, metadata)
    val materializedRowCommitVersionColumnNameOpt =
      MaterializedRowCommitVersion.getMaterializedColumnName(protocol, metadata)

    SchemaMergingUtils.transformColumns(fileSchema) { (path, field, _) =>
      // 1. check whether the field exists in the `tablePhysicalSchema`.
      val fullName = path :+ field.name
      val inTableFieldOpt = SchemaUtils.findNestedFieldIgnoreCase(
        tablePhysicalSchema, fullName, includeCollections = true)

      // 2. check whether the current `field` is `row_id` or `row_commit_version`
      //    column; if so, we need to explicitly keep these columns since they are
      //    not part of the table schema but exist in the parquet file.
      val isRowIdOrRowCommitVersion = materializedRowIdColumnNameOpt.contains(field.name) ||
        materializedRowCommitVersionColumnNameOpt.contains(field.name)

      if (inTableFieldOpt.isEmpty && !isRowIdOrRowCommitVersion) {
        return true
      }
      field
    }
    false
  }

  /**
   * Apply a filter on the list of AddFile to only keep the files that have physical parquet schema
   * that satisfies the given filter function.
   *
   * Note: Filtering happens on the executors: **any variable captured by `filterFileFn` must be
   * Serializable**
   */
  protected def filterParquetFilesOnExecutors(
      spark: SparkSession,
      files: Seq[AddFile],
      snapshot: Snapshot,
      ignoreCorruptFiles: Boolean)(
      filterFileFn: StructType => Boolean): Seq[AddFile] = {

    val serializedConf = new SerializableConfiguration(snapshot.deltaLog.newDeltaHadoopConf())
    val assumeBinaryIsString = spark.sessionState.conf.isParquetBinaryAsString
    val assumeInt96IsTimestamp = spark.sessionState.conf.isParquetINT96AsTimestamp
    val dataPath = new Path(snapshot.deltaLog.dataPath.toString)

    import org.apache.spark.sql.delta.implicits._

    files.toDF(spark).as[AddFile].mapPartitions { iter =>
        filterParquetFiles(iter.toList, dataPath, serializedConf.value, ignoreCorruptFiles,
          assumeBinaryIsString, assumeInt96IsTimestamp)(filterFileFn).toIterator
    }.collect()
  }

  protected def filterParquetFiles(
      files: Seq[AddFile],
      dataPath: Path,
      configuration: Configuration,
      ignoreCorruptFiles: Boolean,
      assumeBinaryIsString: Boolean,
      assumeInt96IsTimestamp: Boolean)(
      filterFileFn: StructType => Boolean): Seq[AddFile] = {
    val nameToAddFileMap = generateCandidateFileMap(dataPath, files)

    val fileStatuses = nameToAddFileMap.map { case (absPath, addFile) =>
      new FileStatus(
        /* length */ addFile.size,
        /* isDir */ false,
        /* blockReplication */ 0,
        /* blockSize */ 1,
        /* modificationTime */ addFile.modificationTime,
        new Path(absPath)
      )
    }

    val footers = DeltaFileOperations.readParquetFootersInParallel(
      configuration,
      fileStatuses.toList,
      ignoreCorruptFiles)

    val converter =
      new ParquetToSparkSchemaConverter(assumeBinaryIsString, assumeInt96IsTimestamp)

    val filesNeedToRewrite = footers.filter { footer =>
      val fileSchema = ParquetFileFormat.readSchemaFromFooter(footer, converter)
      filterFileFn(fileSchema)
    }.map(_.getFile.toString)
    filesNeedToRewrite.map(absPath => nameToAddFileMap(absPath))
  }
}
