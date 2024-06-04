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

import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.VacuumCommand.generateCandidateFileMap
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetToSparkSchemaConverter}
import org.apache.spark.sql.types.{AtomicType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

trait ReorgTableHelper extends Serializable {
  /**
   * Determine whether `fileSchema` has any columns that has a type that differs from
   * `tablePhysicalSchema`.
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
