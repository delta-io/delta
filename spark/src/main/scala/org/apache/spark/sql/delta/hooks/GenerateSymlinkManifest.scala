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

package org.apache.spark.sql.delta.hooks

// scalastyle:off import.ordering.noEmptyLine
import java.net.URI

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.DeletionVectorUtils.isTableDVFree
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkEnv
import org.apache.spark.internal.MDC
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Concat, Expression, Literal, ScalaUDF}
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.SerializableConfiguration

/**
 * Post commit hook to generate hive-style manifests for Delta table. This is useful for
 * compatibility with Presto / Athena.
 */
object GenerateSymlinkManifest extends GenerateSymlinkManifestImpl

// A separate singleton to avoid creating encoders from scratch every time
object GenerateSymlinkManifestUtils extends DeltaLogging {
  private[hooks] lazy val mapEncoder = try {
    ExpressionEncoder[Map[String, String]]()
  } catch {
    case e: Throwable =>
      logError(e.getMessage, e)
      throw e
  }
}

trait GenerateSymlinkManifestImpl extends PostCommitHook with DeltaLogging with Serializable {
  val CONFIG_NAME_ROOT = "compatibility.symlinkFormatManifest"

  val MANIFEST_LOCATION = "_symlink_format_manifest"

  val OP_TYPE_ROOT = "delta.compatibility.symlinkFormatManifest"
  val FULL_MANIFEST_OP_TYPE = s"$OP_TYPE_ROOT.full"
  val INCREMENTAL_MANIFEST_OP_TYPE = s"$OP_TYPE_ROOT.incremental"

  override val name: String = "Generate Symlink Format Manifest"

  override def run(
      spark: SparkSession,
      txn: OptimisticTransactionImpl,
      committedVersion: Long,
      postCommitSnapshot: Snapshot,
      committedActions: Seq[Action]): Unit = {
    generateIncrementalManifest(
      spark, txn.deltaLog, txn.snapshot, postCommitSnapshot, committedActions)
  }

  override def handleError(spark: SparkSession, error: Throwable, version: Long): Unit = {
    error match {
      case e: ColumnMappingUnsupportedException => throw e
      case e: DeltaCommandUnsupportedWithDeletionVectorsException => throw e
      case _ =>
        throw DeltaErrors.postCommitHookFailedException(this, version, name, error)
    }
  }

  /**
   * Generate manifest files incrementally, that is, only for the table partitions touched by the
   * given actions.
   */
  protected def generateIncrementalManifest(
      spark: SparkSession,
      deltaLog: DeltaLog,
      txnReadSnapshot: Snapshot,
      currentSnapshot: Snapshot,
      actions: Seq[Action]): Unit = recordManifestGeneration(deltaLog, full = false) {

    import org.apache.spark.sql.delta.implicits._

    checkColumnMappingMode(currentSnapshot.metadata)

    val partitionCols = currentSnapshot.metadata.partitionColumns
    val manifestRootDirPath = new Path(deltaLog.dataPath, MANIFEST_LOCATION)
    val hadoopConf = new SerializableConfiguration(deltaLog.newDeltaHadoopConf())
    val fs = deltaLog.dataPath.getFileSystem(hadoopConf.value)
    if (!fs.exists(manifestRootDirPath)) {
      generateFullManifest(spark, deltaLog)
      return
    }

    // Find all the manifest partitions that need to updated or deleted
    val (allFilesInUpdatedPartitions, nowEmptyPartitions) = if (partitionCols.nonEmpty) {
      // Get the partitions where files were added
      val partitionsOfAddedFiles = actions.collect { case a: AddFile => a.partitionValues }.toSet

      // Get the partitions where files were deleted
      val removedFileNames =
        spark.createDataset(actions.collect { case r: RemoveFile => r.path }).toDF("path")
      val partitionValuesOfRemovedFiles =
        txnReadSnapshot.allFiles.join(removedFileNames, "path").select("partitionValues").persist()
      try {
        val partitionsOfRemovedFiles = partitionValuesOfRemovedFiles
          .as[Map[String, String]](GenerateSymlinkManifestUtils.mapEncoder).collect().toSet

        // Get the files present in the updated partitions
        val partitionsUpdated: Set[Map[String, String]] =
          partitionsOfAddedFiles ++ partitionsOfRemovedFiles
        val filesInUpdatedPartitions = currentSnapshot.allFiles.filter { a =>
          partitionsUpdated.contains(a.partitionValues)
        }

        // Find the current partitions
        val currentPartitionRelativeDirs =
          withRelativePartitionDir(spark, partitionCols, currentSnapshot.allFiles)
            .select("relativePartitionDir").distinct()

        // Find the partitions that became empty and delete their manifests
        val partitionRelativeDirsOfRemovedFiles =
          withRelativePartitionDir(spark, partitionCols, partitionValuesOfRemovedFiles)
            .select("relativePartitionDir").distinct()

        val partitionsThatBecameEmpty =
          partitionRelativeDirsOfRemovedFiles.join(
            currentPartitionRelativeDirs, Seq("relativePartitionDir"), "leftanti")
            .as[String].collect()

        (filesInUpdatedPartitions, partitionsThatBecameEmpty)
      } finally {
        partitionValuesOfRemovedFiles.unpersist()
      }
    } else {
      (currentSnapshot.allFiles, Array.empty[String])
    }

    val manifestFilePartitionsWritten = writeManifestFiles(
      deltaLog.dataPath,
      manifestRootDirPath.toString,
      allFilesInUpdatedPartitions,
      partitionCols,
      hadoopConf)

    if (nowEmptyPartitions.nonEmpty) {
      deleteManifestFiles(manifestRootDirPath.toString, nowEmptyPartitions, hadoopConf)
    }

    // Post stats
    val stats = SymlinkManifestStats(
      filesWritten = manifestFilePartitionsWritten.size,
      filesDeleted = nowEmptyPartitions.length,
      partitioned = partitionCols.nonEmpty)
    recordDeltaEvent(deltaLog, s"$INCREMENTAL_MANIFEST_OP_TYPE.stats", data = stats)
  }

  /**
   * Generate manifest files for all the partitions in the table. Note, this will ensure that
   * that stale and unnecessary files will be vacuumed.
   */
  def generateFullManifest(
      spark: SparkSession,
      deltaLog: DeltaLog): Unit = {
    val snapshot = deltaLog.update(stalenessAcceptable = false)
    assertTableIsDVFree(spark, snapshot)
    generateFullManifestWithSnapshot(spark, deltaLog, snapshot)
  }

  // Separated out to allow overriding with a specific snapshot.
  protected def generateFullManifestWithSnapshot(
      spark: SparkSession,
      deltaLog: DeltaLog,
      snapshot: Snapshot): Unit = recordManifestGeneration(deltaLog, full = true) {
    val partitionCols = snapshot.metadata.partitionColumns
    val manifestRootDirPath = new Path(deltaLog.dataPath, MANIFEST_LOCATION).toString
    val hadoopConf = new SerializableConfiguration(deltaLog.newDeltaHadoopConf())

    checkColumnMappingMode(snapshot.metadata)

    // Update manifest files of the current partitions
    val newManifestPartitionRelativePaths = writeManifestFiles(
      deltaLog.dataPath,
      manifestRootDirPath,
      snapshot.allFiles,
      partitionCols,
      hadoopConf)

    // Get the existing manifest files as relative partition paths, that is,
    // [ "col1=0/col2=0", "col1=1/col2=1", "col1=2/col2=2" ]
    val fs = deltaLog.dataPath.getFileSystem(hadoopConf.value)
    val existingManifestPartitionRelativePaths = {
      val manifestRootDirAbsPath = fs.makeQualified(new Path(manifestRootDirPath))
      if (fs.exists(manifestRootDirAbsPath)) {
        val index = new InMemoryFileIndex(
          spark,
          Seq(manifestRootDirAbsPath),
          deltaLog.options,
          None)
        val prefixToStrip = manifestRootDirAbsPath.toUri.getPath
        index.inputFiles.map { p =>
          // Remove root directory "rootDir" path from the manifest file paths like
          // "rootDir/col1=0/col2=0/manifest" to get the relative partition dir "col1=0/col2=0".
          // Note: It important to compare only the "path" in the URI and not the user info in it.
          // In s3a://access-key:secret-key@host/path, the access-key and secret-key may change
          // unknowingly to `\` and `%` encoding between the root dir and file names generated
          // by listing.
          val relativeManifestFilePath =
            new URI(p).getPath.stripPrefix(prefixToStrip).stripPrefix(Path.SEPARATOR)
          new Path(relativeManifestFilePath).getParent.toString // returns "col1=0/col2=0"
        }.filterNot(_.trim.isEmpty).toSet
      } else Set.empty[String]
    }
    // paths returned from inputFiles are URI encoded so we need to convert them back to string.
    // So that they can compared with newManifestPartitionRelativePaths in the next step.

    // Delete manifest files for partitions that are not in current and so weren't overwritten
    val manifestFilePartitionsToDelete =
      existingManifestPartitionRelativePaths.diff(newManifestPartitionRelativePaths)
    deleteManifestFiles(manifestRootDirPath, manifestFilePartitionsToDelete, hadoopConf)

    // Post stats
    val stats = SymlinkManifestStats(
      filesWritten = newManifestPartitionRelativePaths.size,
      filesDeleted = manifestFilePartitionsToDelete.size,
      partitioned = partitionCols.nonEmpty)
    recordDeltaEvent(deltaLog, s"$FULL_MANIFEST_OP_TYPE.stats", data = stats)
  }

  protected def assertTableIsDVFree(spark: SparkSession, snapshot: Snapshot): Unit = {
    if (!isTableDVFree(snapshot)) {
      throw DeltaErrors.generateNotSupportedWithDeletionVectors()
    }
  }

  /**
   * Write the manifest files and return the partition relative paths of the manifests written.
   *
   * @param deltaLogDataPath     path of the table data (e.g., tablePath which has _delta_log in it)
   * @param manifestRootDirPath  root directory of the manifest files (e.g., tablePath/_manifest/)
   * @param fileNamesForManifest relative paths or file names of data files for being written into
   *                             the manifest (e.g., partition=1/xyz.parquet)
   * @param partitionCols        Table partition columns
   * @param hadoopConf           Hadoop configuration to use
   * @return Set of partition relative paths of the written manifest files (e.g., part1=1/part2=2)
   */
  private def writeManifestFiles(
      deltaLogDataPath: Path,
      manifestRootDirPath: String,
      fileNamesForManifest: Dataset[AddFile],
      partitionCols: Seq[String],
      hadoopConf: SerializableConfiguration): Set[String] = {

    val spark = fileNamesForManifest.sparkSession
    import org.apache.spark.sql.delta.implicits._

    val tableAbsPathForManifest = LogStore(spark)
      .resolvePathOnPhysicalStorage(deltaLogDataPath, hadoopConf.value).toString

    /** Write the data file relative paths to manifestDirAbsPath/manifest as absolute paths */
    def writeSingleManifestFile(
      manifestDirAbsPath: String,
      dataFileRelativePaths: Iterator[String]): Unit = {

      val manifestFilePath = new Path(manifestDirAbsPath, "manifest")
      val fs = manifestFilePath.getFileSystem(hadoopConf.value)
      fs.mkdirs(manifestFilePath.getParent())

      val manifestContent = dataFileRelativePaths.map { relativePath =>
        DeltaFileOperations.absolutePath(tableAbsPathForManifest, relativePath).toString
      }
      val logStore = LogStore(SparkEnv.get.conf, hadoopConf.value)
      logStore.write(manifestFilePath, manifestContent, overwrite = true, hadoopConf.value)
    }

    val newManifestPartitionRelativePaths =
      if (fileNamesForManifest.isEmpty && partitionCols.isEmpty) {
        writeSingleManifestFile(manifestRootDirPath, Iterator())
        Set.empty[String]
      } else {
        withRelativePartitionDir(spark, partitionCols, fileNamesForManifest)
          .select("relativePartitionDir", "path").as[(String, String)]
          .groupByKey(_._1).mapGroups {
          (relativePartitionDir: String, relativeDataFilePath: Iterator[(String, String)]) =>
            val manifestPartitionDirAbsPath = {
              if (relativePartitionDir == null || relativePartitionDir.isEmpty) manifestRootDirPath
              else new Path(manifestRootDirPath, relativePartitionDir).toString
            }
            writeSingleManifestFile(manifestPartitionDirAbsPath, relativeDataFilePath.map(_._2))
            relativePartitionDir
        }.collect().toSet
      }

    logInfo(log"Generated manifest partitions for ${MDC(DeltaLogKeys.PATH, deltaLogDataPath)} " +
      log"[${MDC(DeltaLogKeys.NUM_PARTITIONS, newManifestPartitionRelativePaths.size)}]:\n\t" +
      log"${MDC(DeltaLogKeys.PATHS, newManifestPartitionRelativePaths.mkString("\n\t"))}")

    newManifestPartitionRelativePaths
  }

  /**
   * Delete manifest files in the given paths.
   *
   * @param manifestRootDirPath root directory of the manifest files (e.g., tablePath/_manifest/)
   * @param partitionRelativePathsToDelete partitions to delete manifest files from
   *                                       (e.g., part1=1/part2=2/)
   * @param hadoopConf Hadoop configuration to use
   */
  private def deleteManifestFiles(
      manifestRootDirPath: String,
      partitionRelativePathsToDelete: Iterable[String],
      hadoopConf: SerializableConfiguration): Unit = {

    val fs = new Path(manifestRootDirPath).getFileSystem(hadoopConf.value)
    partitionRelativePathsToDelete.foreach { path =>
      val absPathToDelete = new Path(manifestRootDirPath, path)
      fs.delete(absPathToDelete, true)
    }

    logInfo(log"Deleted manifest partitions [" +
      log"${MDC(DeltaLogKeys.NUM_FILES, partitionRelativePathsToDelete.size)}]:\n\t" +
      log"${MDC(DeltaLogKeys.PATHS, partitionRelativePathsToDelete.mkString("\n\t"))}")
  }

  /**
   * Append a column `relativePartitionDir` to the given Dataset which has `partitionValues` as
   * one of the columns. `partitionValues` is a map-type column that contains values of the
   * given `partitionCols`.
   */
  private def withRelativePartitionDir(
      spark: SparkSession,
      partitionCols: Seq[String],
      datasetWithPartitionValues: Dataset[_]) = {

    require(datasetWithPartitionValues.schema.fieldNames.contains("partitionValues"))
    val colNamePrefix = "_col_"

    // Flatten out nested partition value columns while renaming them, so that the new columns do
    // not conflict with existing columns in DF `pathsWithPartitionValues.
    val colToRenamedCols = partitionCols.map { column => column -> s"$colNamePrefix$column" }

    val df = colToRenamedCols.foldLeft(datasetWithPartitionValues.toDF()) {
      case(currentDs, (column, renamedColumn)) =>
        currentDs.withColumn(renamedColumn, col(s"partitionValues.`$column`"))
    }

    // Mapping between original column names to use for generating partition path and
    // attributes referring to corresponding columns added to DF `pathsWithPartitionValues`.
    val colNameToAttribs =
      colToRenamedCols.map { case (col, renamed) => col -> UnresolvedAttribute.quoted(renamed) }

    // Build an expression that can generate the path fragment col1=value/col2=value/ from the
    // partition columns. Note: The session time zone maybe different from the time zone that was
    // used to write the partition structure of the actual data files. This may lead to
    // inconsistencies between the partition structure of metadata files and data files.
    val relativePartitionDirExpression = generatePartitionPathExpression(
      colNameToAttribs,
      spark.sessionState.conf.sessionLocalTimeZone)

    df.withColumn("relativePartitionDir", new Column(relativePartitionDirExpression))
      .drop(colToRenamedCols.map(_._2): _*)
  }

  /** Expression that given partition columns builds a path string like: col1=val/col2=val/... */
  protected def generatePartitionPathExpression(
      partitionColNameToAttrib: Seq[(String, Attribute)],
      timeZoneId: String): Expression = Concat(

    partitionColNameToAttrib.zipWithIndex.flatMap { case ((colName, col), i) =>
      val partitionName = ScalaUDF(
        ExternalCatalogUtils.getPartitionPathString _,
        StringType,
        Seq(Literal(colName), Cast(col, StringType, Option(timeZoneId))))
      if (i == 0) Seq(partitionName) else Seq(Literal(Path.SEPARATOR), partitionName)
    }
  )


  private def recordManifestGeneration(deltaLog: DeltaLog, full: Boolean)(thunk: => Unit): Unit = {
    val (opType, manifestType) =
      if (full) FULL_MANIFEST_OP_TYPE -> "full"
      else INCREMENTAL_MANIFEST_OP_TYPE -> "incremental"
    recordDeltaOperation(deltaLog, opType) {
      withStatusCode("DELTA", s"Updating $manifestType Hive manifest for the Delta table") {
        thunk
      }
    }
  }

  /**
   * Generating manifests, when column mapping used is not supported,
   * because external systems will not be able to read Delta tables that leverage
   * column mapping correctly.
   */
  private def checkColumnMappingMode(metadata: Metadata): Unit = {
    if (metadata.columnMappingMode != NoMapping) {
      throw DeltaErrors.generateManifestWithColumnMappingNotSupported
    }
  }

  case class SymlinkManifestStats(
      filesWritten: Int,
      filesDeleted: Int,
      partitioned: Boolean)
}
