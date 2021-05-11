/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.files

import java.net.URI
import java.util.Objects

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, Snapshot}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.actions.SingleAction.addFileEncoder
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType

/**
 * A [[FileIndex]] that generates the list of files managed by the Tahoe protocol.
 */
abstract class TahoeFileIndex(
    val spark: SparkSession,
    val deltaLog: DeltaLog,
    val path: Path) extends FileIndex {

  def tableVersion: Long = deltaLog.snapshot.version

  override def rootPaths: Seq[Path] = path :: Nil

  /**
   * Returns all matching/valid files by the given `partitionFilters` and `dataFilters`.
   * Implementations may avoid evaluating data filters when doing so would be expensive, but
   * *must* evaluate the partition filters; wrong results will be produced if AddFile entries
   * which don't match the partition filters are returned.
   */
  def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[AddFile]

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val timeZone = spark.sessionState.conf.sessionLocalTimeZone
    matchingFiles(partitionFilters, dataFilters).groupBy(_.partitionValues).map {
      case (partitionValues, files) =>
        val rowValues: Array[Any] = partitionSchema.map { p =>
          Cast(Literal(partitionValues(p.name)), p.dataType, Option(timeZone)).eval()
        }.toArray


        val fileStats = files.map { f =>
          new FileStatus(
            /* length */ f.size,
            /* isDir */ false,
            /* blockReplication */ 0,
            /* blockSize */ 1,
            /* modificationTime */ f.modificationTime,
            absolutePath(f.path))
        }.toArray

        PartitionDirectory(new GenericInternalRow(rowValues), fileStats)
    }.toSeq
  }

  override def partitionSchema: StructType = deltaLog.snapshot.metadata.partitionSchema

  protected def absolutePath(child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      p
    } else {
      new Path(path, p)
    }
  }

  override def toString: String = {
    // the rightmost 100 characters of the path
    val truncatedPath = truncateRight(path.toString, len = 100)
    s"Delta[version=$tableVersion, $truncatedPath]"
  }

  /**
   * Gets the rightmost {@code len} characters of a String.
   *
   * @return the trimmed and formatted string.
   */
  private def truncateRight(input: String, len: Int): String = {
    if (input.length > len) {
      "... " + input.takeRight(len)
    } else {
      input
    }
  }

  /**
   * Returns the path of the base directory of the given file path (i.e. its parent directory with
   * all the partition directories stripped off).
   */
  def getBasePath(filePath: Path): Option[Path] = Some(path)

}


/**
 * A [[TahoeFileIndex]] that generates the list of files from DeltaLog with given partition filters.
 */
case class TahoeLogFileIndex(
    override val spark: SparkSession,
    override val deltaLog: DeltaLog,
    override val path: Path,
    snapshotAtAnalysis: Snapshot,
    partitionFilters: Seq[Expression] = Nil,
    isTimeTravelQuery: Boolean = false)
  extends TahoeFileIndex(spark, deltaLog, path) {

  override def tableVersion: Long = {
    if (isTimeTravelQuery) snapshotAtAnalysis.version else deltaLog.snapshot.version
  }

  private def checkSchemaOnRead: Boolean = {
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_SCHEMA_ON_READ_CHECK_ENABLED)
  }

  protected def getSnapshotToScan: Snapshot = {
    if (isTimeTravelQuery) snapshotAtAnalysis else deltaLog.update(stalenessAcceptable = true)
  }

  /** Provides the version that's being used as part of the scan if this is a time travel query. */
  def versionToUse: Option[Long] = if (isTimeTravelQuery) Some(snapshotAtAnalysis.version) else None

  def getSnapshot: Snapshot = {
    val snapshotToScan = getSnapshotToScan
    if (checkSchemaOnRead) {
      // Ensure that the schema hasn't changed in an incompatible manner since analysis time
      val snapshotSchema = snapshotToScan.metadata.schema
      if (!SchemaUtils.isReadCompatible(snapshotAtAnalysis.schema, snapshotSchema)) {
        throw DeltaErrors.schemaChangedSinceAnalysis(
            snapshotAtAnalysis.schema,
            snapshotSchema,
            mentionLegacyFlag = true)
      }
    }
    snapshotToScan
  }

  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[AddFile] = {
    getSnapshot.filesForScan(
      projection = Nil, this.partitionFilters ++ partitionFilters ++ dataFilters).files
  }

  override def inputFiles: Array[String] = {
    getSnapshot.filesForScan(
      projection = Nil, partitionFilters).files.map(f => absolutePath(f.path).toString).toArray
  }

  override def refresh(): Unit = {}
  override val sizeInBytes: Long = deltaLog.snapshot.sizeInBytes

  override def equals(that: Any): Boolean = that match {
    case t: TahoeLogFileIndex =>
      t.path == path && t.deltaLog.isSameLogAs(deltaLog) &&
        t.versionToUse == versionToUse && t.partitionFilters == partitionFilters
    case _ => false
  }

  override def hashCode: scala.Int = {
    Objects.hashCode(path, deltaLog.compositeId, versionToUse, partitionFilters)
  }

  override def partitionSchema: StructType = snapshotAtAnalysis.metadata.partitionSchema
}

object TahoeLogFileIndex {
  def apply(spark: SparkSession, deltaLog: DeltaLog): TahoeLogFileIndex =
    TahoeLogFileIndex(spark, deltaLog, deltaLog.dataPath, deltaLog.snapshot)
}

/**
 * A [[TahoeFileIndex]] that generates the list of files from a given list of files
 * that are within a version range of DeltaLog.
 */
class TahoeBatchFileIndex(
    override val spark: SparkSession,
    val actionType: String,
    val addFiles: Seq[AddFile],
    deltaLog: DeltaLog,
    path: Path,
    val snapshot: Snapshot,
    val partitionFiltersGenerated: Boolean = false)
  extends TahoeFileIndex(spark, deltaLog, path) {

  override def tableVersion: Long = snapshot.version

  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[AddFile] = {
    DeltaLog.filterFileList(
      snapshot.metadata.partitionSchema,
      spark.createDataset(addFiles)(addFileEncoder).toDF(), partitionFilters)
      .as[AddFile](addFileEncoder)
      .collect()
  }

  override def inputFiles: Array[String] = {
    addFiles.map(a => absolutePath(a.path).toString).toArray
  }

  override def partitionSchema: StructType = snapshot.metadata.partitionSchema

  override def refresh(): Unit = {}
  override val sizeInBytes: Long = addFiles.map(_.size).sum
}

/**
 * A [[TahoeFileIndex]] that generates the list of files from the given [[Snapshot]].
 */
case class PinnedTahoeFileIndex(
    override val spark: SparkSession,
    override val deltaLog: DeltaLog,
    override val path: Path,
    snapshot: Snapshot) extends TahoeFileIndex(spark, deltaLog, path) {

  override def tableVersion: Long = snapshot.version

  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[AddFile] = {
    snapshot.filesForScan(projection = Nil, partitionFilters ++ dataFilters).files
  }

  override def inputFiles: Array[String] = {
    snapshot.filesForScan(Nil, Nil).files.map(f => absolutePath(f.path).toString).toArray
  }

  override def refresh(): Unit = {}

  override val sizeInBytes: Long = snapshot.sizeInBytes

  override def equals(that: Any): Boolean = that match {
    case t: PinnedTahoeFileIndex =>
      t.path == path && t.deltaLog.isSameLogAs(deltaLog) &&
        t.snapshot.version == snapshot.version
    case _ => false
  }

  override def hashCode: scala.Int = {
    Objects.hash(path, deltaLog.compositeId, java.lang.Long.valueOf(snapshot.version))
  }

  override def partitionSchema: StructType = snapshot.metadata.partitionSchema
}
