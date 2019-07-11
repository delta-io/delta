/*
 * Copyright 2019 Databricks, Inc.
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

import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.actions.SingleAction.addFileEncoder
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

  /** Get a snapshot of the Delta table. */
  def getSnapshot(stalenessAcceptable: Boolean): Snapshot

  override def rootPaths: Seq[Path] = path :: Nil

  /**
   * Returns all matching/valid files by the given `partitionFilters` and `dataFilters`
   */
  def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression],
      keepStats: Boolean = false): Seq[AddFile]

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
}


/**
 * A [[TahoeFileIndex]] that generates the list of files from DeltaLog with given partition filters.
 */
case class TahoeLogFileIndex(
    override val spark: SparkSession,
    override val deltaLog: DeltaLog,
    override val path: Path,
    partitionFilters: Seq[Expression] = Nil,
    versionToUse: Option[Long] = None)
  extends TahoeFileIndex(spark, deltaLog, path) {

  override def tableVersion: Long = versionToUse.getOrElse(deltaLog.snapshot.version)

  private lazy val historicalSnapshotOpt: Option[Snapshot] =
    versionToUse.map(deltaLog.getSnapshotAt(_))

  override def getSnapshot(stalenessAcceptable: Boolean): Snapshot = {
    historicalSnapshotOpt.getOrElse(deltaLog.update(stalenessAcceptable))
  }

  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression],
      keepStats: Boolean = false): Seq[AddFile] = {
    getSnapshot(stalenessAcceptable = false).filesForScan(
      projection = Nil, this.partitionFilters ++ partitionFilters ++ dataFilters, keepStats).files
  }

  override def inputFiles: Array[String] = {
    getSnapshot(stalenessAcceptable = false).filesForScan(
      projection = Nil, partitionFilters).files.map(f => absolutePath(f.path).toString).toArray
  }

  override def refresh(): Unit = {}
  override val sizeInBytes: Long = deltaLog.snapshot.sizeInBytes

  override def equals(that: Any): Boolean = that match {
    case t: TahoeLogFileIndex =>
      t.deltaLog.tableId == deltaLog.tableId && t.versionToUse == versionToUse &&
        t.path == path && t.partitionFilters == partitionFilters
    case _ => false
  }

  override def hashCode: scala.Int = {
    31 * path.hashCode() + partitionFilters.hashCode()
  }

  override def partitionSchema: StructType = historicalSnapshotOpt.map(_.metadata.partitionSchema)
    .getOrElse(super.partitionSchema)
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
    snapshot: Snapshot)
  extends TahoeFileIndex(spark, deltaLog, path) {

  override def tableVersion: Long = snapshot.version

  override def getSnapshot(stalenessAcceptable: Boolean): Snapshot = {
    snapshot
  }

  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression],
      keepStats: Boolean = false): Seq[AddFile] = {
    DeltaLog.filterFileList(
      snapshot.metadata.partitionColumns,
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
