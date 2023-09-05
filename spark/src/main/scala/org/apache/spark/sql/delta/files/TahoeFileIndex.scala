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

package org.apache.spark.sql.delta.files

// scalastyle:off import.ordering.noEmptyLine
import java.net.URI
import java.util.Objects

import org.apache.spark.sql.delta.RowIndexFilterType
import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaErrors, DeltaLog, NoMapping, Snapshot, SnapshotDescriptor}
import org.apache.spark.sql.delta.actions.{AddFile, Metadata, Protocol}
import org.apache.spark.sql.delta.implicits._
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType

/**
 * A [[FileIndex]] that generates the list of files managed by the Tahoe protocol.
 */
abstract class TahoeFileIndex(
    val spark: SparkSession,
    override val deltaLog: DeltaLog,
    val path: Path)
  extends FileIndex
  with SupportsRowIndexFilters
  with SnapshotDescriptor {

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
    val partitionValuesToFiles = listAddFiles(partitionFilters, dataFilters)
    makePartitionDirectories(partitionValuesToFiles.toSeq)
  }


  private def listAddFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Map[Map[String, String], Seq[AddFile]] = {
    matchingFiles(partitionFilters, dataFilters).groupBy(_.partitionValues)
  }

  private def makePartitionDirectories(
      partitionValuesToFiles: Seq[(Map[String, String], Seq[AddFile])]): Seq[PartitionDirectory] = {
    val timeZone = spark.sessionState.conf.sessionLocalTimeZone
    partitionValuesToFiles.map {
      case (partitionValues, files) =>
        val partitionValuesRow = getPartitionValuesRow(partitionValues)

        val fileStatuses = files.map { f =>
          new FileStatus(
            /* length */ f.size,
            /* isDir */ false,
            /* blockReplication */ 0,
            /* blockSize */ 1,
            /* modificationTime */ f.modificationTime,
            absolutePath(f.path))
        }.toArray

        PartitionDirectory(partitionValuesRow, fileStatuses)
    }
  }

  protected def getPartitionValuesRow(partitionValues: Map[String, String]): GenericInternalRow = {
    val timeZone = spark.sessionState.conf.sessionLocalTimeZone
    val partitionRowValues = partitionSchema.map { p =>
      val colName = DeltaColumnMapping.getPhysicalName(p)
      val partValue = Literal(partitionValues.get(colName).orNull)
      Cast(partValue, p.dataType, Option(timeZone), ansiEnabled = false).eval()
    }.toArray
    new GenericInternalRow(partitionRowValues)
  }

  override def partitionSchema: StructType = metadata.partitionSchema

  protected def absolutePath(child: String): Path = {
    // scalastyle:off pathfromuri
    val p = new Path(new URI(child))
    // scalastyle:on pathfromuri
    if (p.isAbsolute) {
      p
    } else {
      new Path(path, p)
    }
  }

  override def toString: String = {
    // the rightmost 100 characters of the path
    val truncatedPath = truncateRight(path.toString, len = 100)
    s"Delta[version=$version, $truncatedPath]"
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

/** A [[TahoeFileIndex]] that works with a specific [[SnapshotDescriptor]]. */
abstract class TahoeFileIndexWithSnapshotDescriptor(
    spark: SparkSession,
    deltaLog: DeltaLog,
    path: Path,
    snapshot: SnapshotDescriptor) extends TahoeFileIndex(spark, deltaLog, path) {

  override def version: Long = snapshot.version
  override def metadata: Metadata = snapshot.metadata
  override def protocol: Protocol = snapshot.protocol


  protected[delta] def numOfFilesIfKnown: Option[Long] = snapshot.numOfFilesIfKnown
  protected[delta] def sizeInBytesIfKnown: Option[Long] = snapshot.sizeInBytesIfKnown
}


/**
 * A [[TahoeFileIndex]] that generates the list of files from DeltaLog with given partition filters.
 *
 * NOTE: This is NOT a [[TahoeFileIndexWithSnapshotDescriptor]] because we only use
 * [[snapshotAtAnalysis]] for actual data skipping if this is a time travel query.
 */
case class TahoeLogFileIndex(
    override val spark: SparkSession,
    override val deltaLog: DeltaLog,
    override val path: Path,
    snapshotAtAnalysis: Snapshot,
    partitionFilters: Seq[Expression] = Nil,
    isTimeTravelQuery: Boolean = false)
  extends TahoeFileIndex(spark, deltaLog, path) {


  // WARNING: Stability of this method is _NOT_ guaranteed!
  override def version: Long = {
    if (isTimeTravelQuery) snapshotAtAnalysis.version else deltaLog.unsafeVolatileSnapshot.version
  }

  // WARNING: These methods are intentionally pinned to the analysis-time snapshot, which may differ
  // from the one returned by [[getSnapshot]] that we will eventually scan.
  override def metadata: Metadata = snapshotAtAnalysis.metadata
  override def protocol: Protocol = snapshotAtAnalysis.protocol

  private def checkSchemaOnRead: Boolean = {
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_SCHEMA_ON_READ_CHECK_ENABLED)
  }

  protected def getSnapshotToScan: Snapshot = {
    if (isTimeTravelQuery) {
      snapshotAtAnalysis
    } else {
      deltaLog.update(stalenessAcceptable = true)
    }
  }

  /** Provides the version that's being used as part of the scan if this is a time travel query. */
  def versionToUse: Option[Long] = if (isTimeTravelQuery) Some(snapshotAtAnalysis.version) else None

  def getSnapshot: Snapshot = {
    val snapshotToScan = getSnapshotToScan
    // Always check read compatibility with column mapping tables
    if (checkSchemaOnRead) {
      // Ensure that the schema hasn't changed in an incompatible manner since analysis time:
      // 1. Check logical schema incompatibility
      // 2. Check column mapping read compatibility. The above check is not sufficient
      //    when the schema's logical names are not changing but the underlying physical name has
      //    changed. In this case, the data files cannot be read using the old schema any more.
      val snapshotSchema = snapshotToScan.metadata.schema
      if (!SchemaUtils.isReadCompatible(snapshotAtAnalysis.schema, snapshotSchema) ||
          !DeltaColumnMapping.hasNoColumnMappingSchemaChanges(
            snapshotToScan.metadata, snapshotAtAnalysis.metadata)) {
        throw DeltaErrors.schemaChangedSinceAnalysis(snapshotAtAnalysis.schema, snapshotSchema)
      }
    }

    // disallow reading table with empty schema, which we support creating now
    if (snapshotToScan.schema.isEmpty) {
      // print the catalog identifier or delta.`/path/to/table`
      var message = TableIdentifier(deltaLog.dataPath.toString, Some("delta")).quotedString
      throw DeltaErrors.readTableWithoutSchemaException(message)
    }

    snapshotToScan
  }

  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[AddFile] = {
    getSnapshot.filesForScan(this.partitionFilters ++ partitionFilters ++ dataFilters).files
  }

  override def inputFiles: Array[String] = {
    getSnapshot
      .filesForScan(partitionFilters).files
      .map(f => absolutePath(f.path).toString)
      .toArray
  }

  override def refresh(): Unit = {}
  override def sizeInBytes: Long = deltaLog.unsafeVolatileSnapshot.sizeInBytes

  override def equals(that: Any): Boolean = that match {
    case t: TahoeLogFileIndex =>
      t.path == path && t.deltaLog.isSameLogAs(deltaLog) &&
        t.versionToUse == versionToUse && t.partitionFilters == partitionFilters
    case _ => false
  }

  override def hashCode: scala.Int = {
    Objects.hashCode(path, deltaLog.compositeId, versionToUse, partitionFilters)
  }

  protected[delta] def numOfFilesIfKnown: Option[Long] =
    deltaLog.unsafeVolatileSnapshot.numOfFilesIfKnown

  protected[delta] def sizeInBytesIfKnown: Option[Long] =
    deltaLog.unsafeVolatileSnapshot.sizeInBytesIfKnown
}

object TahoeLogFileIndex {
  def apply(spark: SparkSession, deltaLog: DeltaLog): TahoeLogFileIndex =
    TahoeLogFileIndex(spark, deltaLog, deltaLog.dataPath, deltaLog.unsafeVolatileSnapshot)
}

/**
 * A [[TahoeFileIndex]] that generates the list of files from a given list of files
 * that are within a version range of DeltaLog.
 */
class TahoeBatchFileIndex(
    spark: SparkSession,
    val actionType: String,
    val addFiles: Seq[AddFile],
    deltaLog: DeltaLog,
    path: Path,
    val snapshot: SnapshotDescriptor,
    val partitionFiltersGenerated: Boolean = false)
  extends TahoeFileIndexWithSnapshotDescriptor(spark, deltaLog, path, snapshot) {

  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[AddFile] = {
    DeltaLog.filterFileList(partitionSchema, addFiles.toDF(spark), partitionFilters)
      .as[AddFile]
      .collect()
  }

  override def inputFiles: Array[String] = {
    addFiles.map(a => absolutePath(a.path).toString).toArray
  }

  override def refresh(): Unit = {}
  override lazy val sizeInBytes: Long = addFiles.map(_.size).sum
}

trait SupportsRowIndexFilters {
  /**
   * If we know a-priori which exact rows we want to read (e.g., from a previous scan)
   * find the per-file filter here, which must be passed down to the appropriate reader.
   *
   * @return a mapping from file names to the row index filter for that file.
   */
  def rowIndexFilters: Option[Map[String, RowIndexFilterType]] = None
}

