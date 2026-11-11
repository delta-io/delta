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

package org.apache.spark.sql.delta.amt

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{Checkpoint, ContentRoot}
import org.apache.spark.sql.delta.deletionvectors.ManifestBitmap
import org.apache.spark.sql.delta.implicits.amtSingleActionEncoder
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataType

/**
 * Fixtures for tests that replace an AMT table's manifest tree with a hand-built one, for entries
 * the AMT writer does not produce (it materializes every leaf value, so none of its trees exercise
 * inheritance) or refuses to produce.
 */
trait AMTSyntheticTreeTestBase extends AMTCheckpointTestBase {

  import testImplicits._

  /** File sequence number assigned on the root pointer in inheritance scenarios. */
  protected val inheritedFileSequenceNumber = 42L
  protected val inheritedSnapshotId = 41L

  /** The row-tracking view of one file the reader reconstructed from a manifest tree. */
  protected case class ReconstructedFile(
      path: String,
      baseRowId: Option[Long],
      defaultRowCommitVersion: Option[Long],
      snapshotId: Option[Long],
      modifiedSnapshotId: Option[Long])

  /** A leaf manifest written to disk, with the entries it was written from. */
  protected case class WrittenLeaf(location: String, sizeInBytes: Long, entries: Seq[DataEntry])

  /**
   * An AMT table whose manifest tree a test replaces with a hand-built one.
   *
   * @param deltaLog The table's log.
   * @param base A real emitted checkpoint action, whose protocol and metadata the synthetic trees
   *             reuse so only the `contentRoot` differs.
   */
  protected case class SyntheticTree(deltaLog: DeltaLog, base: Checkpoint) {

    /** Writes `rows` as a single manifest parquet, returning its stored location and size. */
    private def writeManifest(file: Path, rows: Seq[AMTSingleAction]): (String, Long) = {
      val hadoopConf = deltaLog.newDeltaHadoopConf()
      AMTWriteHelper.writeAMTParquet(
        spark, hadoopConf, file, base.metaData, base.protocol, rows)
      val fs = file.getFileSystem(hadoopConf)
      val location = AMTUtils.relativizeLocation(deltaLog.dataPath.toString, file.toString)
      (location, fs.getFileStatus(file).getLen)
    }

    /** Writes `entries` to a fresh leaf manifest, in the order given. */
    def writeLeaf(entries: Seq[DataEntry]): WrittenLeaf = {
      val metadataDir = FileNames.amtMetadataDirPath(deltaLog.dataPath)
      val leafFile = FileNames.newAMTLeafManifestFile(metadataDir)
      val (location, sizeInBytes) = writeManifest(leafFile, entries.map(_.wrap))
      // A row's inherited row id depends on the entries physically before it, so tests that state
      // expected row ids are only meaningful if the rows landed in the order they were written.
      val onDisk = allowReadWithinDeltaLog {
        spark.read.parquet(leafFile.toString)
          .orderBy(col("_metadata.row_index"))
          .select(col("location")).as[String].collect().toSeq
      }
      assert(onDisk == entries.map(_.location),
        s"leaf rows must keep the order they were written in; got $onDisk")
      WrittenLeaf(location, sizeInBytes, entries)
    }

    /**
     * Rewrites `leaf` with `column` of every row set to `value` (given the column's type), so a
     * test can persist an entry row that [[AMTSingleAction]] refuses to build.
     */
    def corruptLeaf(
        leaf: WrittenLeaf, column: String)(value: DataType => Column): WrittenLeaf = {
      val hadoopConf = deltaLog.newDeltaHadoopConf()
      val leafFile = new Path(deltaLog.dataPath, leaf.location)
      val fs = leafFile.getFileSystem(hadoopConf)
      val schema = AMTSingleAction.persistedSchema(base.metaData, base.protocol)
      val rewritten = new Path(deltaLog.dataPath, "_corrupt_" + leafFile.getName)
      allowReadWithinDeltaLog {
        spark.read.schema(schema).parquet(leafFile.toString)
          .select(schema.fields.toIndexedSeq.map { field =>
            // Keeps the Parquet field id the reader resolves the column by.
            if (field.name == column) {
              value(field.dataType).cast(field.dataType).as(field.name, field.metadata)
            } else {
              col(field.name)
            }
          }: _*)
          .coalesce(1)
          .write.parquet(rewritten.toString)
      }
      val part = fs.listStatus(rewritten).map(_.getPath).find(_.getName.endsWith(".parquet"))
        .getOrElse(fail("the rewritten leaf must have a parquet file"))
      fs.delete(leafFile, false)
      fs.rename(part, leafFile)
      fs.delete(rewritten, true)
      leaf.copy(sizeInBytes = fs.getFileStatus(leafFile).getLen)
    }

    /** Writes `rootEntries` as the root manifest and returns the checkpoint pointing at it. */
    def checkpoint(rootEntries: Seq[AMTSingleAction]): Checkpoint = {
      val metadataDir = FileNames.amtMetadataDirPath(deltaLog.dataPath)
      val (location, sizeInBytes) =
        writeManifest(FileNames.newAMTRootManifestFile(metadataDir), rootEntries)
      updateCheckpointActions(base) {
        case _: ContentRoot =>
          ContentRoot(path = location, sizeInBytes = sizeInBytes, version = base.version)
      }
    }

    /** Writes `rootEntries` as the root manifest and returns a provider over the resulting tree. */
    def provider(rootEntries: Seq[AMTSingleAction]): AMTCheckpointProvider = {
      val tree = checkpoint(rootEntries)
      AMTCheckpointProvider.fromCheckpoint(
        spark, deltaLog, tree, manifestCommitVersion = tree.version)
    }

    /**
     * Writes `rootEntries` as the root manifest and returns the files a reader reconstructs from
     * the resulting tree.
     */
    def reconstruct(rootEntries: Seq[AMTSingleAction]): Map[String, ReconstructedFile] =
      provider(rootEntries).loadActionsForStateReconstruction(spark, deltaLog)
        .getOrElse(fail("AMT provider must contribute reconstructed actions."))
        .where("add is not null")
        .select(
          col("add.path"),
          col("add.baseRowId"),
          col("add.defaultRowCommitVersion"),
          col("add.snapshotId"),
          col("add.modifiedSnapshotId"))
        .as[(String, Option[Long], Option[Long], Option[Long], Option[Long])]
        .collect()
        .map { case (path, baseRowId, commitVersion, snapshotId, modifiedSnapshotId) =>
          path -> ReconstructedFile(
            path, baseRowId, commitVersion, snapshotId, modifiedSnapshotId)
        }
        .toMap
  }

  /**
   * Creates an AMT table with a real emitted tree, then hands `body` the fixture it
   * uses to replace that tree with a hand-built one.
   */
  protected def withSyntheticTree(
      tableName: String,
      tableSchema: String = "id INT",
      partitionColumns: Seq[String] = Seq.empty)(body: SyntheticTree => Unit): Unit = {
    withTable(tableName) {
      createAMTTable(
        tableName,
        checkpointInterval = 2,
        tableSchema = tableSchema,
        partitionColumns = partitionColumns)
      sql(s"INSERT INTO $tableName (id) VALUES (1)")
      // Emits a real AMT checkpoint action, whose non-content state the synthetic trees reuse.
      sql(s"INSERT INTO $tableName (id) VALUES (2)")
      val deltaLog = deltaLogForName(tableName)
      val checkpointAction = amtProvider(deltaLog.update())
        .getOrElse(fail("expected an AMT-backed checkpoint provider"))
        .checkpointAction
      body(SyntheticTree(deltaLog, checkpointAction))
    }
  }

  /** Tracking with nothing assigned, as a writer records it before the commit version is known. */
  protected def unassignedTracking(status: Int = Tracking.Status.Added): Tracking = Tracking(
    status = status,
    snapshot_id = None,
    modified_snapshot_id = None,
    sequence_number = None,
    file_sequence_number = None,
    first_row_id = None,
    deleted_positions = None,
    replaced_positions = None)

  /**
   * Tracking for a root `DATA_MANIFEST` pointer, which is written at commit time and so carries
   * the explicit values its leaf's entries inherit.
   */
  protected def pointerTracking(
      snapshotId: Option[Long] = Some(inheritedSnapshotId),
      modifiedSnapshotId: Option[Long] = None,
      sequenceNumber: Option[Long] = Some(inheritedFileSequenceNumber),
      fileSequenceNumber: Option[Long] = Some(inheritedFileSequenceNumber),
      firstRowId: Option[Long] = Some(1000L)): Tracking =
    unassignedTracking().copy(
      snapshot_id = snapshotId,
      modified_snapshot_id = modifiedSnapshotId,
      sequence_number = sequenceNumber,
      file_sequence_number = fileSequenceNumber,
      first_row_id = firstRowId)

  /** A leaf DATA entry for a data file of `recordCount` physical rows. */
  protected def dataEntry(
      path: String,
      recordCount: Long,
      tracking: Tracking = unassignedTracking()): DataEntry = DataEntry(
    location = path,
    file_format = AMTSingleAction.FileFormatParquet,
    tracking = tracking,
    record_count = recordCount,
    file_size_in_bytes = 128L)

  /** A root pointer to `leaf`, optionally masking some of its positions with an MDV. */
  protected def leafPointer(
      leaf: WrittenLeaf,
      tracking: Tracking = pointerTracking(),
      mdvPositions: Seq[Long] = Seq.empty): DataManifestEntry = {
    val mdv = Some(mdvPositions).filter(_.nonEmpty)
    DataManifestEntry(
      location = leaf.location,
      file_format = AMTSingleAction.FileFormatParquet,
      tracking = tracking,
      record_count = leaf.entries.size.toLong,
      file_size_in_bytes = leaf.sizeInBytes,
      manifest_info = ManifestInfo(
        format_version = AMTSingleAction.FormatVersionV4,
        added_files_count = leaf.entries.size,
        existing_files_count = 0,
        deleted_files_count = 0,
        replaced_files_count = 0,
        modified_files_count = 0,
        added_rows_count = leaf.entries.map(_.record_count).sum,
        existing_rows_count = 0L,
        deleted_rows_count = 0L,
        replaced_rows_count = 0L,
        modified_rows_count = 0L,
        min_sequence_number = 0L,
        dv = mdv.map(positions =>
          AMTUtils.serializeMdv(ManifestBitmap.fromPositions(positions.map(_.toInt))))))
  }

  /** The messages of `error` and of every exception it wraps. */
  protected def causeMessages(error: Throwable): String =
    Iterator.iterate(Option(error))(_.flatMap(e => Option(e.getCause)))
      .takeWhile(_.isDefined)
      .flatten
      .map(e => String.valueOf(e.getMessage))
      .mkString("\n")
}
