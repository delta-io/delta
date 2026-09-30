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

import org.apache.spark.sql.functions.col

/**
 * The AMT writer materializes every leaf value today, so no tree it produces exercises
 * inheritance. These tests therefore hand-build manifest trees with deliberately null leaf
 * tracking.
 */
class AMTInheritanceReadSuite extends AMTCheckpointTestBase {

  import testImplicits._

  /** File sequence number assigned on the root pointer in inheritance scenarios. */
  private val inheritedFileSequenceNumber = 42L

  /** The row-tracking view of one file the reader reconstructed from a manifest tree. */
  private case class ReconstructedFile(
      path: String,
      baseRowId: Option[Long],
      defaultRowCommitVersion: Option[Long])

  /** A leaf manifest written to disk, with the entries it was written from. */
  private case class WrittenLeaf(location: String, sizeInBytes: Long, entries: Seq[DataEntry])

  /**
   * An AMT table whose manifest tree a test replaces with a hand-built one.
   *
   * @param deltaLog The table's log.
   * @param base A real emitted checkpoint action, whose protocol and metadata the synthetic trees
   *             reuse so only the `contentRoot` differs.
   */
  private case class SyntheticTree(deltaLog: DeltaLog, base: Checkpoint) {

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
     * Writes `rootEntries` as the root manifest and returns the files a reader reconstructs from
     * the resulting tree.
     */
    def reconstruct(rootEntries: Seq[AMTSingleAction]): Map[String, ReconstructedFile] = {
      val metadataDir = FileNames.amtMetadataDirPath(deltaLog.dataPath)
      val (location, sizeInBytes) =
        writeManifest(FileNames.newAMTRootManifestFile(metadataDir), rootEntries)
      val checkpoint =
        base.copy(contentRoot =
          ContentRoot(path = location, sizeInBytes = sizeInBytes, version = base.version))
      val provider = AMTCheckpointProvider.fromCheckpoint(
        deltaLog, checkpoint, manifestCommitVersion = checkpoint.version)
      provider.loadActionsForStateReconstruction(spark, deltaLog)
        .getOrElse(fail("AMT provider must contribute reconstructed actions."))
        .where("add is not null")
        .select(col("add.path"), col("add.baseRowId"), col("add.defaultRowCommitVersion"))
        .as[(String, Option[Long], Option[Long])]
        .collect()
        .map { case (path, baseRowId, commitVersion) =>
          path -> ReconstructedFile(path, baseRowId, commitVersion)
        }
        .toMap
    }
  }

  /**
   * Creates an AMT table with a real emitted tree, then hands `body` the fixture it
   * uses to replace that tree with a hand-built one.
   */
  private def withSyntheticTree(tableName: String)(body: SyntheticTree => Unit): Unit = {
    withTable(tableName) {
      createAMTTable(tableName, checkpointInterval = 2)
      sql(s"INSERT INTO $tableName VALUES (1)")
      // Emits a real AMT checkpoint action, whose non-content state the synthetic trees reuse.
      sql(s"INSERT INTO $tableName VALUES (2)")
      val deltaLog = deltaLogForName(tableName)
      val checkpointAction = amtProvider(deltaLog.update())
        .getOrElse(fail("expected an AMT-backed checkpoint provider"))
        .checkpointAction
      body(SyntheticTree(deltaLog, checkpointAction))
    }
  }

  /** Tracking with nothing assigned, as a writer records it before the commit version is known. */
  private def unassignedTracking(status: Int = Tracking.Status.Added): Tracking = Tracking(
    status = status,
    snapshot_id = None,
    dv_snapshot_id = None,
    sequence_number = None,
    file_sequence_number = None,
    first_row_id = None,
    deleted_positions = None,
    replaced_positions = None)

  /**
   * Tracking for a root `DATA_MANIFEST` pointer, which is written at commit time and so carries
   * the explicit values its leaf's entries inherit.
   */
  private def pointerTracking(
      sequenceNumber: Option[Long] = Some(inheritedFileSequenceNumber),
      fileSequenceNumber: Option[Long] = Some(inheritedFileSequenceNumber),
      firstRowId: Option[Long] = Some(1000L)): Tracking =
    unassignedTracking().copy(
      sequence_number = sequenceNumber,
      file_sequence_number = fileSequenceNumber,
      first_row_id = firstRowId)

  /** A leaf DATA entry for a data file of `recordCount` physical rows. */
  private def dataEntry(
      path: String,
      recordCount: Long,
      tracking: Tracking = unassignedTracking()): DataEntry = DataEntry(
    location = path,
    file_format = AMTSingleAction.FileFormatParquet,
    tracking = tracking,
    record_count = recordCount,
    file_size_in_bytes = 128L)

  /** A root pointer to `leaf`, optionally masking some of its positions with an MDV. */
  private def leafPointer(
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
          AMTUtils.serializeMdv(ManifestBitmap.fromPositions(positions.map(_.toInt)))),
        dv_cardinality = mdv.map(_.size.toLong)))
  }

  /** The messages of `error` and of every exception it wraps. */
  private def causeMessages(error: Throwable): String =
    Iterator.iterate(Option(error))(_.flatMap(e => Option(e.getCause)))
      .takeWhile(_.isDefined)
      .flatten
      .map(e => String.valueOf(e.getMessage))
      .mkString("\n")

  test("leaf entries inherit the file sequence number and row ids of their root pointer") {
    withSyntheticTree("amt_inherit_all") { tree =>
      val leaf = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L),
        dataEntry("data/b.parquet", recordCount = 20L),
        dataEntry("data/c.parquet", recordCount = 30L)))
      val files = tree.reconstruct(Seq(leafPointer(leaf).wrap))

      assert(files.keySet == Set("data/a.parquet", "data/b.parquet", "data/c.parquet"))
      assert(files.values.forall(_.defaultRowCommitVersion.contains(inheritedFileSequenceNumber)),
        s"every entry must inherit the pointer's file sequence number; got ${files.values}")
      // Row ids run from the pointer's first_row_id in the leaf's physical order, each file
      // starting where the previous one's records ended.
      assert(files("data/a.parquet").baseRowId.contains(1000L))
      assert(files("data/b.parquet").baseRowId.contains(1010L))
      assert(files("data/c.parquet").baseRowId.contains(1030L))
    }
  }

  test("a materialized first_row_id is kept and does not advance the ones after it") {
    withSyntheticTree("amt_inherit_mixed") { tree =>
      val materialized = unassignedTracking().copy(first_row_id = Some(5000L))
      val leaf = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L),
        dataEntry("data/b.parquet", recordCount = 20L, tracking = materialized),
        dataEntry("data/c.parquet", recordCount = 30L)))
      val files = tree.reconstruct(Seq(leafPointer(leaf).wrap))

      assert(files("data/a.parquet").baseRowId.contains(1000L))
      assert(files("data/b.parquet").baseRowId.contains(5000L))
      // Only the entries that inherit a row id contribute to the running offset, so `c` follows
      // `a` rather than `b`.
      assert(files("data/c.parquet").baseRowId.contains(1010L))
    }
  }

  test("an MDV-masked entry is skipped but still advances the row ids after it") {
    withSyntheticTree("amt_inherit_mdv") { tree =>
      val leaf = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L),
        dataEntry("data/b.parquet", recordCount = 20L),
        dataEntry("data/c.parquet", recordCount = 30L)))
      val files = tree.reconstruct(Seq(leafPointer(leaf, mdvPositions = Seq(1L)).wrap))

      assert(files.keySet == Set("data/a.parquet", "data/c.parquet"),
        "the MDV-masked entry must not appear in the reconstructed live set")
      assert(files("data/a.parquet").baseRowId.contains(1000L))
      // Masking `b` must not pull `c`'s row id down to 1010.
      assert(files("data/c.parquet").baseRowId.contains(1030L))
    }
  }

  test("a non-live entry with a materialized row id does not consume the inherited range") {
    withSyntheticTree("amt_inherit_dead_entry") { tree =>
      val deletedTracking = unassignedTracking(Tracking.Status.Deleted).copy(
        sequence_number = Some(9L),
        file_sequence_number = Some(9L),
        first_row_id = Some(5000L))
      val leaf = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L),
        dataEntry("data/b.parquet", recordCount = 20L,
          tracking = deletedTracking),
        dataEntry("data/c.parquet", recordCount = 30L)))
      val files = tree.reconstruct(Seq(leafPointer(leaf).wrap))

      assert(files.keySet == Set("data/a.parquet", "data/c.parquet"))
      assert(files("data/a.parquet").baseRowId.contains(1000L))
      assert(files("data/c.parquet").baseRowId.contains(1010L))
    }
  }

  test("each leaf inherits from its own root pointer") {
    withSyntheticTree("amt_inherit_two_leaves") { tree =>
      val first = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L),
        dataEntry("data/b.parquet", recordCount = 20L)))
      val second = tree.writeLeaf(Seq(
        dataEntry("data/c.parquet", recordCount = 5L),
        dataEntry("data/d.parquet", recordCount = 7L)))
      val files = tree.reconstruct(Seq(
        leafPointer(first).wrap,
        leafPointer(second, pointerTracking(
          sequenceNumber = Some(99L),
          fileSequenceNumber = Some(99L), firstRowId = Some(9000L))).wrap))

      assert(files("data/b.parquet").defaultRowCommitVersion.contains(inheritedFileSequenceNumber))
      assert(files("data/d.parquet").defaultRowCommitVersion.contains(99L))
      assert(files("data/a.parquet").baseRowId.contains(1000L))
      assert(files("data/b.parquet").baseRowId.contains(1010L))
      assert(files("data/c.parquet").baseRowId.contains(9000L))
      assert(files("data/d.parquet").baseRowId.contains(9005L))
    }
  }

  test("a root pointer that assigns a file sequence number but no first_row_id is rejected") {
    withSyntheticTree("amt_inherit_no_row_ids") { tree =>
      val leaf = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L),
        dataEntry("data/b.parquet", recordCount = 20L)))
      val error = intercept[Exception] {
        tree.reconstruct(Seq(leafPointer(leaf, pointerTracking(firstRowId = None)).wrap))
      }
      assert(causeMessages(error).contains("tracking.first_row_id must not be null"))
    }
  }

  test("a root pointer that declares no inheritable tracking is rejected") {
    withSyntheticTree("amt_inherit_nothing") { tree =>
      val leaf = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L),
        dataEntry("data/b.parquet", recordCount = 20L)))
      val error = intercept[Exception] {
        tree.reconstruct(Seq(leafPointer(leaf, unassignedTracking()).wrap))
      }
      assert(causeMessages(error).contains("tracking.sequence_number must not be null"))
    }
  }

  test("root-resident entries are read verbatim") {
    withSyntheticTree("amt_inherit_root_entries") { tree =>
      val leaf = tree.writeLeaf(Seq(dataEntry("data/leaf.parquet", recordCount = 10L)))
      // A root DATA entry has nothing above it to inherit from, so it keeps its own tracking even
      // while a sibling pointer hands values down to its leaf.
      val files = tree.reconstruct(Seq(
        dataEntry("data/inline.parquet", recordCount = 4L).wrap,
        leafPointer(leaf).wrap))

      assert(files("data/inline.parquet").defaultRowCommitVersion.isEmpty)
      assert(files("data/inline.parquet").baseRowId.isEmpty)
      assert(files("data/leaf.parquet").defaultRowCommitVersion
        .contains(inheritedFileSequenceNumber))
      assert(files("data/leaf.parquet").baseRowId.contains(1000L))
    }
  }

  test("a live entry that is not ADDED and has no file sequence number is rejected") {
    withSyntheticTree("amt_inherit_malformed") { tree =>
      // Only an ADDED entry may leave its file sequence number to inheritance. An EXISTING entry
      // predates the tree, so there is no value it could correctly inherit.
      val leaf = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L,
          tracking = unassignedTracking(Tracking.Status.Existing).copy(
            sequence_number = Some(7L),
            first_row_id = Some(9L)))))
      val error = intercept[Exception] {
        tree.reconstruct(Seq(leafPointer(leaf).wrap))
      }
      val messages = causeMessages(error)
      assert(messages.contains("tracking.file_sequence_number is null"), messages)
      assert(messages.contains("EXISTING"), messages)
    }
  }

  test("a live entry that is not ADDED and has no first_row_id is rejected") {
    withSyntheticTree("amt_inherit_malformed_row_id") { tree =>
      val leaf = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L,
          tracking = unassignedTracking(Tracking.Status.Existing)
            .copy(
              sequence_number = Some(7L),
              file_sequence_number = Some(8L)))))
      val error = intercept[Exception] {
        tree.reconstruct(Seq(leafPointer(leaf).wrap))
      }
      val messages = causeMessages(error)
      assert(messages.contains("tracking.first_row_id is null"), messages)
      assert(messages.contains("EXISTING"), messages)
    }
  }

  test("a materialized file_sequence_number on a leaf entry is read without inheriting") {
    Tracking.Status.liveEntryStatuses.foreach { status =>
      withSyntheticTree(s"amt_materialized_leaf_${Tracking.Status.nameOf(status)}") { tree =>
        val materialized = 77L
        val materializedFirstRowId =
          if (status == Tracking.Status.Added) None else Some(1000L)
        val materializedSequenceNumber =
          if (status == Tracking.Status.Added) None else Some(76L)
        val leaf = tree.writeLeaf(Seq(
          dataEntry("data/a.parquet", recordCount = 10L,
            tracking = unassignedTracking(status).copy(
              sequence_number = materializedSequenceNumber,
              file_sequence_number = Some(materialized),
              first_row_id = materializedFirstRowId))))
        val files = tree.reconstruct(Seq(
          leafPointer(leaf).wrap))
        assert(files("data/a.parquet").defaultRowCommitVersion.contains(materialized),
          s"${Tracking.Status.nameOf(status)} must keep its materialized value")
      }
    }
  }

  test("a materialized file_sequence_number on a root-resident entry is read without inheriting") {
    Tracking.Status.liveEntryStatuses.foreach { status =>
      withSyntheticTree(s"amt_materialized_root_${Tracking.Status.nameOf(status)}") { tree =>
        val materialized = 55L
        val files = tree.reconstruct(Seq(
          dataEntry("data/inline.parquet", recordCount = 4L,
            tracking = unassignedTracking(status).copy(file_sequence_number = Some(materialized)))
            .wrap))
        assert(files("data/inline.parquet").defaultRowCommitVersion.contains(materialized),
          s"${Tracking.Status.nameOf(status)} must keep its materialized value")
      }
    }
  }
}
