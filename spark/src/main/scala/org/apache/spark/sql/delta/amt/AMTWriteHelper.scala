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

import java.util.concurrent.TimeUnit.NANOSECONDS

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{Checkpoints, DeltaLog, Snapshot}
import org.apache.spark.sql.delta.actions.{AddFile, Checkpoint, ContentRoot, DomainMetadata, Metadata, Protocol, SetTransaction}
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.TaskContext
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions.{col, hash, struct}
import org.apache.spark.util.SerializableConfiguration

/** Helpers for emitting an inline AMT checkpoint during a commit. */
object AMTWriteHelper extends DeltaLogging {

  /**
   * Writes a new AMT from scratch representing the readSnapshot content metadata. Unlike the
   * driver-side incremental path, the live file set is clustered and flushed into leaves by
   * executors (a distributed rewrite). Always a full (non-incremental) rewrite: the "last full
   * rewrite" marker is reset to the version the tree describes.
   */
  def writeFullMaterialization(
      spark: SparkSession,
      readSnapshot: Snapshot,
      commitVersion: Long,
      postCommitProtocol: Protocol,
      postCommitMetadata: Metadata,
      trigger: String): (AMTWriteResult, SingleAMTWriteMetrics) = {
    val deltaLog = readSnapshot.deltaLog
    val startNanos = System.nanoTime()
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    val entriesPerLeaf = spark.sessionState.conf.getConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF)

    // A full rewrite of the read snapshot's live files; it carries no commit actions, so the tree
    // describes the read snapshot's version.
    val contentStateVersion = readSnapshot.version
    val (contentRootBase, leaves) = writeClusteredManifestTree(
      spark = spark,
      deltaLog = deltaLog,
      readSnapshot = readSnapshot,
      hadoopConf = hadoopConf,
      metadata = postCommitMetadata,
      protocol = postCommitProtocol,
      entriesPerLeaf = entriesPerLeaf,
      contentStateVersion = contentStateVersion)
    val contentRoot = policyTaggedContentRoot(
      readSnapshot, contentRootBase, incremental = false, contentStateVersion,
      numLeaves = leaves.size.toLong)
    buildResult(
      contentStateVersion = contentStateVersion,
      contentRoot = contentRoot,
      leaves = leaves,
      postCommitProtocol = postCommitProtocol,
      postCommitMetadata = postCommitMetadata,
      domainMetadata = readSnapshot.domainMetadata,
      txns = readSnapshot.setTransactions,
      trigger = trigger,
      startNanos = startNanos)
  }

  // Tags the freshly written root with how the tree was produced, so a reader/maintenance job can
  // tell incremental trees apart from full re-materializations without inspecting the leaves. A
  // full rewrite resets the "last full rewrite" marker to `contentStateVersion`; an incremental
  // rewrite carries forward the previous tree's marker.
  private def policyTaggedContentRoot(
      readSnapshot: Snapshot,
      contentRootBase: ContentRoot,
      incremental: Boolean,
      contentStateVersion: Long,
      numLeaves: Long): ContentRoot = {
    val lastFullRewriteVersion =
      if (incremental) {
        previousAMTContentRoot(readSnapshot)
          .flatMap(_.lastManifestCommitWithFullRewrite)
          .getOrElse(contentStateVersion)
      } else {
        contentStateVersion
      }
    ContentRoot(
      path = contentRootBase.path,
      sizeInBytes = contentRootBase.sizeInBytes,
      version = contentStateVersion,
      isIncremental = incremental,
      lastManifestCommitWithFullRewrite = lastFullRewriteVersion,
      numLeaves = numLeaves)
  }

  /**
   * Assembles the inline Checkpoint action, write result, and metric shared by both materialization
   * paths. `contentStateVersion` is the table version the tree describes and is stamped on the
   * checkpoint action and write result.
   */
  private def buildResult(
      contentStateVersion: Long,
      contentRoot: ContentRoot,
      leaves: Seq[DataManifestEntry],
      postCommitProtocol: Protocol,
      postCommitMetadata: Metadata,
      domainMetadata: Seq[DomainMetadata],
      txns: Seq[SetTransaction],
      trigger: String,
      startNanos: Long): (AMTWriteResult, SingleAMTWriteMetrics) = {
    val checkpoint = Checkpoint(
      version = contentStateVersion,
      contentRoot = contentRoot,
      protocol = postCommitProtocol,
      metaData = postCommitMetadata,
      domainMetadata = domainMetadata,
      txns = txns,
      sidecars = Seq.empty)
    val result = AMTWriteResult(
      contentRootVersion = contentStateVersion,
      checkpoint = checkpoint,
      leaves = leaves,
      includeActionsInCommitJson = true)
    val singleMetric = SingleAMTWriteMetrics(
      trigger = trigger,
      incremental = contentRoot.isIncremental.map(_.toString).getOrElse("UNKNOWN"),
      materializeDurationMs = NANOSECONDS.toMillis(System.nanoTime() - startNanos))
    (result, singleMetric)
  }

  // The ContentRoot of the AMT tree `snapshot` is already backed by, if any. Used to carry forward
  // the "last full rewrite" marker across an incremental rewrite and to decide the next trigger.
  def previousAMTContentRoot(snapshot: Snapshot): Option[ContentRoot] =
    snapshot.checkpointProvider match {
      case amt: AMTCheckpointProvider => Some(amt.checkpointAction.contentRoot)
      case _ => None
    }

  /**
   * Writes a clustered AMT manifest tree for a full checkpoint of `readSnapshot`'s live files.
   * Live files are clustered and flushed into manifests -- one per Spark partition, written by
   * executors. A snapshot small enough to produce a single manifest needs no tree: that manifest is
   * promoted to the root and no leaf pointers are returned. Otherwise a root listing one pointer
   * per leaf is written. Returns the [[ContentRoot]] plus the per-leaf [[DataManifestEntry]]
   * pointers, which are empty for a promoted single-manifest checkpoint.
   */
  private def writeClusteredManifestTree(
      spark: SparkSession,
      deltaLog: DeltaLog,
      readSnapshot: Snapshot,
      hadoopConf: Configuration,
      metadata: Metadata,
      protocol: Protocol,
      entriesPerLeaf: Int,
      contentStateVersion: Long): (ContentRoot, Seq[DataManifestEntry]) = {
    require(entriesPerLeaf > 0, "entriesPerLeaf must be positive.")
    val tableRoot = deltaLog.dataPath
    val fs = tableRoot.getFileSystem(hadoopConf)
    val metadataDir = FileNames.amtMetadataDirPath(tableRoot)

    val numFiles = readSnapshot.numOfFiles
    val desiredNumLeaves =
      math.max(1, math.ceil(numFiles.toDouble / entriesPerLeaf).toInt)
    val addFilesDf =
      readSnapshot.allFiles.toDF().repartition(desiredNumLeaves, col("path"))

    val leafEntries = writeLeavesDistributed(
      spark = spark,
      hadoopConf = hadoopConf,
      tableRoot = tableRoot,
      metadataDir = metadataDir,
      addFilesDf = addFilesDf,
      metadata = metadata,
      protocol = protocol,
      desiredNumLeaves = desiredNumLeaves)
    leafEntries match {
      case Seq(onlyLeaf) =>
        // If there is only one leaf, promote it to the root.
        val contentRoot =
          ContentRoot(
            path = onlyLeaf.location,
            sizeInBytes = onlyLeaf.file_size_in_bytes,
            version = contentStateVersion)
        (contentRoot, Seq.empty)
      case _ =>
        val contentRoot = writeRoot(
          spark = spark,
          fs = fs,
          hadoopConf = hadoopConf,
          tableRoot = tableRoot,
          metadataDir = metadataDir,
          metadata = metadata,
          protocol = protocol,
          rows = leafEntries.map(_.wrap),
          version = contentStateVersion)
        (contentRoot, leafEntries)
    }
  }


  private def writeLeavesDistributed(
      spark: SparkSession,
      hadoopConf: Configuration,
      tableRoot: Path,
      metadataDir: Path,
      addFilesDf: DataFrame,
      metadata: Metadata,
      protocol: Protocol,
      desiredNumLeaves: Int): Seq[DataManifestEntry] = {
    import org.apache.spark.sql.delta.implicits._
    val addFilesDs = addFilesDf.as[AddFile]

    // Capture values so the closures do not reach back into the object / non-serializable Path.
    // The rewritten files already exist in the table, so their leaf entries are EXISTING.
    val tracking = existingTrackingForDataEntry()
    val tableRootSparkPath = SparkPath.fromPath(tableRoot)
    val metadataDirSparkPath = SparkPath.fromPath(metadataDir)

    val amtDs = addFilesDs.map { add =>
      DataEntry.fromAddFile(add, tracking, tableRootSparkPath.toPath).wrap
    }
    val amtWithPartition = AMTPartitionValues.forWrite(amtDs.toDF(), metadata.partitionSchema)
    val amtDf = AMTContentStats.forWrite(amtWithPartition, metadata, protocol)
    val schema = AMTSingleAction.persistedSchema(metadata, protocol)
    val recordCountIdx = amtDf.schema.fieldIndex("record_count")
    val (factory, serConf) = {
      val format = new ParquetFileFormat()
      val job = Job.getInstance(hadoopConf)
      val f = format.prepareWrite(spark, job, Map.empty, schema)
      // Write as an Iceberg-V4 manifest (nested field ids + int64 micros timestamps). Applied after
      // prepareWrite (before snapshotting the conf) so it flows to executors.
      Checkpoints.configureIcebergManifestParquetWrite(job)
      (f, new SerializableConfiguration(job.getConfiguration))
    }

    val qe = amtDf.queryExecution
    SQLExecution.withNewExecutionId(qe, Some("AMT leaf checkpoint")) {
      qe.executedPlan.execute().mapPartitions { iter =>
        // Skip empty partitions (so the root never points at an empty leaf for a non-empty table).
        if (!iter.hasNext) {
          Iterator.empty
        } else {
          val conf = serConf.value
          val leafFile = FileNames.newAMTLeafManifestFile(metadataDirSparkPath.toPath)

          var entryCount = 0
          var entryRows = 0L
          val countingRows = iter.map { row =>
            entryCount += 1
            entryRows += row.getLong(recordCountIdx)
            row
          }
          val status = Checkpoints.writeSingleFileOnExecutor(
            conf = conf,
            factory = factory,
            schema = schema,
            writePath = leafFile,
            finalPath = leafFile,
            useRename = false,
            partition = TaskContext.getPartitionId(),
            expectedNumParts = desiredNumLeaves,
            rows = countingRows
          )
          val leafFs = leafFile.getFileSystem(conf)
          // The root pointer to this leaf is newly ADDED, even though the leaf's own DATA entries
          // are EXISTING (the referenced data files already lived in the table), so manifest_info
          // counts every entry and its rows as EXISTING.
          val (tracking, manifestInfo) = addedTrackingForLeaf(
            addedFileAndRowCount = emptyFileRowCount,
            existingFileAndRowCount = FileRowCount(entryCount, entryRows),
            deletedFileAndRowCount = emptyFileRowCount,
            replacedFileAndRowCount = emptyFileRowCount,
            modifiedFileAndRowCount = emptyFileRowCount)
          Iterator.single(DataManifestEntry(
            location = AMTUtils.relativizeManifestPathToTableRoot(
              leafFs, tableRootSparkPath.toPath, leafFile),
            file_format = AMTSingleAction.FileFormatParquet,
            tracking = tracking,
            record_count = entryCount,
            file_size_in_bytes = status.length,
            manifest_info = manifestInfo))
        }
      }.collect().toSeq
    }
  }

  // Initializes a tracking envelope for a freshly written entry with the given status.
  private def initializeTracking(status: Int): Tracking = Tracking(
    status = status,
    snapshot_id = None,
    dv_snapshot_id = None,
    sequence_number = None,
    file_sequence_number = None,
    first_row_id = None,
    deleted_positions = None,
    replaced_positions = None)

  /** Tracking helpers for [[DataEntry]] */
  private[amt] def addedTrackingForDataEntry() = initializeTracking(Tracking.Status.Added)
  private[amt] def existingTrackingForDataEntry() = initializeTracking(Tracking.Status.Existing)
  private[amt] def modifiedTrackingForDataEntry() = initializeTracking(Tracking.Status.Modified)
  private[amt] def removedTrackingForDataEntry() = initializeTracking(Tracking.Status.Deleted)
  private[amt] def replacedTrackingForDataEntry() = initializeTracking(Tracking.Status.Replaced)

  /** Paired file and row tallies for a leaf's entries in one tracking-status group. */
  private[amt] case class FileRowCount(fileCount: Int, rowCount: Long)
  private[amt] val emptyFileRowCount = FileRowCount(0, 0L)

  /**
   * Tallies a freshly written leaf's entries into per-status file and row counts in a single pass,
   * then builds its `(Tracking, ManifestInfo)`. Each entry contributes one file and its physical
   * `record_count` rows to the count group for its tracking status (ADDED, EXISTING, DELETED,
   * REPLACED, or MODIFIED).
   */
  private[amt] def addedTrackingForLeaf(entries: Seq[DataEntry]): (Tracking, ManifestInfo) = {
    // Accumulate per-status file and row counts in a single pass. Plain Long accumulators avoid
    // allocating an intermediate object per entry.
    var addedFiles, existingFiles, deletedFiles, replacedFiles, modifiedFiles = 0
    var addedRows, existingRows, deletedRows, replacedRows, modifiedRows = 0L
    entries.foreach { entry =>
      val rows = entry.record_count
      entry.tracking.status match {
        case Tracking.Status.Added =>
          addedFiles += 1
          addedRows += rows
        case Tracking.Status.Existing =>
          existingFiles += 1
          existingRows += rows
        case Tracking.Status.Deleted =>
          deletedFiles += 1
          deletedRows += rows
        case Tracking.Status.Replaced =>
          replacedFiles += 1
          replacedRows += rows
        case Tracking.Status.Modified =>
          modifiedFiles += 1
          modifiedRows += rows
        case other =>
          throw new IllegalStateException(s"Unexpected leaf entry tracking status: $other.")
      }
    }
    addedTrackingForLeaf(
      addedFileAndRowCount = FileRowCount(addedFiles, addedRows),
      existingFileAndRowCount = FileRowCount(existingFiles, existingRows),
      deletedFileAndRowCount = FileRowCount(deletedFiles, deletedRows),
      replacedFileAndRowCount = FileRowCount(replacedFiles, replacedRows),
      modifiedFileAndRowCount = FileRowCount(modifiedFiles, modifiedRows))
  }

  /** Tracking + ManifestInfo for a freshly written leaf from its per-status file and row counts. */
  private[amt] def addedTrackingForLeaf(
      addedFileAndRowCount: FileRowCount,
      existingFileAndRowCount: FileRowCount,
      deletedFileAndRowCount: FileRowCount,
      replacedFileAndRowCount: FileRowCount,
      modifiedFileAndRowCount: FileRowCount): (Tracking, ManifestInfo) = {
    val tracking = initializeTracking(Tracking.Status.Added)
    val manifestInfo = emptyManifestInfo.copy(
      added_files_count = addedFileAndRowCount.fileCount,
      existing_files_count = existingFileAndRowCount.fileCount,
      deleted_files_count = deletedFileAndRowCount.fileCount,
      replaced_files_count = replacedFileAndRowCount.fileCount,
      modified_files_count = modifiedFileAndRowCount.fileCount,
      added_rows_count = addedFileAndRowCount.rowCount,
      existing_rows_count = existingFileAndRowCount.rowCount,
      deleted_rows_count = deletedFileAndRowCount.rowCount,
      replaced_rows_count = replacedFileAndRowCount.rowCount,
      modified_rows_count = modifiedFileAndRowCount.rowCount)
    (tracking, manifestInfo)
  }

  /**
   * Tracking + ManifestInfo for a carried leaf that holds no live file (its live entries are all
   * MDV-masked, or it only ever held tombstones): it decays to DELETED with its manifest_info kept
   * and per-commit CDF positions cleared, so the next AMT drops it.
   */
  private[amt] def deletedTrackingForCarriedLeaf(
      oldEntry: DataManifestEntry): (Tracking, ManifestInfo) = {
    val newTracking = oldEntry.tracking.copy(
      status = Tracking.Status.Deleted,
      // Clear the per-commit CDF positions: the leaf is being dropped, not changed this commit, so
      // it must not contribute any deleted/replaced rows to this commit's Change Data Feed.
      deleted_positions = None,
      replaced_positions = None)
    (newTracking, oldEntry.manifest_info)
  }

  /**
   * Tracking + ManifestInfo for a leaf carried unchanged into this tree: re-emitted EXISTING with
   * its MDV kept and per-commit CDF positions cleared.
   */
  private[amt] def existingTrackingForLeaf(
      oldEntry: DataManifestEntry): (Tracking, ManifestInfo) = {
    val newTracking = oldEntry.tracking.copy(
      status = Tracking.Status.Existing,
      deleted_positions = None,
      replaced_positions = None)
    // manifest_info file/row counts are immutable (they describe the leaf as written), and its MDV
    // is unchanged here (no new masking), so carry manifest_info forward untouched.
    (newTracking, oldEntry.manifest_info)
  }

  /**
   * Tracking + ManifestInfo for a carried leaf whose MDV grew this commit: MODIFIED, with
   * `mdvPositions` accumulated into the cumulative MDV, and this commit's `deletedPositions` /
   * `replacedPositions` recorded for CDF. If all the MDV bits are set, the leaf decays to DELETED.
   */
  private[amt] def modifiedOrDeletedTrackingForLeaf(
      oldEntry: DataManifestEntry,
      mdvPositions: Seq[Long],
      deletedPositions: Seq[Long],
      replacedPositions: Seq[Long]): (Tracking, ManifestInfo) = {
    val cumulativeMdv = oldEntry.manifest_info.dv
      .map(AMTUtils.deserializeMdv).getOrElse(new RoaringBitmapArray)
    mdvPositions.foreach(cumulativeMdv.add)
    def bitmapOf(positions: Seq[Long]): Option[Array[Byte]] = {
      if (positions.isEmpty) None
      else Some(AMTUtils.serializeMdv(RoaringBitmapArray(positions: _*)))
    }
    // Every masked / CDF position indexes an entry within this leaf, so no count can exceed the
    // leaf's entry count; a larger value signals a corrupt bitmap or a double-counted position.
    def assertWithinLeaf(count: Long, label: String): Unit =
      assert(count <= oldEntry.record_count,
        s"leaf ${oldEntry.location}: $label $count exceeds record_count ${oldEntry.record_count}.")
    assertWithinLeaf(cumulativeMdv.cardinality, "MDV cardinality")
    assertWithinLeaf(deletedPositions.size.toLong, "deleted_positions")
    assertWithinLeaf(replacedPositions.size.toLong, "replaced_positions")
    // A leaf whose every live entry is masked by the cumulative MDV holds no live file, so it
    // decays to DELETED rather than MODIFIED.
    val noActiveFiles =
      oldEntry.manifest_info.liveFilesCount.toLong - cumulativeMdv.cardinality <= 0
    val status = if (noActiveFiles) Tracking.Status.Deleted else Tracking.Status.Modified
    val newTracking = oldEntry.tracking.copy(
      status = status,
      deleted_positions = bitmapOf(deletedPositions),
      replaced_positions = bitmapOf(replacedPositions))
    // manifest_info file/row counts are immutable; only the MDV (dv/dv_cardinality) grows to mask
    // the newly superseded positions. Live count is record_count - dv_cardinality, and this
    // commit's CDF positions live on the tracking, not in the counts.
    val manifestInfo = withUpdatedMdv(oldEntry.manifest_info, cumulativeMdv)
    (newTracking, manifestInfo)
  }

  /**
   * Writes a single leaf parquet file (DATA entries) and returns the DataManifestEntry
   * corresponding to it.
   */
  private[amt] def writeLeaf(
      spark: SparkSession,
      fs: FileSystem,
      hadoopConf: Configuration,
      tableRoot: Path,
      metadataDir: Path,
      metadata: Metadata,
      protocol: Protocol,
      entries: Seq[DataEntry]): DataManifestEntry = {
    val leafFile = FileNames.newAMTLeafManifestFile(metadataDir)
    writeAMTParquet(spark, hadoopConf, leafFile, metadata, protocol, entries.map(_.wrap))
    val fileStatus = fs.getFileStatus(leafFile)
    // A freshly written leaf is always ADDED -- even one holding only tombstones, whose
    // manifest_info still counts the DELETED / REPLACED entries and their rows.
    val (tracking, manifestInfo) = addedTrackingForLeaf(entries)
    DataManifestEntry(
      location = AMTUtils.relativizeManifestPathToTableRoot(fs, tableRoot, leafFile),
      file_format = AMTSingleAction.FileFormatParquet,
      tracking = tracking,
      // Number of content entries the referenced leaf manifest holds.
      record_count = entries.size.toLong,
      file_size_in_bytes = fileStatus.getLen,
      manifest_info = manifestInfo)
  }

  /**
   * Writes the root parquet file from a pre-built row set and returns a [[ContentRoot]]
   * carrying its path, size, and version. The rows may be `DATA_MANIFEST` leaf pointers,
   * root-resident `DATA` entries, or both. Callers still attach tags before embedding it in a
   * [[Checkpoint]].
   */
  private[amt] def writeRoot(
      spark: SparkSession,
      fs: FileSystem,
      hadoopConf: Configuration,
      tableRoot: Path,
      metadataDir: Path,
      metadata: Metadata,
      protocol: Protocol,
      rows: Seq[AMTSingleAction],
      version: Long): ContentRoot = {
    val rootFile = FileNames.newAMTRootManifestFile(metadataDir)
    writeAMTParquet(spark, hadoopConf, rootFile, metadata, protocol, rows)
    val status = fs.getFileStatus(rootFile)
    ContentRoot(
      path = AMTUtils.relativizeManifestPathToTableRoot(fs, tableRoot, rootFile),
      sizeInBytes = status.getLen,
      version = version)
  }

  // Returns a copy of a carried-forward leaf's ManifestInfo with `mdv` recorded as its Manifest
  // Deletion Vector.
  private[amt] def withUpdatedMdv(base: ManifestInfo, mdv: RoaringBitmapArray): ManifestInfo = {
    if (mdv.isEmpty) {
      base.copy(dv = None, dv_cardinality = None)
    } else {
      base.copy(
        dv = Some(AMTUtils.serializeMdv(mdv)),
        dv_cardinality = Some(mdv.cardinality))
    }
  }

  private def emptyManifestInfo: ManifestInfo =
    ManifestInfo(
      added_files_count = 0,
      existing_files_count = 0,
      deleted_files_count = 0,
      replaced_files_count = 0,
      modified_files_count = 0,
      added_rows_count = 0L,
      existing_rows_count = 0L,
      deleted_rows_count = 0L,
      replaced_rows_count = 0L,
      modified_rows_count = 0L,
      // No data sequence numbers are assigned yet; 0 is the conventional "unset" minimum.
      min_sequence_number = 0L,
      dv = None,
      dv_cardinality = None)

  /**
   * Writes a sequence of AMTSingleActions to a Parquet file.
   */
  private def writeAMTParquet(
      spark: SparkSession,
      hadoopConf: Configuration,
      finalPath: Path,
      metadata: Metadata,
      protocol: Protocol,
      rows: Seq[AMTSingleAction]): Unit = {
    import org.apache.spark.sql.delta.implicits._
    val withPartition = AMTPartitionValues.forWrite(
      spark.createDataset(rows).toDF(), metadata.partitionSchema)
    val df = AMTContentStats.forWrite(withPartition, metadata, protocol)
    Checkpoints.writeAtomicCheckpointParquetFile(
      spark = spark,
      df = df,
      finalPath = finalPath,
      hadoopConf = hadoopConf,
      useRename = false,
      outputSchema = Some(AMTSingleAction.persistedSchema(metadata, protocol)),
      writeAsIcebergManifest = true)
  }
}
