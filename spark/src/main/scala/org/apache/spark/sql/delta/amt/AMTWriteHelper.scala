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
import org.apache.spark.sql.delta.actions.{Action, AddFile, Checkpoint, ContentRoot, DomainMetadata, InMemoryLogReplay, Metadata, Protocol, SetTransaction}
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
   * Incremental materialization (interval / size triggers): the post-commit live files are packed
   * into leaves in input order on the driver. `actionsToCommit` are replayed onto the read snapshot
   * via [[computePostCommitState]] to derive both the live file set and the non-file state. Returns
   * the write result plus its metrics.
   */
  def writeIncrementalMaterialization(
      spark: SparkSession,
      readSnapshot: Snapshot,
      commitVersion: Long,
      actionsToCommit: Seq[Action],
      postCommitProtocol: Protocol,
      postCommitMetadata: Metadata,
      trigger: String,
      incremental: Boolean): (AMTWriteResult, SingleAMTWriteMetrics) = {
    val deltaLog = readSnapshot.deltaLog
    val startNanos = System.nanoTime()
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    val entriesPerLeaf = spark.sessionState.conf.getConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF)

    val postCommitState = computePostCommitState(readSnapshot, actionsToCommit)
    val (contentRootBase, leaves) = writeManifestTree(
      spark = spark,
      hadoopConf = hadoopConf,
      tableRoot = deltaLog.dataPath,
      liveAddFiles = postCommitState.allFiles,
      entriesPerLeaf = entriesPerLeaf)
    val contentStateVersion =
      if (actionsToCommit.isEmpty) readSnapshot.version else commitVersion
    val contentRoot = policyTaggedContentRoot(
      readSnapshot, contentRootBase, incremental, contentStateVersion)
    buildResult(
      contentStateVersion = contentStateVersion,
      contentRoot = contentRoot,
      leaves = leaves,
      postCommitProtocol = postCommitProtocol,
      postCommitMetadata = postCommitMetadata,
      domainMetadata = postCommitState.getDomainMetadatas.toSeq,
      txns = postCommitState.getTransactions.toSeq,
      trigger = trigger,
      startNanos = startNanos)
  }

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

    val (contentRootBase, leaves) = writeClusteredManifestTree(
      spark = spark,
      deltaLog = deltaLog,
      readSnapshot = readSnapshot,
      hadoopConf = hadoopConf,
      entriesPerLeaf = entriesPerLeaf)
    // A full rewrite of the read snapshot's live files; it carries no commit actions, so the tree
    // describes the read snapshot's version.
    val contentStateVersion = readSnapshot.version
    val contentRoot = policyTaggedContentRoot(
      readSnapshot, contentRootBase, incremental = false, contentStateVersion)
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
      contentStateVersion: Long): ContentRoot = {
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
      isIncremental = incremental,
      lastManifestCommitWithFullRewrite = lastFullRewriteVersion)
  }

  /**
   * Assembles the inline Checkpoint action, write result, and metric shared by both materialization
   * paths. `contentStateVersion` is the table version the tree describes and is stamped on the
   * checkpoint action and write result.
   */
  private def buildResult(
      contentStateVersion: Long,
      contentRoot: ContentRoot,
      leaves: Seq[AMTCheckpointProvider.LeafInfo],
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

  // Post-commit table state = the read snapshot's state with this commit's actions applied,
  // computed via InMemoryLogReplay (the same machinery snapshot state reconstruction uses).
  private def computePostCommitState(
      readSnapshot: Snapshot, commitActions: Seq[Action]): InMemoryLogReplay = {
    val logReplay = new InMemoryLogReplay(
      minFileRetentionTimestamp = None,
      minSetTransactionRetentionTimestamp = None)
    val baseline = readSnapshot.allFiles.collect().iterator ++
      readSnapshot.domainMetadata.iterator ++
      readSnapshot.setTransactions.iterator
    logReplay.append(readSnapshot.version, baseline)
    logReplay.append(readSnapshot.version + 1, commitActions.iterator)
    logReplay
  }

  /**
   * Writes a two-level AMT (Adaptive Metadata Tree) manifest tree under `<tableRoot>/metadata/`:
   * `liveAddFiles` is packed into leaf parquet files (up to `entriesPerLeaf` entries each, in input
   * order), and a single root parquet file lists one pointer per leaf. Returns the [[ContentRoot]]
   * pointing at the written root file, for embedding in the inline `Checkpoint` action.
   */
  private def writeManifestTree(
      spark: SparkSession,
      hadoopConf: Configuration,
      tableRoot: Path,
      liveAddFiles: Seq[AddFile],
      entriesPerLeaf: Int): (ContentRoot, Seq[AMTCheckpointProvider.LeafInfo]) = {
    require(entriesPerLeaf > 0, "entriesPerLeaf must be positive.")

    val fs = tableRoot.getFileSystem(hadoopConf)
    val metadataDir = FileNames.amtMetadataDirPath(tableRoot)
    if (!fs.exists(metadataDir)) {
      // mkdirs is idempotent and safe under concurrent writers.
      fs.mkdirs(metadataDir)
    }

    val leafBatches = if (liveAddFiles.isEmpty) {
      // A checkpoint with no live files still gets a single empty leaf so the root always
      // points at something. Keeps readers from special-casing zero-leaf trees.
      Seq(Seq.empty[AddFile])
    } else {
      liveAddFiles.grouped(entriesPerLeaf).toSeq
    }

    val leafPointers = leafBatches.map { batch =>
      writeLeaf(spark, fs, hadoopConf, tableRoot, metadataDir, batch)
    }

    val contentRoot = writeRoot(spark, fs, hadoopConf, metadataDir, leafPointers)
    (contentRoot, leafInfosFor(leafPointers))
  }

  // Builds the pointer-only [[AMTCheckpointProvider.LeafInfo]] view from the written leaf pointers.
  private def leafInfosFor(
      leafPointers: Seq[LeafPointer]): Seq[AMTCheckpointProvider.LeafInfo] =
    leafPointers.map { leaf =>
      AMTCheckpointProvider.LeafInfo(
        path = leaf.path,
        sizeInBytes = leaf.sizeInBytes,
        numEntries = leaf.manifestInfo.added_files_count.toLong)
    }

  /**
   * Writes a clustered AMT manifest tree for a full checkpoint of `readSnapshot`'s live files.
   * All files are clustered and flushed into leaf manifests -- one leaf per Spark partition,
   * written by executors. The root lists only leaf pointers (no inline data entries). Returns the
   * [[ContentRoot]] plus the per-leaf [[AMTCheckpointProvider.LeafInfo]] pointers.
   */
  private def writeClusteredManifestTree(
      spark: SparkSession,
      deltaLog: DeltaLog,
      readSnapshot: Snapshot,
      hadoopConf: Configuration,
      entriesPerLeaf: Int): (ContentRoot, Seq[AMTCheckpointProvider.LeafInfo]) = {
    require(entriesPerLeaf > 0, "entriesPerLeaf must be positive.")
    val tableRoot = deltaLog.dataPath
    val fs = tableRoot.getFileSystem(hadoopConf)
    val metadataDir = FileNames.amtMetadataDirPath(tableRoot)

    val numFiles = readSnapshot.numOfFiles
    val desiredNumLeaves =
      math.max(1, math.ceil(numFiles.toDouble / entriesPerLeaf).toInt)
    val addFilesDf =
      readSnapshot.allFiles.toDF().repartition(desiredNumLeaves, col("path"))

    val leafPointers = writeLeavesDistributed(
      spark = spark,
      hadoopConf = hadoopConf,
      tableRoot = tableRoot,
      metadataDir = metadataDir,
      addFilesDf = addFilesDf,
      desiredNumLeaves = desiredNumLeaves)
    val contentRoot = writeRoot(spark, fs, hadoopConf, metadataDir, leafPointers)
    (contentRoot, leafInfosFor(leafPointers))
  }


  private def writeLeavesDistributed(
      spark: SparkSession,
      hadoopConf: Configuration,
      tableRoot: Path,
      metadataDir: Path,
      addFilesDf: DataFrame,
      desiredNumLeaves: Int): Seq[LeafPointer] = {
    import org.apache.spark.sql.delta.implicits._
    val addFilesDs = addFilesDf.as[AddFile]

    // Capture values so the closures do not reach back into the object / non-serializable Path.
    // The rewritten files already exist in the table, so their leaf entries are EXISTING.
    val tracking = trackingWithStatus(Tracking.Status.Existing)
    val tableRootSparkPath = SparkPath.fromPath(tableRoot)
    val metadataDirSparkPath = SparkPath.fromPath(metadataDir)

    val amtDs = addFilesDs.map { add =>
      DataEntry.fromAddFile(add, tracking, tableRootSparkPath.toPath).wrap
    }
    val schema = amtDs.schema.asNullable
    val (factory, serConf) = {
      val format = new ParquetFileFormat()
      val job = Job.getInstance(hadoopConf)
      (format.prepareWrite(spark, job, Map.empty, schema),
        new SerializableConfiguration(job.getConfiguration))
    }

    val qe = amtDs.queryExecution
    SQLExecution.withNewExecutionId(qe, Some("AMT leaf checkpoint")) {
      qe.executedPlan.execute().mapPartitions { iter =>
        // Skip empty partitions (so the root never points at an empty leaf for a non-empty table).
        if (!iter.hasNext) {
          Iterator.empty
        } else {
          val conf = serConf.value
          val leafFile = FileNames.newAMTLeafManifestFile(metadataDirSparkPath.toPath)

          val status = Checkpoints.writeSingleFileOnExecutor(
            conf = conf,
            factory = factory,
            schema = schema,
            writePath = leafFile,
            finalPath = leafFile,
            useRename = false,
            partition = TaskContext.getPartitionId(),
            expectedNumParts = desiredNumLeaves,
            rows = iter
          )
          Iterator.single(LeafPointer(
            path = leafFile.toString,
            sizeInBytes = status.length,
            // TODO: populate the per-leaf summary for the distributed write path.
            manifestInfo = manifestInfoFor(Seq.empty)))
        }
      }.collect().toSeq
    }
  }

  /**
   * Pointer to one written leaf, used to build the root's `DATA_MANIFEST` entry for it.
   *
   * @param path         Absolute path to the leaf parquet file.
   * @param sizeInBytes  Size of the leaf parquet file on disk.
   * @param manifestInfo Per-leaf summary (file/row counts by status) for the root pointer.
   */
  private case class LeafPointer(path: String, sizeInBytes: Long, manifestInfo: ManifestInfo)

  // Tracking envelope for a freshly written entry with the given status, no lineage/sequence
  // numbers yet.
  private def trackingWithStatus(status: Int): Tracking = Tracking(
    status = status,
    snapshot_id = None,
    dv_snapshot_id = None,
    sequence_number = None,
    file_sequence_number = None,
    first_row_id = None,
    deleted_positions = None,
    replaced_positions = None)

  /**
   * Writes a single leaf parquet file (DATA entries) and returns a pointer row for it.
   */
  private def writeLeaf(
      spark: SparkSession,
      fs: FileSystem,
      hadoopConf: Configuration,
      tableRoot: Path,
      metadataDir: Path,
      batch: Seq[AddFile]): LeafPointer = {
    val leafFile = FileNames.newAMTLeafManifestFile(metadataDir)
    val rows: Seq[AMTSingleAction] =
      batch.map(add =>
        DataEntry
          .fromAddFile(
            add,
            trackingWithStatus(Tracking.Status.Added),
            tableRoot
          )
          .wrap
      )
    writeAMTParquet(spark, hadoopConf, leafFile, rows)
    val status = fs.getFileStatus(leafFile)
    LeafPointer(
      path = leafFile.toString,
      sizeInBytes = status.getLen,
      manifestInfo = manifestInfoFor(rows)
    )
  }

  /**
   * Writes the root parquet file (DATA_MANIFEST pointers only) and returns the ContentRoot.
   */
  private def writeRoot(
      spark: SparkSession,
      fs: FileSystem,
      hadoopConf: Configuration,
      metadataDir: Path,
      leafPointers: Seq[LeafPointer]): ContentRoot = {
    val rootFile = FileNames.newAMTRootManifestFile(metadataDir)
    val rows: Seq[AMTSingleAction] = leafPointers.map { leaf =>
      DataManifestEntry(
        location = leaf.path,
        file_format = AMTSingleAction.FileFormatParquet,
        tracking = trackingWithStatus(Tracking.Status.Added),
        // Number of content entries the referenced leaf manifest holds.
        record_count = leaf.manifestInfo.added_files_count.toLong,
        file_size_in_bytes = leaf.sizeInBytes,
        manifest_info = leaf.manifestInfo).wrap
    }
    writeAMTParquet(spark, hadoopConf, rootFile, rows)
    val status = fs.getFileStatus(rootFile)
    ContentRoot(
      path = rootFile.toString,
      sizeInBytes = status.getLen)
  }

  /**
   * Summarizes a leaf's batch into a ManifestInfo.
   */
  private def manifestInfoFor(batch: Seq[AMTSingleAction]): ManifestInfo = {
    ManifestInfo(
      added_files_count = batch.size,
      existing_files_count = 0,
      deleted_files_count = 0,
      replaced_files_count = 0,
      added_rows_count = 0L,
      existing_rows_count = 0L,
      deleted_rows_count = 0L,
      replaced_rows_count = 0L,
      // No data sequence numbers are assigned yet; 0 is the conventional "unset" minimum.
      min_sequence_number = 0L,
      dv = None,
      dv_cardinality = None)
  }

  /**
   * Writes a batch of AMTSingleAction rows to `finalPath` as a single parquet file.
   */
  private def writeAMTParquet(
      spark: SparkSession,
      hadoopConf: Configuration,
      finalPath: Path,
      rows: Seq[AMTSingleAction]): Unit = {
    import org.apache.spark.sql.delta.implicits._
    val df = spark.createDataset(rows).toDF()
    // AMT manifest tree files are UUID-named, so we write straight to the final path
    // (useRename = false): a retried or zombie task never collides with a file already
    // reported to a Manifest commit.
    Checkpoints.writeAtomicCheckpointParquetFile(
      spark = spark,
      df = df,
      finalPath = finalPath,
      hadoopConf = hadoopConf,
      useRename = false)
  }
}
