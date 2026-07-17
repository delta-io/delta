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

import org.apache.spark.sql.delta.{Checkpoints, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, AddFile, Checkpoint, ContentRoot, InMemoryLogReplay, Metadata, Protocol}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.SparkSession

/** Helpers for emitting an inline AMT checkpoint during a commit. */
object AMTWriteHelper {

  /**
   * Materializes the full live file set into a fresh manifest tree and builds the inline Checkpoint
   * action, returning the write result plus its metrics.
   */
  def writeFullMaterialization(
      spark: SparkSession,
      readSnapshot: Snapshot,
      commitVersion: Long,
      actionsToCommit: Seq[Action],
      postCommitProtocol: Protocol,
      postCommitMetadata: Metadata,
      trigger: AmtTrigger): (AMTWriteResult, SingleAMTWriteMetrics) = {
    val deltaLog = readSnapshot.deltaLog
    val startNanos = System.nanoTime()
    val postCommitState = computePostCommitState(readSnapshot, actionsToCommit)
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    val entriesPerLeaf = spark.sessionState.conf.getConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF)
    val (contentRoot, leaves) = writeManifestTree(
      spark = spark,
      hadoopConf = hadoopConf,
      tableRoot = deltaLog.dataPath,
      useRename = deltaLog.store.isPartialWriteVisible(deltaLog.logPath, hadoopConf),
      liveAddFiles = postCommitState.allFiles,
      entriesPerLeaf = entriesPerLeaf)
    val checkpoint = Checkpoint(
      version = commitVersion,
      contentRoot = contentRoot,
      protocol = postCommitProtocol,
      metaData = postCommitMetadata,
      domainMetadata = postCommitState.getDomainMetadatas.toSeq,
      txns = postCommitState.getTransactions.toSeq,
      sidecars = Seq.empty)
    val result = AMTWriteResult(
      contentRootVersion = commitVersion,
      checkpoint = checkpoint,
      leaves = leaves,
      includeActionsInCommitJson = true)
    val singleMetric = SingleAMTWriteMetrics(
      trigger = trigger.toString,
      materializeDurationMs = NANOSECONDS.toMillis(System.nanoTime() - startNanos))
    (result, singleMetric)
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
      useRename: Boolean,
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
      writeLeaf(spark, fs, hadoopConf, tableRoot, metadataDir, useRename, batch)
    }

    val contentRoot = writeRoot(spark, fs, hadoopConf, metadataDir, useRename, leafPointers)
    val leafInfos = leafPointers.map { leaf =>
      AMTCheckpointProvider.LeafInfo(
        path = leaf.path,
        sizeInBytes = leaf.sizeInBytes,
        numEntries = leaf.manifestInfo.added_files_count.toLong)
    }
    (contentRoot, leafInfos)
  }

  /**
   * Pointer to one written leaf, used to build the root's `DATA_MANIFEST` entry for it.
   *
   * @param path         Absolute path to the leaf parquet file.
   * @param sizeInBytes  Size of the leaf parquet file on disk.
   * @param manifestInfo Per-leaf summary (file/row counts by status) for the root pointer.
   */
  private case class LeafPointer(path: String, sizeInBytes: Long, manifestInfo: ManifestInfo)

  // Tracking envelope for a freshly written entry: ADDED, no lineage/sequence numbers yet.
  private def addedTracking: Tracking = Tracking(
    status = Tracking.Status.Added,
    snapshot_id = None,
    dv_snapshot_id = None,
    sequence_number = None,
    file_sequence_number = None,
    first_row_id = None,
    deleted_positions = None,
    replaced_positions = None)

  // Writes a single leaf parquet file (DATA entries) and returns a pointer row for it.
  private def writeLeaf(
      spark: SparkSession,
      fs: FileSystem,
      hadoopConf: Configuration,
      tableRoot: Path,
      metadataDir: Path,
      useRename: Boolean,
      batch: Seq[AddFile]): LeafPointer = {
    val leafFile = FileNames.newAMTLeafManifestFile(metadataDir)
    val rows: Seq[AMTSingleAction] =
      batch.map(add => DataEntry.fromAddFile(add, addedTracking, tableRoot).wrap)
    writeAMTParquet(spark, hadoopConf, leafFile, useRename, rows)
    val status = fs.getFileStatus(leafFile)
    LeafPointer(
      path = leafFile.toString,
      sizeInBytes = status.getLen,
      manifestInfo = manifestInfoFor(batch))
  }

  // Writes the root parquet file (DATA_MANIFEST pointers only) and returns the ContentRoot.
  private def writeRoot(
      spark: SparkSession,
      fs: FileSystem,
      hadoopConf: Configuration,
      metadataDir: Path,
      useRename: Boolean,
      leafPointers: Seq[LeafPointer]): ContentRoot = {
    val rootFile = FileNames.newAMTRootManifestFile(metadataDir)
    val rows: Seq[AMTSingleAction] = leafPointers.map { leaf =>
      DataManifestEntry(
        location = leaf.path,
        file_format = AMTSingleAction.FileFormatParquet,
        tracking = addedTracking,
        // Number of content entries the referenced leaf manifest holds.
        record_count = leaf.manifestInfo.added_files_count.toLong,
        file_size_in_bytes = leaf.sizeInBytes,
        manifest_info = leaf.manifestInfo).wrap
    }
    writeAMTParquet(spark, hadoopConf, rootFile, useRename, rows)
    val status = fs.getFileStatus(rootFile)
    ContentRoot(
      path = rootFile.toString,
      sizeInBytes = status.getLen)
  }

  // Summarizes a leaf's batch into a ManifestInfo. In this PR every entry is ADDED with no
  // assigned sequence number, so only the added_* counts are populated.
  private def manifestInfoFor(batch: Seq[AddFile]): ManifestInfo = {
    val addedRows = batch.map(_.numLogicalRecords.getOrElse(0L)).sum
    ManifestInfo(
      added_files_count = batch.size,
      existing_files_count = 0,
      deleted_files_count = 0,
      replaced_files_count = 0,
      added_rows_count = addedRows,
      existing_rows_count = 0L,
      deleted_rows_count = 0L,
      replaced_rows_count = 0L,
      // No data sequence numbers are assigned yet; 0 is the conventional "unset" minimum.
      min_sequence_number = 0L,
      dv = None,
      dv_cardinality = None)
  }

  // Writes a batch of AMTSingleAction rows to `finalPath` as a single parquet file.
  private def writeAMTParquet(
      spark: SparkSession,
      hadoopConf: Configuration,
      finalPath: Path,
      useRename: Boolean,
      rows: Seq[AMTSingleAction]): Unit = {
    import org.apache.spark.sql.delta.implicits._
    val df = spark.createDataset(rows).toDF()
    Checkpoints.writeAtomicCheckpointParquetFile(
      spark = spark,
      df = df,
      finalPath = finalPath,
      hadoopConf = hadoopConf,
      useRename = useRename)
  }
}
