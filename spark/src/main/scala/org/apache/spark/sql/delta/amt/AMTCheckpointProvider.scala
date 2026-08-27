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

import org.apache.spark.sql.delta.{CheckpointPolicy, CheckpointProvider, DeltaLog, DeltaLogFileIndex, Snapshot}
import org.apache.spark.sql.delta.DeltaLogFileIndex.COMMIT_VERSION_COLUMN
import org.apache.spark.sql.delta.actions.{Action, AddFile, BackReference, Checkpoint, ContentRoot, Metadata, Protocol, RemoveFile, SingleAction}
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray
import org.apache.spark.sql.delta.util.DeltaEncoder
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.execution.datasources.FileFormat.{FILE_PATH, METADATA_NAME}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * A [[CheckpointProvider]] backed by an AMT (Adaptive Metadata Tree) manifest tree.
 *
 * This provider is only for inline manifest-commit checkpoints, and this is not intended for
 * standalone checkpoint which also refers to an AMT.
 *
 * @param manifestCommitVersion The version of the manifest commit that wrote this checkpoint.
 * @param checkpointAction The inline-emitted Checkpoint action this tree was committed with;
 *                         carries the version, contentRoot, and inline non-file state.
 * @param leaves           The root's `DATA_MANIFEST` pointer entries, one per leaf reachable from
 *                         the root. Each entry's `location` is stored table-root-relative; use
 *                         [[liveLeafManifestAbsolutePaths]] to resolve them against the table root.
 * @param tableRoot        The table's data path.
 */
final class AMTCheckpointProvider(
    val manifestCommitVersion: Long,
    val checkpointAction: Checkpoint,
    val leaves: Seq[DataManifestEntry],
    val tableRoot: Path)
  extends CheckpointProvider {

  /** The table version the manifest tree describes. */
  def checkpointVersion: Long = checkpointAction.version

  /** Pointer to the root manifest parquet. */
  private def contentRoot: ContentRoot = checkpointAction.contentRoot

  /** Absolute [[Path]] to the root manifest parquet, resolved against the table root. */
  private val rootManifestAbsolutePath: Path = contentRoot.getAbsolutePath(tableRoot)

  /** The live leaf pointers which must have all the live [[DataEntry]]s. */
  private lazy val liveLeaves: Seq[DataManifestEntry] =
    leaves.filter(l =>
      AMTCheckpointProvider.liveDataManifestEntryStatuses.contains(l.tracking.status))

  /** Absolute [[Path]]s to the live leaf manifest parquet files, resolved against the root. */
  lazy val liveLeafManifestAbsolutePaths: Seq[Path] = liveLeaves.map(_.getAbsolutePath(tableRoot))

  /** The root manifest as a [[FileStatus]]. */
  private lazy val rootFile: FileStatus = contentRoot.toFileStatus(tableRoot)

  /** Live leaf manifests as [[FileStatus]]es. */
  private lazy val liveLeafFiles: Seq[FileStatus] = liveLeaves.map(_.toFileStatus(tableRoot))

  override def version: Long = checkpointAction.version

  override def topLevelFiles: Seq[FileStatus] = {
    Seq(new FileStatus(
      /* length = */ contentRoot.sizeInBytes,
      /* isdir = */ false,
      /* block_replication = */ 0,
      /* blocksize = */ 0L,
      // modificationTime is not tracked on the ContentRoot, so report 0.
      // This should not impact readers.
      /* modification_time = */ 0L,
      rootManifestAbsolutePath))
  }

  override def effectiveCheckpointSizeInBytes(): Long =
    contentRoot.sizeInBytes + liveLeaves.map(_.file_size_in_bytes).sum

  override def checkpointPolicyForLogging: Option[CheckpointPolicy.Policy] = None

  // Protocol and Metadata are carried inline on the Checkpoint action (not in the AMT), so convert
  // them to a DataFrame with the required schema.
  override def loadProtocolMetadataActions(
      spark: SparkSession, deltaLog: DeltaLog): Option[DataFrame] = {
    import org.apache.spark.sql.delta.implicits._
    val rows = Seq(
      SingleAction(protocol = checkpointAction.protocol),
      SingleAction(metaData = checkpointAction.metaData))
    val df = spark.createDataset(rows).toDF()
      .select(Snapshot.pAndMQuerySchema.fieldNames.toIndexedSeq.map(col): _*)
      .withColumn(COMMIT_VERSION_COLUMN, lit(version))
    Some(df)
  }

  override def loadActionsForStateReconstruction(
      spark: SparkSession, deltaLog: DeltaLog): Option[DataFrame] = {
    val df = allActions(spark, deltaLog).toDF()
      .withColumn(COMMIT_VERSION_COLUMN, lit(version))
      .withColumn(Snapshot.ADD_STATS_TO_USE_COL_NAME, col("add.stats"))
    Some(df)
  }
  /**
   * The full action set of this checkpoint as a distributed [[Dataset]] of [[SingleAction]]: the
   * live file `AddFile`s reconstructed from the AMT(root + leaves), unioned with the inline
   * non-content actions (protocol, metadata, domain metadata, txns) built on the driver.
   *
   * Note: Iceberg metadata inheritance (manifest entries inheriting fields such as partition
   * values, sequence numbers, or snapshot id from the parent manifest) is not supported yet;
   * entries are read as fully materialized rows.
   */
  private def allActions(spark: SparkSession, deltaLog: DeltaLog): Dataset[SingleAction] = {
    import org.apache.spark.sql.delta.implicits._
    val nonFileActions = spark.createDataset(nonContentSingleActions)
    liveAddSingleActions(spark, deltaLog).union(nonFileActions)
  }

  /** The inline, non-content actions carried directly on the [[Checkpoint]] action. */
  private def nonContentSingleActions: Seq[SingleAction] =
    Seq(
      SingleAction(protocol = checkpointAction.protocol),
      SingleAction(metaData = checkpointAction.metaData)) ++
    checkpointAction.domainMetadata.map(dm => SingleAction(domainMetadata = dm)) ++
    checkpointAction.txns.map(txn => SingleAction(txn = txn))

  /**
   * Reconstructs the live-file AddFile actions from the AMT as a [[Dataset]].
   */
  private def liveAddSingleActions(
      spark: SparkSession, deltaLog: DeltaLog): Dataset[SingleAction] = {
    import org.apache.spark.sql.delta.implicits._
    // Bind to a local so the `mapPartitions` closure captures it, not the (non-serializable)
    // provider.
    val localTableRoot = tableRoot
    val encodedRootPath = SparkPath.fromPath(rootManifestAbsolutePath).urlEncoded
    val serializableConf = new SerializableConfiguration(deltaLog.newDeltaHadoopConf())

    val files = rootFile +: liveLeafFiles
    val fmt = DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET
    // Read every leaf row, then drop the MDV-marked (leaf, rowIndex) entries with a filter on
    // top, using a broadcast of each leaf's manifest DV bitmap bytes keyed by its `_metadata`
    // file path.
    val index = DeltaLogFileIndex(fmt, files.toArray)
    val mdvByLeaf: Map[String, Array[Byte]] = liveLeaves.flatMap { leaf =>
      leaf.manifestDV.map { case (dvBytes, _) =>
        SparkPath.fromPath(leaf.getAbsolutePath(localTableRoot)).urlEncoded -> dvBytes
      }
    }.toMap
    val mdvBroadcast = spark.sparkContext.broadcast(mdvByLeaf)
    val dataEntries = AMTCheckpointProvider.loadEntriesWithLocation(
      deltaLog, index, checkpointAction.metaData, checkpointAction.protocol)
      .where(col("entry.content_type") === lit(AMTSingleAction.ContentType.Type.Data))
      .where(col("entry.tracking.status").isin(
        AMTCheckpointProvider.liveDataEntryStatuses.toSeq: _*))
      .filter { entryWithLoc =>
        mdvBroadcast.value.get(entryWithLoc.leafPath)
          .forall(bytes => !RoaringBitmapArray.readFrom(bytes).contains(entryWithLoc.pos))
      }

    dataEntries
      .mapPartitions { entries =>
        val fs = localTableRoot.getFileSystem(serializableConf.value)
        entries.map { entryWithLoc =>
          entryWithLoc.entry.unwrap match {
            case data: DataEntry =>
              val backReference = if (entryWithLoc.leafPath == encodedRootPath) {
                None
              } else {
                val absLeaf = SparkPath.fromUrlString(entryWithLoc.leafPath).toPath
                val relManifest =
                  AMTUtils.relativizeManifestPathToTableRoot(fs, localTableRoot, absLeaf)
                Some(BackReference(relManifest, entryWithLoc.pos))
              }
              val add = data.toAddFile(localTableRoot).copy(backReference = backReference)
              SingleAction(add = add)
            case other => throw new IllegalStateException(
              s"Expected a DATA entry after filtering, got ${other.getClass.getSimpleName}.")
          }
        }
      }
  }

  /**
   * Test-only invariant: verify the AMT back references carried by the current proposed commit's
   * file actions.
   *
   * On an AMT-backed table a leaf-resident file's AddFile / RemoveFile carries a [[BackReference]]
   * to the (leaf manifest, row position) its entry occupies in the tree, so a later commit can mask
   * or supersede that leaf slot; a root-resident file carries none. `committedActions` are the
   * current proposed commit's actions; they are checked against the live set of the AMT checkpoint
   * this (pre-commit) snapshot is backed by, keyed by (path, dv id):
   *   - a file whose (path, dv) is live in the AMT checkpoint leaf must carry the back reference;
   *   - a file whose (path, dv) is absent from the AMT checkpoint leaf must carry none -- a net-new
   *     file, or the re-added copy of a same-path replace (re-added under a new dv).
   * A (path, dv) that an intermediate commit (landed after the AMT checkpoint but before this one)
   * already superseded is relaxed: this commit's later add/remove of it may omit backreference.
   *
   * Example: the AMT checkpoint is at version 10 and commits 11/12/13 sit on top of it while this
   * commit is 14. File f1 lives at leaf-1 / pos-1 in the checkpoint. If commit 12 (say an
   * ANALYZE TABLE COMPUTE STATS) already re-committed f1 -- carrying its back reference at that
   * point -- then f1's add/remove in commit 14 need not carry a back reference.
   */
  private[delta] def verifyCommitBackReferences(
      spark: SparkSession,
      deltaLog: DeltaLog,
      committedActions: Seq[Action]): Unit = {
    // Key by (path, dv) so a same-path replace is handled: the removed (path, oldDv) is checked
    // against the AMT, while the re-added (path, newDv) is a distinct key absent from the tree.
    val committedFiles = committedActions.collect {
      case a: AddFile => (a.path, a.getLegacyDeletionVectorUniqueId) -> a.backReference
      case r: RemoveFile => (r.path, r.getLegacyDeletionVectorUniqueId) -> r.backReference
    }
    if (committedFiles.isEmpty) return

    val expectedKeyToBackreferenceMap =
      liveAddSingleActions(spark, deltaLog)
        .collect()
        .map(sa => (sa.add.path, sa.add.getLegacyDeletionVectorUniqueId) -> sa.add.backReference)
        .toMap

    // Keys an intermediate commit (after this AMT) already re-committed. The first superseding
    // add/remove must carry a back reference; a 2nd superseding one of the same key need not.
    val intermediateCommittedKeys =
      deltaLog.getChanges(checkpointVersion + 1).flatMap(_._2).collect {
        case a: AddFile => (a.path, a.getLegacyDeletionVectorUniqueId)
        case r: RemoveFile => (r.path, r.getLegacyDeletionVectorUniqueId)
      }.toSet

    committedFiles.foreach { case (key, actual) =>
      expectedKeyToBackreferenceMap.get(key) match {
        case Some(expected)
            if actual != expected && !(intermediateCommittedKeys.contains(key) && actual.isEmpty) =>
          throw new IllegalStateException(
            s"AMT back reference for file '${key._1}' does not match the AMT. " +
            s"Expected $expected but the committed action carried $actual.")
        case None if actual.isDefined =>
          throw new IllegalStateException(
            s"File '${key._1}' carries a back reference $actual but is not present in the AMT " +
            "tree, so it must not carry one.")
        case _ => // Matching, omitted after a window supersession, or absent+empty: as expected.
      }
    }
  }
}

object AMTCheckpointProvider {

  /**
   * An [[AMTSingleAction]] entry paired with its physical read location in its manifest parquet.
   *
   * @param entry    The manifest content entry.
   * @param leafPath The URL-encoded absolute path of the manifest parquet the entry was read from
   *                 (Spark's `_metadata.file_path`).
   * @param pos      The 0-based position of the entry inside the manifest (Spark's
   *                 `_metadata.row_index`).
   */
  case class AMTDataEntryWithLocation(entry: AMTSingleAction, leafPath: String, pos: Long)

  private lazy val amtDataEntryWithLocationEncoder: Encoder[AMTDataEntryWithLocation] =
    new DeltaEncoder[AMTDataEntryWithLocation].get

  /**
   * Builds a provider from an emitted [[Checkpoint]] action by reading the leaf pointers out of the
   * root manifest parquet.
   *
   * @param deltaLog   The table's DeltaLog, used to read the root via `loadIndex` (which bypasses
   *                   the path-based Delta format check the root file under the table root would
   *                   otherwise trip).
   * @param checkpoint The inline-emitted checkpoint action carrying the `contentRoot`.
   */
  def fromCheckpoint(
      deltaLog: DeltaLog,
      checkpoint: Checkpoint,
      manifestCommitVersion: Long): AMTCheckpointProvider = {
    val tableRoot = deltaLog.dataPath
    val rootFile = checkpoint.contentRoot.toFileStatus(tableRoot)
    val index =
      DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET, Array(rootFile))
    // The root manifest is small (one row per leaf), so collect it to the driver to enumerate the
    // leaf pointers.
    val leaves = loadEntries(deltaLog, index, checkpoint.metaData, checkpoint.protocol)
      .collect().toSeq
      .filter(_.content_type == AMTSingleAction.ContentType.Type.DataManifest)
      .map(_.unwrap.asInstanceOf[DataManifestEntry])
    new AMTCheckpointProvider(
      manifestCommitVersion = manifestCommitVersion,
      checkpointAction = checkpoint,
      leaves = leaves,
      tableRoot = tableRoot)
  }

  /** Tracking Status representing the live [[DataEntry]] in an AMT. */
  private[amt] val liveDataEntryStatuses: Set[Int] =
    Set(Tracking.Status.Existing, Tracking.Status.Added, Tracking.Status.Modified)

  /** All Tracking Status for leafs which may have any live files. */
  private[amt] val liveDataManifestEntryStatuses: Set[Int] =
    Set(Tracking.Status.Added, Tracking.Status.Existing, Tracking.Status.Modified)

  /** Reads the AMT root and returns the live [[DataEntry]]s tracked by root. */
  private[amt] def readLiveRootDataEntries(
      deltaLog: DeltaLog,
      checkpoint: Checkpoint): Seq[AddFile] = {
    val tableRoot = deltaLog.dataPath
    val rootFile = checkpoint.contentRoot.toFileStatus(tableRoot)
    val index =
      DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET, Array(rootFile))
    loadEntries(deltaLog, index, checkpoint.metaData, checkpoint.protocol)
      .collect().toSeq
      .filter(_.content_type == AMTSingleAction.ContentType.Type.Data)
      .map(_.unwrap.asInstanceOf[DataEntry])
      .filter(e => liveDataEntryStatuses.contains(e.tracking.status))
      .map(_.toAddFile(tableRoot))
  }

  /**
   * Reads AMT manifest parquet files (root or leaves) into a [[Dataset]] of
   * [[AMTSingleAction]].
   */
  private def loadEntries(
      deltaLog: DeltaLog,
      index: DeltaLogFileIndex,
      metadata: Metadata,
      protocol: Protocol): Dataset[AMTSingleAction] = {
    import org.apache.spark.sql.delta.implicits._
    val persistedSchema =
      AMTSingleAction.persistedSchema(metadata, protocol)
    val persisted = deltaLog.loadIndex(index, persistedSchema)
    val withPartition = AMTPartitionValues.forRead(persisted, metadata.partitionSchema)
    AMTContentStats.forRead(withPartition, metadata, protocol)
      .as[AMTSingleAction]
  }

  /**
   * Like [[loadEntries]], but also captures each row's physical read location.
   */
  private def loadEntriesWithLocation(
      deltaLog: DeltaLog,
      index: DeltaLogFileIndex,
      metadata: Metadata,
      protocol: Protocol): Dataset[AMTDataEntryWithLocation] = {
    import org.apache.spark.sql.delta.implicits._
    implicit val entryLocEncoder: Encoder[AMTDataEntryWithLocation] =
      amtDataEntryWithLocationEncoder
    val persistedSchema =
      AMTSingleAction.persistedSchema(metadata, protocol)
    val persisted = deltaLog.loadIndex(index, persistedSchema)
      .select(
        persistedSchema.fieldNames.toIndexedSeq.map(col) :+
          col(s"$METADATA_NAME.$FILE_PATH").as("leafPath") :+
          col(s"$METADATA_NAME.${ParquetFileFormat.ROW_INDEX}").as("pos"): _*)
    val withPartition = AMTPartitionValues.forRead(persisted, metadata.partitionSchema)
    AMTContentStats.forRead(withPartition, metadata, protocol)
      .select(
        struct(
          amtSingleActionEncoder
            .schema
            .fieldNames
            .toIndexedSeq
            .map(col): _*).as("entry"),
        col("leafPath"),
        col("pos"))
      .as[AMTDataEntryWithLocation]
  }
}
