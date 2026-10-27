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

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import org.apache.spark.sql.delta.{RowIndexFilter, RowIndexFilterProvider}
import org.apache.spark.sql.delta.{CheckpointPolicy, CheckpointProvider, DeltaLog, DeltaLogFileIndex, Snapshot, UninitializedCheckpointProvider}
import org.apache.spark.sql.delta.DeltaLogFileIndex.COMMIT_VERSION_COLUMN
import org.apache.spark.sql.delta.SnapshotManagement.checkpointThreadPool
import org.apache.spark.sql.delta.actions.{Action, AddFile, BackReference, Checkpoint, ContentRoot, FileAction, Metadata, Protocol, RemoveFile, SingleAction}
import org.apache.spark.sql.delta.actions.FileAction.UniqueFileActionTuple
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.DeltaEncoder
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.datasources.FileFormat.{FILE_PATH, METADATA_NAME}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types.StructType

/** Common checkpoint information available before and after the AMT root has been loaded. */
trait AMTCheckpointProviderBase extends UninitializedCheckpointProvider {
  def manifestCommitVersion: Long
  def checkpointAction: Checkpoint
  def tableRoot: Path

  override def version: Long = checkpointAction.version

  private[delta] def numLeaves: Int

  override def topLevelFiles: Seq[FileStatus] = {
    val contentRoot = checkpointAction.contentRoot
    Seq(new FileStatus(
      /* length = */ contentRoot.sizeInBytes,
      /* isdir = */ false,
      /* block_replication = */ 0,
      /* blocksize = */ 0L,
      // modificationTime is not tracked on the ContentRoot, so report 0.
      // This should not impact readers.
      /* modification_time = */ 0L,
      contentRoot.getAbsolutePath(tableRoot)))
  }

  // Protocol and Metadata are carried inline on the Checkpoint action (not in the AMT), so convert
  // them to a DataFrame with the required schema without waiting for the root-manifest read.
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
}

/**
 * A [[CheckpointProvider]] backed by an AMT (Adaptive Metadata Tree) manifest tree.
 *
 * This provider is only for inline manifest-commit checkpoints, and this is not intended for
 * standalone checkpoint which also refers to an AMT.
 *
 * @param manifestCommitVersion The version of the manifest commit that wrote this checkpoint.
 * @param checkpointAction The inline-emitted Checkpoint action this tree was committed with;
 *                         carries the version, contentRoot, and inline non-file state.
 * @param loadLeaves       Loads the root's `DATA_MANIFEST` pointer entries, one per reachable leaf.
 *                         Each entry's `location` is stored table-root-relative; use
 *                         [[liveLeafManifestAbsolutePaths]] to resolve it against the table root.
 * @param tableRoot        The table's data path.
 */
class AMTCheckpointProvider private[amt] (
    val manifestCommitVersion: Long,
    val checkpointAction: Checkpoint,
    loadLeaves: () => Seq[DataManifestEntry],
    val tableRoot: Path)
  extends AMTCheckpointProviderBase
  with AMTCheckpointProviderImpl
{
  lazy val leaves: Seq[DataManifestEntry] = loadLeaves()

  override private[delta] def numLeaves: Int = leaves.size
}

/**
 * An AMT checkpoint whose root-manifest read starts immediately and completes asynchronously.
 */
final class UninitializedAMTCheckpointProvider(
    val deltaLog: DeltaLog,
    override val checkpointAction: Checkpoint,
    override val manifestCommitVersion: Long)
  extends AMTCheckpointProviderBase {

  override val tableRoot: Path = deltaLog.dataPath

  private[delta] val nonFateSharingCheckpointReadFuture =
    checkpointThreadPool.submitNonFateSharing { _: SparkSession =>
      AMTCheckpointProvider.loadFromCheckpoint(
        deltaLog, checkpointAction, manifestCommitVersion)
    }

  private[delta] def completeCheckpointProvider(): AMTCheckpointProvider =
    nonFateSharingCheckpointReadFuture.get(Duration.Inf)

  override private[delta] def numLeaves: Int =
    checkpointAction.contentRoot.numLeaves
      .map(Math.toIntExact)
      .getOrElse(completeCheckpointProvider().leaves.size)
}

/**
 * A complete AMT checkpoint provider whose leaf pointers are resolved only when first needed.
 */
final class LazyCompleteAMTCheckpointProvider(
    uninitializedCheckpointProvider: UninitializedAMTCheckpointProvider)
  extends AMTCheckpointProvider(
    uninitializedCheckpointProvider.manifestCommitVersion,
    uninitializedCheckpointProvider.checkpointAction,
    () => uninitializedCheckpointProvider.completeCheckpointProvider().leaves,
    uninitializedCheckpointProvider.tableRoot) {

  private[delta] lazy val underlyingCheckpointProvider: AMTCheckpointProvider =
    uninitializedCheckpointProvider.completeCheckpointProvider()

  override private[delta] def numLeaves: Int =
    uninitializedCheckpointProvider.numLeaves
}

trait AMTCheckpointProviderImpl extends CheckpointProvider {
  self: AMTCheckpointProvider =>

  import AMTCheckpointProvider.AMTDataEntryExtended

  /** The table version the manifest tree describes. */
  def checkpointVersion: Long = checkpointAction.version

  /** Pointer to the root manifest parquet. */
  private def contentRoot: ContentRoot = checkpointAction.contentRoot

  /** Absolute [[Path]] to the root manifest parquet, resolved against the table root. */
  private val rootManifestAbsolutePath: Path = contentRoot.getAbsolutePath(tableRoot)

  /** The live leaf pointers which must have all the live [[DataEntry]]s. */
  private lazy val liveLeaves: Seq[DataManifestEntry] =
    leaves.filter(l => Tracking.Status.liveEntryStatuses.contains(l.tracking.status))

  /** The tracking to inherit from. */
  private lazy val parentTrackingByManifestPath: Map[String, InheritableTracking] =
    liveLeaves.map { leaf =>
      SparkPath.fromPath(leaf.getAbsolutePath(tableRoot)).urlEncoded -> InheritableTracking(leaf)
    }.toMap

  /** Absolute [[Path]]s to the live leaf manifest parquet files, resolved against the root. */
  lazy val liveLeafManifestAbsolutePaths: Seq[Path] = liveLeaves.map(_.getAbsolutePath(tableRoot))

  /** The root manifest as a [[FileStatus]]. */
  private lazy val rootFile: FileStatus = contentRoot.toFileStatus(tableRoot)

  override def effectiveCheckpointSizeInBytes(): Long =
    contentRoot.sizeInBytes + liveLeaves.map(_.file_size_in_bytes).sum

  override private[delta] def estimatedStateReconstructionSizeInBytes(): Long = {
    val liveLeafEntries = liveLeaves.iterator.map { leaf =>
      leaf.manifest_info.liveFilesCount.toLong - leaf.manifest_info.dvCardinality
    }.sum
    math.max(
      effectiveCheckpointSizeInBytes(),
      liveLeafEntries * AMTCheckpointProvider.MIN_RECONSTRUCTION_BYTES_PER_LIVE_FILE)
  }

  override def checkpointPolicyForLogging: Option[CheckpointPolicy.Policy] = None

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
    val root = rootLiveAddSingleActions(deltaLog)
    if (liveLeaves.isEmpty) root else root.union(liveLeafAddSingleActions(deltaLog))
  }

  /**
   * Reconstructs root-resident AddFiles. Root entries have no parent and therefore no inheritance.
   */
  private def rootLiveAddSingleActions(deltaLog: DeltaLog): Dataset[SingleAction] = {
    val index = DeltaLogFileIndex(
      DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET, Array(rootFile))
    val entries = liveDataEntries(deltaLog, index)
    toAddSingleActions(entries)
  }

  /** Reconstructs the AddFiles of every live leaf, taking care of inheritance. */
  private def liveLeafAddSingleActions(
      deltaLog: DeltaLog): Dataset[SingleAction] = {
    val files = liveLeaves.map(_.toFileStatus(tableRoot)).toArray
    val entries = dataEntriesForRowIdInheritance(deltaLog, files, liveLeaves)
    toAddSingleActions(entries)
  }

  /**
   * Reads the DATA entries needed for row-ID inheritance. [[toAddSingleActions]] computes each
   * entry's prefix before applying status and MDV filtering.
   */
  private def dataEntriesForRowIdInheritance(
      deltaLog: DeltaLog,
      files: Array[FileStatus],
      dvLeaves: Seq[DataManifestEntry]): Dataset[AMTDataEntryExtended] = {
    val index = leafFileIndexWithManifestFilters(files, dvLeaves)
    asExtendedEntries(loadEntriesWithExtendedMetadata(
        deltaLog, index, checkpointAction.metaData, checkpointAction.protocol,
        includeRowIndexFilterMarker = true)
      .where(col("entry.content_type") === lit(AMTSingleAction.ContentType.Type.Data)))
  }

  /**
   * Reads visible DATA entries when no row-ID inheritance is needed.
   */
  private def liveDataEntries(
      deltaLog: DeltaLog, index: DeltaLogFileIndex): Dataset[AMTDataEntryExtended] =
    asExtendedEntries(
      loadEntriesWithExtendedMetadata(
          deltaLog, index, checkpointAction.metaData, checkpointAction.protocol)
        .where(col("entry.content_type") === lit(AMTSingleAction.ContentType.Type.Data))
        .where(col("entry.tracking.status").isin(Tracking.Status.liveEntryStatuses.toSeq: _*))
        .withColumn(ROW_INDEX_FILTER_KEEP_COLUMN, lit(true)))


  /** Builds a leaf index with bitmap-format-independent manifest filter providers. */
  protected def leafFileIndexWithManifestFilters(
      files: Array[FileStatus],
      dvLeaves: Seq[DataManifestEntry]): DeltaLogFileIndex = {
    DeltaLogFileIndex(
      new AMTParquetFileFormat,
      files,
      manifestRowIndexFilters(dvLeaves))
  }

  /** Builds the per-leaf Manifest-DV filters shared by relational and inheritance reads. */
  private def manifestRowIndexFilters(
      dvLeaves: Seq[DataManifestEntry]): Map[String, RowIndexFilterProvider] = {
    dvLeaves.flatMap { leaf =>
      leaf.manifestDV.map { case (dvBytes, _) =>
        leaf.getAbsolutePath(tableRoot).toString ->
          AMTUtils.deserializeMdv(dvBytes).toRowIndexFilterProvider(tableRoot)
      }
    }.toMap
  }

  /** Extends the Parquet read schema with the keep/drop marker. */
  protected def rowIndexFilterReadSchema(persistedSchema: StructType): StructType =
    persistedSchema.add(AMTParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD)

  /** Name of the raw keep/drop marker exposed by the Parquet scan. */
  protected def rowIndexFilterMarkerColumnName: String =
    AMTParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME

  /** Selects the raw marker and converts it into whether the manifest DV keeps this row. */
  protected def rowIndexFilterKeepColumn: Column =
    col(rowIndexFilterMarkerColumnName) === lit(RowIndexFilter.KEEP_ROW_VALUE)

  /**
   * Converts manifest entries into the `AddFile` actions of the reconstructed state.
   */
  private def toAddSingleActions(
      entries: Dataset[AMTDataEntryExtended]): Dataset[SingleAction] = {
    import org.apache.spark.sql.delta.implicits._
    val localTableRoot = tableRoot
    val encodedRootPath = SparkPath.fromPath(rootManifestAbsolutePath).urlEncoded
    val parentTracking = parentTrackingByManifestPath

    entries.mapPartitions { iter =>
      // AMT leaf files are unsplittable, and each file's rows are returned in physical row-index
      // order. A task may read several leaves, so reset the running prefix at each file boundary.
      // Computing the prefix here avoids a Window over (leafPath, pos), whose distribution and
      // ordering requirements would otherwise add a shuffle and sort after the file scan.
      var currentLeafPath: String = null
      var previousPosition = -1L
      var precedingNullFirstRowIdRecords = 0L

      iter.flatMap { entryWithLoc =>
        val isRootEntry = entryWithLoc.leafPath == encodedRootPath
        val prefix = if (isRootEntry) {
          0L
        } else {
          if (entryWithLoc.leafPath != currentLeafPath) {
            currentLeafPath = entryWithLoc.leafPath
            previousPosition = -1L
            precedingNullFirstRowIdRecords = 0L
          }
          require(entryWithLoc.pos > previousPosition,
            s"AMT leaf entries must be read in physical order: ${entryWithLoc.leafPath} " +
              s"position ${entryWithLoc.pos} followed $previousPosition.")
          previousPosition = entryWithLoc.pos
          val result = precedingNullFirstRowIdRecords
          if (entryWithLoc.entry.tracking.status == Tracking.Status.Added &&
              entryWithLoc.entry.tracking.first_row_id.isEmpty) {
            precedingNullFirstRowIdRecords += entryWithLoc.entry.record_count
          }
          result
        }

        if (!entryWithLoc.rowIndexFilterKeep ||
            !Tracking.Status.liveEntryStatuses.contains(entryWithLoc.entry.tracking.status)) {
          None
        } else entryWithLoc.entry.unwrap match {
          case data: DataEntry =>
            val backReference = if (isRootEntry) {
              None
            } else {
              val absLeaf = SparkPath.fromUrlString(entryWithLoc.leafPath).toPath
              val relManifest =
                AMTUtils.relativizeLocation(localTableRoot.toString, absLeaf.toString)
              Some(BackReference(relManifest, entryWithLoc.pos.toInt))
            }
            val resolvedTracking =
              if (isRootEntry) {
                // Root-resident DATA entries have no parent and retain their persisted tracking.
                data.tracking
              } else {
                Tracking.resolve(
                  childTracking = data.tracking,
                  parentTracking = parentTracking(entryWithLoc.leafPath),
                  prefixSumRecordCountForNullFirstRowId = prefix,
                  childEntryLocationForLogging = data.location)
              }
            val add = data.copy(tracking = resolvedTracking)
              .toAddFile(localTableRoot)
              .copy(backReference = backReference)
            Some(SingleAction(add = add))
          case other => throw new IllegalStateException(
            s"Expected a DATA entry after filtering, got ${other.getClass.getSimpleName}.")
        }
      }
    }
  }

  private def asExtendedEntries(df: DataFrame): Dataset[AMTDataEntryExtended] = {
    implicit val encoder: Encoder[AMTDataEntryExtended] =
      AMTCheckpointProvider.amtDataEntryExtendedEncoder
    df.as[AMTDataEntryExtended]
  }

  /** Name of the normalized manifest-DV keep/drop column. */
  private val ROW_INDEX_FILTER_KEEP_COLUMN: String = "rowIndexFilterKeep"

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
      catalogTableOpt: Option[CatalogTable],
      committedActions: Seq[Action]): Unit = {
    // Key by (path, dv) so a same-path replace is handled: the removed (path, oldDv) is checked
    // against the AMT, while the re-added (path, newDv) is a distinct key absent from the tree.
    val committedFiles = committedActions.collect {
      case a: AddFile =>
        a.toUniqueFileActionTuple(tableRoot, useObjectIdentity = true) -> a.backReference
      case r: RemoveFile =>
        r.toUniqueFileActionTuple(tableRoot, useObjectIdentity = true) -> r.backReference
    }
    if (committedFiles.isEmpty) return

    val expectedKeyToBackreferenceMap =
        liveAddSingleActions(spark, deltaLog).collect()
        .map { singleAction =>
          singleAction.add.toUniqueFileActionTuple(tableRoot, useObjectIdentity = true) ->
            singleAction.add.backReference
        }
        .toMap

    // Keys an intermediate commit (after this AMT) already re-committed. The first superseding
    // add/remove must carry a back reference; a 2nd superseding one of the same key need not.
    val intermediateCommittedKeys =
      deltaLog.getChanges(checkpointVersion + 1, catalogTableOpt).flatMap(_._2).collect {
        case a: AddFile => a.toUniqueFileActionTuple(tableRoot, useObjectIdentity = true)
        case r: RemoveFile => r.toUniqueFileActionTuple(tableRoot, useObjectIdentity = true)
      }.toSet

    committedFiles.foreach { case (key, actual) =>
      expectedKeyToBackreferenceMap.get(key) match {
        case Some(expected)
            if actual != expected && !(intermediateCommittedKeys.contains(key) && actual.isEmpty) =>
          throw new IllegalStateException(
            s"AMT back reference for file '${key.fileURI}' does not match the AMT. " +
            s"Expected $expected but the committed action carried $actual.")
        case None if actual.isDefined =>
          throw new IllegalStateException(
            s"File '${key.fileURI}' carries a back reference $actual but is not present in " +
            "the AMT tree, so it must not carry one.")
        case _ => // Matching, omitted after a window supersession, or absent+empty: as expected.
      }
    }
  }

  private def getBackreferencesForFilePaths(
      spark: SparkSession,
      deltaLog: DeltaLog,
      filePaths: Set[String]): Map[UniqueFileActionTuple, Option[BackReference]] =
    liveAddSingleActions(spark, deltaLog)
      .where(col("add.path").isin(filePaths.toSeq: _*))
      .collect()
      .map { singleAction =>
        singleAction.add.toUniqueFileActionTuple(tableRoot, useObjectIdentity = true) ->
          singleAction.add.backReference
      }
      .toMap

  /**
   * Re-derives file actions' back references to match this (the latest) tree.
   * The actions which have backreferences corresponding to leaves which
   * hasn't changed (Same old leaf is still present with the new tree and MDV
   * also hasn't changed) doesn't need to be re-calculated as the old
   * backreferences are still valid.
   */
  private[delta] def reStampBackReferences(
      spark: SparkSession,
      deltaLog: DeltaLog,
      actions: Seq[Action]): AMTCheckpointProvider.ReStampedBackReferences = {
    def existingBackReference(action: Action): Option[BackReference] = action match {
      case a: AddFile => a.backReference
      case r: RemoveFile => r.backReference
      case _ => None
    }
    def isFileAction(action: Action): Boolean = action match {
      case _: AddFile | _: RemoveFile => true
      case _ => false
    }
    // An `isMasked(pos)` test over each referenced leaf's manifest DV (a leaf with no DV masks
    // nothing), keyed by the leaf `location` a back reference's `manifest` holds. Only the leaves
    // some back reference points at are deserialized.
    val referencedManifests =
      actions.iterator.flatMap(existingBackReference).map(_.manifest).toSet
    val isMaskedByLocation: Map[String, Int => Boolean] = liveLeaves.iterator
      .filter(leaf => referencedManifests.contains(leaf.location))
      .map { leaf =>
        val isMasked: Int => Boolean = leaf.manifestDV match {
          case Some((dvBytes, _)) =>
            val mdv = AMTUtils.deserializeMdv(dvBytes)
            pos => mdv.contains(pos)
          case None => _ => false
        }
        leaf.location -> isMasked
      }.toMap
    // A back reference is still valid iff its leaf is present in this tree and its position there
    // is not MDV-masked. If an action has no back reference against the older tree, we recalculate
    // it too, as the entry might have spilled from the root to a new leaf in the latest tree.
    def stillValid(backReference: Option[BackReference]): Boolean =
      backReference.exists { case BackReference(manifest, pos) =>
        isMaskedByLocation.get(manifest).exists(isMasked => !isMasked(pos))
      }
    def needsReStamp(action: Action): Boolean = action match {
      case a: AddFile => !stillValid(a.backReference)
      case r: RemoveFile => !stillValid(r.backReference)
      case _ => false
    }
    if (!actions.exists(needsReStamp)) {
      return AMTCheckpointProvider.ReStampedBackReferences(
        actions,
        numActionsReusingBackref = actions.count(isFileAction),
        numActionsRegeneratingBackref = 0)
    }
    val pathsToReStamp: Set[String] = actions.iterator.collect {
      case a: AddFile if needsReStamp(a) => a.path
      case r: RemoveFile if needsReStamp(r) => r.path
    }.toSet
    val keyToBackRef = getBackreferencesForFilePaths(spark, deltaLog, pathsToReStamp)
    def backRefFor(action: FileAction): Option[BackReference] =
      keyToBackRef.getOrElse(
        action.toUniqueFileActionTuple(tableRoot, useObjectIdentity = true), None)
    val restamped = actions.map {
      case a: AddFile if needsReStamp(a) => a.copy(backReference = backRefFor(a))
      case r: RemoveFile if needsReStamp(r) => r.copy(backReference = backRefFor(r))
      case other => other
    }
    AMTCheckpointProvider.ReStampedBackReferences(
      restamped,
      numActionsReusingBackref = actions.count(a => isFileAction(a) && !needsReStamp(a)),
      numActionsRegeneratingBackref = actions.count(needsReStamp))
  }

  /**
   * Like [[AMTCheckpointProvider.loadEntries]], but also captures each row's physical location.
   * The optional row-index-filter marker is carried through manifest decoding so callers can apply
   * it after computing any row-ID prefix.
   */
  private def loadEntriesWithExtendedMetadata(
      deltaLog: DeltaLog,
      index: DeltaLogFileIndex,
      metadata: Metadata,
      protocol: Protocol,
      includeRowIndexFilterMarker: Boolean = false): DataFrame = {
    import org.apache.spark.sql.delta.implicits._
    val persistedSchema = AMTSingleAction.persistedSchema(metadata, protocol)
    val readSchema =
      if (includeRowIndexFilterMarker) rowIndexFilterReadSchema(persistedSchema)
      else persistedSchema
    val rowIndexFilterKeepColumns =
      if (includeRowIndexFilterMarker) {
        Seq(rowIndexFilterKeepColumn.as(ROW_INDEX_FILTER_KEEP_COLUMN))
      } else Seq.empty
    val persisted = deltaLog.loadIndex(index, readSchema)
      .select(
        (persistedSchema.fieldNames.toIndexedSeq.map(col) :+
          col(s"$METADATA_NAME.$FILE_PATH").as("leafPath") :+
          col(s"$METADATA_NAME.${ParquetFileFormat.ROW_INDEX}").as("pos")) ++
          rowIndexFilterKeepColumns: _*)
    val decodedRowIndexFilterKeepColumns =
      if (includeRowIndexFilterMarker) Seq(col(ROW_INDEX_FILTER_KEEP_COLUMN))
      else Seq.empty
    val withPartition = AMTPartitionValues.forRead(persisted, metadata.partitionSchema)
    AMTContentStats.forRead(withPartition, metadata, protocol)
      .select(
        (Seq(
          struct(amtSingleActionEncoder.schema.fieldNames.toIndexedSeq.map(col): _*).as("entry"),
          col("leafPath"),
          col("pos")) ++ decodedRowIndexFilterKeepColumns): _*)
  }
}

object AMTCheckpointProvider extends DeltaLogging {


  /**
   * Lower-bound compressed-size estimate per live AMT file action for replay partition sizing.
   * With the existing compression factor of 4 and 10 MiB target, this yields roughly one replay
   * partition per 160K live files. The physical-size estimate remains the lower bound.
   */
  private[amt] val MIN_RECONSTRUCTION_BYTES_PER_LIVE_FILE: Long = 16L


  /**
   * @param actions                       the actions with re-derived back references where needed.
   * @param numActionsReusingBackref      file actions whose existing back reference stayed valid.
   * @param numActionsRegeneratingBackref file actions whose back reference was re-derived.
   */
  case class ReStampedBackReferences(
      actions: Seq[Action],
      numActionsReusingBackref: Int,
      numActionsRegeneratingBackref: Int)

  /**
   * An [[AMTSingleAction]] entry paired with its physical read location in its manifest parquet.
   *
   * @param entry    The manifest content entry.
   * @param leafPath The URL-encoded absolute path of the manifest parquet the entry was read from
   *                 (Spark's `_metadata.file_path`).
   * @param pos      The 0-based position of the entry inside the manifest (Spark's
   *                 `_metadata.row_index`).
   * @param rowIndexFilterKeep Whether the manifest DV keeps this entry.
   */
  case class AMTDataEntryExtended(
      entry: AMTSingleAction,
      leafPath: String,
      pos: Long,
      rowIndexFilterKeep: Boolean)

  private[amt] lazy val amtDataEntryExtendedEncoder: Encoder[AMTDataEntryExtended] =
    new DeltaEncoder[AMTDataEntryExtended].get

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
      manifestCommitVersion: Long): AMTCheckpointProvider =
    loadFromCheckpoint(deltaLog, checkpoint, manifestCommitVersion)

  private[amt] def loadFromCheckpoint(
      deltaLog: DeltaLog,
      checkpoint: Checkpoint,
      manifestCommitVersion: Long): AMTCheckpointProvider = {
    val startNanos = System.nanoTime()
    val tableRoot = deltaLog.dataPath
    val rootFile = checkpoint.contentRoot.toFileStatus(tableRoot)
    val index =
      DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET, Array(rootFile))
    // The root manifest is small (one row per leaf), so collect it to the driver to enumerate the
    // leaf pointers.
    val leaves =
        loadEntries(deltaLog, index, checkpoint.metaData, checkpoint.protocol).collect()
        .toSeq
        .filter(_.content_type == AMTSingleAction.ContentType.Type.DataManifest)
        .map(_.unwrap.asInstanceOf[DataManifestEntry])
    val provider = new AMTCheckpointProvider(
      manifestCommitVersion = manifestCommitVersion,
      checkpointAction = checkpoint,
      loadLeaves = () => leaves,
      tableRoot = tableRoot)
    recordDeltaEvent(
      deltaLog,
      opType = AMTUsageLogs.CHECKPOINT_PROVIDER_INITIALIZE_FROM_CHECKPOINT_ACTION,
      data = Map(
        "durationMs" -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos),
        "manifestCommitVersion" -> manifestCommitVersion,
        "contentRootVersion" -> checkpoint.contentRoot.version,
        "checkpointVersion" -> checkpoint.version,
        "numLeaves" -> leaves.size.toLong,
        "contentRootSizeInBytes" -> checkpoint.contentRoot.sizeInBytes)
    )
    provider
  }


  /**
   * Builds a provider from an [[AMTWriteResult]] without reading the root manifest from storage.
   */
  def fromWriteResult(
      deltaLog: DeltaLog,
      writeResult: AMTWriteResult,
      manifestCommitVersion: Long): AMTCheckpointProvider = {
    new AMTCheckpointProvider(
      manifestCommitVersion = manifestCommitVersion,
      checkpointAction = writeResult.checkpoint,
      loadLeaves = () => writeResult.leaves,
      tableRoot = deltaLog.dataPath)
  }

  /** Reads the AMT root and returns the live [[DataEntry]]s tracked by root. */
  private[amt] def readLiveRootDataEntries(
      deltaLog: DeltaLog,
      checkpoint: Checkpoint): Seq[AddFile] = {
    val tableRoot = deltaLog.dataPath
    val rootFile = checkpoint.contentRoot.toFileStatus(tableRoot)
    val index =
      DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET, Array(rootFile))
    loadEntries(deltaLog, index, checkpoint.metaData, checkpoint.protocol).collect()
      .toSeq
      .filter(_.content_type == AMTSingleAction.ContentType.Type.Data)
      .map(_.unwrap.asInstanceOf[DataEntry])
      .filter(e => Tracking.Status.liveEntryStatuses.contains(e.tracking.status))
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

}
