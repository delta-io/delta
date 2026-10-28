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

import scala.collection.mutable

import org.apache.spark.sql.delta.{RowIndexFilter, RowIndexFilterProvider}
import org.apache.spark.sql.delta.{CheckpointPolicy, CheckpointProvider, DeltaLog, DeltaLogFileIndex, Snapshot}
import org.apache.spark.sql.delta.DeltaLogFileIndex.COMMIT_VERSION_COLUMN
import org.apache.spark.sql.delta.actions.{Action, AddFile, BackReference, Checkpoint, ContentRoot, FileAction, Metadata, Protocol, RemoveFile, SingleAction}
import org.apache.spark.sql.delta.actions.FileAction.UniqueFileActionTuple
import org.apache.spark.sql.delta.deletionvectors.ManifestBitmap
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.DeltaEncoder
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.datasources.FileFormat.{FILE_PATH, METADATA_NAME}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, lit, struct, sum, when}
import org.apache.spark.sql.types.StructType

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
  extends AMTCheckpointProviderImpl
{
}

trait AMTCheckpointProviderImpl extends CheckpointProvider {
  self: AMTCheckpointProvider =>

  import AMTCheckpointProvider.{AMTDataEntryExtended, DataEntryResolverProvider}

  /** The table version the manifest tree describes. */
  def checkpointVersion: Long = checkpointAction.version

  /** Pointer to the root manifest parquet. */
  private def contentRoot: ContentRoot = checkpointAction.contentRoot

  /** Absolute [[Path]] to the root manifest parquet, resolved against the table root. */
  private val rootManifestAbsolutePath: Path = contentRoot.getAbsolutePath(tableRoot)

  /** The live leaf pointers which must have all the live [[DataEntry]]s. */
  private lazy val liveLeaves: Seq[DataManifestEntry] =
    leaves.filter(l => Tracking.Status.liveEntryStatuses.contains(l.tracking.status))

  /** Absolute [[Path]]s to the live leaf manifest parquet files, resolved against the root. */
  lazy val liveLeafManifestAbsolutePaths: Seq[Path] = liveLeaves.map(_.getAbsolutePath(tableRoot))

  /** The root manifest as a [[FileStatus]]. */
  private lazy val rootFile: FileStatus = contentRoot.toFileStatus(tableRoot)

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
    toAddSingleActions(entries, dataEntryResolverProvider(parentLeaves = Seq.empty))
  }

  /** Reconstructs the AddFiles of every live leaf, taking care of inheritance. */
  private def liveLeafAddSingleActions(
      deltaLog: DeltaLog): Dataset[SingleAction] = {
    val leavesWithFilters = liveLeaves.map { leaf =>
      leaf -> leaf.manifestDV.map { case (dvBytes, _) =>
        AMTUtils.deserializeMdv(dvBytes).toRowIndexFilterProvider(tableRoot)
      }
    }
    toAddSingleActions(
      loadLeafDataEntriesWithRowIdPrefix(deltaLog, leavesWithFilters),
      dataEntryResolverProvider(parentLeaves = liveLeaves))
  }

  /**
   * Reads all live DATA entries of the given leaves, computes their row-ID prefix over each
   * unfiltered leaf, then applies status filtering and each leaf's row-index filter, if any. A
   * leaf without a filter is read unfiltered. The returned entries retain the computed prefix.
   */
  private def loadLeafDataEntriesWithRowIdPrefix(
      deltaLog: DeltaLog,
      leavesWithRowIndexFilters: Seq[(DataManifestEntry, Option[RowIndexFilterProvider])])
      : Dataset[AMTDataEntryExtended] = {
    val files =
      leavesWithRowIndexFilters.map { case (leaf, _) => leaf.toFileStatus(tableRoot) }.toArray
    val perFileRowIndexFilters: Map[String, RowIndexFilterProvider] =
      leavesWithRowIndexFilters.collect { case (leaf, Some(provider)) =>
        leaf.getAbsolutePath(tableRoot).toString -> provider
      }.toMap
    val index = DeltaLogFileIndex(new AMTParquetFileFormat, files, perFileRowIndexFilters)
    val withPrefix = loadEntriesWithExtendedMetadata(
        deltaLog, index, checkpointAction.metaData, checkpointAction.protocol,
        includeRowIndexFilterMarker = true)
      .where(col("entry.content_type") === lit(AMTSingleAction.ContentType.Type.Data))
      .withColumn(PRECEDING_RECORDS_COLUMN, precedingNullRowIdRecords)
      .where(col("entry.tracking.status").isin(Tracking.Status.liveEntryStatuses.toSeq: _*))
    asExtendedEntries(filterOutRowIndexFilterEntries(withPrefix))
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
        .withColumn(PRECEDING_RECORDS_COLUMN, lit(0L)))

  /** Extends the Parquet read schema with the keep/drop marker. */
  protected def rowIndexFilterReadSchema(persistedSchema: StructType): StructType =
    persistedSchema.add(AMTParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD)

  /** Name of the keep/drop marker carried through manifest decoding. */
  protected def rowIndexFilterMarkerColumnName: String =
    AMTParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME

  /** Selects the keep/drop marker from the Parquet scan. */
  protected def rowIndexFilterMarkerColumn: Column =
    col(rowIndexFilterMarkerColumnName)

  /** Keeps the rows the row-index filter does not mark deleted, then removes the marker column. */
  protected def filterOutRowIndexFilterEntries(dataFrame: DataFrame): DataFrame =
    dataFrame
      .where(col(AMTParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME) ===
        lit(RowIndexFilter.KEEP_ROW_VALUE))
      .drop(AMTParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME)

  /**
   * Converts manifest entries into the `AddFile` actions of the reconstructed state. Each leaf
   * entry carries a back reference to its position in its leaf and has its inheritance resolved.
   *
   * @param entries The live DATA entries to convert, read from the root or from leaves, each with
   *                its read location and `first_row_id` prefix sum.
   * @param resolverProvider The [[DataEntryResolverProvider]] for the leaves `entries` were read
   *                         from.
   */
  private def toAddSingleActions(
      entries: Dataset[AMTDataEntryExtended],
      resolverProvider: DataEntryResolverProvider): Dataset[SingleAction] = {
    import org.apache.spark.sql.delta.implicits._
    entries.mapPartitions { iter =>
      val resolver = resolverProvider.createResolver()
      iter.map { entryWithLoc =>
        val (add, leafLocation) = resolver.resolve(entryWithLoc)
        SingleAction(add = add.copy(
          backReference = leafLocation.map(BackReference(_, entryWithLoc.pos.toInt))))
      }
    }
  }

  private def asExtendedEntries(df: DataFrame): Dataset[AMTDataEntryExtended] = {
    implicit val encoder: Encoder[AMTDataEntryExtended] =
      AMTCheckpointProvider.amtDataEntryExtendedEncoder
    df.as[AMTDataEntryExtended]
  }

  /** Name of the [[AMTDataEntryExtended.precedingNullFirstRowIdRecords]] column. */
  private val PRECEDING_RECORDS_COLUMN: String = "precedingNullFirstRowIdRecords"

  /**
   * The `first_row_id` prefix sum of a manifest row: the total `record_count` of the entries that
   * precede it in the same manifest, have `ADDED` status, and carry a null `first_row_id`.
   */
  private def precedingNullRowIdRecords: Column = {
    val precedingRowsOfManifest = Window
      .partitionBy(col("leafPath"))
      .orderBy(col("pos"))
      .rowsBetween(Window.unboundedPreceding, -1)
    val contribution = when(
      col("entry.tracking.status") === lit(Tracking.Status.Added) &&
        col("entry.tracking.first_row_id").isNull,
      col("entry.record_count")).otherwise(lit(0L))
    // The frame is empty for the first row of a manifest, where the sum is null and the offset 0.
    coalesce(sum(contribution).over(precedingRowsOfManifest), lit(0L))
  }

  /**
   * Reconstructs the AddFile and RemoveFile actions of the single commit this manifest tree
   * describes. The AMT does not persist `dataChange` or when a file was removed, so callers supply
   * the commit-level `dataChange` and the commit timestamp, which becomes every reconstructed
   * RemoveFile's `deletionTimestamp`. The actions come in no particular order.
   */
  private[delta] def reconstructFileActions(
      deltaLog: DeltaLog,
      dataChange: Boolean,
      commitTimestamp: Long): Seq[FileAction] =
    mdvDerivedChangeActions(deltaLog, dataChange, commitTimestamp)
      .foldLeft(trackingDerivedChangeActions(deltaLog, dataChange, commitTimestamp))(_ union _)
      .collect().toSeq
      .map(_.unwrap.asInstanceOf[FileAction])

  /**
   * Change files derived from tracking statuses from both root and new leaves, stamped with the
   * commit-level `dataChange` and `commitTimestamp` (see [[reconstructFileActions]]).
   */
  private def trackingDerivedChangeActions(
      deltaLog: DeltaLog,
      dataChange: Boolean,
      commitTimestamp: Long): Dataset[SingleAction] = {
    import org.apache.spark.sql.delta.implicits._
    // Only Added leaves are considered here.
    // Existing leaves with MDV changes are handled by mdvDerivedChangeActions.
    // Existing leaves removed entirely will be a subset of mdvDerivedChangeActions.
    val addedLeaves = leaves.filter { leaf =>
      leaf.tracking.status == Tracking.Status.Added && !holdsOnlyExistingEntries(leaf)
    }
    val manifestIndex = DeltaLogFileIndex(
      DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET,
      (rootFile +: addedLeaves.map(_.toFileStatus(tableRoot))).toArray)

    val resolverProvider = dataEntryResolverProvider(addedLeaves)
    // Only leaf entries inherit a row ID, so skip the prefix's shuffle when there is no leaf.
    val precedingRecords =
      if (addedLeaves.isEmpty) lit(0L) else precedingNullRowIdRecords

    asExtendedEntries(
      loadEntriesWithExtendedMetadata(
          deltaLog, manifestIndex, checkpointAction.metaData, checkpointAction.protocol)
        .where(col("entry.content_type") === lit(AMTSingleAction.ContentType.Type.Data))
        // The row-ID prefix must see every entry of a leaf, so compute it before status filtering.
        .withColumn(PRECEDING_RECORDS_COLUMN, precedingRecords)
        .where(col("entry.tracking.status").isin(Tracking.Status.changeEntryStatuses.toSeq: _*)))
      .mapPartitions { entries =>
        val resolver = resolverProvider.createResolver()
        entries.map { entryWithLoc =>
          val (add, _) = resolver.resolve(entryWithLoc)
          val status = entryWithLoc.entry.unwrap.asInstanceOf[DataEntry].tracking.status
          if (Tracking.Status.liveEntryStatuses.contains(status)) {
            SingleAction(add = add.copy(dataChange = dataChange))
          } else {
            SingleAction(remove = add.removeWithTimestamp(commitTimestamp, dataChange))
          }
        }
      }
  }

  /**
   * The change files coming from MDV-marked positions in the leaves, stamped with the commit-level
   * `dataChange` and `commitTimestamp` (see [[reconstructFileActions]]).
   */
  private def mdvDerivedChangeActions(
      deltaLog: DeltaLog,
      dataChange: Boolean,
      commitTimestamp: Long): Option[Dataset[SingleAction]] = {
    import org.apache.spark.sql.delta.implicits._
    // These are the leaves that contain MDV-masked entries.
    // By definition these leaves cannot have ADDED statuses.
    val touchedLeaves = leaves.filter(_.changedPositionsBitmaps.nonEmpty)
    assert(touchedLeaves.forall(_.tracking.status != Tracking.Status.Added))
    if (touchedLeaves.isEmpty) return None

    val leavesWithFilters = touchedLeaves.map { leaf =>
      leaf -> Some(ManifestBitmap.toRowIndexFilterProvider(
        leaf.changedPositionsBitmaps, tableRoot, inverse = true))
    }
    val resolverProvider = dataEntryResolverProvider(touchedLeaves)
    Some(loadLeafDataEntriesWithRowIdPrefix(deltaLog, leavesWithFilters)
      .mapPartitions { entries =>
        val resolver = resolverProvider.createResolver()
        entries.map { entryWithLoc =>
          val (add, _) = resolver.resolve(entryWithLoc)
          SingleAction(remove = add.removeWithTimestamp(commitTimestamp, dataChange))
        }
      })
  }

  /**
   * Whether every entry of `leaf` is EXISTING, per its `manifest_info` counts. Such a leaf holds
   * only spilled files and carries no data change.
   */
  private def holdsOnlyExistingEntries(leaf: DataManifestEntry): Boolean = {
    val info = leaf.manifest_info
    AMTUtils.invariantCheckWithLogging(
      checkInvariant = info.totalFilesCount == leaf.record_count,
      opTypeSuffix = AMTUsageLogs.ALERT_INCONSISTENT_MANIFEST_INFO_FILE_COUNTS,
      message = s"Leaf ${leaf.location} has record_count ${leaf.record_count}, but its " +
        s"manifest_info file counts sum to ${info.totalFilesCount}.",
      data = Map(
        "leafLocation" -> leaf.location,
        "recordCount" -> leaf.record_count,
        "addedFilesCount" -> info.added_files_count,
        "existingFilesCount" -> info.existing_files_count,
        "deletedFilesCount" -> info.deleted_files_count,
        "replacedFilesCount" -> info.replaced_files_count,
        "modifiedFilesCount" -> info.modified_files_count))
    info.existing_files_count == leaf.record_count
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
    val markerColumns =
      if (includeRowIndexFilterMarker) Seq(rowIndexFilterMarkerColumn)
      else Seq.empty
    val persisted = deltaLog.loadIndex(index, readSchema)
      .select(
        (persistedSchema.fieldNames.toIndexedSeq.map(col) :+
          col(s"$METADATA_NAME.$FILE_PATH").as("leafPath") :+
          col(s"$METADATA_NAME.${ParquetFileFormat.ROW_INDEX}").as("pos")) ++ markerColumns: _*)
    val decodedMarkerColumns =
      if (includeRowIndexFilterMarker) Seq(col(rowIndexFilterMarkerColumnName))
      else Seq.empty
    val withPartition = AMTPartitionValues.forRead(persisted, metadata.partitionSchema)
    AMTContentStats.forRead(withPartition, metadata, protocol)
      .select(
        (Seq(
          struct(amtSingleActionEncoder.schema.fieldNames.toIndexedSeq.map(col): _*).as("entry"),
          col("leafPath"),
          col("pos")) ++ decodedMarkerColumns): _*)
  }

  /**
   * Builds the [[DataEntryResolverProvider]] for DATA entries read from the root or from
   * `parentLeaves`.
   *
   * @param parentLeaves The leaf pointers the scanned leaf entries were read from.
   */
  private def dataEntryResolverProvider(
      parentLeaves: Seq[DataManifestEntry]): DataEntryResolverProvider = {
    val tableRootString = tableRoot.toString
    DataEntryResolverProvider(
      tableRoot = tableRoot,
      urlEncodedRootManifestPath = SparkPath.fromPath(rootManifestAbsolutePath).urlEncoded,
      parentTrackingByLeafLocation = parentLeaves.map { leaf =>
        AMTUtils.relativizeLocation(tableRootString, leaf.getAbsolutePath(tableRoot).toString) ->
          InheritableTracking(leaf)
      }.toMap)
  }
}

object AMTCheckpointProvider extends DeltaLogging {

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
   * The identity a file action reconstructed by [[AMTCheckpointProvider.reconstructFileActions]]
   * is matched to a committed one by.
   *
   * @param file  The file's (path, deletion vector) tuple, using deletion vector object identity.
   * @param isAdd Whether the action is an [[AddFile]] rather than a [[RemoveFile]].
   */
  private[delta] case class FileActionKey(file: UniqueFileActionTuple, isAdd: Boolean)

  private[delta] object FileActionKey {
    def apply(action: FileAction, tableRoot: Path): FileActionKey =
      FileActionKey(
        action.toUniqueFileActionTuple(tableRoot, useObjectIdentity = true),
        isAdd = action.isInstanceOf[AddFile])
  }

  /**
   * An [[AMTSingleAction]] entry paired with its physical read location in its manifest parquet.
   *
   * @param entry    The manifest content entry.
   * @param leafPath The URL-encoded absolute path of the manifest parquet the entry was read from
   *                 (Spark's `_metadata.file_path`).
   * @param pos      The 0-based position of the entry inside the manifest (Spark's
   *                 `_metadata.row_index`).
   * @param precedingNullFirstRowIdRecords The entry's `first_row_id` prefix sum.
   */
  case class AMTDataEntryExtended(
      entry: AMTSingleAction,
      leafPath: String,
      pos: Long,
      precedingNullFirstRowIdRecords: Long)

  private[amt] lazy val amtDataEntryExtendedEncoder: Encoder[AMTDataEntryExtended] =
    new DeltaEncoder[AMTDataEntryExtended].get

  /**
   * Everything needed to convert [[AMTDataEntryExtended]] DataEntries read from the root or from
   * leaves into [[AddFile]]s with their tracking resolved.
   *
   * @param tableRoot                    The table's data path.
   * @param urlEncodedRootManifestPath   The URL-encoded absolute path of the root manifest, which
   *                                     tells root entries apart from leaf entries.
   * @param parentTrackingByLeafLocation The inheritable tracking of each leaf the entries were read
   *                                     from, keyed by the leaf's `location` relativized against
   *                                     `tableRoot`.
   */
  private[amt] case class DataEntryResolverProvider(
      tableRoot: Path,
      urlEncodedRootManifestPath: String,
      parentTrackingByLeafLocation: Map[String, InheritableTracking]) {

    /** Creates the [[DataEntryResolver]] for one partition. Call it on the executor. */
    def createResolver(): DataEntryResolver = new DataEntryResolver(this)
  }

  /**
   * Resolves the DataEntries of one partition (see [[DataEntryResolverProvider]]). All entries of
   * a leaf share its path, so each distinct leaf path is relativized once per partition rather than
   * once per entry.
   */
  private[amt] final class DataEntryResolver(provider: DataEntryResolverProvider) {
    private val tableRootString = provider.tableRoot.toString

    /** Cache for each URL-encoded leaf path to (its raw relative path, its tracking). */
    private val leafByUrlEncodedPath = mutable.HashMap.empty[String, (String, InheritableTracking)]

    /** The uncoded relative path and parent tracking of the leaf at `urlEncodedPath`. */
    private def getOrCacheLeaf(urlEncodedPath: String): (String, InheritableTracking) =
      leafByUrlEncodedPath.getOrElseUpdate(urlEncodedPath, {
        val leafUncodedRelPath = AMTUtils.relativizeLocation(
          tableRootString, SparkPath.fromUrlString(urlEncodedPath).toPath.toString)
        (leafUncodedRelPath, provider.parentTrackingByLeafLocation(leafUncodedRelPath))
      })

    /**
     * Converts `entryWithLoc` into an [[AddFile]] with its tracking resolved. It also returns the
     * uncoded relative path of the leaf the entry was read from, or None for a root entry.
     */
    def resolve(entryWithLoc: AMTDataEntryExtended): (AddFile, Option[String]) =
      entryWithLoc.entry.unwrap match {
        case data: DataEntry =>
          if (entryWithLoc.leafPath == provider.urlEncodedRootManifestPath) {
            // No inheritence for root entries.
            (data.toAddFile(provider.tableRoot), None)
          } else {
            // Resolve inheritence for leaf entries.
            val (leafUncodedRelPath, parentTracking) = getOrCacheLeaf(
              entryWithLoc.leafPath
            )
            val resolvedTracking = Tracking.resolve(
              childTracking = data.tracking,
              parentTracking = parentTracking,
              prefixSumRecordCountForNullFirstRowId = entryWithLoc.precedingNullFirstRowIdRecords,
              childEntryLocationForLogging = data.location)
            (
              data.copy(tracking = resolvedTracking).toAddFile(provider.tableRoot),
              Some(leafUncodedRelPath)
            )
          }
        case other => throw new IllegalStateException(
          s"Expected a DATA entry after filtering, got ${other.getClass.getSimpleName}.")
      }
  }

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
      leaves = leaves,
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
      leaves = writeResult.leaves,
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
