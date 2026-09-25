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

import org.apache.spark.sql.delta.{RowIndexFilter, RowIndexFilterType}
import org.apache.spark.sql.delta.{CheckpointPolicy, CheckpointProvider, DeletionVectorsTableFeature, DeltaLog, DeltaLogFileIndex, DeltaParquetFileFormat, Snapshot}
import org.apache.spark.sql.delta.DeltaLogFileIndex.COMMIT_VERSION_COLUMN
import org.apache.spark.sql.delta.actions.{Action, AddFile, BackReference, Checkpoint, ContentRoot, FileAction, Metadata, Protocol, RemoveFile, SingleAction}
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.actions.FileAction.UniqueFileActionTuple
import org.apache.spark.sql.delta.util.DeltaEncoder
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, SparkSession}
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
    toAddSingleActions(entries)
  }

  /** Reconstructs the AddFiles of every live leaf, taking care of inheritance. */
  private def liveLeafAddSingleActions(
      deltaLog: DeltaLog): Dataset[SingleAction] = {
    val files = liveLeaves.map(_.toFileStatus(tableRoot)).toArray
    val entries = liveDataEntriesWithRowIdPrefix(deltaLog, files, liveLeaves)
    toAddSingleActions(entries)
  }

  /**
   * Reads all DATA entries, computes their row-ID prefix over each unfiltered leaf, then applies
   * status and MDV filtering. The returned live entries retain the computed prefix.
   */
  private def liveDataEntriesWithRowIdPrefix(
      deltaLog: DeltaLog,
      files: Array[FileStatus],
      dvLeaves: Seq[DataManifestEntry]): Dataset[AMTDataEntryExtended] = {
    val index = rowIdInheritanceFileIndex(files, dvLeaves)
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

  /**
   * Builds a leaf index that materializes each manifest DV as
   * [[DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME]].
   */
  protected def rowIdInheritanceFileIndex(
      files: Array[FileStatus],
      dvLeaves: Seq[DataManifestEntry]): DeltaLogFileIndex = {
    val format = DeltaParquetFileFormat(
      // Use neutral table metadata so manifest columns are not remapped as user-table columns.
      // Advertise DV readability so `_metadata.row_index` remains available to the reader.
      protocol = Protocol().withFeatures(Set(DeletionVectorsTableFeature)),
      metadata = Metadata(),
      // Keep each leaf unsplit and disable pushed filters so the prefix sees every physical entry.
      optimizationsEnabled = false,
      tablePath = Some(tableRoot.toString))
    val perFileMetadata: Map[String, Map[String, Any]] = dvLeaves.flatMap { leaf =>
      leaf.manifestDV.map { case (dvBytes, cardinality) =>
        val encoded =
          DeletionVectorDescriptor.inlineInLog(dvBytes, cardinality).serializeToBase64()
        leaf.getAbsolutePath(tableRoot).toString -> Map[String, Any](
          DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED -> encoded,
          DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE -> RowIndexFilterType.IF_CONTAINED)
      }
    }.toMap
    new DeltaLogFileIndex(format, files, perFileMetadata = perFileMetadata)
  }

  /** Extends the Parquet read schema with the keep/drop marker. */
  protected def rowIndexFilterReadSchema(persistedSchema: StructType): StructType =
    persistedSchema.add(DeltaParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD)

  /** Name of the keep/drop marker carried through manifest decoding. */
  protected def rowIndexFilterMarkerColumnName: String =
    DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME

  /** Selects the keep/drop marker from the Parquet scan. */
  protected def rowIndexFilterMarkerColumn: Column =
    col(rowIndexFilterMarkerColumnName)

  /** Drops rows marked deleted by the manifest DV, then removes the marker. */
  protected def filterOutRowIndexFilterEntries(dataFrame: DataFrame): DataFrame =
    dataFrame
      .where(col(DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME) ===
        lit(RowIndexFilter.KEEP_ROW_VALUE))
      .drop(DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME)

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
      iter.map { entryWithLoc =>
        entryWithLoc.entry.unwrap match {
          case data: DataEntry =>
            val isRootEntry = entryWithLoc.leafPath == encodedRootPath
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
                  prefixSumRecordCountForNullFirstRowId =
                    entryWithLoc.precedingNullFirstRowIdRecords,
                  childEntryLocationForLogging = data.location)
              }
            val add = data.copy(tracking = resolvedTracking)
              .toAddFile(localTableRoot)
              .copy(backReference = backReference)
            SingleAction(add = add)
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
      deltaLog.getChanges(checkpointVersion + 1).flatMap(_._2).collect {
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
}

object AMTCheckpointProvider {

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
    val leaves =
      loadEntries(deltaLog, index, checkpoint.metaData, checkpoint.protocol).collect()
        .toSeq
        .filter(_.content_type == AMTSingleAction.ContentType.Type.DataManifest)
        .map(_.unwrap.asInstanceOf[DataManifestEntry])
    new AMTCheckpointProvider(
      manifestCommitVersion = manifestCommitVersion,
      checkpointAction = checkpoint,
      leaves = leaves,
      tableRoot = tableRoot)
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
