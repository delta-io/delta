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

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

import org.apache.spark.sql.delta.RowIndexFilterProvider
import org.apache.spark.sql.delta.{CheckpointPolicy, CheckpointProvider, DataFrameUtils, DeltaLog, DeltaLogFileIndex, Snapshot, UninitializedCheckpointProvider}
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.DeltaLogFileIndex.COMMIT_VERSION_COLUMN
import org.apache.spark.sql.delta.SnapshotManagement.checkpointThreadPool
import org.apache.spark.sql.delta.actions.{Action, AddFile, BackReference, Checkpoint, ContentRoot, FileAction, LastManifestCommit, Metadata, Protocol, RemoveFile, SingleAction}
import org.apache.spark.sql.delta.actions.FileAction.UniqueFileActionTuple
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.threads.NonFateSharingFuture
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType

/**
 * Leaf pointers and optional cached root rows. The cached schema is
 * [[AMTSingleAction.persistedSchema]]. Rows contain DATA and DATA_MANIFEST entries as
 * [[InternalRow]]s, with partition and content_stats kept as typed structs when present.
 */
private[delta] class AMTRootInfo(
    val leaves: Seq[DataManifestEntry],
    private val cachedRows: Option[(StructType, Seq[InternalRow])]) {
  def hasCachedRows: Boolean = cachedRows.isDefined

  // Build a fresh plan for the caller's session over the collected rows.
  def toDF(spark: SparkSession): Option[DataFrame] = cachedRows.map { case (schema, rows) =>
    DataFrameUtils.ofRows(spark, LocalRelation(toAttributes(schema), rows))
  }
}

/** Common checkpoint information available before and after the AMT root has been loaded. */
trait AMTCheckpointProviderBase
  extends UninitializedCheckpointProvider
  with DeltaLogging {
  /** The version of the manifest commit that wrote this checkpoint. */
  def manifestCommitVersion: Long

  /** The inline action describing this AMT checkpoint. */
  def checkpointAction: Checkpoint

  /** The [[LastManifestCommit]] that names this tree. */
  def lastManifestCommit: LastManifestCommit =
    LastManifestCommit(manifestCommitVersion, checkpointAction.contentRoot.version)

  /** The table data path used to resolve manifest paths. */
  def tableRoot: Path

  override def version: Long = checkpointAction.version

  /** The number of leaf manifests referenced by the root. */
  private[delta] def numLeaves: Long

  override def topLevelFiles: Seq[FileStatus] = {
    val contentRoot = checkpointAction.contentRoot
    Seq(contentRoot.toFileStatus(tableRoot))
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
 * standalone checkpoint which also refers to an AMT. State reconstruction builds each `AddFile`
 * field as a column over the persisted manifest rows (see `AMTManifestReaderImpl.addFileColumn`).
 *
 * @param manifestCommitVersion The version of the manifest commit that wrote this checkpoint.
 * @param checkpointAction The inline-emitted Checkpoint action this tree was committed with;
 *                         carries the version, contentRoot, and inline non-file state.
 * @param tableRoot        The table's data path.
 * @param rootInfo         The root's leaf pointers and optional prefetched rows.
 */
final class AMTCheckpointProvider(
    override val manifestCommitVersion: Long,
    override val checkpointAction: Checkpoint,
    override val tableRoot: Path,
    private val rootInfo: AMTRootInfo)
  extends AMTCheckpointProviderBase
  with AMTCheckpointProviderImpl
{
  /**
   * The root's `DATA_MANIFEST` pointer entries, one per reachable leaf. Each entry's `location` is
   * stored table-root-relative; use [[liveLeafManifestAbsolutePaths]] to resolve it against the
   * table root.
   */
  val leaves: Seq[DataManifestEntry] = rootInfo.leaves

  override private[delta] def numLeaves: Long =
    checkpointAction.contentRoot.numLeaves.getOrElse(leaves.size.toLong)

  override protected def rootLiveAddSingleActions(
      spark: SparkSession, deltaLog: DeltaLog): Dataset[SingleAction] =
    toAddSingleActions(
      manifestReader.withStatuses(rootEntries(spark, deltaLog), Tracking.Status.liveEntryStatuses),
      baseRowId = col("tracking.first_row_id"))

  /**
   * Like `AMTManifestReaderImpl.rootEntries`, but reuses the prefetched root if possible, with
   * the same columns, so that the rows union with leaf entry rows.
   */
  private[amt] def rootEntries(spark: SparkSession, deltaLog: DeltaLog): DataFrame =
    rootInfo.toDF(spark).map { root =>
      val persistedSchema =
        AMTSingleAction.persistedSchema(checkpointAction.metaData, checkpointAction.protocol)
      val columns = persistedSchema.fieldNames.toIndexedSeq.map(col) :+
        lit(SparkPath.fromPath(rootFile.getPath).urlEncoded).as("leafPath") :+
        // The shared back-reference expression requires a non-null pos, unused for root entries.
        lit(0L).as("pos")
      manifestReader.withRootEntryContext(manifestReader.dataEntries(root.select(columns: _*)))
    }.getOrElse(manifestReader.rootEntries(deltaLog, Seq(rootFile)))

  override protected def leafLiveAddSingleActions(
      spark: SparkSession,
      deltaLog: DeltaLog): Dataset[SingleAction] =
    toAddSingleActions(
      manifestReader.withStatuses(
        manifestReader.leafEntriesWithRowIdPrefix(spark, deltaLog, liveLeavesWithManifestFilters),
        Tracking.Status.liveEntryStatuses),
      baseRowId = manifestReader.inheritedBaseRowId)

  /** Reuses prefetched root for incremental writes as well as snapshot state reconstruction. */
  override private[amt] def readLiveRootDataEntries(
      spark: SparkSession, deltaLog: DeltaLog): Seq[AddFile] =
    rootLiveAddSingleActions(spark, deltaLog).collect().toSeq.map(_.add)
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

  private[delta] val nonFateSharingCheckpointReadFuture
      : NonFateSharingFuture[AMTRootInfo] =
    checkpointThreadPool.submitNonFateSharing { spark: SparkSession =>
      AMTCheckpointProvider.loadRootFromCheckpoint(
        spark, deltaLog, checkpointAction, manifestCommitVersion)
    }

  override private[delta] def numLeaves: Long =
    checkpointAction.contentRoot.numLeaves.getOrElse {
      // ContentRoot.tags normally provides numLeaves without waiting. Only when that information
      // is absent from the tags must we wait for the existing prefetch future to derive the count.
      nonFateSharingCheckpointReadFuture.get(Duration.Inf).leaves.size.toLong
    }
}

/**
 * A complete-provider facade which waits for the prefetched AMT root only when an API requiring
 * the complete checkpoint is first used.
 */
abstract class LazyCompleteAMTCheckpointProvider(
    uninitializedCheckpointProvider: UninitializedAMTCheckpointProvider)
  extends AMTCheckpointProviderBase
  with CheckpointProvider {

  override val manifestCommitVersion: Long =
    uninitializedCheckpointProvider.manifestCommitVersion
  override val checkpointAction: Checkpoint = uninitializedCheckpointProvider.checkpointAction
  override val tableRoot: Path = uninitializedCheckpointProvider.tableRoot

  protected def createCheckpointProvider(): AMTCheckpointProvider

  lazy val underlyingCheckpointProvider: AMTCheckpointProvider = createCheckpointProvider()

  override private[delta] def numLeaves: Long = uninitializedCheckpointProvider.numLeaves

  override def effectiveCheckpointSizeInBytes(): Long =
    underlyingCheckpointProvider.effectiveCheckpointSizeInBytes()

  override def checkpointPolicyForLogging: Option[CheckpointPolicy.Policy] =
    underlyingCheckpointProvider.checkpointPolicyForLogging

  override def createCompatibilityCheckpoint(
      spark: SparkSession,
      deltaLog: DeltaLog,
      logPath: Path,
      hadoopConf: Configuration): Unit =
    underlyingCheckpointProvider.createCompatibilityCheckpoint(
      spark, deltaLog, logPath, hadoopConf)

  override def loadActionsForStateReconstruction(
      spark: SparkSession, deltaLog: DeltaLog): Option[DataFrame] =
    underlyingCheckpointProvider.loadActionsForStateReconstruction(spark, deltaLog)

}

trait AMTCheckpointProviderImpl extends CheckpointProvider {
  self: AMTCheckpointProvider =>

  import AMTManifestReader.SingleManifest

  /** The table version the manifest tree describes. */
  def checkpointVersion: Long = checkpointAction.version

  /** Pointer to the root manifest parquet. */
  private def contentRoot: ContentRoot = checkpointAction.contentRoot

  /** The live leaf pointers which must have all the live [[DataEntry]]s. */
  protected lazy val liveLeaves: Seq[DataManifestEntry] =
    leaves.filter(l => Tracking.Status.liveEntryStatuses.contains(l.tracking.status))

  /** Absolute [[Path]]s to the live leaf manifest parquet files, resolved against the root. */
  lazy val liveLeafManifestAbsolutePaths: Seq[Path] = liveLeaves.map(_.getAbsolutePath(tableRoot))

  /** The root manifest as a [[FileStatus]]. */
  protected lazy val rootFile: FileStatus = contentRoot.toFileStatus(tableRoot)

  /** Reads the manifests of this tree, in the schema its metadata and protocol give. */
  private[amt] lazy val manifestReader: AMTManifestReader =
    new AMTManifestReader(tableRoot, checkpointAction.metaData, checkpointAction.protocol)

  override def effectiveCheckpointSizeInBytes(): Long =
    contentRoot.sizeInBytes + liveLeaves.map(_.file_size_in_bytes).sum

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
   * non-content actions (protocol, metadata, domain metadata, txns) built on the driver, and
   * the txns and domain metadata carried by the checkpoint's non-content sidecars (read lazily).
   */
  private def allActions(spark: SparkSession, deltaLog: DeltaLog): Dataset[SingleAction] = {
    import org.apache.spark.sql.delta.implicits._
    val nonFileActions = spark.createDataset(nonContentSingleActions)
    val allActionsDFExceptNonContentSidecars =
      liveAddSingleActions(spark, deltaLog).union(nonFileActions)
    AMTNonContentSidecarReader.loadSidecarActions(spark, deltaLog, checkpointAction)
      .map(allActionsDFExceptNonContentSidecars.union)
      .getOrElse(allActionsDFExceptNonContentSidecars)
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
      spark: SparkSession,
      deltaLog: DeltaLog): Dataset[SingleAction] = {
    val root = rootLiveAddSingleActions(spark, deltaLog)
    if (liveLeaves.isEmpty) root else root.union(leafLiveAddSingleActions(spark, deltaLog))
  }

  /**
   * Reconstructs root-resident AddFiles. Root entries have no parent and therefore no inheritance.
   */
  protected def rootLiveAddSingleActions(
      spark: SparkSession, deltaLog: DeltaLog): Dataset[SingleAction]

  /** Reconstructs the AddFiles of every live leaf, taking care of inheritance. */
  protected def leafLiveAddSingleActions(
      spark: SparkSession,
      deltaLog: DeltaLog): Dataset[SingleAction]

  /** The live AddFiles that the root holds, for the incremental writer. */
  private[amt] def readLiveRootDataEntries(
      spark: SparkSession, deltaLog: DeltaLog): Seq[AddFile]

  /** The live leaves, each with the row-index filter of its manifest DV, if it has one. */
  protected def liveLeavesWithManifestFilters
      : Seq[(DataManifestEntry, Option[RowIndexFilterProvider])] =
    liveLeaves.map { leaf =>
      leaf -> leaf.manifestDV.map { case (dvBytes, _) =>
        AMTUtils.deserializeMdv(dvBytes).toRowIndexFilterProvider(tableRoot)
      }
    }

  /**
   * Converts live DATA entry rows, with their entry context, into the `AddFile` actions of the
   * reconstructed state, as columns.
   *
   * @param baseRowId The entry's first row ID; leaf entries may inherit it from their parent.
   */
  protected def toAddSingleActions(entries: DataFrame, baseRowId: Column): Dataset[SingleAction] = {
    import org.apache.spark.sql.delta.implicits._
    val entriesWithAdd = manifestReader.addFileEntries(entries, baseRowId)
    entriesWithAdd.select(Action.logSchema.fields.toIndexedSeq.map { field =>
      (if (field.name == "add") col("add") else lit(null).cast(field.dataType)).as(field.name)
    }: _*).as[SingleAction]
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

  ////////////////////////////////
  // Change file reconstruction //
  ////////////////////////////////

  /**
   * Reconstructs the AddFile and RemoveFile actions of the single commit this manifest tree
   * describes. The AMT does not persist `dataChange` or when a file was removed, so callers supply
   * the commit-level `dataChange` and the commit timestamp, which becomes every reconstructed
   * RemoveFile's `deletionTimestamp`.
   *
   * The recontructed actions can be returned in deterministic order if desired.
   * Manifest entries are ordered by their path, and entries within a manifest are ordered by
   * their offset.
   */
  private[delta] def reconstructFileActions(
      spark: SparkSession,
      deltaLog: DeltaLog,
      dataChange: Boolean,
      commitTimestamp: Long,
      ordered: Boolean = true): Seq[FileAction] =
    toSingleActions(
        manifestReader.loadFilteredChangeEntries(spark, deltaLog, changeManifests),
        dataChange,
        commitTimestamp,
        ordered)
      .collect().toSeq
      .map(_.unwrap.asInstanceOf[FileAction])

  /**
   * Like [[reconstructFileActions]], in its order, but pulls the actions to the driver one
   * partition of the sort at a time, so a large commit need not fit in driver memory.
   */
  private[delta] def reconstructFileActionsIterator(
      spark: SparkSession,
      deltaLog: DeltaLog,
      dataChange: Boolean,
      commitTimestamp: Long): Iterator[FileAction] =
    toSingleActions(
        manifestReader.loadFilteredChangeEntries(spark, deltaLog, changeManifests),
        dataChange,
        commitTimestamp,
        ordered = true)
      .toLocalIterator().asScala
      .map(_.unwrap.asInstanceOf[FileAction])

  /**
   * The manifests that record the changes of the commit this tree describes:
   *  - the root, whose DATA entries record the changes to root-resident files;
   *  - the leaves the commit wrote, except those holding only EXISTING entries;
   *  - the carried leaves the commit deleted or replaced entries of, read only at those positions.
   */
  private[amt] def changeManifests: Seq[SingleManifest] = {
    val addedLeaves = leaves.filter { leaf =>
      leaf.tracking.status == Tracking.Status.Added && !holdsOnlyExistingEntries(leaf)
    }
    // An earlier commit wrote each leaf with changed positions, so by definition none is ADDED.
    val changedPositionsLeaves = leaves.filter(_.changedPositionsBitmaps.nonEmpty)
    assert(changedPositionsLeaves.forall(_.tracking.status != Tracking.Status.Added))

    val rootManifest =
      SingleManifest(manifestCommitVersion, rootFile, leafOpt = None, changedPositions = false)
    val addedLeafManifests = addedLeaves.map { leaf =>
      SingleManifest(
        manifestCommitVersion, leaf.toFileStatus(tableRoot), Some(leaf), changedPositions = false)
    }
    val changedPositionsLeafManifests = changedPositionsLeaves.map { leaf =>
      SingleManifest(
        manifestCommitVersion, leaf.toFileStatus(tableRoot), Some(leaf), changedPositions = true)
    }
    rootManifest +: (addedLeafManifests ++ changedPositionsLeafManifests)
  }

  /**
   * The AddFile and RemoveFile actions of `changeEntries`, read from [[changeManifests]], stamped
   * with the commit-level `dataChange` and `commitTimestamp`, and optionally ordered.
   */
  private def toSingleActions(
      changeEntries: DataFrame,
      dataChange: Boolean,
      commitTimestamp: Long,
      ordered: Boolean): Dataset[SingleAction] =
    manifestReader.toSingleActions(changeEntries, dataChange, commitTimestamp, ordered)

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

  /** A provider over the given tree, without prefetched root rows. */
  def apply(
      manifestCommitVersion: Long,
      checkpointAction: Checkpoint,
      leaves: Seq[DataManifestEntry],
      tableRoot: Path): AMTCheckpointProvider =
    apply(
      manifestCommitVersion,
      checkpointAction,
      new AMTRootInfo(leaves, cachedRows = None),
      tableRoot)

  def apply(
      manifestCommitVersion: Long,
      checkpointAction: Checkpoint,
      rootInfo: AMTRootInfo,
      tableRoot: Path): AMTCheckpointProvider =
    new AMTCheckpointProvider(manifestCommitVersion, checkpointAction, tableRoot, rootInfo)

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
      spark: SparkSession,
      deltaLog: DeltaLog,
      checkpoint: Checkpoint,
      manifestCommitVersion: Long): AMTCheckpointProvider = {
    val rootInfo = loadRootFromCheckpoint(spark, deltaLog, checkpoint, manifestCommitVersion)
    AMTCheckpointProvider(
      manifestCommitVersion = manifestCommitVersion,
      checkpointAction = checkpoint,
      rootInfo = rootInfo,
      tableRoot = deltaLog.dataPath)
  }

  private[delta] def loadRootFromCheckpoint(
      spark: SparkSession,
      deltaLog: DeltaLog,
      checkpoint: Checkpoint,
      manifestCommitVersion: Long): AMTRootInfo = {
    val startNanos = System.nanoTime()
    val tableRoot = deltaLog.dataPath
    val rootFile = checkpoint.contentRoot.toFileStatus(tableRoot)
    val root = {
      val persistedSchema =
        AMTSingleAction.persistedSchema(checkpoint.metaData, checkpoint.protocol)
      // Preserve null entry kinds so discovery can reject them.
      val readSchema = StructType(persistedSchema.map { field =>
        if (field.name == "content_type") field.copy(nullable = true) else field
      })
      val shouldCacheRoot = checkpoint.contentRoot.numRootEntries.exists { numRootEntries =>
        AMTUtils.shouldCacheRootRows(spark, numRootEntries)
      }
      val index =
        DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET, Array(rootFile))
      val persisted = deltaLog.loadIndex(index, readSchema)
      val collectedRows = if (shouldCacheRoot) {
        Some(persisted.queryExecution.executedPlan.executeCollect().toSeq)
      } else {
        None
      }
      val rootEntries = collectedRows.map { rows =>
        DataFrameUtils.ofRows(spark, LocalRelation(toAttributes(persisted.schema), rows))
      }.getOrElse(persisted)
      // Decode pointers and reject unsupported entry kinds without decoding DATA rows.
      val pointers = rootEntries.where(
        col("content_type").isNull ||
          col("content_type") =!= lit(AMTSingleAction.ContentType.Type.Data))
      val decodedPointers = decodeEntries(pointers, checkpoint.metaData, checkpoint.protocol)
      val pointersToCollect = if (shouldCacheRoot) {
        // Evaluate these projections locally even when the general optimizer disables this rule.
        val localPointers =
          AMTUtils.convertToLocalRelationFully(decodedPointers.queryExecution.analyzed)
        DataFrameUtils.ofRows(spark, localPointers).as[AMTSingleAction](decodedPointers.encoder)
      } else {
        decodedPointers
      }
      val leaves = pointersToCollect
        .collect().toSeq.map(_.unwrap.asInstanceOf[DataManifestEntry])
      new AMTRootInfo(leaves, collectedRows.map(persistedSchema -> _))
    }
    recordDeltaEvent(
      deltaLog,
      opType = AMTUsageLogs.CHECKPOINT_PROVIDER_INITIALIZE_FROM_CHECKPOINT_ACTION,
      data = Map(
        "durationMs" -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos),
        "manifestCommitVersion" -> manifestCommitVersion,
        "contentRootVersion" -> checkpoint.contentRoot.version,
        "checkpointVersion" -> checkpoint.version,
        "numLeaves" -> root.leaves.size.toLong,
        "contentRootSizeInBytes" -> checkpoint.contentRoot.sizeInBytes)
    )
    root
  }

  /**
   * Builds a provider from an [[AMTWriteResult]] without reading the root manifest from storage.
   */
  def fromWriteResult(
      deltaLog: DeltaLog,
      writeResult: AMTWriteResult,
      manifestCommitVersion: Long): AMTCheckpointProvider = {
    AMTCheckpointProvider(
      manifestCommitVersion = manifestCommitVersion,
      checkpointAction = writeResult.checkpoint,
      rootInfo = writeResult.rootInfo,
      tableRoot = deltaLog.dataPath)
  }

  /** Decodes persisted rows after callers have selected the entries they need. */
  private def decodeEntries(
      persisted: DataFrame,
      metadata: Metadata,
      protocol: Protocol): Dataset[AMTSingleAction] = {
    import org.apache.spark.sql.delta.implicits._
    val withPartition = AMTPartitionValues.forRead(persisted, metadata.partitionSchema)
    AMTContentStats.forRead(withPartition, metadata, protocol)
      .as[AMTSingleAction]
  }
}
