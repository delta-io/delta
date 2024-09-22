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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import org.apache.spark.sql.delta.DeltaOperations.ROW_TRACKING_BACKFILL_OPERATION_NAME
import org.apache.spark.sql.delta.RowId.RowTrackingMetadataDomain
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.DeltaSparkPlanUtils.CheckDeterministicOptions
import org.apache.spark.sql.delta.util.FileNames
import io.delta.storage.commit.UpdatedActions
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.internal.{MDC, MessageWithContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionSet, Or}
import org.apache.spark.sql.types.StructType

/**
 * A class representing different attributes of current transaction needed for conflict detection.
 *
 * @param readPredicates predicates by which files have been queried by the transaction
 * @param readFiles files that have been seen by the transaction
 * @param readWholeTable whether the whole table was read during the transaction
 * @param readAppIds appIds that have been seen by the transaction
 * @param metadata table metadata for the transaction
 * @param actions delta log actions that the transaction wants to commit
 * @param readSnapshot read [[Snapshot]] used for the transaction
 * @param commitInfo [[CommitInfo]] for the commit
 */
private[delta] case class CurrentTransactionInfo(
    val txnId: String,
    val readPredicates: Seq[DeltaTableReadPredicate],
    val readFiles: Set[AddFile],
    val readWholeTable: Boolean,
    val readAppIds: Set[String],
    val metadata: Metadata,
    val protocol: Protocol,
    val actions: Seq[Action],
    val readSnapshot: Snapshot,
    val commitInfo: Option[CommitInfo],
    val readRowIdHighWatermark: Long,
    val domainMetadata: Seq[DomainMetadata],
    val op: DeltaOperations.Operation) {

  /**
   * Final actions to commit - including the [[CommitInfo]] which should always come first so we can
   * extract it easily from a commit without having to parse an arbitrarily large file.
   *
   * TODO: We might want to cluster all non-file actions at the front, for similar reasons.
   */
  lazy val finalActionsToCommit: Seq[Action] = commitInfo ++: actions

  private var newMetadata: Option[Metadata] = None

  actions.foreach {
    case m: Metadata => newMetadata = Some(m)
    case _ => // do nothing
  }
  def getUpdatedActions(
      oldMetadata: Metadata,
      oldProtocol: Protocol): UpdatedActions = {
    new UpdatedActions(commitInfo.get, metadata, protocol, oldMetadata, oldProtocol)
  }

  /** Whether this transaction wants to make any [[Metadata]] update */
  lazy val metadataChanged: Boolean = newMetadata.nonEmpty

  /**
   * Partition schema corresponding to the read snapshot for this transaction.
   * NOTE: In conflict detection, we should be careful around whether we want to use the new schema
   * which this txn wants to update OR the old schema from the read snapshot.
   * e.g. the ConcurrentAppend check makes sure that no new files have been added concurrently
   * that this transaction should have read. So this should use the read snapshot partition schema
   * and not the new partition schema which this txn is introducing. Using the new schema can cause
   * issues.
   */
  val partitionSchemaAtReadTime: StructType = readSnapshot.metadata.partitionSchema

  // Whether this is a row tracking backfill transaction or not.
  val isRowTrackingBackfillTxn = op.name == ROW_TRACKING_BACKFILL_OPERATION_NAME

  def isConflict(winningTxn: SetTransaction): Boolean = readAppIds.contains(winningTxn.appId)
}

/**
 * Summary of the Winning commit against which we want to check the conflict
 * @param actions - delta log actions committed by the winning commit
 * @param commitVersion - winning commit version
 */
private[delta] class WinningCommitSummary(val actions: Seq[Action], val commitVersion: Long) {

  val metadataUpdates: Seq[Metadata] = actions.collect { case a: Metadata => a }
  val appLevelTransactions: Seq[SetTransaction] = actions.collect { case a: SetTransaction => a }
  val protocol: Option[Protocol] = actions.collectFirst { case a: Protocol => a }
  val commitInfo: Option[CommitInfo] = actions.collectFirst { case a: CommitInfo => a }.map(
    ci => ci.copy(version = Some(commitVersion)))
  // Whether this is a row tracking backfill transaction or not.
  val isRowTrackingBackfillTxn =
    commitInfo.exists(_.operation == ROW_TRACKING_BACKFILL_OPERATION_NAME)
  val removedFiles: Seq[RemoveFile] = actions.collect { case a: RemoveFile => a }
  val addedFiles: Seq[AddFile] = actions.collect { case a: AddFile => a }
  // This is used in resolveRowTrackingBackfillConflicts.
  lazy val addedFilePathToActionMap: Map[String, AddFile] =
    addedFiles.map(af => (af.path, af)).toMap
  val isBlindAppendOption: Option[Boolean] = commitInfo.flatMap(_.isBlindAppend)
  val blindAppendAddedFiles: Seq[AddFile] = if (isBlindAppendOption.getOrElse(false)) {
    addedFiles
  } else {
    Seq()
  }
  val changedDataAddedFiles: Seq[AddFile] = if (isBlindAppendOption.getOrElse(false)) {
    Seq()
  } else {
    addedFiles
  }
  val onlyAddFiles: Boolean = actions.collect { case f: FileAction => f }
    .forall(_.isInstanceOf[AddFile])

  // This indicates this commit contains metadata action that is solely for the purpose for
  // updating IDENTITY high water marks. This is used by [[ConflictChecker]] to avoid certain
  // conflict in [[checkNoMetadataUpdates]].
  val identityOnlyMetadataUpdate = DeltaCommitTag
    .getTagValueFromCommitInfo(commitInfo, DeltaSourceUtils.IDENTITY_COMMITINFO_TAG)
    .exists(_.toBoolean)
}

private[delta] class ConflictChecker(
    spark: SparkSession,
    initialCurrentTransactionInfo: CurrentTransactionInfo,
    winningCommitFileStatus: FileStatus,
    isolationLevel: IsolationLevel) extends DeltaLogging with ConflictCheckerPredicateElimination {

  protected val winningCommitVersion = FileNames.deltaVersion(winningCommitFileStatus)
  protected val startTimeMs = System.currentTimeMillis()
  protected val timingStats = mutable.HashMap[String, Long]()
  protected val deltaLog = initialCurrentTransactionInfo.readSnapshot.deltaLog

  protected var currentTransactionInfo: CurrentTransactionInfo = initialCurrentTransactionInfo

  protected lazy val winningCommitSummary: WinningCommitSummary = createWinningCommitSummary()

  protected def recordSkippedPhase(phase: String): Unit = timingStats += phase -> 0

  /**
   * This function checks conflict of the `initialCurrentTransactionInfo` against the
   * `winningCommitVersion` and returns an updated [[CurrentTransactionInfo]] that represents
   * the transaction as if it had started while reading the `winningCommitVersion`.
   */
  def checkConflicts(): CurrentTransactionInfo = {
    // Check early the protocol and metadata compatibility that is required for subsequent
    // file-level checks.
    checkProtocolCompatibility()
    checkNoMetadataUpdates()
    checkIfDomainMetadataConflict()

    // Perform cheap check for transaction dependencies before we start checks files.
    checkForUpdatedApplicationTransactionIdsThatCurrentTxnDependsOn()

    resolveRowTrackingBackfillConflicts()
    // Row Tracking reconciliation. We perform this before the file checks to ensure that
    // no files have duplicate row IDs and avoid interacting with files that don't comply with
    // the protocol.
    reassignOverlappingRowIds()
    reassignRowCommitVersions()

    // Update the table version in newly added type widening metadata.
    updateTypeWideningMetadata()

    // Data file checks.
    checkForAddedFilesThatShouldHaveBeenReadByCurrentTxn()
    checkForDeletedFilesAgainstCurrentTxnReadFiles()
    checkForDeletedFilesAgainstCurrentTxnDeletedFiles()
    resolveTimestampOrderingConflicts()

    logMetrics()
    currentTransactionInfo
  }

  /**
   * Initializes [[WinningCommitSummary]] for the already committed
   * transaction (winning transaction).
   */
  protected def createWinningCommitSummary(): WinningCommitSummary = {
    recordTime("initialize-old-commit") {
      val winningCommitActions = deltaLog.store.read(
        winningCommitFileStatus,
        deltaLog.newDeltaHadoopConf()
      ).map(Action.fromJson)
      new WinningCommitSummary(winningCommitActions, winningCommitVersion)
    }
  }

  /**
   * Asserts that the client is up to date with the protocol and is allowed to read and write
   * against the protocol set by the committed transaction.
   */
  protected def checkProtocolCompatibility(): Unit = {
    if (winningCommitSummary.protocol.nonEmpty) {
      winningCommitSummary.protocol.foreach { p =>
        deltaLog.protocolRead(p)
        deltaLog.protocolWrite(p)
        currentTransactionInfo = currentTransactionInfo.copy(protocol = p)
      }
      if (currentTransactionInfo.actions.exists(_.isInstanceOf[Protocol])) {
        throw DeltaErrors.protocolChangedException(winningCommitSummary.commitInfo)
      }
      // When a protocol downgrade occurs all other interleaved txns abort. Note, that in the
      // opposite scenario, when the current transaction is the protocol downgrade, we resolve
      // the conflict and proceed with the downgrade. This is because a protocol downgrade would
      // be hard to succeed in concurrent workloads. On the other hand, a protocol downgrade is
      // a rare event and thus not that disruptive if other concurrent transactions fail.
      val winningProtocol = winningCommitSummary.protocol.get
      val readProtocol = currentTransactionInfo.readSnapshot.protocol
      val isWinnerDroppingFeatures = TableFeature.isProtocolRemovingExplicitFeatures(
        newProtocol = winningProtocol,
        oldProtocol = readProtocol)
      if (isWinnerDroppingFeatures) {
        throw DeltaErrors.protocolChangedException(winningCommitSummary.commitInfo)
      }
    }
    // When the winning transaction does not change the protocol but the losing txn is
    // a protocol downgrade, we re-validate the invariants of the removed feature.
    // TODO: only revalidate against the snapshot of the last interleaved txn.
    val currentProtocol = currentTransactionInfo.protocol
    val readProtocol = currentTransactionInfo.readSnapshot.protocol
    if (TableFeature.isProtocolRemovingExplicitFeatures(currentProtocol, readProtocol)) {
      val winningSnapshot = deltaLog.getSnapshotAt(winningCommitSummary.commitVersion)
      val isDowngradeCommitValid = TableFeature.validateFeatureRemovalAtSnapshot(
        newProtocol = currentProtocol,
        oldProtocol = readProtocol,
        snapshot = winningSnapshot)
      if (!isDowngradeCommitValid) {
        throw DeltaErrors.dropTableFeatureConflictRevalidationFailed(
          winningCommitSummary.commitInfo)
      }
    }
  }

  /**
   * RowTrackingBackfill (or backfill for short for this function) is a special operation that
   * materializes and recommits all existing files in table using one or several commits to ensure
   * that every AddFile has a base row ID and a default row commit version. When enabling
   * row tracking on an existing table, the following occurs:
   *    1. (If necessary) Protocol upgrade + Table Feature Support is added
   *    2. RowTrackingBackfill commit(s)
   *    3. Table property and metadata are updated.
   * RowTrackingBackfill does not do any data change. It doesn't matter whether a file is
   * recommitted after the table feature support from Backfill or some other concurrent transaction;
   * every AddFile just needs to have a base row ID and a default row commit version somehow.
   * However, correctness issues can arise if we don't do the checks in this method.
   *
   * Check that RowTrackingBackfill is not resurrecting files that were removed concurrently and
   * that an AddFile and its corresponding RemoveFile have the same base row ID and
   * default row commit version. To do this, we:
   *    1. remove AddFile's from a backfill commit if an AddFile or a RemoveFile with the same path
   *       was added in the winning concurrent transactions. Files in a winning transaction can be
   *       removed from backfill because they were already re-committed.
   *    2. copy over base row IDs and default row commit versions if the current transaction re-adds
   *       or delete an AddFile with the same path as an Addfile from a winning backfill commit.
   */
  private def resolveRowTrackingBackfillConflicts(): Unit = {
    // If row tracking is not supported, there can be no backfill commit.
    if (!RowTracking.isSupported(currentTransactionInfo.protocol)) {
      assert(!currentTransactionInfo.isRowTrackingBackfillTxn)
      assert(!winningCommitSummary.isRowTrackingBackfillTxn)
      return
    }

    val timerPhaseName = "checked-row-tracking-backfill"
    if (currentTransactionInfo.isRowTrackingBackfillTxn) {
      recordTime(timerPhaseName) {
        // Any winning commit seen by backfill must have row IDs and row commit versions, because
        // `reassignOverlappingRowIds` will add a base row ID and `reassignRowCommitVersions`
        // will add a default row commit versions to all files. So we don't need
        // Backfill to commit the same file again.
        val filePathsToRemoveFromBackfill = winningCommitSummary.actions.collect {
          case a: AddFile => a.path
          case r: RemoveFile => r.path
        }.toSet

        // Remove files from this Backfill commit if they were removed or re-committed by
        // a concurrent winning txn.
        if (filePathsToRemoveFromBackfill.nonEmpty) {
          // We keep the Row Tracking high-water mark action here but it might
          // be outdated since the winning commit could have increased the high-water mark.
          // We will reassign the current transaction's high water-mark if that is
          // the case, in `reassignOverlappingRowIds` which is called after
          // `resolveRowTrackingBackfillConflicts` in `checkConflicts`.
          val newActions = currentTransactionInfo.actions.filterNot {
            case a: AddFile => filePathsToRemoveFromBackfill.contains(a.path)
            case d: DomainMetadata if RowTrackingMetadataDomain.isSameDomain(d) => false
            case _ => throw new IllegalStateException(
              "RowTrackingBackfill commit has an unexpected action")
          }

          val newReadFiles = currentTransactionInfo.readFiles.filterNot(
            a => filePathsToRemoveFromBackfill.contains(a.path))

          currentTransactionInfo = currentTransactionInfo.copy(
            actions = newActions, readFiles = newReadFiles)
        }
      }
    }

    if (winningCommitSummary.isRowTrackingBackfillTxn) {
      recordTime(timerPhaseName) {
        val backfillActionMap = winningCommitSummary.addedFilePathToActionMap
        // Copy over the base row ID and default row commit version assigned so that the AddFiles
        // and RemoveFiles have matching base row ID and default row commit version.
        // If an AddFile is re-committed, it should have the same base row ID and
        // default row commit version as the one assigned by Backfill.
        val newActions = currentTransactionInfo.actions.map {
          case a: AddFile if backfillActionMap.contains(a.path) =>
            val backfillAction = backfillActionMap(a.path)
            a.copy(baseRowId = backfillAction.baseRowId,
              defaultRowCommitVersion = backfillAction.defaultRowCommitVersion)
          case r: RemoveFile if backfillActionMap.contains(r.path) =>
            val backfillAction = backfillActionMap(r.path)
            r.copy(baseRowId = backfillAction.baseRowId,
              defaultRowCommitVersion = backfillAction.defaultRowCommitVersion)
          case a => a
        }
        currentTransactionInfo = currentTransactionInfo.copy(actions = newActions)
      }
    }
  }

  /**
   * If the winning commit only does row tracking enablement (i.e. set the table property to
   * true and assigns materialized row tracking column names), we can safely allow the metadata
   * update not to fail the current txn if we copy over the table property, materialized column
   * name assignments and correctly tag the current commit as not preserving row tracking data. It
   * is not possible to preserve row tracking data prior to the table property being set to true
   * since there is no guarantee of row tracking data being available on all rows.
   */
  protected def tryResolveRowTrackingEnablementOnlyMetadataUpdateConflict(): Boolean = {
    if (RowTracking.canResolveMetadataUpdateConflict(
        currentTransactionInfo, winningCommitSummary)) {
      currentTransactionInfo = RowTracking.resolveRowTrackingEnablementOnlyMetadataUpdateConflict(
        currentTransactionInfo, winningCommitSummary)
      return true
    }
    false
  }

  // scalastyle:off line.size.limit
  /**
   * Check if the committed transaction has changed metadata.
   *
   * We want to deal with (and optimize for) the case where the winning commit's metadata update is
   * solely for updating IDENTITY high water marks. In addition, we want to allow a metadata update
   * that only sets the table property for row tracking enablement to true not to fail concurrent
   * transactions if the current transaction does not do a metadata update.
   *
   * The conflict matrix is as follows:
   *
   * |                                               | Winning Metadata (id) | Winning Metadata Row Tracking Enablement Only | Winning Metadata (other) | Winning No Metadata |
   * | --------------------------------------------- | --------------------- | --------------------------------------------- | ------------------------ | ------------------- |
   * | Current Metadata (id)                         | Conflict              | Conflict (3)                                  | Conflict                 | No conflict         |
   * | Current Metadata Row Tracking Enablement Only | Conflict (1)          | Conflict (3)                                  | Conflict                 | No conflict         |
   * | Current Metadata (other)                      | Conflict (1)          | Conflict (3)                                  | Conflict                 | No conflict         |
   * | Current No Metadata                           | No conflict (2)       | No conflict (4)                               | Conflict                 | No conflict         |
   *
   * The differences in cases (1), (2), (3), and (4) are:
   * (1) This is a case we could have done something to avoid conflict, e.g., current transaction
   * adds a column, while winning transaction does blind append that generates IDENTITY values. But
   * it's not a common case and the change to avoid conflict is non-trivial (we have to somehow
   * merge the metadata from winning txn and current txn). We decide to not do that and let it
   * conflict.
   * (2) This is a case that is more common (e.g., current = delete/update, winning = update high
   * water mark) and we will not let it conflict here. Note that it might still cause conflict in
   * other conflict checks.
   * (3) If the current txn changes the metadata too, we will fail the current txn. While it is
   * possible to copy over the metadata information, this scenario is unlikely to happen in practice
   * and properly handling this for the many edge case (e.g current txn sets the table property
   * to false) is risky.
   * (4) In a row tracking enablement only metadata update, the only difference with the previous
   * metadata are the row tracking table property and materialized column names. These metadata
   * information only affect the preservation of row tracking. If we copy over the new metadata
   * configurations and mark the current txn as not preserving row tracking, then the current txn
   * is respecting the metadata update and does not need to fail.
   *
   */
  // scalastyle:on line.size.limit
  protected def checkNoMetadataUpdates(): Unit = {
    // If winning commit does not contain metadata update, no conflict.
    if (winningCommitSummary.metadataUpdates.isEmpty) return

    if (tryResolveRowTrackingEnablementOnlyMetadataUpdateConflict()) {
      return
    }

    // The only case in the remaining cases that we will not conflict is winning commit is
    // identity only metadata update and current commit has no metadata update.
    val tolerateIdentityOnlyMetadataUpdate = winningCommitSummary.identityOnlyMetadataUpdate &&
      !currentTransactionInfo.metadataChanged

    if (!tolerateIdentityOnlyMetadataUpdate) {
      if (winningCommitSummary.identityOnlyMetadataUpdate) {
        IdentityColumn.logTransactionAbort(deltaLog)
      }
      throw DeltaErrors.metadataChangedException(winningCommitSummary.commitInfo)
    }
  }

  /**
   * Filters the [[files]] list with the partition predicates of the current transaction
   * and returns the first file that is matching.
   */
  protected def getFirstFileMatchingPartitionPredicates(files: Seq[AddFile]): Option[AddFile] = {
    // Blind appends do not read the table.
    if (currentTransactionInfo.commitInfo.flatMap(_.isBlindAppend).getOrElse(false)) {
      assert(currentTransactionInfo.readPredicates.isEmpty)
      return None
    }

    // There is no reason to filter files if the table is not partitioned.
    if (currentTransactionInfo.readWholeTable ||
        currentTransactionInfo.readSnapshot.metadata.partitionColumns.isEmpty) {
      return files.headOption
    }

    import org.apache.spark.sql.delta.implicits._
    val filesDf = files.toDF(spark)

    spark.conf.get(DeltaSQLConf.DELTA_CONFLICT_DETECTION_WIDEN_NONDETERMINISTIC_PREDICATES) match {
      case DeltaSQLConf.NonDeterministicPredicateWidening.OFF =>
        getFirstFileMatchingPartitionPredicatesInternal(
          filesDf, shouldWidenNonDeterministicPredicates = false, shouldWidenAllUdf = false)
      case wideningMode =>
        val fileWithWidening = getFirstFileMatchingPartitionPredicatesInternal(
          filesDf, shouldWidenNonDeterministicPredicates = true, shouldWidenAllUdf = true)

        fileWithWidening.flatMap { fileWithWidening =>
          val fileWithoutWidening =
            getFirstFileMatchingPartitionPredicatesInternal(
              filesDf, shouldWidenNonDeterministicPredicates = false, shouldWidenAllUdf = false)
          if (fileWithoutWidening.isEmpty) {
            // Conflict due to widening of non-deterministic predicate.
            recordDeltaEvent(deltaLog,
              opType = "delta.conflictDetection.partitionLevelConcurrency." +
                "additionalConflictDueToWideningOfNonDeterministicPredicate",
              data = Map(
                "wideningMode" -> wideningMode,
                "predicate" ->
                  currentTransactionInfo.readPredicates.map(_.partitionPredicate.toString),
                "deterministicUDFs" -> containsDeterministicUDF(
                  currentTransactionInfo.readPredicates, partitionedOnly = true))
            )
          }
          if (wideningMode == DeltaSQLConf.NonDeterministicPredicateWidening.ON) {
            Some(fileWithWidening)
          } else {
            fileWithoutWidening
          }
        }
    }
  }

  private def getFirstFileMatchingPartitionPredicatesInternal(
      filesDf: DataFrame,
      shouldWidenNonDeterministicPredicates: Boolean,
      shouldWidenAllUdf: Boolean): Option[AddFile] = {

    def rewritePredicateFn(
        predicate: Expression,
        shouldRewriteFilter: Boolean): DeltaTableReadPredicate = {
      val rewrittenPredicate = if (shouldWidenNonDeterministicPredicates) {
        val checkDeterministicOptions =
          CheckDeterministicOptions(allowDeterministicUdf = !shouldWidenAllUdf)
        eliminateNonDeterministicPredicates(Seq(predicate), checkDeterministicOptions).newPredicates
      } else {
        Seq(predicate)
      }
      DeltaTableReadPredicate(
        partitionPredicates = rewrittenPredicate,
        shouldRewriteFilter = shouldRewriteFilter)
    }

    // we need to canonicalize the partition predicates per each group of rewrites vs. nonRewrites
    val canonicalPredicates = currentTransactionInfo.readPredicates
      .partition(_.shouldRewriteFilter) match {
        case (rewrites, nonRewrites) =>
          val canonicalRewrites =
            ExpressionSet(rewrites.map(_.partitionPredicate)).map(
              predicate => rewritePredicateFn(predicate, shouldRewriteFilter = true))
          val canonicalNonRewrites =
            ExpressionSet(nonRewrites.map(_.partitionPredicate)).map(
              predicate => rewritePredicateFn(predicate, shouldRewriteFilter = false))
          canonicalRewrites ++ canonicalNonRewrites
      }

    import org.apache.spark.sql.delta.implicits._
    val filesMatchingPartitionPredicates = canonicalPredicates.iterator
      .flatMap { readPredicate =>
        DeltaLog.filterFileList(
          partitionSchema = currentTransactionInfo.partitionSchemaAtReadTime,
          files = filesDf,
          partitionFilters = readPredicate.partitionPredicates,
          shouldRewritePartitionFilters = readPredicate.shouldRewriteFilter
        ).as[AddFile].head(1).headOption
      }.take(1).toArray

    filesMatchingPartitionPredicates.headOption
  }

  /**
   * RowTrackingBackfill does not do any data change. If backfill is the winning commit, the
   * current transaction does not need to read its AddFiles -- the exact same AddFiles have
   * already been read. If the current commit is backfill, it doesn't need to read the AddFiles
   * added by the winning transaction. Any winning transaction seen by backfill will commit base
   * row IDs and default row commit versions, since backfill is only done after table feature
   * support is added. Removing duplicate AddFiles is handled in
   * [[resolveRowTrackingBackfillConflicts]].
   */
  protected def skipCheckedAppendsIfExistsRowTrackingBackfillTransaction(): Boolean = {
    if (winningCommitSummary.isRowTrackingBackfillTxn ||
        currentTransactionInfo.isRowTrackingBackfillTxn) {
      recordSkippedPhase("checked-appends")
      return true
    }
    false
  }

  /**
   * Check if the new files added by the already committed transactions should have been read by
   * the current transaction.
   */
  protected def checkForAddedFilesThatShouldHaveBeenReadByCurrentTxn(): Unit = {
    if (skipCheckedAppendsIfExistsRowTrackingBackfillTransaction()) {
      return
    }

    recordTime("checked-appends") {
      // Fail if new files have been added that the txn should have read.
      val addedFilesToCheckForConflicts = isolationLevel match {
        case WriteSerializable if !currentTransactionInfo.metadataChanged =>
          winningCommitSummary.changedDataAddedFiles // don't conflict with blind appends
        case Serializable | WriteSerializable =>
          winningCommitSummary.changedDataAddedFiles ++ winningCommitSummary.blindAppendAddedFiles
        case SnapshotIsolation =>
          Seq.empty
      }

      val fileMatchingPartitionReadPredicates =
        getFirstFileMatchingPartitionPredicates(addedFilesToCheckForConflicts)

      if (fileMatchingPartitionReadPredicates.nonEmpty) {
        val isWriteSerializable = isolationLevel == WriteSerializable

        val retryMsg = if (isWriteSerializable && winningCommitSummary.onlyAddFiles &&
          winningCommitSummary.isBlindAppendOption.isEmpty) {
          // The transaction was made by an older version which did not set `isBlindAppend` flag
          // So even if it looks like an append, we don't know for sure if it was a blind append
          // or not. So we suggest them to upgrade all there workloads to latest version.
          Some(
            "Upgrading all your concurrent writers to use the latest Delta Lake may " +
              "avoid this error. Please upgrade and then retry this operation again.")
        } else None
        throw DeltaErrors.concurrentAppendException(
          winningCommitSummary.commitInfo,
          getPrettyPartitionMessage(fileMatchingPartitionReadPredicates.get.partitionValues),
          retryMsg)
      }
    }
  }

  /**
   * Check if [[RemoveFile]] actions added by already committed transactions conflicts with files
   * read by the current transaction.
   */
  protected def checkForDeletedFilesAgainstCurrentTxnReadFiles(): Unit = {
    recordTime("checked-deletes") {
      // Fail if files have been deleted that the txn read.
      val readFilePaths = currentTransactionInfo.readFiles.map(
        f => f.path -> f.partitionValues).toMap
      val deleteReadOverlap = winningCommitSummary.removedFiles
        .find(r => readFilePaths.contains(r.path))
      if (deleteReadOverlap.nonEmpty) {
        val filePath = deleteReadOverlap.get.path
        val partition = getPrettyPartitionMessage(readFilePaths(filePath))
        throw DeltaErrors.concurrentDeleteReadException(
          winningCommitSummary.commitInfo, s"$filePath in $partition")
      }
      if (winningCommitSummary.removedFiles.nonEmpty && currentTransactionInfo.readWholeTable) {
        val filePath = winningCommitSummary.removedFiles.head.path
        throw DeltaErrors.concurrentDeleteReadException(
          winningCommitSummary.commitInfo, s"$filePath")
      }
    }
  }

  /**
   * Check if [[RemoveFile]] actions added by already committed transactions conflicts with
   * [[RemoveFile]] actions this transaction is trying to add.
   */
  protected def checkForDeletedFilesAgainstCurrentTxnDeletedFiles(): Unit = {
    recordTime("checked-2x-deletes") {
      // Fail if a file is deleted twice.
      val txnDeletes = currentTransactionInfo.actions
        .collect { case r: RemoveFile => r }
        .map(_.path).toSet
      val deleteOverlap = winningCommitSummary.removedFiles.map(_.path).toSet intersect txnDeletes
      if (deleteOverlap.nonEmpty) {
        throw DeltaErrors.concurrentDeleteDeleteException(
          winningCommitSummary.commitInfo, deleteOverlap.head)
      }
    }
  }

  /**
   * Checks if the winning transaction corresponds to some AppId on which current transaction
   * also depends.
   */
  protected def checkForUpdatedApplicationTransactionIdsThatCurrentTxnDependsOn(): Unit = {
    // Fail if the appIds seen by the current transaction has been updated by the winning
    // transaction i.e. the winning transaction have [[SetTransaction]] corresponding to
    // some appId on which current transaction depends on. Example - This can happen when
    // multiple instances of the same streaming query are running at the same time.
    if (winningCommitSummary.appLevelTransactions.exists(currentTransactionInfo.isConflict(_))) {
      throw DeltaErrors.concurrentTransactionException(winningCommitSummary.commitInfo)
    }
  }

  /**
   * Checks [[DomainMetadata]] to capture whether the current transaction conflicts with the
   * winning transaction at any domain.
   *     1. Accept the current transaction if its set of metadata domains do not overlap with the
   *        winning transaction's set of metadata domains.
   *     2. Otherwise, fail the current transaction unless each conflicting domain is associated
   *        with a table feature that defines a domain-specific way of resolving the conflict.
   */
  private def checkIfDomainMetadataConflict(): Unit = {
    if (!DomainMetadataUtils.domainMetadataSupported(currentTransactionInfo.protocol)) {
      return
    }
    val winningDomainMetadataMap =
      DomainMetadataUtils.extractDomainMetadatasMap(winningCommitSummary.actions)

    /**
     * Any new well-known domains that need custom conflict resolution need to add new cases in
     * below case match clause. E.g.
     * case MonotonicCounter(value), Some(MonotonicCounter(conflictingValue)) =>
     *   MonotonicCounter(Math.max(value, conflictingValue))
     */
    def resolveConflict(domainMetadataFromCurrentTransaction: DomainMetadata): DomainMetadata =
      (domainMetadataFromCurrentTransaction,
        winningDomainMetadataMap.get(domainMetadataFromCurrentTransaction.domain)) match {
        // No-conflict case.
        case (domain, None) => domain
        case (domain, _) if RowTrackingMetadataDomain.isSameDomain(domain) => domain
        case (_, Some(_)) =>
          // Any conflict not specifically handled by a previous case must fail the transaction.
          throw new io.delta.exceptions.ConcurrentTransactionException(
            s"A conflicting metadata domain ${domainMetadataFromCurrentTransaction.domain} is " +
              "added.")
      }

    val mergedDomainMetadata = mutable.Buffer.empty[DomainMetadata]
    // Resolve physical [[DomainMetadata]] conflicts (fail on logical conflict).
    val updatedActions: Seq[Action] = currentTransactionInfo.actions.map {
      case domainMetadata: DomainMetadata =>
        val mergedAction = resolveConflict(domainMetadata)
        mergedDomainMetadata += mergedAction
        mergedAction
      case other => other
    }

    currentTransactionInfo = currentTransactionInfo.copy(
      domainMetadata = mergedDomainMetadata.toSeq,
      actions = updatedActions)
  }

  /**
   * Metadata is recorded in the table schema on type changes. This includes the table version that
   * the change was made in, which needs to be updated when there's a conflict.
   */
  private def updateTypeWideningMetadata(): Unit = {
    if (!TypeWidening.isEnabled(currentTransactionInfo.protocol, currentTransactionInfo.metadata)) {
      return
    }
    val newActions = currentTransactionInfo.actions.map {
      case metadata: Metadata =>
        val updatedSchema = TypeWideningMetadata.updateTypeChangeVersion(
          schema = metadata.schema,
          fromVersion = winningCommitVersion,
          toVersion = winningCommitVersion + 1L)
        metadata.copy(schemaString = updatedSchema.json)
      case a => a
    }
    currentTransactionInfo = currentTransactionInfo.copy(actions = newActions)
  }

  /**
   * Checks whether the Row IDs assigned by the current transaction overlap with the Row IDs
   * assigned by the winning transaction. I.e. this function checks whether both the winning and the
   * current transaction assigned new Row IDs. If this the case, then this check assigns new Row IDs
   * to the new files added by the current transaction so that they no longer overlap.
   */
  private def reassignOverlappingRowIds(): Unit = {
    // The current transaction should only assign Row Ids if they are supported.
    if (!RowId.isSupported(currentTransactionInfo.protocol)) return

    val readHighWaterMark = currentTransactionInfo.readRowIdHighWatermark

    // The winning transaction might have bumped the high water mark or not in case it did
    // not add new files to the table.
    val winningHighWaterMark = winningCommitSummary.actions.collectFirst {
      case RowTrackingMetadataDomain(domain) => domain.rowIdHighWaterMark
    }.getOrElse(readHighWaterMark)

    var highWaterMark = winningHighWaterMark
    val actionsWithReassignedRowIds = currentTransactionInfo.actions.flatMap {
      // We should only set missing row IDs and update the row IDs that were assigned by this
      // transaction, and not the row IDs that were assigned by an earlier transaction and merely
      // copied over to a new AddFile as part of this transaction. I.e., we should only update the
      // base row IDs that are larger than the read high watermark.
      case a: AddFile if !a.baseRowId.exists(_ <= readHighWaterMark) =>
        val newBaseRowId = highWaterMark + 1L
        highWaterMark += a.numPhysicalRecords.getOrElse {
          throw DeltaErrors.rowIdAssignmentWithoutStats
        }
        Some(a.copy(baseRowId = Some(newBaseRowId)))
      // The row ID high water mark will be replaced if it exists.
      case d: DomainMetadata if RowTrackingMetadataDomain.isSameDomain(d) => None
      case a => Some(a)
    }
    currentTransactionInfo = currentTransactionInfo.copy(
      // Add row ID high water mark at the front for faster retrieval.
      actions = RowTrackingMetadataDomain(highWaterMark).toDomainMetadata +:
        actionsWithReassignedRowIds,
      readRowIdHighWatermark = winningHighWaterMark)
  }

  /**
   * Reassigns default row commit versions to correctly handle the winning transaction.
   * Concretely:
   *  1. Reassigns all default row commit versions (of AddFiles in the current transaction) equal to
   *     the version of the winning transaction to the next commit version.
   *  2. Assigns all unassigned default row commit versions that do not have one assigned yet
   *     to handle the row tracking feature being enabled by the winning transaction.
   */
  private def reassignRowCommitVersions(): Unit = {
    if (!RowTracking.isSupported(currentTransactionInfo.protocol) &&
      // Type widening relies on default row commit versions to be set.
      !TypeWidening.isSupported(currentTransactionInfo.protocol)) {
      return
    }

    val newActions = currentTransactionInfo.actions.map {
      case a: AddFile if a.defaultRowCommitVersion.contains(winningCommitVersion) =>
        a.copy(defaultRowCommitVersion = Some(winningCommitVersion + 1L))

      case a: AddFile if a.defaultRowCommitVersion.isEmpty =>
        // A concurrent transaction has turned on support for Row Tracking.
        a.copy(defaultRowCommitVersion = Some(winningCommitVersion + 1L))

      case a => a
    }

    currentTransactionInfo = currentTransactionInfo.copy(actions = newActions)
  }

  /**
   * Adjust the current transaction's commit timestamp to account for the winning
   * transaction's commit timestamp. If this transaction newly enabled ICT, also update
   * the table properties to reflect the adjusted enablement version and timestamp.
   */
  private def resolveTimestampOrderingConflicts(): Unit = {
    if (!DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(currentTransactionInfo.metadata)) {
      return
    }

    val winningCommitTimestamp =
      if (InCommitTimestampUtils.didCurrentTransactionEnableICT(
              currentTransactionInfo.metadata, currentTransactionInfo.readSnapshot)) {
        // Since the current transaction enabled inCommitTimestamps, we should use the file
        // timestamp from the winning transaction as its commit timestamp.
        winningCommitFileStatus.getModificationTime
    } else {
      // Get the inCommitTimestamp from the winning transaction.
      CommitInfo.getRequiredInCommitTimestamp(
        winningCommitSummary.commitInfo, winningCommitVersion.toString)
    }
    val currentTransactionTimestamp = CommitInfo.getRequiredInCommitTimestamp(
      currentTransactionInfo.commitInfo, "NEW_COMMIT")
    // getRequiredInCommitTimestamp will throw an exception if commitInfo is None.
    val currentTransactionCommitInfo = currentTransactionInfo.commitInfo.get
    val updatedCommitTimestamp = Math.max(currentTransactionTimestamp, winningCommitTimestamp + 1)
    val updatedCommitInfo =
      currentTransactionCommitInfo.copy(inCommitTimestamp = Some(updatedCommitTimestamp))
    currentTransactionInfo = currentTransactionInfo.copy(commitInfo = Some(updatedCommitInfo))
    val nextAvailableVersion = winningCommitVersion + 1L
    val updatedMetadata =
      InCommitTimestampUtils.getUpdatedMetadataWithICTEnablementInfo(
        updatedCommitTimestamp,
        currentTransactionInfo.readSnapshot,
        currentTransactionInfo.metadata,
        nextAvailableVersion)
    updatedMetadata.foreach { updatedMetadata =>
      currentTransactionInfo = currentTransactionInfo.copy(
        metadata = updatedMetadata,
        actions = currentTransactionInfo.actions.map {
          case _: Metadata => updatedMetadata
          case other => other
        }
      )
    }
  }

  /** A helper function for pretty printing a specific partition directory. */
  protected def getPrettyPartitionMessage(partitionValues: Map[String, String]): String = {
    val partitionColumns = currentTransactionInfo.partitionSchemaAtReadTime
    if (partitionColumns.isEmpty) {
      "the root of the table"
    } else {
      val partition = partitionColumns.map { field =>
        s"${field.name}=${partitionValues(DeltaColumnMapping.getPhysicalName(field))}"
      }.mkString("[", ", ", "]")
      s"partition ${partition}"
    }
  }

  protected def recordTime[T](phase: String)(f: => T): T = {
    val startTimeNs = System.nanoTime()
    val ret = f
    val timeTakenMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)
    timingStats += phase -> timeTakenMs
    ret
  }

  protected def logMetrics(): Unit = {
    val totalTimeTakenMs = System.currentTimeMillis() - startTimeMs
    val timingStr = timingStats.keys.toSeq.sorted.map(k => s"$k=${timingStats(k)}").mkString(",")
    logInfo(log"[" + logPrefix + log"] Timing stats against " +
      log"${MDC(DeltaLogKeys.VERSION, winningCommitVersion)} " +
      log"[${MDC(DeltaLogKeys.TIME_STATS, timingStr)}, totalTimeTakenMs: " +
      log"${MDC(DeltaLogKeys.TIME_MS, totalTimeTakenMs)}]")
  }

  protected lazy val logPrefix: MessageWithContext = {
    def truncate(uuid: String): String = uuid.split("-").head
    log"[tableId=${MDC(DeltaLogKeys.TABLE_ID,
      truncate(initialCurrentTransactionInfo.readSnapshot.metadata.id))}," +
    log"txnId=${MDC(DeltaLogKeys.TXN_ID, truncate(initialCurrentTransactionInfo.txnId))}] "
  }
}
