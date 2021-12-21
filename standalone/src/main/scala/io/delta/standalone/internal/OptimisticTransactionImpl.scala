/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.internal

import java.nio.file.FileAlreadyExistsException
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path

import io.delta.standalone.{CommitResult, DeltaScan, NAME, Operation, OptimisticTransaction, VERSION}
import io.delta.standalone.actions.{Action => ActionJ, Metadata => MetadataJ}
import io.delta.standalone.exceptions.DeltaStandaloneException
import io.delta.standalone.expressions.{Expression, Literal}
import io.delta.standalone.types.StructType

import io.delta.standalone.internal.actions.{Action, AddFile, CommitInfo, FileAction, Metadata, Protocol, RemoveFile}
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.logging.Logging
import io.delta.standalone.internal.util.{ConversionUtils, FileNames, SchemaMergingUtils, SchemaUtils}
import io.delta.standalone.internal.util.DeltaFileOperations

private[internal] class OptimisticTransactionImpl(
    deltaLog: DeltaLogImpl,
    snapshot: SnapshotImpl) extends OptimisticTransaction with Logging {
  val DELTA_MAX_RETRY_COMMIT_ATTEMPTS = 10000000

  /** Used for logging */
  private val txnId = UUID.randomUUID().toString

  /** Tracks the appIds that have been seen by this transaction. */
  private val readTxn = new ArrayBuffer[String]

  /**
   * Tracks the data that could have been seen by recording the partition
   * predicates by which files have been queried by this transaction.
   */
  private val readPredicates = new ArrayBuffer[Expression]

  /** Tracks specific files that have been seen by this transaction. */
  private val readFiles = new scala.collection.mutable.HashSet[AddFile]

  /** Whether the whole table was read during the transaction. */
  private var readTheWholeTable = false

  /** Tracks if this transaction has already committed. */
  private var committed = false

  /** Stores the updated metadata (if any) that will result from this txn. */
  private var newMetadata: Option[Metadata] = None

  /** Stores the updated protocol (if any) that will result from this txn. */
  private var newProtocol: Option[Protocol] = None

  /** Whether this transaction is creating a new table. */
  private var isCreatingNewTable: Boolean = false

  /**
   * Tracks the start time since we started trying to write a particular commit.
   * Used for logging duration of retried transactions.
   */
  private var commitAttemptStartTime: Long = _

  /** The protocol of the snapshot that this transaction is reading at. */
  def protocol: Protocol = newProtocol.getOrElse(snapshot.protocolScala)

  /**
   * Returns the metadata for this transaction. The metadata refers to the metadata of the snapshot
   * at the transaction's read version unless updated during the transaction.
   */
  def metadataScala: Metadata = newMetadata.getOrElse(snapshot.metadataScala)

  /** The version that this transaction is reading from. */
  private def readVersion: Long = snapshot.version

  ///////////////////////////////////////////////////////////////////////////
  // Public Java API Methods
  ///////////////////////////////////////////////////////////////////////////

  override def metadata: MetadataJ = ConversionUtils.convertMetadata(metadataScala)

  override def commit[T <: ActionJ](
      actionsJ: java.lang.Iterable[T],
      op: Operation,
      engineInfo: String): CommitResult = {

    actionsJ.asScala.collect { case m: MetadataJ => m }.foreach { m =>
      updateMetadata(m)
    }

    val actions = actionsJ.asScala
      .map(ConversionUtils.convertActionJ)
      .filter(!_.isInstanceOf[Metadata])
      .toSeq

    // Try to commit at the next version.
    var preparedActions = prepareCommit(actions)

    // Find the isolation level to use for this commit
    val noDataChanged = actions.collect { case f: FileAction => f.dataChange }.forall(_ == false)
    val isolationLevelToUse = if (noDataChanged) {
      // If no data has changed (i.e. its is only being rearranged), then SnapshotIsolation
      // provides Serializable guarantee. Hence, allow reduced conflict detection by using
      // SnapshotIsolation of what the table isolation level is.
      SnapshotIsolation
    } else {
      Serializable
    }

    val isBlindAppend = {
      val dependsOnFiles = readPredicates.nonEmpty || readFiles.nonEmpty
      val onlyAddFiles =
        preparedActions.collect { case f: FileAction => f }.forall(_.isInstanceOf[AddFile])
      onlyAddFiles && !dependsOnFiles
    }

    val commitInfo = CommitInfo(
      deltaLog.clock.getTimeMillis(),
      op.getName.toString,
      if (op.getParameters == null) null else op.getParameters.asScala.toMap,
      Map.empty,
      Some(readVersion).filter(_ >= 0),
      Option(isolationLevelToUse.toString),
      Some(isBlindAppend),
      Some(op.getMetrics.asScala.toMap),
      if (op.getUserMetadata.isPresent) Some(op.getUserMetadata.get()) else None,
      Some(s"${engineInfo.replaceAll("\\s", "-")} ${NAME.replaceAll("\\s", "-")}/$VERSION")
    )

    preparedActions = commitInfo +: preparedActions

    commitAttemptStartTime = deltaLog.clock.getTimeMillis()

    val commitVersion = doCommitRetryIteratively(
      snapshot.version + 1,
      preparedActions,
      isolationLevelToUse)

    postCommit(commitVersion)

    logInfo(s"Committed delta #$commitVersion to ${deltaLog.logPath}")

    new CommitResult(commitVersion)
  }

  /** Returns files matching the given predicates. */
  override def markFilesAsRead(readPredicate: Expression): DeltaScan = {
    val scan = snapshot.scanScala(readPredicate)
    val matchedFiles = scan.getFilesScala

    if (scan.getPushedPredicate.isPresent) {
      readPredicates += scan.getPushedPredicate.get()
    }
    readFiles ++= matchedFiles

    scan
  }

  /**
   * All [[Metadata]] actions must go through this function, and be added to the committed actions
   * via `newMetadata` (they shouldn't ever be passed into `prepareCommit`.)
   * This function enforces:
   * - At most one unique [[Metadata]] is committed in a single transaction.
   * - If this is the first commit, the committed metadata configuration includes global Delta
   *   configuration defaults.
   * - Checks for unenforceable NOT NULL constraints in the table schema.
   * - Checks for column name duplication.
   * - Verifies column names are parquet compatible.
   * - Enforces that protocol versions are not part of the table properties.
   */
  override def updateMetadata(metadataJ: MetadataJ): Unit = {

    var latestMetadata = ConversionUtils.convertMetadataJ(metadataJ)

    // this Metadata instance was previously added
    if (newMetadata.contains(latestMetadata)) {
      return
    }

    assert(newMetadata.isEmpty,
      "Cannot change the metadata more than once in a transaction.")

    if (readVersion == -1 || isCreatingNewTable) {
      latestMetadata = withGlobalConfigDefaults(latestMetadata)
      isCreatingNewTable = true
    }

    if (snapshot.metadataScala.schemaString != latestMetadata.schemaString) {
      SchemaUtils.checkUnenforceableNotNullConstraints(latestMetadata.schema)
    }

    verifyNewMetadata(latestMetadata)

    logInfo(s"Updated metadata from ${newMetadata.getOrElse("-")} to $latestMetadata")

    newMetadata = Some(latestMetadata)
  }

  override def readWholeTable(): Unit = {
    readPredicates += Literal.True
    readTheWholeTable = true
  }

  override def txnVersion(id: String): Long = {
    readTxn += id
    snapshot.transactions.getOrElse(id, -1L)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Critical Internal-Only Methods
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Prepare for a commit by doing all necessary pre-commit checks and modifications to the actions.
   * @return The finalized set of actions.
   */
  private def prepareCommit(actions: Seq[Action]): Seq[Action] = {
    assert(!committed, "Transaction already committed.")

    val customCommitInfo = actions.exists(_.isInstanceOf[CommitInfo])
    assert(!customCommitInfo, "Cannot commit a custom CommitInfo in a transaction.")

    // Convert AddFile paths to relative paths if they're in the table path
    var finalActions = actions.map {
      case addFile: AddFile =>
        addFile.copy(path =
          DeltaFileOperations.tryRelativizePath(
            deltaLog.fs,
            deltaLog.getPath,
            new Path(addFile.path)
          ).toString)
      case a: Action => a
    }

    newMetadata.foreach { m =>
      verifySchemaCompatibility(snapshot.metadataScala.schema, m.schema, actions)
    }

    // If the metadata has changed, add that to the set of actions
    finalActions = newMetadata.toSeq ++ finalActions

    if (snapshot.version == -1) {
      deltaLog.ensureLogDirectoryExist()

      // If this is the first commit and no protocol is specified, initialize the protocol version.
      if (!finalActions.exists(_.isInstanceOf[Protocol])) {
        finalActions = protocol +: finalActions
      }

      // If this is the first commit and no metadata is specified, throw an exception
      if (!finalActions.exists(_.isInstanceOf[Metadata])) {
        throw DeltaErrors.metadataAbsentException()
      }
    }

    val protocolOpt = finalActions.collectFirst{ case p: Protocol => p }
    if (protocolOpt.isDefined) {
      assert(protocolOpt.get == Protocol(), s"Invalid Protocol ${protocolOpt.get.simpleString}. " +
        s"Currently only Protocol readerVersion 1 and writerVersion 2 is supported.")
    }

    val partitionColumns = metadataScala.partitionColumns.toSet
    finalActions.foreach {
      case a: AddFile if partitionColumns != a.partitionValues.keySet =>
        throw DeltaErrors.addFilePartitioningMismatchException(
          a.partitionValues.keySet.toSeq, partitionColumns.toSeq)
      case _ => // nothing
    }

    deltaLog.assertProtocolWrite(snapshot.protocolScala)

    // We make sure that this isn't an appendOnly table as we check if we need to delete files.
    val removes = actions.collect { case r: RemoveFile => r }
    if (removes.exists(_.dataChange)) deltaLog.assertRemovable()

    finalActions
  }

  /**
   * Commit `actions` using `attemptVersion` version number. If there are any conflicts that are
   * found, we will retry a fixed number of times.
   *
   * @return the real version that was committed
   */
  protected def doCommitRetryIteratively(
      attemptVersion: Long,
      actions: Seq[Action],
      isolationLevel: IsolationLevel): Long = deltaLog.lockInterruptibly {
    var tryCommit = true
    var commitVersion = attemptVersion
    var attemptNumber = 0

    while (tryCommit) {
      try {
        if (attemptNumber == 0) {
          doCommit(commitVersion, actions, isolationLevel)
        } else if (attemptNumber > DELTA_MAX_RETRY_COMMIT_ATTEMPTS) {
          val totalCommitAttemptTime = deltaLog.clock.getTimeMillis() - commitAttemptStartTime
          throw DeltaErrors.maxCommitRetriesExceededException(
            attemptNumber,
            commitVersion,
            attemptVersion,
            actions.length,
            totalCommitAttemptTime)
        } else {
          commitVersion = checkForConflicts(commitVersion, actions, attemptNumber, isolationLevel)
          doCommit(commitVersion, actions, isolationLevel)
        }
        tryCommit = false
      } catch {
        case _: FileAlreadyExistsException => attemptNumber += 1
      }
    }
    commitVersion
  }

  /**
   * Commit `actions` using `attemptVersion` version number.
   *
   * If you detect any conflicts, try to resolve logical conflicts and commit using a new version.
   *
   * @return the real version that was committed.
   * @throws IllegalStateException if the attempted commit version is ahead of the current delta log
   *                               version
   */
  private def doCommit(
      attemptVersion: Long,
      actions: Seq[Action],
      isolationLevel: IsolationLevel): Long = {
    logInfo(
      s"Attempting to commit version $attemptVersion with ${actions.size} actions with " +
        s"$isolationLevel isolation level")

    if (readVersion > -1 && metadata.getId != snapshot.getMetadata.getId) {
      logError(s"Change in the table id detected in txn. Table id for txn on table at " +
        s"${deltaLog.dataPath} was ${snapshot.getMetadata.getId} when the txn was created and " +
        s"is now changed to ${metadata.getId}.")
    }

    deltaLog.store.write(
      FileNames.deltaFile(deltaLog.logPath, attemptVersion),
      actions.map(_.json).toIterator.asJava,
      false, // overwrite = false
      deltaLog.hadoopConf
    )

    val postCommitSnapshot = deltaLog.update()
    if (postCommitSnapshot.version < attemptVersion) {
      throw new IllegalStateException(
        s"The committed version is $attemptVersion " +
          s"but the current version is ${postCommitSnapshot.version}.")
    }

    attemptVersion
  }

  /**
   * Perform post-commit operations
   */
  private def postCommit(commitVersion: Long): Unit = {
    committed = true

    if (shouldCheckpoint(commitVersion)) {
      try {
        // We checkpoint the version to be committed to so that no two transactions will checkpoint
        // the same version.
        deltaLog.checkpoint(deltaLog.getSnapshotForVersionAsOf(commitVersion))
      } catch {
        case e: IllegalStateException => logWarning("Failed to checkpoint table state.", e)
      }
    }
  }

  /**
   * Looks at actions that have happened since the txn started and checks for logical
   * conflicts with the read/writes. If no conflicts are found return the commit version to attempt
   * next.
   */
  private def checkForConflicts(
      checkVersion: Long,
      actions: Seq[Action],
      attemptNumber: Int,
      commitIsolationLevel: IsolationLevel): Long = {
    val nextAttemptVersion = getNextAttemptVersion

    val currentTransactionInfo = CurrentTransactionInfo(
      readPredicates = readPredicates,
      readFiles = readFiles.toSet,
      readWholeTable = readTheWholeTable,
      readAppIds = readTxn.toSet,
      metadata = metadataScala,
      actions = actions,
      deltaLog = deltaLog)

    val logPrefixStr = s"[attempt $attemptNumber]"
    val txnDetailsLogStr = {
      var adds = 0L
      var removes = 0L
      currentTransactionInfo.actions.foreach {
        case _: AddFile => adds += 1
        case _: RemoveFile => removes += 1
        case _ =>
      }
      s"$adds adds, $removes removes, ${readPredicates.size} read predicates, " +
        s"${readFiles.size} read files"
    }

    logInfo(s"$logPrefixStr Checking for conflicts with versions " +
      s"[$checkVersion, $nextAttemptVersion) with current txn having $txnDetailsLogStr")

    (checkVersion until nextAttemptVersion).foreach { otherCommitVersion =>
      val conflictChecker = new ConflictChecker(
        currentTransactionInfo,
        otherCommitVersion,
        commitIsolationLevel,
        logPrefixStr)

      conflictChecker.checkConflicts()

      logInfo(s"$logPrefixStr No conflicts in version $otherCommitVersion, " +
        s"${deltaLog.clock.getTimeMillis() - commitAttemptStartTime} ms since start")
    }

    logInfo(s"$logPrefixStr No conflicts with versions [$checkVersion, $nextAttemptVersion) " +
      s"with current txn having $txnDetailsLogStr, " +
      s"${deltaLog.clock.getTimeMillis() - commitAttemptStartTime} ms since start")

    nextAttemptVersion
  }

  ///////////////////////////////////////////////////////////////////////////
  // Helper Methods
  ///////////////////////////////////////////////////////////////////////////

  private def verifyNewMetadata(metadata: Metadata): Unit = {
    SchemaMergingUtils.checkColumnNameDuplication(metadata.schema, "in the metadata update")
    SchemaUtils.checkFieldNames(SchemaMergingUtils.explodeNestedFieldNames(metadata.dataSchema))

    try {
      SchemaUtils.checkFieldNames(metadata.partitionColumns)
    } catch {
      case e: DeltaStandaloneException => throw DeltaErrors.invalidPartitionColumn(e)
    }

    Protocol.checkMetadataProtocolProperties(metadata, protocol)
  }

  /**
   * We want to check that the [[newSchema]] is compatible with the [[existingSchema]].
   *
   * If the table is empty, or if the current commit is removing all the files in the table,
   * then we do not need to perform this compatibility check.
   */
  private def verifySchemaCompatibility(
      existingSchema: StructType,
      newSchema: StructType,
      actions: Seq[Action]): Unit = {
    val tableEmpty = snapshot.numOfFiles == 0

    lazy val allCurrentFilesRemoved = {
      val removeFiles = actions.collect { case r: RemoveFile => r }
      removeFiles.map(_.path).toSet == snapshot.allFilesScala.map(_.path).toSet
    }

    if (tableEmpty || allCurrentFilesRemoved) return

    if (!existingSchema.isWriteCompatible(newSchema)) {
      throw DeltaErrors.schemaChangedException(existingSchema, newSchema)
    }
  }

  /**
   * Returns true if we should checkpoint the version that has just been committed.
   */
  private def shouldCheckpoint(committedVersion: Long): Boolean = {
    committedVersion != 0 && committedVersion % deltaLog.checkpointInterval == 0
  }

  /** Returns the next attempt version given the last attempted version */
  private def getNextAttemptVersion: Long = {
    deltaLog.update()
    deltaLog.snapshot.version + 1
  }

  /** Creates new metadata with global Delta configuration defaults. */
  private def withGlobalConfigDefaults(metadata: Metadata): Metadata = {
    metadata.copy(configuration =
      DeltaConfigs.mergeGlobalConfigs(deltaLog.hadoopConf, metadata.configuration))
  }

  ///////////////////////////////////////////////////////////////////////////
  // Logging Override Methods
  ///////////////////////////////////////////////////////////////////////////

  protected lazy val logPrefix: String = {
    def truncate(uuid: String): String = uuid.split("-").head
    s"[tableId=${truncate(snapshot.metadataScala.id)},txnId=${truncate(txnId)}] "
  }

  override def logInfo(msg: => String): Unit = {
    super.logInfo(logPrefix + msg)
  }

  override def logWarning(msg: => String): Unit = {
    super.logWarning(logPrefix + msg)
  }

  override def logWarning(msg: => String, throwable: Throwable): Unit = {
    super.logWarning(logPrefix + msg, throwable)
  }

  override def logError(msg: => String): Unit = {
    super.logError(logPrefix + msg)
  }

  override def logError(msg: => String, throwable: Throwable): Unit = {
    super.logError(logPrefix + msg, throwable)
  }

}
