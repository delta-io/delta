/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.standalone.{CommitResult, Operation, OptimisticTransaction}
import io.delta.standalone.actions.{Action => ActionJ, AddFile => AddFileJ, Metadata => MetadataJ}
import io.delta.standalone.expressions.Expression
import io.delta.standalone.internal.actions.{Action, AddFile, CommitInfo, FileAction, Metadata, Protocol, RemoveFile}
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.util.{ConversionUtils, FileNames, JsonUtils, SchemaMergingUtils, SchemaUtils}

private[internal] class OptimisticTransactionImpl(
    deltaLog: DeltaLogImpl,
    snapshot: SnapshotImpl) extends OptimisticTransaction {
  import OptimisticTransactionImpl._

  /**
   * Tracks the data that could have been seen by recording the partition
   * predicates by which files have been queried by this transaction.
   */
  protected val readPredicates = new ArrayBuffer[Expression]

  /** Tracks specific files that have been seen by this transaction. */
  protected val readFiles = new scala.collection.mutable.HashSet[AddFile]

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
  def metadata: Metadata = newMetadata.getOrElse(snapshot.metadataScala)

  /** The version that this transaction is reading from. */
  private def readVersion: Long = snapshot.version

  ///////////////////////////////////////////////////////////////////////////
  // Public Java API Methods
  ///////////////////////////////////////////////////////////////////////////

  override def commit(
      actionsJ: java.lang.Iterable[ActionJ],
      op: Operation,
      engineInfo: String): CommitResult = {
    val actions = actionsJ.asScala.map(ConversionUtils.convertActionJ).toSeq

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
      null,
      Map.empty,
      Some(readVersion).filter(_ >= 0),
      Option(isolationLevelToUse.toString),
      Some(isBlindAppend),
      Some(op.getOperationMetrics.asScala.toMap),
      if (op.getUserMetadata.isPresent) Some(op.getUserMetadata.get()) else None,
      Some(engineInfo)
    )

    preparedActions = commitInfo +: preparedActions

    commitAttemptStartTime = deltaLog.clock.getTimeMillis()

    val commitVersion = doCommitRetryIteratively(
      snapshot.version + 1,
      preparedActions,
      isolationLevelToUse)

    postCommit(commitVersion)

    new CommitResult(commitVersion)
  }

  override def markFilesAsRead(
      _readPredicates: java.lang.Iterable[Expression]): java.util.List[AddFileJ] = {
    // TODO: PartitionFiltering::filesForScan
    // TODO
    //    val partitionFilters = filters.filter { f =>
    //      DeltaTableUtils.isPredicatePartitionColumnsOnly(f, metadata.partitionColumns, spark)
    //    }
    // TODO readPredicates += ...
    // TODO readFiles ++=
    null
  }

  override def updateMetadata(metadataJ: MetadataJ): Unit = {
    assert(newMetadata.isEmpty,
      "Cannot change the metadata more than once in a transaction.")

    var latestMetadata = ConversionUtils.convertMetadataJ(metadataJ)

    if (readVersion == -1 || isCreatingNewTable) {
      latestMetadata = withGlobalConfigDefaults(latestMetadata)
      isCreatingNewTable = true
      newProtocol = Some(Protocol())
    }

    latestMetadata = if (snapshot.metadataScala.schemaString == latestMetadata.schemaString) {
        // Shortcut when the schema hasn't changed to avoid generating spurious schema change logs.
        // It's fine if two different but semantically equivalent schema strings skip this special
        // case - that indicates that something upstream attempted to do a no-op schema change, and
        // we'll just end up doing a bit of redundant work in the else block.
        latestMetadata
      } else {
        // TODO getJson()
        //   val fixedSchema =
        //   SchemaUtils.removeUnenforceableNotNullConstraints(metadata.schema).getJson()
        // metadata.copy(schemaString = fixedSchema)

        latestMetadata
      }

    // Remove the protocol version properties
    val noProtocolVersionConfig = latestMetadata.configuration.filter {
      case (Protocol.MIN_READER_VERSION_PROP, _) => false
      case (Protocol.MIN_WRITER_VERSION_PROP, _) => false
      case _ => true
    }

    latestMetadata = latestMetadata.copy(configuration = noProtocolVersionConfig)
    verifyNewMetadata(latestMetadata)
    newMetadata = Some(latestMetadata)
  }

  override def readWholeTable(): Unit = {

  }

  override def txnVersion(id: String): Long = {
    // TODO
    0L
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

    // If the metadata has changed, add that to the set of actions
    var finalActions = newMetadata.toSeq ++ actions

    val metadataChanges = finalActions.collect { case m: Metadata => m }
    assert(metadataChanges.length <= 1,
      "Cannot change the metadata more than once in a transaction.")

    metadataChanges.foreach(m => verifyNewMetadata(m))

    finalActions = newProtocol.toSeq ++ finalActions

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

    val partitionColumns = metadata.partitionColumns.toSet
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
          doCommit(commitVersion, actions)
        } else if (attemptNumber > DELTA_MAX_RETRY_COMMIT_ATTEMPTS) {
          val totalCommitAttemptTime = deltaLog.clock.getTimeMillis() - commitAttemptStartTime
          throw DeltaErrors.maxCommitRetriesExceededException(
            attemptNumber,
            commitVersion,
            attemptVersion,
            actions.length,
            totalCommitAttemptTime)
        } else {
          commitVersion = checkForConflicts(commitVersion, actions, isolationLevel)
          doCommit(commitVersion, actions)
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
  private def doCommit(attemptVersion: Long, actions: Seq[Action]): Long =
    deltaLog.lockInterruptibly {
      deltaLog.store.write(
        FileNames.deltaFile(deltaLog.logPath, attemptVersion),
        actions.map(_.json).toIterator
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
      // We checkpoint the version to be committed to so that no two transactions will checkpoint
      // the same version.
      deltaLog.checkpoint(deltaLog.getSnapshotForVersionAsOf(commitVersion))
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
      commitIsolationLevel: IsolationLevel): Long = {
    val nextAttemptVersion = getNextAttemptVersion

    val currentTransactionInfo = CurrentTransactionInfo(
      readPredicates = readPredicates,
      readFiles = readFiles.toSet,
      readWholeTable = false, // TODO readTheWholeTable
      readAppIds = Nil.toSet, // TODO: readTxn.toSet,
      metadata = metadata,
      actions = actions,
      deltaLog = deltaLog)

    (checkVersion until nextAttemptVersion).foreach { otherCommitVersion =>
      val conflictChecker = new ConflictChecker(
        currentTransactionInfo,
        otherCommitVersion,
        commitIsolationLevel)

      conflictChecker.checkConflicts()
    }

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
      // TODO: case e: AnalysisException ?
      case e: RuntimeException => throw DeltaErrors.invalidPartitionColumn(e)
    }

    Protocol.checkMetadataProtocolProperties(metadata, protocol)
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
    // TODO
    metadata
  }
}

private[internal] object OptimisticTransactionImpl {
  val DELTA_MAX_RETRY_COMMIT_ATTEMPTS = 10000000

  def getOperationJsonEncodedParameters(op: Operation): Map[String, String] = {
      op.getParameters.asScala.mapValues(JsonUtils.toJson(_)).toMap
  }
}
