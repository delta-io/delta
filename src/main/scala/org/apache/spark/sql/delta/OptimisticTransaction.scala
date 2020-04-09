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

package org.apache.spark.sql.delta

import java.net.URI
import java.util.ConcurrentModificationException
import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashSet}
import scala.util.control.NonFatal

import com.databricks.spark.util.TagDefinitions.TAG_LOG_STORE_CLASS
import org.apache.spark.sql.delta.DeltaOperations.Operation
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.files._
import org.apache.spark.sql.delta.hooks.{GenerateSymlinkManifest, PostCommitHook}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaConverter
import org.apache.spark.util.{Clock, Utils}

/** Record metrics about a successful commit. */
case class CommitStats(
  /** The version read by the txn when it starts. */
  startVersion: Long,
  /** The version committed by the txn. */
  commitVersion: Long,
  /** The version read by the txn right after it commits. It usually equals to commitVersion,
   * but can be larger than commitVersion when there are concurrent commits. */
  readVersion: Long,
  txnDurationMs: Long,
  commitDurationMs: Long,
  numAdd: Int,
  numRemove: Int,
  bytesNew: Long,
  /** The number of files in the table as of version `readVersion`. */
  numFilesTotal: Long,
  /** The table size in bytes as of version `readVersion`. */
  sizeInBytesTotal: Long,
  /** The protocol as of version `readVersion`. */
  protocol: Protocol,
  info: CommitInfo,
  newMetadata: Option[Metadata],
  numAbsolutePathsInAdd: Int,
  numDistinctPartitionsInAdd: Int,
  isolationLevel: String)

/**
 * Used to perform a set of reads in a transaction and then commit a set of updates to the
 * state of the log.  All reads from the [[DeltaLog]], MUST go through this instance rather
 * than directly to the [[DeltaLog]] otherwise they will not be check for logical conflicts
 * with concurrent updates.
 *
 * This class is not thread-safe.
 *
 * @param deltaLog The Delta Log for the table this transaction is modifying.
 * @param snapshot The snapshot that this transaction is reading at.
 */
class OptimisticTransaction
    (override val deltaLog: DeltaLog, override val snapshot: Snapshot)
    (implicit override val clock: Clock)
  extends OptimisticTransactionImpl
  with DeltaLogging {

  /** Creates a new OptimisticTransaction.
   *
   * @param deltaLog The Delta Log for the table this transaction is modifying.
   */
  def this(deltaLog: DeltaLog)(implicit clock: Clock) {
    this(deltaLog, deltaLog.snapshot)
  }
}

object OptimisticTransaction {

  private val active = new ThreadLocal[OptimisticTransaction]

  /** Get the active transaction */
  def getActive(): Option[OptimisticTransaction] = Option(active.get())

  /**
   * Sets a transaction as the active transaction.
   *
   * @note This is not meant for being called directly, only from
   *       `OptimisticTransaction.withNewTransaction`. Use that to create and set active txns.
   */
  private[delta] def setActive(txn: OptimisticTransaction): Unit = {
    if (active.get != null) {
      throw new IllegalStateException("Cannot set a new txn as active when one is already active")
    }
    active.set(txn)
  }

  /**
   * Clears the active transaction as the active transaction.
   *
   * @note This is not meant for being called directly, `OptimisticTransaction.withNewTransaction`.
   */
  private[delta] def clearActive(): Unit = {
    active.set(null)
  }
}

/**
 * Used to perform a set of reads in a transaction and then commit a set of updates to the
 * state of the log.  All reads from the [[DeltaLog]], MUST go through this instance rather
 * than directly to the [[DeltaLog]] otherwise they will not be check for logical conflicts
 * with concurrent updates.
 *
 * This trait is not thread-safe.
 */
trait OptimisticTransactionImpl extends TransactionalWrite with SQLMetricsReporting
  with DeltaLogging {

  import org.apache.spark.sql.delta.util.FileNames._

  val deltaLog: DeltaLog
  val snapshot: Snapshot
  implicit val clock: Clock

  protected def spark = SparkSession.active
  protected val _spark = spark

  /** The protocol of the snapshot that this transaction is reading at. */
  val protocol = snapshot.protocol

  /** Tracks the appIds that have been seen by this transaction. */
  protected val readTxn = new ArrayBuffer[String]

  /**
   * Tracks the data that could have been seen by recording the partition
   * predicates by which files have been queried by by this transaction.
   */
  protected val readPredicates = new ArrayBuffer[Expression]

  /** Tracks specific files that have been seen by this transaction. */
  protected val readFiles = new HashSet[AddFile]

  /** Tracks if this transaction has already committed. */
  protected var committed = false

  /** Stores the updated metadata (if any) that will result from this txn. */
  protected var newMetadata: Option[Metadata] = None

  protected val txnStartNano = System.nanoTime()
  protected var commitStartNano = -1L
  protected var commitInfo: CommitInfo = _

  /** The version that this transaction is reading from. */
  def readVersion: Long = snapshot.version

  /** For new tables, fetch global configs as metadata. */
  val snapshotMetadata = if (readVersion == -1) {
    val updatedConfig = DeltaConfigs.mergeGlobalConfigs(
      spark.sessionState.conf, Map.empty, Protocol())
    Metadata(configuration = updatedConfig)
  } else {
    snapshot.metadata
  }

  protected val postCommitHooks = new ArrayBuffer[PostCommitHook]()

  /** Returns the metadata at the current point in the log. */
  def metadata: Metadata = newMetadata.getOrElse(snapshotMetadata)

  /**
   * Records an update to the metadata that should be committed with this transaction.
   * Note that this must be done before writing out any files so that file writing
   * and checks happen with the final metadata for the table.
   *
   * IMPORTANT: It is the responsibility of the caller to ensure that files currently
   * present in the table are still valid under the new metadata.
   */
  def updateMetadata(metadata: Metadata): Unit = {
    assert(!hasWritten,
      "Cannot update the metadata in a transaction that has already written data.")
    assert(newMetadata.isEmpty,
      "Cannot change the metadata more than once in a transaction.")

    val updatedMetadata = if (readVersion == -1) {
      val updatedConfigs = DeltaConfigs.mergeGlobalConfigs(
        spark.sessionState.conf, metadata.configuration, Protocol())
      metadata.copy(configuration = updatedConfigs)
    } else {
      metadata
    }
    verifyNewMetadata(updatedMetadata)
    logInfo(s"Updated metadata from ${newMetadata.getOrElse("-")} to $updatedMetadata")

    newMetadata = Some(updatedMetadata)
  }

  protected def verifyNewMetadata(metadata: Metadata): Unit = {
    SchemaUtils.checkColumnNameDuplication(metadata.schema, "in the metadata update")
    SchemaUtils.checkFieldNames(SchemaUtils.explodeNestedFieldNames(metadata.dataSchema))
    val partitionColCheckIsFatal =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_PARTITION_COLUMN_CHECK_ENABLED)
    try {
      SchemaUtils.checkFieldNames(metadata.partitionColumns)
    } catch {
      case e: AnalysisException =>
        recordDeltaEvent(
          deltaLog,
          "delta.schema.invalidPartitionColumn",
          data = Map(
            "checkEnabled" -> partitionColCheckIsFatal,
            "columns" -> metadata.partitionColumns
          )
        )
        if (partitionColCheckIsFatal) throw DeltaErrors.invalidPartitionColumn(e)
    }
  }

  /** Returns files matching the given predicates. */
  def filterFiles(): Seq[AddFile] = filterFiles(Seq(Literal.apply(true)))

  /** Returns files matching the given predicates. */
  def filterFiles(filters: Seq[Expression]): Seq[AddFile] = {
    val scan = snapshot.filesForScan(Nil, filters)
    val partitionFilters = filters.filter { f =>
      DeltaTableUtils.isPredicatePartitionColumnsOnly(f, metadata.partitionColumns, spark)
    }
    readPredicates += partitionFilters.reduceLeftOption(And).getOrElse(Literal(true))
    readFiles ++= scan.files
    scan.files
  }

  /** Mark the entire table as tainted by this transaction. */
  def readWholeTable(): Unit = {
    readPredicates += Literal(true)
  }

  /**
   * Returns the latest version that has committed for the idempotent transaction with given `id`.
   */
  def txnVersion(id: String): Long = {
    readTxn += id
    snapshot.transactions.getOrElse(id, -1L)
  }

  /**
   * Return the operation metrics for the operation if it is enabled
   */
  def getOperationMetrics(op: Operation): Option[Map[String, String]] = {
    if (spark.conf.get(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)) {
      Some(getMetricsForOperation(op))
    } else {
      None
    }
  }

  /**
   * Modifies the state of the log by adding a new commit that is based on a read at
   * the given `lastVersion`.  In the case of a conflict with a concurrent writer this
   * method will throw an exception.
   *
   * @param actions     Set of actions to commit
   * @param op          Details of operation that is performing this transactional commit
   */
  @throws(classOf[ConcurrentModificationException])
  def commit(actions: Seq[Action], op: DeltaOperations.Operation): Long = recordDeltaOperation(
      deltaLog,
      "delta.commit") {
    val version = try {
      // Try to commit at the next version.
      var finalActions = prepareCommit(actions, op)

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
          finalActions.collect { case f: FileAction => f }.forall(_.isInstanceOf[AddFile])
        onlyAddFiles && !dependsOnFiles
      }

      if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_COMMIT_INFO_ENABLED)) {
        commitInfo = CommitInfo(
          clock.getTimeMillis(),
          op.name,
          op.jsonEncodedValues,
          Map.empty,
          Some(readVersion).filter(_ >= 0),
          None,
          Some(isBlindAppend),
          getOperationMetrics(op))
        finalActions = commitInfo +: finalActions
      }

      // Register post-commit hooks if any
      lazy val hasFileActions = finalActions.collect { case f: FileAction => f }.nonEmpty
      if (DeltaConfigs.SYMLINK_FORMAT_MANIFEST_ENABLED.fromMetaData(metadata) && hasFileActions) {
        registerPostCommitHook(GenerateSymlinkManifest)
      }

      val commitVersion = doCommit(snapshot.version + 1, finalActions, 0, isolationLevelToUse)
      logInfo(s"Committed delta #$commitVersion to ${deltaLog.logPath}")
      postCommit(commitVersion, finalActions)
      commitVersion
    } catch {
      case e: DeltaConcurrentModificationException =>
        recordDeltaEvent(deltaLog, "delta.commit.conflict." + e.conflictType)
        throw e
      case NonFatal(e) =>
        recordDeltaEvent(
          deltaLog, "delta.commit.failure", data = Map("exception" -> Utils.exceptionString(e)))
        throw e
    }

    runPostCommitHooks(version, actions)

    version
  }

  /**
   * Prepare for a commit by doing all necessary pre-commit checks and modifications to the actions.
   * @return The finalized set of actions.
   */
  protected def prepareCommit(
      actions: Seq[Action],
      op: DeltaOperations.Operation): Seq[Action] = {

    assert(!committed, "Transaction already committed.")

    // If the metadata has changed, add that to the set of actions
    var finalActions = newMetadata.toSeq ++ actions
    val metadataChanges = finalActions.collect { case m: Metadata => m }
    assert(
      metadataChanges.length <= 1,
      "Cannot change the metadata more than once in a transaction.")
    metadataChanges.foreach(m => verifyNewMetadata(m))

    // If this is the first commit and no protocol is specified, initialize the protocol version.
    if (snapshot.version == -1) {
      deltaLog.ensureLogDirectoryExist()
      if (!finalActions.exists(_.isInstanceOf[Protocol])) {
        finalActions = Protocol() +: finalActions
      }
    }

    finalActions = finalActions.map {
      // Fetch global config defaults for the first commit
      case m: Metadata if snapshot.version == -1 =>
        val updatedConf = DeltaConfigs.mergeGlobalConfigs(
          spark.sessionState.conf, m.configuration, Protocol())
        m.copy(configuration = updatedConf)
      case other => other
    }

    deltaLog.protocolWrite(
      snapshot.protocol,
      logUpgradeMessage = !actions.headOption.exists(_.isInstanceOf[Protocol]))

    // We make sure that this isn't an appendOnly table as we check if we need to delete
    // files.
    val removes = actions.collect { case r: RemoveFile => r }
    if (removes.exists(_.dataChange)) deltaLog.assertRemovable()

    finalActions
  }

  /** Perform post-commit operations */
  protected def postCommit(commitVersion: Long, commitActions: Seq[Action]): Unit = {
    committed = true
    if (commitVersion != 0 && commitVersion % deltaLog.checkpointInterval == 0) {
      try {
        deltaLog.checkpoint()
      } catch {
        case e: IllegalStateException =>
          logWarning("Failed to checkpoint table state.", e)
      }
    }
  }

  /**
   * Commit `actions` using `attemptVersion` version number. If detecting any conflicts, try to
   * resolve logical conflicts and commit using a new version.
   *
   * @return the real version that was committed.
   */
  protected def doCommit(
      attemptVersion: Long,
      actions: Seq[Action],
      attemptNumber: Int,
      isolationLevel: IsolationLevel): Long = deltaLog.lockInterruptibly {
    try {
      logInfo(
        s"Attempting to commit version $attemptVersion with ${actions.size} actions with " +
          s"$isolationLevel isolation level")

      if (readVersion > -1 && metadata.id != snapshot.metadata.id) {
        val msg = s"Change in the table id detected in txn. Table id for txn on table at " +
          s"${deltaLog.dataPath} was ${snapshot.metadata.id} when the txn was created and " +
          s"is now changed to ${metadata.id}."
        logError(msg)
        recordDeltaEvent(deltaLog, "delta.metadataCheck.commit", data = Map(
          "readSnapshotTableId" -> snapshot.metadata.id,
          "txnTableId" -> metadata.id,
          "txnMetadata" -> metadata,
          "commitAttemptVersion" -> attemptVersion,
          "commitAttemptNumber" -> attemptNumber))
      }

      deltaLog.store.write(
        deltaFile(deltaLog.logPath, attemptVersion),
        actions.map(_.json).toIterator)

      val commitTime = System.nanoTime()
      val postCommitSnapshot = deltaLog.update()

      if (postCommitSnapshot.version < attemptVersion) {
        throw new IllegalStateException(
          s"The committed version is $attemptVersion " +
            s"but the current version is ${postCommitSnapshot.version}.")
      }

      // Post stats
      var numAbsolutePaths = 0
      var pathHolder: Path = null
      val distinctPartitions = new mutable.HashSet[Map[String, String]]
      val adds = actions.collect {
        case a: AddFile =>
          pathHolder = new Path(new URI(a.path))
          if (pathHolder.isAbsolute) numAbsolutePaths += 1
          distinctPartitions += a.partitionValues
          a
      }
      val stats = CommitStats(
        startVersion = snapshot.version,
        commitVersion = attemptVersion,
        readVersion = postCommitSnapshot.version,
        txnDurationMs = NANOSECONDS.toMillis(commitTime - txnStartNano),
        commitDurationMs = NANOSECONDS.toMillis(commitTime - commitStartNano),
        numAdd = adds.size,
        numRemove = actions.collect { case r: RemoveFile => r }.size,
        bytesNew = adds.filter(_.dataChange).map(_.size).sum,
        numFilesTotal = postCommitSnapshot.numOfFiles,
        sizeInBytesTotal = postCommitSnapshot.sizeInBytes,
        protocol = postCommitSnapshot.protocol,
        info = Option(commitInfo).map(_.copy(readVersion = None, isolationLevel = None)).orNull,
        newMetadata = newMetadata,
        numAbsolutePaths,
        numDistinctPartitionsInAdd = distinctPartitions.size,
        isolationLevel = null)
      recordDeltaEvent(deltaLog, "delta.commit.stats", data = stats)

      attemptVersion
    } catch {
      case e: java.nio.file.FileAlreadyExistsException =>
        checkAndRetry(attemptVersion, actions, attemptNumber, isolationLevel)
    }
  }

  /**
   * Looks at actions that have happened since the txn started and checks for logical
   * conflicts with the read/writes. If no conflicts are found, try to commit again
   * otherwise, throw an exception.
   */
  protected def checkAndRetry(
      checkVersion: Long,
      actions: Seq[Action],
      attemptNumber: Int,
      commitIsolationLevel: IsolationLevel): Long = recordDeltaOperation(
        deltaLog,
        "delta.commit.retry",
        tags = Map(TAG_LOG_STORE_CLASS -> deltaLog.store.getClass.getName)) {

    import _spark.implicits._

    val nextAttemptVersion = getNextAttemptVersion(checkVersion)
    (checkVersion until nextAttemptVersion).foreach { version =>
      // Actions of a commit which went in before ours
      val winningCommitActions =
        deltaLog.store.read(deltaFile(deltaLog.logPath, version)).map(Action.fromJson)

      // Categorize all the actions that have happened since the transaction read.
      val metadataUpdates = winningCommitActions.collect { case a: Metadata => a }
      val removedFiles = winningCommitActions.collect { case a: RemoveFile => a }
      val txns = winningCommitActions.collect { case a: SetTransaction => a }
      val protocol = winningCommitActions.collect { case a: Protocol => a }
      val commitInfo = winningCommitActions.collectFirst { case a: CommitInfo => a }.map(
        ci => ci.copy(version = Some(version)))

      val blindAppendAddedFiles = mutable.ArrayBuffer[AddFile]()
      val changedDataAddedFiles = mutable.ArrayBuffer[AddFile]()

      val isBlindAppendOption = commitInfo.flatMap(_.isBlindAppend)
      if (isBlindAppendOption.getOrElse(false)) {
        blindAppendAddedFiles ++= winningCommitActions.collect { case a: AddFile => a }
      } else {
        changedDataAddedFiles ++= winningCommitActions.collect { case a: AddFile => a }
      }

      // If the log protocol version was upgraded, make sure we are still okay.
      // Fail the transaction if we're trying to upgrade protocol ourselves.
      if (protocol.nonEmpty) {
        protocol.foreach { p =>
          deltaLog.protocolRead(p)
          deltaLog.protocolWrite(p)
        }
        actions.foreach {
          case Protocol(_, _) => throw new ProtocolChangedException(commitInfo)
          case _ =>
        }
      }

      // Fail if the metadata is different than what the txn read.
      if (metadataUpdates.nonEmpty) {
        throw new MetadataChangedException(commitInfo)
      }

      // Fail if new files have been added that the txn should have read.
      val addedFilesToCheckForConflicts = commitIsolationLevel match {
        case Serializable => changedDataAddedFiles ++ blindAppendAddedFiles
        case WriteSerializable => changedDataAddedFiles // don't conflict with blind appends
        case SnapshotIsolation => Seq.empty
      }
      val predicatesMatchingAddedFiles = ExpressionSet(readPredicates).iterator.flatMap { p =>
        val conflictingFile = DeltaLog.filterFileList(
          metadata.partitionSchema,
          addedFilesToCheckForConflicts.toDF(), p :: Nil).as[AddFile].take(1)

        conflictingFile.headOption.map(f => getPrettyPartitionMessage(f.partitionValues))
      }.take(1).toArray

      if (predicatesMatchingAddedFiles.nonEmpty) {
        val isWriteSerializable = commitIsolationLevel == WriteSerializable
        val onlyAddFiles =
          winningCommitActions.collect { case f: FileAction => f }.forall(_.isInstanceOf[AddFile])

        val retryMsg =
          if (isWriteSerializable && onlyAddFiles && isBlindAppendOption.isEmpty) {
            // This transaction was made by an older version which did not set `isBlindAppend` flag.
            // So even if it looks like an append, we don't know for sure if it was a blind append
            // or not. So we suggest them to upgrade all there workloads to latest version.
            Some(
              "Upgrading all your concurrent writers to use the latest Delta Lake may " +
                "avoid this error. Please upgrade and then retry this operation again.")
          } else None
        throw new ConcurrentAppendException(commitInfo, predicatesMatchingAddedFiles.head, retryMsg)
      }

      // Fail if files have been deleted that the txn read.
      val readFilePaths = readFiles.map(f => f.path -> f.partitionValues).toMap
      val deleteReadOverlap = removedFiles.find(r => readFilePaths.contains(r.path))
      if (deleteReadOverlap.nonEmpty) {
        val filePath = deleteReadOverlap.get.path
        val partition = getPrettyPartitionMessage(readFilePaths(filePath))
        throw new ConcurrentDeleteReadException(commitInfo, s"$filePath in $partition")
      }

      // Fail if a file is deleted twice.
      val txnDeletes = actions.collect { case r: RemoveFile => r }.map(_.path).toSet
      val deleteOverlap = removedFiles.map(_.path).toSet intersect txnDeletes
      if (deleteOverlap.nonEmpty) {
        throw new ConcurrentDeleteDeleteException(commitInfo, deleteOverlap.head)
      }

      // Fail if idempotent transactions have conflicted.
      val txnOverlap = txns.map(_.appId).toSet intersect readTxn.toSet
      if (txnOverlap.nonEmpty) {
        throw new ConcurrentTransactionException(commitInfo)
      }
    }

    logInfo(s"No logical conflicts with deltas [$checkVersion, $nextAttemptVersion), retrying.")
    doCommit(nextAttemptVersion, actions, attemptNumber + 1, commitIsolationLevel)
  }

  /** Returns the next attempt version given the last attempted version */
  protected def getNextAttemptVersion(previousAttemptVersion: Long): Long = {
    deltaLog.update()
    deltaLog.snapshot.version + 1
  }

  /** A helper function for pretty printing a specific partition directory. */
  protected def getPrettyPartitionMessage(partitionValues: Map[String, String]): String = {
    if (metadata.partitionColumns.isEmpty) {
      "the root of the table"
    } else {
      val partition = metadata.partitionColumns.map { name =>
        s"$name=${partitionValues(name)}"
      }.mkString("[", ", ", "]")
      s"partition ${partition}"
    }
  }

  /** Register a hook that will be executed once a commit is successful. */
  def registerPostCommitHook(hook: PostCommitHook): Unit = {
    if (!postCommitHooks.contains(hook)) {
      postCommitHooks.append(hook)
    }
  }

  /** Executes the registered post commit hooks. */
  protected def runPostCommitHooks(
      version: Long,
      committedActions: Seq[Action]): Unit = {
      assert(committed, "Can't call post commit hooks before committing")

    // Keep track of the active txn because hooks may create more txns and overwrite the active one.
    val activeCommit = OptimisticTransaction.getActive()
    OptimisticTransaction.clearActive()

    try {
      postCommitHooks.foreach { hook =>
        try {
          hook.run(spark, this, committedActions)
        } catch {
          case NonFatal(e) =>
            logWarning(s"Error when executing post-commit hook ${hook.name} " +
              s"for commit $version", e)
            recordDeltaEvent(deltaLog, "delta.commit.hook.failure", data = Map(
              "hook" -> hook.name,
              "version" -> version,
              "exception" -> e.toString
            ))
            hook.handleError(e, version)
        }
      }
    } finally {
      activeCommit.foreach(OptimisticTransaction.setActive)
    }
  }

  override def logInfo(msg: => String): Unit = {
    super.logInfo(s"[tableId=${snapshot.metadata.id}] " + msg)
  }

  override def logWarning(msg: => String): Unit = {
    super.logWarning(s"[tableId=${snapshot.metadata.id}] " + msg)
  }

  override def logWarning(msg: => String, throwable: Throwable): Unit = {
    super.logWarning(s"[tableId=${snapshot.metadata.id}] " + msg, throwable)
  }

  override def logError(msg: => String): Unit = {
    super.logError(s"[tableId=${snapshot.metadata.id}] " + msg)
  }

  override def logError(msg: => String, throwable: Throwable): Unit = {
    super.logError(s"[tableId=${snapshot.metadata.id}] " + msg, throwable)
  }
}
