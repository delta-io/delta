/*
 * Copyright 2019 Databricks, Inc.
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
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.databricks.spark.util.TagDefinitions.TAG_LOG_STORE_CLASS
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.files._
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
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
trait OptimisticTransactionImpl extends TransactionalWrite {

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

  /** Tracks if this transaction has already committed. */
  protected var committed = false

  /** Stores the updated metadata (if any) that will result from this txn. */
  protected var newMetadata: Option[Metadata] = None

  protected val txnStartNano = System.nanoTime()
  protected var commitStartNano = -1L
  protected var commitInfo: CommitInfo = _

  /**
   * Tracks if this transaction depends on any data files. This flag must be set if this transaction
   * reads any data explicitly or implicitly (e.g., delete, update and overwrite).
   */
  protected var dependsOnFiles: Boolean = false

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
    newMetadata = Some(updatedMetadata)
  }

  protected def verifyNewMetadata(metadata: Metadata): Unit = {
    SchemaUtils.checkColumnNameDuplication(metadata.schema, "in the metadata update")
    ParquetSchemaConverter.checkFieldNames(SchemaUtils.explodeNestedFieldNames(metadata.dataSchema))
  }

  /** Returns files matching the given predicates. */
  def filterFiles(): Seq[AddFile] = filterFiles(Seq(Literal.apply(true)))

  /** Returns files matching the given predicates. */
  def filterFiles(filters: Seq[Expression]): Seq[AddFile] = {
    dependsOnFiles = true
    snapshot.filesForScan(Nil, filters).files
  }

  /** Mark the entire table as tainted by this transaction. */
  def readWholeTable(): Unit = {
    dependsOnFiles = true
  }

  /**
   * Returns the latest version that has committed for the idempotent transaction with given `id`.
   */
  def txnVersion(id: String): Long = {
    readTxn += id
    snapshot.transactions.getOrElse(id, -1L)
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

      val isBlindAppend = {
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
          Some(isBlindAppend))
        finalActions = commitInfo +: finalActions
      }

      val commitVersion = doCommit(snapshot.version + 1, finalActions, 0)
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
  private def doCommit(
      attemptVersion: Long,
      actions: Seq[Action],
      attemptNumber: Int): Long = deltaLog.lockInterruptibly {
    try {
      logDebug(s"Attempting to commit version $attemptVersion with ${actions.size} actions")

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
        checkAndRetry(attemptVersion, actions, attemptNumber)
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
      attemptNumber: Int): Long = recordDeltaOperation(
        deltaLog,
        "delta.commit.retry",
        tags = Map(TAG_LOG_STORE_CLASS -> deltaLog.store.getClass.getName)) {
    deltaLog.update()
    val nextAttempt = deltaLog.snapshot.version + 1

    (checkVersion until nextAttempt).foreach { version =>
      val winningCommitActions =
        deltaLog.store.read(deltaFile(deltaLog.logPath, version)).map(Action.fromJson)
      val metadataUpdates = winningCommitActions.collect { case a: Metadata => a }
      val txns = winningCommitActions.collect { case a: SetTransaction => a }
      val protocol = winningCommitActions.collect { case a: Protocol => a }
      val commitInfo = winningCommitActions.collectFirst { case a: CommitInfo => a }.map(
        ci => ci.copy(version = Some(version)))
      val fileActions = winningCommitActions.collect { case f: FileAction => f }
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
      // Fail if the data is different than what the txn read.
      if (dependsOnFiles && fileActions.nonEmpty) {
        throw new ConcurrentWriteException(commitInfo)
      }
      // Fail if idempotent transactions have conflicted.
      val txnOverlap = txns.map(_.appId).toSet intersect readTxn.toSet
      if (txnOverlap.nonEmpty) {
        throw new ConcurrentTransactionException(commitInfo)
      }
    }
    logInfo(s"No logical conflicts with deltas [$checkVersion, $nextAttempt), retrying.")
    doCommit(nextAttempt, actions, attemptNumber + 1)
  }
}
