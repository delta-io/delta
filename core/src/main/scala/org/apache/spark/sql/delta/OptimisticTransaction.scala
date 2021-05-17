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
import java.nio.file.FileAlreadyExistsException
import java.util.{ConcurrentModificationException, Locale}
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
  /** The number and size of CDC files added in this operation. */
  numCdcFiles: Long,
  cdcBytesNew: Long,
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
   * Runs the passed block of code with the given active transaction
   */
  def withActive[T](activeTransaction: OptimisticTransaction)(block: => T): T = {
    val original = getActive()
    setActive(activeTransaction)
    try {
      block
    } finally {
      if (original.isDefined) {
        setActive(original.get)
      } else {
        clearActive()
      }
    }
  }

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

  /** Tracks the appIds that have been seen by this transaction. */
  protected val readTxn = new ArrayBuffer[String]

  /**
   * Tracks the data that could have been seen by recording the partition
   * predicates by which files have been queried by this transaction.
   */
  protected val readPredicates = new ArrayBuffer[Expression]

  /** Tracks specific files that have been seen by this transaction. */
  protected val readFiles = new HashSet[AddFile]

  /** Whether the whole table was read during the transaction. */
  protected var readTheWholeTable = false

  /** Tracks if this transaction has already committed. */
  protected var committed = false

  /** Stores the updated metadata (if any) that will result from this txn. */
  protected var newMetadata: Option[Metadata] = None

  /** Stores the updated protocol (if any) that will result from this txn. */
  protected var newProtocol: Option[Protocol] = None

  protected val txnStartNano = System.nanoTime()
  protected var commitStartNano = -1L
  protected var commitInfo: CommitInfo = _

  // Whether this transaction is creating a new table.
  private var isCreatingNewTable: Boolean = false

  /**
   * Tracks the start time since we started trying to write a particular commit.
   * Used for logging duration of retried transactions.
   */
  protected var commitAttemptStartTime: Long = _

  /** The version that this transaction is reading from. */
  def readVersion: Long = snapshot.version

  /** Creates new metadata with global Delta configuration defaults. */
  private def withGlobalConfigDefaults(metadata: Metadata): Metadata = {
    val conf = spark.sessionState.conf
    metadata.copy(configuration = DeltaConfigs.mergeGlobalConfigs(
      conf, metadata.configuration))
  }

  protected val postCommitHooks = new ArrayBuffer[PostCommitHook]()

  /** The protocol of the snapshot that this transaction is reading at. */
  def protocol: Protocol = newProtocol.getOrElse(snapshot.protocol)

  /** Start time of txn in nanoseconds */
  def txnStartTimeNs: Long = txnStartNano

  /**
   * Returns the metadata for this transaction. The metadata refers to the metadata of the snapshot
   * at the transaction's read version unless updated during the transaction.
   */
  def metadata: Metadata = newMetadata.getOrElse(snapshot.metadata)

  /**
   * Records an update to the metadata that should be committed with this transaction.
   * Note that this must be done before writing out any files so that file writing
   * and checks happen with the final metadata for the table.
   *
   * IMPORTANT: It is the responsibility of the caller to ensure that files currently
   * present in the table are still valid under the new metadata.
   */
  def updateMetadata(_metadata: Metadata): Unit = {
    assert(!hasWritten,
      "Cannot update the metadata in a transaction that has already written data.")
    assert(newMetadata.isEmpty,
      "Cannot change the metadata more than once in a transaction.")

    val metadata = if (_metadata.schemaString != null) {
      val schema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(_metadata.schema)
      _metadata.copy(schemaString = schema.json)
    } else {
      _metadata
    }

    val protocolBeforeUpdate = protocol

    val metadataWithFixedSchema =
      if (snapshot.metadata.schemaString == metadata.schemaString) {
        // Shortcut when the schema hasn't changed to avoid generating spurious schema change logs.
        // It's fine if two different but semantically equivalent schema strings skip this special
        // case - that indicates that something upstream attempted to do a no-op schema change, and
        // we'll just end up doing a bit of redundant work in the else block.
        metadata
      } else {
        val fixedSchema = SchemaUtils.removeUnenforceableNotNullConstraints(
          metadata.schema, spark.sessionState.conf).json
        metadata.copy(schemaString = fixedSchema)
      }
    val updatedMetadata = if (readVersion == -1) {
      val m = withGlobalConfigDefaults(metadataWithFixedSchema)
      isCreatingNewTable = true
      newProtocol = Some(Protocol.forNewTable(spark, m))
      m
    } else {
      metadataWithFixedSchema
    }
    // Remove the protocol version properties
    val configs = updatedMetadata.configuration.filter {
      case (Protocol.MIN_READER_VERSION_PROP, value) =>
        if (!isCreatingNewTable) {
          newProtocol = Some(protocol.copy(
            minReaderVersion = Protocol.getVersion(Protocol.MIN_READER_VERSION_PROP, value)))
        }
        false
      case (Protocol.MIN_WRITER_VERSION_PROP, value) =>
        if (!isCreatingNewTable) {
          newProtocol = Some(protocol.copy(
            minWriterVersion = Protocol.getVersion(Protocol.MIN_WRITER_VERSION_PROP, value)))
        }
        false
      case _ => true
    }
    val generationExpressionsFixed =
      if (isCreatingNewTable) {
        // Creating a new table will drop all existing data, so we don't need to fix the old
        // metadata.
        updatedMetadata
      } else {
        // This is not a new table. The new schema may be merged from the existing schema.
        if (GeneratedColumn.satisfyGeneratedColumnProtocol(protocolBeforeUpdate)) {
          // The protocol matches so this is a valid generated column table. Do nothing.
          updatedMetadata
        } else {
          // As the protocol doesn't match, this table is created by an old version that doesn't
          // support generated columns. We should remove the generation expressions to fix the
          // schema to avoid bumping the writer version incorrectly.
          val newSchema = GeneratedColumn.removeGenerationExpressions(updatedMetadata.schema)
          if (newSchema ne updatedMetadata.schema) {
            updatedMetadata.copy(schemaString = newSchema.json)
          } else {
            updatedMetadata
          }
        }
      }
    val noProtocolVersions = generationExpressionsFixed.copy(configuration = configs)
    verifyNewMetadata(noProtocolVersions)
    logInfo(s"Updated metadata from ${newMetadata.getOrElse("-")} to $noProtocolVersions")

    newMetadata = Some(noProtocolVersions)
  }

  /**
   * Records an update to the metadata that should be committed with this transaction and when
   * this transaction is logically creating a new table, e.g. replacing a previous table with new
   * metadata. Note that this must be done before writing out any files so that file writing
   * and checks happen with the final metadata for the table.
   *
   * IMPORTANT: It is the responsibility of the caller to ensure that files currently
   * present in the table are still valid under the new metadata.
   */
  def updateMetadataForNewTable(metadata: Metadata): Unit = {
    val m = withGlobalConfigDefaults(metadata)
    newProtocol = Some(Protocol.forNewTable(spark, m))
    isCreatingNewTable = true
    updateMetadata(m)
  }

  protected def verifyNewMetadata(metadata: Metadata): Unit = {
    assert(!CharVarcharUtils.hasCharVarchar(metadata.schema),
      "The schema in Delta log should not contain char/varchar type.")
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

    if (GeneratedColumn.hasGeneratedColumns(metadata.schema)) {
      recordDeltaOperation(deltaLog, "delta.generatedColumns.check") {
        GeneratedColumn.validateGeneratedColumns(spark, metadata.schema)
      }
      recordDeltaEvent(deltaLog, "delta.generatedColumns.definition")
    }

    val needsProtocolUpdate = Protocol.checkProtocolRequirements(spark, metadata, protocol)
    if (needsProtocolUpdate.isDefined) {
      newProtocol = needsProtocolUpdate
    }
  }

  /** Returns files matching the given predicates. */
  def filterFiles(): Seq[AddFile] = filterFiles(Seq(Literal.TrueLiteral))

  /** Returns files matching the given predicates. */
  def filterFiles(filters: Seq[Expression]): Seq[AddFile] = {
    val scan = snapshot.filesForScan(Nil, filters)
    val partitionFilters = filters.filter { f =>
      DeltaTableUtils.isPredicatePartitionColumnsOnly(f, metadata.partitionColumns, spark)
    }
    readPredicates += partitionFilters.reduceLeftOption(And).getOrElse(Literal.TrueLiteral)
    readFiles ++= scan.files
    scan.files
  }

  /** Mark the entire table as tainted by this transaction. */
  def readWholeTable(): Unit = {
    readPredicates += Literal.TrueLiteral
    readTheWholeTable = true
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
   * Return the user-defined metadata for the operation.
   */
  def getUserMetadata(op: Operation): Option[String] = {
    // option wins over config if both are set
    op.userMetadata match {
      case data @ Some(_) => data
      case None => spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_USER_METADATA)
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
    commitStartNano = System.nanoTime()

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
          getOperationMetrics(op),
          getUserMetadata(op))
        finalActions = commitInfo +: finalActions
      }

      // Register post-commit hooks if any
      lazy val hasFileActions = finalActions.collect { case f: FileAction => f }.nonEmpty
      if (DeltaConfigs.SYMLINK_FORMAT_MANIFEST_ENABLED.fromMetaData(metadata) && hasFileActions) {
        registerPostCommitHook(GenerateSymlinkManifest)
      }

      commitAttemptStartTime = clock.getTimeMillis()
      val commitVersion = doCommitRetryIteratively(
        snapshot.version + 1,
        finalActions,
        isolationLevelToUse)
      logInfo(s"Committed delta #$commitVersion to ${deltaLog.logPath}")
      postCommit(commitVersion)
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
    if (metadataChanges.length > 1) {
      recordDeltaEvent(deltaLog, "delta.metadataCheck.multipleMetadataActions", data = Map(
        "metadataChanges" -> metadataChanges
      ))
      assert(
        metadataChanges.length <= 1, "Cannot change the metadata more than once in a transaction.")
    }
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
        recordDeltaEvent(deltaLog, "delta.metadataCheck.noMetadataInInitialCommit")
        if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_COMMIT_VALIDATION_ENABLED)) {
          throw DeltaErrors.metadataAbsentException()
        }
        logWarning(
          s"""
            |Detected no metadata in initial commit but commit validation was turned off. To turn
            |it back on set ${DeltaSQLConf.DELTA_COMMIT_VALIDATION_ENABLED} to "true"
          """.stripMargin)
      }
    }

    val partitionColumns = metadata.partitionColumns.toSet
    finalActions = finalActions.map {
      case newVersion: Protocol =>
        require(newVersion.minReaderVersion > 0, "The reader version needs to be greater than 0")
        require(newVersion.minWriterVersion > 0, "The writer version needs to be greater than 0")
        if (!isCreatingNewTable) {
          val currentVersion = snapshot.protocol
          if (newVersion.minReaderVersion < currentVersion.minReaderVersion ||
              newVersion.minWriterVersion < currentVersion.minWriterVersion) {
            throw new ProtocolDowngradeException(currentVersion, newVersion)
          }
        }
        newVersion

      case a: AddFile if partitionColumns != a.partitionValues.keySet =>
        // If the partitioning in metadata does not match the partitioning in the AddFile
        recordDeltaEvent(deltaLog, "delta.metadataCheck.partitionMismatch", data = Map(
          "tablePartitionColumns" -> metadata.partitionColumns,
          "filePartitionValues" -> a.partitionValues
        ))
        if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_COMMIT_VALIDATION_ENABLED)) {
          throw DeltaErrors.addFilePartitioningMismatchException(
            a.partitionValues.keySet.toSeq, partitionColumns.toSeq)
        }
        logWarning(
          s"""
             |Detected mismatch in partition values between AddFile and table metadata but
             |commit validation was turned off.
             |To turn it back on set ${DeltaSQLConf.DELTA_COMMIT_VALIDATION_ENABLED} to "true"
          """.stripMargin)
        a
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

  /**
   * Returns true if we should checkpoint the version that has just been committed.
   */
  protected def shouldCheckpoint(committedVersion: Long): Boolean = {
    committedVersion != 0 && committedVersion % deltaLog.checkpointInterval == 0
  }

  /** Perform post-commit operations */
  protected def postCommit(commitVersion: Long): Unit = {
    committed = true
    if (shouldCheckpoint(commitVersion)) {
      try {
        // We checkpoint the version to be committed to so that no two transactions will checkpoint
        // the same version.
        deltaLog.checkpoint(deltaLog.getSnapshotAt(commitVersion))
      } catch {
        case e: IllegalStateException =>
          logWarning("Failed to checkpoint table state.", e)
      }
    }
  }

  private def lockCommitIfEnabled[T](body: => T): T = {
    if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_COMMIT_LOCK_ENABLED)) {
      deltaLog.lockInterruptibly(body)
    } else {
      body
    }
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
      isolationLevel: IsolationLevel): Long = lockCommitIfEnabled {

    var tryCommit = true
    var commitVersion = attemptVersion
    var attemptNumber = 0
    recordDeltaOperation(deltaLog, "delta.commit.allAttempts") {
      while (tryCommit) {
        try {
          if (attemptNumber == 0) {
            doCommit(commitVersion, actions, attemptNumber, isolationLevel)
          } else if (attemptNumber > spark.conf.get(DeltaSQLConf.DELTA_MAX_RETRY_COMMIT_ATTEMPTS)) {
            val totalCommitAttemptTime = clock.getTimeMillis() - commitAttemptStartTime
            throw DeltaErrors.maxCommitRetriesExceededException(
              attemptNumber,
              commitVersion,
              attemptVersion,
              actions.length,
              totalCommitAttemptTime)
          } else recordDeltaOperation(deltaLog, "delta.commit.retry") {
            commitVersion = checkForConflicts(commitVersion, actions, attemptNumber, isolationLevel)
            doCommit(commitVersion, actions, attemptNumber, isolationLevel)
          }
          tryCommit = false
        } catch {
          case _: FileAlreadyExistsException => attemptNumber += 1
        }
      }
      commitVersion
    }
  }

  /**
   * Commit `actions` using `attemptVersion` version number. Throws a FileAlreadyExistsException
   * if any conflicts are detected.
   *
   * @return the real version that was committed.
   */
  protected def doCommit(
      attemptVersion: Long,
      actions: Seq[Action],
      attemptNumber: Int,
      isolationLevel: IsolationLevel): Long = {
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

    spark.sessionState.conf.setConf(
      DeltaSQLConf.DELTA_LAST_COMMIT_VERSION_IN_SESSION,
      Some(attemptVersion))

    val commitTime = System.nanoTime()
    val postCommitSnapshot = deltaLog.update()

    if (postCommitSnapshot.version < attemptVersion) {
      recordDeltaEvent(deltaLog, "delta.commit.inconsistentList", data = Map(
        "committedVersion" -> attemptVersion,
        "currentVersion" -> postCommitSnapshot.version
      ))
      throw new IllegalStateException(
        s"The committed version is $attemptVersion " +
          s"but the current version is ${postCommitSnapshot.version}.")
    }

    // Post stats
    var numAbsolutePaths = 0
    val distinctPartitions = new mutable.HashSet[Map[String, String]]
    val adds = actions.collect {
      case a: AddFile =>
        if (a.pathAsUri.isAbsolute) numAbsolutePaths += 1
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
      numCdcFiles = 0,
      cdcBytesNew = 0,
      protocol = postCommitSnapshot.protocol,
      info = Option(commitInfo).map(_.copy(readVersion = None, isolationLevel = None)).orNull,
      newMetadata = newMetadata,
      numAbsolutePathsInAdd = numAbsolutePaths,
      numDistinctPartitionsInAdd = distinctPartitions.size,
      isolationLevel = null)
    recordDeltaEvent(deltaLog, "delta.commit.stats", data = stats)

    attemptVersion
  }

  /**
   * Looks at actions that have happened since the txn started and checks for logical
   * conflicts with the read/writes. If no conflicts are found return the commit version to attempt
   * next.
   */
  protected def checkForConflicts(
      checkVersion: Long,
      actions: Seq[Action],
      attemptNumber: Int,
      commitIsolationLevel: IsolationLevel): Long = recordDeltaOperation(
        deltaLog,
        "delta.commit.retry.conflictCheck",
        tags = Map(TAG_LOG_STORE_CLASS -> deltaLog.store.getClass.getName)) {

    import _spark.implicits._

    val nextAttemptVersion = getNextAttemptVersion(checkVersion)
    (checkVersion until nextAttemptVersion).foreach { version =>
      val totalCheckAndRetryTime = clock.getTimeMillis() - commitAttemptStartTime
      val baseLog = s" Version: $version Attempt: $attemptNumber Time: $totalCheckAndRetryTime ms"
      logInfo("Checking for conflict" + baseLog)

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
      val actionsCollectionCompleteLog =
        s"Found ${metadataUpdates.length} metadata, ${removedFiles.length} removes, " +
           s"${changedDataAddedFiles.length + blindAppendAddedFiles.length} adds"
      logInfo(actionsCollectionCompleteLog + baseLog)

      // If the log protocol version was upgraded, make sure we are still okay.
      // Fail the transaction if we're trying to upgrade protocol ourselves.
      if (protocol.nonEmpty) {
        protocol.foreach { p =>
          deltaLog.protocolRead(p)
          deltaLog.protocolWrite(p)
        }
        actions.foreach {
          case Protocol(_, _) => throw DeltaErrors.protocolChangedException(commitInfo)
          case _ =>
        }
      }

      // Fail if the metadata is different than what the txn read.
      if (metadataUpdates.nonEmpty) {
        throw DeltaErrors.metadataChangedException(commitInfo)
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
            // The transaction was made by an older version which did not set `isBlindAppend` flag
            // So even if it looks like an append, we don't know for sure if it was a blind append
            // or not. So we suggest them to upgrade all there workloads to latest version.
            Some(
              "Upgrading all your concurrent writers to use the latest Delta Lake may " +
                "avoid this error. Please upgrade and then retry this operation again.")
          } else None
        throw DeltaErrors.concurrentAppendException(
          commitInfo,
          predicatesMatchingAddedFiles.head,
          retryMsg)
      }

      // Fail if files have been deleted that the txn read.
      val readFilePaths = readFiles.map(f => f.path -> f.partitionValues).toMap
      val deleteReadOverlap = removedFiles.find(r => readFilePaths.contains(r.path))
      if (deleteReadOverlap.nonEmpty) {
        val filePath = deleteReadOverlap.get.path
        val partition = getPrettyPartitionMessage(readFilePaths(filePath))
        throw DeltaErrors.concurrentDeleteReadException(commitInfo, s"$filePath in $partition")
      }
      if (removedFiles.nonEmpty && readTheWholeTable) {
        val filePath = removedFiles.head.path
        throw DeltaErrors.concurrentDeleteReadException(commitInfo, s"$filePath")
      }

      // Fail if a file is deleted twice.
      val txnDeletes = actions.collect { case r: RemoveFile => r }.map(_.path).toSet
      val deleteOverlap = removedFiles.map(_.path).toSet intersect txnDeletes
      if (deleteOverlap.nonEmpty) {
        throw DeltaErrors.concurrentDeleteDeleteException(commitInfo, deleteOverlap.head)
      }

      // Fail if idempotent transactions have conflicted.
      val txnOverlap = txns.map(_.appId).toSet intersect readTxn.toSet
      if (txnOverlap.nonEmpty) {
        throw DeltaErrors.concurrentTransactionException(commitInfo)
      }

      logInfo("Completed checking for conflicts" + baseLog)
    }

    logInfo(s"No logical conflicts with deltas [$checkVersion, $nextAttemptVersion), retrying.")
    nextAttemptVersion
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
