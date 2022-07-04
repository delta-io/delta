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

import java.nio.file.FileAlreadyExistsException
import java.util.{ConcurrentModificationException, UUID}
import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashSet}
import scala.util.control.NonFatal

import com.databricks.spark.util.TagDefinitions.TAG_LOG_STORE_CLASS
import org.apache.spark.sql.delta.DeltaOperations.Operation
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.files._
import org.apache.spark.sql.delta.hooks.{GenerateSymlinkManifest, PostCommitHook}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.{SchemaMergingUtils, SchemaUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.{DeltaScan, DeltaScanGenerator, DeltaScanGeneratorBase, FileSizeHistogram}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.types.StructType
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
  fsWriteDurationMs: Long,
  stateReconstructionDurationMs: Long,
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
  /** The size of the newly committed (usually json) file */
  commitSizeBytes: Long,
  /** The size of the checkpoint committed, if present */
  checkpointSizeBytes: Long,
  totalCommitsSizeSinceLastCheckpoint: Long,
  /** Will we attempt a checkpoint after this commit is completed */
  checkpointAttempt: Boolean,
  info: CommitInfo,
  newMetadata: Option[Metadata],
  numAbsolutePathsInAdd: Int,
  numDistinctPartitionsInAdd: Int,
  numPartitionColumnsInTable: Int,
  isolationLevel: String,
  fileSizeHistogram: Option[FileSizeHistogram] = None,
  addFilesHistogram: Option[FileSizeHistogram] = None,
  removeFilesHistogram: Option[FileSizeHistogram] = None,
  txnId: Option[String] = None
)

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
    this(deltaLog, deltaLog.update())
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
      throw DeltaErrors.activeTransactionAlreadySet()
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
trait OptimisticTransactionImpl extends TransactionalWrite
  with SQLMetricsReporting
  with DeltaScanGenerator
  with DeltaLogging {

  import org.apache.spark.sql.delta.util.FileNames._

  val deltaLog: DeltaLog
  val snapshot: Snapshot
  implicit val clock: Clock

  protected def spark = SparkSession.active

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

  /** The transaction start time. */
  protected val txnStartNano = System.nanoTime()

  override val snapshotToScan: Snapshot = snapshot

  /**
   * Tracks the first-access snapshots of other Delta logs read by this transaction.
   * The snapshots are keyed by the log's unique id.
   */
  protected var readSnapshots = new java.util.concurrent.ConcurrentHashMap[(String, Path), Snapshot]

  /** The transaction commit start time. */
  protected var commitStartNano = -1L

  /** The transaction commit end time. */
  protected var commitEndNano = -1L;

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

  /** Unique identifier for the transaction */
  val txnId = UUID.randomUUID().toString

  /** Whether to check unsupported data type when updating the table schema */
  protected var checkUnsupportedDataType: Boolean =
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_SCHEMA_TYPE_CHECK)


  /** The end to end execution time of this transaction. */
  def txnExecutionTimeMs: Option[Long] = if (commitEndNano == -1) {
    None
  } else {
    Some(NANOSECONDS.toMillis((commitEndNano - txnStartNano)))
  }

  /** Gets the stats collector for the table at the snapshot this transaction has. */
  def statsCollector: Column = snapshot.statsCollector

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
    updateMetadataInternal(_metadata)
  }

  /**
   * Do the actual checks and works to update the metadata and save it into the `newMetadata`
   * field, which will be added to the actions to commit in [[prepareCommit]].
   */
  protected def updateMetadataInternal(_metadata: Metadata): Unit = {
    var latestMetadata = _metadata
    if (readVersion == -1 || isCreatingNewTable) {
      latestMetadata = withGlobalConfigDefaults(latestMetadata)
      isCreatingNewTable = true
    }
    val protocolBeforeUpdate = protocol
    // The `.schema` cannot be generated correctly unless the column mapping metadata is correctly
    // filled for all the fields. Therefore, the column mapping changes need to happen first.
    latestMetadata = DeltaColumnMapping.verifyAndUpdateMetadataChange(
      deltaLog,
      protocolBeforeUpdate,
      snapshot.metadata,
      latestMetadata,
      isCreatingNewTable)

    if (latestMetadata.schemaString != null) {
      // Replace CHAR and VARCHAR with StringType
      var schema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(latestMetadata.schema)
      latestMetadata = latestMetadata.copy(schemaString = schema.json)
    }

    latestMetadata = if (snapshot.metadata.schemaString == latestMetadata.schemaString) {
      // Shortcut when the schema hasn't changed to avoid generating spurious schema change logs.
      // It's fine if two different but semantically equivalent schema strings skip this special
      // case - that indicates that something upstream attempted to do a no-op schema change, and
      // we'll just end up doing a bit of redundant work in the else block.
      latestMetadata
    } else {
      val fixedSchema = SchemaUtils.removeUnenforceableNotNullConstraints(
        latestMetadata.schema, spark.sessionState.conf).json
      latestMetadata.copy(schemaString = fixedSchema)
    }


    if (isCreatingNewTable) {
      // Check for the new protocol version after the removal of the unenforceable not null
      // constraints
      newProtocol = Some(Protocol.forNewTable(spark, latestMetadata))
    }
    // Remove the protocol version properties
    val configs = latestMetadata.configuration.filter {
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
    latestMetadata = if (isCreatingNewTable) {
      // Creating a new table will drop all existing data, so we don't need to fix the old
      // metadata.
      latestMetadata
    } else {
      // This is not a new table. The new schema may be merged from the existing schema. We
      // decide whether we should keep the Generated or IDENTITY columns by checking whether the
      // protocol satisfies the requirements.
      val (keepGeneratedColumns, keepIdentityColumns) =
        ColumnWithDefaultExprUtils.satisfyProtocol(protocolBeforeUpdate)
      if (keepIdentityColumns) {
        // If a protocol satisfies IDENTITY column requirement, it must also satisfies Generated
        // column requirement because IDENTITY columns are introduced after Generated columns. In
        // this case we do nothing here.
        assert(keepGeneratedColumns)
        latestMetadata
      } else {
        // As the protocol doesn't match, this table is created by an old version that doesn't
        // support generated columns or identity columns. We should remove the generation
        // expressions to fix the schema to avoid bumping the writer version incorrectly.
        val newSchema = ColumnWithDefaultExprUtils.removeDefaultExpressions(latestMetadata
          .schema, keepGeneratedColumns)
        if (newSchema ne latestMetadata.schema) {
          latestMetadata.copy(schemaString = newSchema.json)
        } else {
          latestMetadata
        }
      }
    }
    latestMetadata = latestMetadata.copy(configuration = configs)
    verifyNewMetadata(latestMetadata)
    logInfo(s"Updated metadata from ${newMetadata.getOrElse("-")} to $latestMetadata")

    newMetadata = Some(latestMetadata)
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
    isCreatingNewTable = true
    updateMetadata(metadata)
  }

  protected def verifyNewMetadata(metadata: Metadata): Unit = {
    assert(!CharVarcharUtils.hasCharVarchar(metadata.schema),
      "The schema in Delta log should not contain char/varchar type.")
    SchemaMergingUtils.checkColumnNameDuplication(metadata.schema, "in the metadata update")
    if (metadata.columnMappingMode == NoMapping) {
      SchemaUtils.checkSchemaFieldNames(metadata.dataSchema, metadata.columnMappingMode)
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
    } else {
      DeltaColumnMapping.checkColumnIdAndPhysicalNameAssignments(
        metadata.schema, metadata.columnMappingMode)
    }

    if (GeneratedColumn.hasGeneratedColumns(metadata.schema)) {
      recordDeltaOperation(deltaLog, "delta.generatedColumns.check") {
        GeneratedColumn.validateGeneratedColumns(spark, metadata.schema)
      }
      recordDeltaEvent(deltaLog, "delta.generatedColumns.definition")
    }

    if (checkUnsupportedDataType) {
      val unsupportedTypes = SchemaUtils.findUnsupportedDataTypes(metadata.schema)
      if (unsupportedTypes.nonEmpty) {
        throw DeltaErrors.unsupportedDataTypes(unsupportedTypes.head, unsupportedTypes.tail: _*)
      }
    }

    val needsProtocolUpdate = Protocol.checkProtocolRequirements(spark, metadata, protocol)
    if (needsProtocolUpdate.isDefined) {
      newProtocol = needsProtocolUpdate
    }
  }

  /**
   * Returns the [[DeltaScanGenerator]] for the given log, which will be used to generate
   * [[DeltaScan]]s. Every time this method is called on a log, the returned generator
   * generator will read a snapshot that is pinned on the first access for that log.
   *
   * Internally, if the given log is the same as the log associated with this
   * transaction, then it returns this transaction, otherwise it will return a snapshot of
   * given log
   */
  def getDeltaScanGenerator(index: TahoeLogFileIndex): DeltaScanGenerator = {
    if (index.deltaLog.isSameLogAs(deltaLog)) {
      this
    } else {
      if (spark.conf.get(DeltaSQLConf.DELTA_SNAPSHOT_ISOLATION)) {
        readSnapshots.computeIfAbsent(index.deltaLog.compositeId, _ => {
          // Will be called only when the log is accessed the first time
          index.getSnapshot
        })
      } else {
        index.getSnapshot
      }
    }
  }

  /** Returns a[[DeltaScan]] based on the given filters and projections. */
  override def filesForScan(projection: Seq[Attribute], filters: Seq[Expression]): DeltaScan = {
    val scan = snapshot.filesForScan(projection, filters)
    val partitionFilters = filters.filter { f =>
      DeltaTableUtils.isPredicatePartitionColumnsOnly(f, metadata.partitionColumns, spark)
    }
    readPredicates += partitionFilters.reduceLeftOption(And).getOrElse(Literal(true))
    readFiles ++= scan.files
    scan
  }

  override def filesWithStatsForScan(partitionFilters: Seq[Expression]): DataFrame = {
    val metadata = snapshot.filesWithStatsForScan(partitionFilters)
    readPredicates += partitionFilters.reduceLeftOption(And).getOrElse(Literal(true))
    withFilesRead(filterFiles(partitionFilters))
    metadata
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

  /** Returns files within the given partitions. */
  def filterFiles(partitions: Set[Map[String, String]]): Seq[AddFile] = {
    import org.apache.spark.sql.functions.{array, col}
    val partitionValues = partitions.map { partition =>
      metadata.physicalPartitionColumns.map(partition).toArray
    }
    val predicate = array(metadata.partitionColumns.map(col): _*)
      .isInCollection(partitionValues)
      .expr
    filterFiles(Seq(predicate))
  }

  /** Mark the entire table as tainted by this transaction. */
  def readWholeTable(): Unit = {
    readPredicates += Literal.TrueLiteral
    readTheWholeTable = true
  }

  /** Mark the given files as read within this transaction. */
  def withFilesRead(files: Seq[AddFile]): Unit = {
    readFiles ++= files
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
   * Checks if the new schema contains any CDC columns (which is invalid) and throws the appropriate
   * error
   */
  protected def performCdcMetadataCheck(): Unit = {
    if (newMetadata.nonEmpty) {
      if (CDCReader.isCDCEnabledOnTable(newMetadata.get)) {
        val schema = newMetadata.get.schema.fieldNames
        val reservedColumnsUsed = CDCReader.cdcReadSchema(new StructType()).fieldNames
          .intersect(schema)
        if (reservedColumnsUsed.length > 0) {
          if (!CDCReader.isCDCEnabledOnTable(snapshot.metadata)) {
            // cdc was not enabled previously but reserved columns are present in the new schema.
            throw DeltaErrors.tableAlreadyContainsCDCColumns(reservedColumnsUsed)
          } else {
            // cdc was enabled but reserved columns are present in the new metadata.
            throw DeltaErrors.cdcColumnsInData(reservedColumnsUsed)
          }
        }
      }
    }
  }

  /**
   * We want to future-proof and explicitly block any occurrences of
   * - table has CDC enabled and there are FileActions to write, AND
   * - table has column mapping enabled and there is a column mapping related metadata action
   *
   * This is because the semantics for this combination of features and file changes is undefined.
   */
  private def performCdcColumnMappingCheck(
      actions: Seq[Action],
      op: DeltaOperations.Operation): Unit = {
    if (newMetadata.nonEmpty) {
      val _newMetadata = newMetadata.get
      val _currentMetadata = snapshot.metadata

      val cdcEnabled = CDCReader.isCDCEnabledOnTable(_newMetadata)

      val columnMappingEnabled = _newMetadata.columnMappingMode != NoMapping

      val isColumnMappingUpgrade = DeltaColumnMapping.isColumnMappingUpgrade(
        oldMode = _currentMetadata.columnMappingMode,
        newMode = _newMetadata.columnMappingMode
      )

      def dropColumnOp: Boolean = DeltaColumnMapping.isDropColumnOperation(
        _newMetadata, _currentMetadata)

      def renameColumnOp: Boolean = DeltaColumnMapping.isRenameColumnOperation(
        _newMetadata, _currentMetadata)

      def columnMappingChange: Boolean = isColumnMappingUpgrade || dropColumnOp || renameColumnOp

      def existsFileActions: Boolean = actions.exists { _.isInstanceOf[FileAction] }

      if (cdcEnabled && columnMappingEnabled && columnMappingChange && existsFileActions) {
        throw DeltaErrors.blockColumnMappingAndCdcOperation(op)
      }
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

    val (version, actualCommittedActions) = try {
      // Check for CDC metadata columns
      performCdcMetadataCheck()

      // Try to commit at the next version.
      val preparedActions = prepareCommit(actions, op)

      // Find the isolation level to use for this commit
      val isolationLevelToUse = getIsolationLevelToUse(preparedActions, op)

      if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_COMMIT_INFO_ENABLED)) {
        val isBlindAppend = {
          val dependsOnFiles = readPredicates.nonEmpty || readFiles.nonEmpty
          val onlyAddFiles =
            preparedActions.collect { case f: FileAction => f }.forall(_.isInstanceOf[AddFile])
          onlyAddFiles && !dependsOnFiles
        }

        commitInfo = CommitInfo(
          clock.getTimeMillis(),
          op.name,
          op.jsonEncodedValues,
          Map.empty,
          Some(readVersion).filter(_ >= 0),
          Option(isolationLevelToUse.toString),
          Some(isBlindAppend),
          getOperationMetrics(op),
          getUserMetadata(op),
          tags = None,
          txnId = Some(txnId))
      }

      val currentTransactionInfo = new CurrentTransactionInfo(
        txnId = txnId,
        readPredicates = readPredicates.toSeq,
        readFiles = readFiles.toSet,
        readWholeTable = readTheWholeTable,
        readAppIds = readTxn.toSet,
        metadata = metadata,
        actions = preparedActions,
        readSnapshot = snapshot,
        commitInfo = Option(commitInfo))

      // Register post-commit hooks if any
      lazy val hasFileActions = preparedActions.exists {
        case _: FileAction => true
        case _ => false
      }
      if (DeltaConfigs.SYMLINK_FORMAT_MANIFEST_ENABLED.fromMetaData(metadata) && hasFileActions) {
        registerPostCommitHook(GenerateSymlinkManifest)
      }

      commitAttemptStartTime = clock.getTimeMillis()
      val (commitVersion, updatedCurrentTransactionInfo, needsCheckpoint) =
        doCommitRetryIteratively(snapshot.version + 1, currentTransactionInfo, isolationLevelToUse)
      logInfo(s"Committed delta #$commitVersion to ${deltaLog.logPath}")
      postCommit(commitVersion, needsCheckpoint)
      (commitVersion, updatedCurrentTransactionInfo.actions)
    } catch {
      case e: DeltaConcurrentModificationException =>
        recordDeltaEvent(deltaLog, "delta.commit.conflict." + e.conflictType)
        throw e
      case NonFatal(e) =>
        recordDeltaEvent(
          deltaLog, "delta.commit.failure", data = Map("exception" -> Utils.exceptionString(e)))
        throw e
    }

    runPostCommitHooks(version, actualCommittedActions)

    version
  }

  /**
   * Create a large commit on the Delta log by directly writing an iterator of FileActions to the
   * LogStore. This function only commits the next possible version and will not check whether the
   * commit is retry-able. If the next version has already been committed, then this function
   * will fail.
   * This bypasses all optimistic concurrency checks. We assume that transaction conflicts should be
   * rare because this method is typically used to create new tables (e.g. CONVERT TO DELTA) or
   * apply some commands which rarely receive other transactions (e.g. CLONE/RESTORE).
   * In addition, the expectation is that the list of actions performed by the transaction
   * remains an iterator and is never materialized, given the nature of a large commit potentially
   * touching many files.
   */
  def commitLarge(
    spark: SparkSession,
    actions: Iterator[Action],
    op: DeltaOperations.Operation,
    context: Map[String, String],
    metrics: Map[String, String]): Long = {
    commitStartNano = System.nanoTime()
    val attemptVersion = readVersion + 1
    try {
      val commitInfo = CommitInfo(
        time = clock.getTimeMillis(),
        operation = op.name,
        operationParameters = op.jsonEncodedValues,
        context,
        readVersion = Some(readVersion),
        isolationLevel = Some(Serializable.toString),
        isBlindAppend = Some(false),
        Some(metrics),
        userMetadata = getUserMetadata(op),
        tags = None,
        txnId = Some(txnId))

      val extraActions = Seq(commitInfo, metadata)
      // We don't expect commits to have more than 2 billion actions
      var commitSize: Int = 0
      var numAbsolutePaths: Int = 0
      var numAddFiles: Int = 0
      var numRemoveFiles: Int = 0
      var bytesNew: Long = 0L
      var addFilesHistogram: Option[FileSizeHistogram] = None
      var removeFilesHistogram: Option[FileSizeHistogram] = None
      var allActions = (extraActions.toIterator ++ actions).map { action =>
        commitSize += 1
        action match {
          case a: AddFile =>
            numAddFiles += 1
            if (a.pathAsUri.isAbsolute) numAbsolutePaths += 1
            if (a.dataChange) bytesNew += a.size
            addFilesHistogram.foreach(_.insert(a.size))
          case r: RemoveFile =>
            numRemoveFiles += 1
            removeFilesHistogram.foreach(_.insert(r.size.getOrElse(0L)))
          case _ =>
        }
        action
      }
      if (readVersion < 0) {
        deltaLog.createLogDirectory()
      }
      val fsWriteStartNano = System.nanoTime()
      val jsonActions = allActions.map(_.json)
      deltaLog.store.write(
        deltaFile(deltaLog.logPath, attemptVersion),
        jsonActions,
        overwrite = false,
        deltaLog.newDeltaHadoopConf())

      spark.sessionState.conf.setConf(
        DeltaSQLConf.DELTA_LAST_COMMIT_VERSION_IN_SESSION,
        Some(attemptVersion))
      commitEndNano = System.nanoTime()
      committed = true
      val postCommitSnapshot =
        updateAndCheckpoint(spark, deltaLog, commitSize, attemptVersion, txnId)
      val postCommitReconstructionTime = System.nanoTime()
      var stats = CommitStats(
        startVersion = readVersion,
        commitVersion = attemptVersion,
        readVersion = postCommitSnapshot.version,
        txnDurationMs = NANOSECONDS.toMillis(commitEndNano - txnStartTimeNs),
        commitDurationMs = NANOSECONDS.toMillis(commitEndNano - commitStartNano),
        fsWriteDurationMs = NANOSECONDS.toMillis(commitEndNano - fsWriteStartNano),
        stateReconstructionDurationMs =
          NANOSECONDS.toMillis(postCommitReconstructionTime - commitEndNano),
        numAdd = numAddFiles,
        numRemove = numRemoveFiles,
        bytesNew = bytesNew,
        numFilesTotal = postCommitSnapshot.numOfFiles,
        sizeInBytesTotal = postCommitSnapshot.sizeInBytes,
        numCdcFiles = 0,
        cdcBytesNew = 0,
        protocol = postCommitSnapshot.protocol,
        commitSizeBytes = jsonActions.map(_.size).sum,
        checkpointSizeBytes = postCommitSnapshot.checkpointSizeInBytes(),
        totalCommitsSizeSinceLastCheckpoint = 0L,
        checkpointAttempt = true,
        info = Option(commitInfo).map(_.copy(readVersion = None, isolationLevel = None)).orNull,
        newMetadata = Some(metadata),
        numAbsolutePathsInAdd = numAbsolutePaths,
        numDistinctPartitionsInAdd = -1, // not tracking distinct partitions as of now
        numPartitionColumnsInTable = postCommitSnapshot.metadata.partitionColumns.size,
        isolationLevel = Serializable.toString,
        txnId = Some(txnId))

      recordDeltaEvent(deltaLog, "delta.commit.stats", data = stats)
      // NOTE: We don't pass in the sequence of actions to runPostCommitHooks because commitLarge
      //       takes in an iterator of actions as a conscious choice, as the large commit may
      //       result in many FileActions we'd have to materialize in memory. All post commit hooks
      //       registered for these commits should be agnostic to the list of actions.
      runPostCommitHooks(attemptVersion, Seq.empty[Action])
      attemptVersion
    } catch {
      case e: java.nio.file.FileAlreadyExistsException =>
        recordDeltaEvent(
          deltaLog,
          "delta.commitLarge.failure",
          data = Map("exception" -> Utils.exceptionString(e), "operation" -> op.name))
        // Actions of a commit which went in before ours
        val logs = deltaLog.store.readAsIterator(
          deltaFile(deltaLog.logPath, attemptVersion),
          deltaLog.newDeltaHadoopConf())
        try {
          val winningCommitActions = logs.map(Action.fromJson)
          val commitInfo = winningCommitActions.collectFirst { case a: CommitInfo => a }
            .map(ci => ci.copy(version = Some(attemptVersion)))
          throw DeltaErrors.concurrentWriteException(commitInfo)
        } finally {
          logs.close()
        }

      case NonFatal(e) =>
        recordDeltaEvent(
          deltaLog,
          "delta.commitLarge.failure",
          data = Map("exception" -> Utils.exceptionString(e), "operation" -> op.name))
        throw e
    }
  }

  /** Update the table now that the commit has been made, and write a checkpoint. */
  protected def updateAndCheckpoint(
    spark: SparkSession,
    deltaLog: DeltaLog,
    commitSize: Int,
    attemptVersion: Long,
    txnId: String): Snapshot = {
    val currentSnapshot = deltaLog.update()
    if (currentSnapshot.version != attemptVersion) {
      throw DeltaErrors.invalidCommittedVersion(attemptVersion, currentSnapshot.version)
    }

    logInfo(s"Committed delta #$attemptVersion to ${deltaLog.logPath}. Wrote $commitSize actions.")

    deltaLog.checkpoint(currentSnapshot)
    currentSnapshot
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


    // Block future cases of CDF + Column Mapping changes + file changes
    // This check requires having called
    // DeltaColumnMapping.checkColumnIdAndPhysicalNameAssignments which is done in the
    // `verifyNewMetadata` call above.
    performCdcColumnMappingCheck(finalActions, op)

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

    val partitionColumns = metadata.physicalPartitionSchema.fieldNames.toSet
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

  // Returns the isolation level to use for committing the transaction
  protected def getIsolationLevelToUse(
      preparedActions: Seq[Action], op: DeltaOperations.Operation): IsolationLevel = {
    val isolationLevelToUse = if (canDowngradeToSnapshotIsolation(preparedActions, op)) {
      SnapshotIsolation
    } else {
      getDefaultIsolationLevel()
    }
    isolationLevelToUse
  }

  protected def canDowngradeToSnapshotIsolation(
      preparedActions: Seq[Action], op: DeltaOperations.Operation): Boolean = {

    var dataChanged = false
    var hasNonFileActions = false
    preparedActions.foreach {
      case f: FileAction =>
        if (f.dataChange) {
          dataChanged = true
        }
      case _ =>
        hasNonFileActions = true
    }
    val noDataChanged = !dataChanged

    if (hasNonFileActions) {
      // if Non-file-actions are present (e.g. METADATA etc.), then don't downgrade the isolation
      // level to SnapshotIsolation.
      return false
    }

    val defaultIsolationLevel = getDefaultIsolationLevel()
    // Note-1: For no-data-change transactions such as OPTIMIZE/Auto Compaction/ZorderBY, we can
    // change the isolation level to SnapshotIsolation. SnapshotIsolation allows reduced conflict
    // detection by skipping the
    // [[ConflictChecker.checkForAddedFilesThatShouldHaveBeenReadByCurrentTxn]] check i.e.
    // don't worry about concurrent appends.
    // Note-2:
    // We can also use SnapshotIsolation for empty transactions. e.g. consider a commit:
    // t0 - Initial state of table
    // t1 - Q1, Q2 starts
    // t2 - Q1 commits
    // t3 - Q2 is empty and wants to commit.
    // In this scenario, we can always allow Q2 to commit without worrying about new files
    // generated by Q1.
    // The final order which satisfies both Serializability and WriteSerializability is: Q2, Q1
    // Note that Metadata only update transactions shouldn't be considered empty. If Q2 above has
    // a Metadata update (say schema change/identity column high watermark update), then Q2 can't
    // be moved above Q1 in the final SERIALIZABLE order. This is because if Q2 is moved above Q1,
    // then Q1 should see the updates from Q2 - which actually didn't happen.

    val allowFallbackToSnapshotIsolation = defaultIsolationLevel match {
      case Serializable => noDataChanged
      case WriteSerializable => noDataChanged && !op.changesData
      case _ => false // This case should never happen
    }
    allowFallbackToSnapshotIsolation
  }

  protected def getDefaultIsolationLevel(): IsolationLevel = {
    Serializable
  }

  /**
   * Returns true if we should checkpoint the version that has just been committed.
   */
  protected def shouldCheckpoint(committedVersion: Long, latestSnapshot: Snapshot): Boolean = {
    committedVersion != 0 && committedVersion % deltaLog.checkpointInterval == 0
  }

  /** Perform post-commit operations */
  protected def postCommit(commitVersion: Long, needsCheckpoint: Boolean): Unit = {
    committed = true
    if (needsCheckpoint) {
      // We checkpoint the version to be committed to so that no two transactions will checkpoint
      // the same version.
      deltaLog.checkpoint(deltaLog.getSnapshotAt(commitVersion))
    }
  }

  private[delta] def isCommitLockEnabled: Boolean = {
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_COMMIT_LOCK_ENABLED).getOrElse(
      deltaLog.store.isPartialWriteVisible(deltaLog.logPath, deltaLog.newDeltaHadoopConf()))
  }

  private def lockCommitIfEnabled[T](body: => T): T = {
    if (isCommitLockEnabled) {
      deltaLog.lockInterruptibly(body)
    } else {
      body
    }
  }

  /**
   * Commit the txn represented by `currentTransactionInfo` using `attemptVersion` version number.
   * If there are any conflicts that are found, we will retry a fixed number of times.
   *
   * @return the real version that was committed, txn info, and if a checkpoint should be made.
   */
  protected def doCommitRetryIteratively(
      attemptVersion: Long,
      currentTransactionInfo: CurrentTransactionInfo,
      isolationLevel: IsolationLevel
  ): (Long, CurrentTransactionInfo, Boolean) = lockCommitIfEnabled {

    var tryCommit = true
    var commitVersion = attemptVersion
    var updatedCurrentTransactionInfo = currentTransactionInfo
    var attemptNumber = 0
    var needsCheckpoint = false
    recordDeltaOperation(deltaLog, "delta.commit.allAttempts") {
      while (tryCommit) {
        try {
          if (attemptNumber == 0) {
            needsCheckpoint =
              doCommit(commitVersion, updatedCurrentTransactionInfo, attemptNumber, isolationLevel)
          } else if (attemptNumber > spark.conf.get(DeltaSQLConf.DELTA_MAX_RETRY_COMMIT_ATTEMPTS)) {
            val totalCommitAttemptTime = clock.getTimeMillis() - commitAttemptStartTime
            throw DeltaErrors.maxCommitRetriesExceededException(
              attemptNumber,
              commitVersion,
              attemptVersion,
              updatedCurrentTransactionInfo.finalActionsToCommit.length,
              totalCommitAttemptTime)
          } else recordDeltaOperation(deltaLog, "delta.commit.retry") {
            val (newCommitVersion, newCurrentTransactionInfo) = checkForConflicts(
              commitVersion, updatedCurrentTransactionInfo, attemptNumber, isolationLevel)
            commitVersion = newCommitVersion
            updatedCurrentTransactionInfo = newCurrentTransactionInfo
            needsCheckpoint =
              doCommit(commitVersion, updatedCurrentTransactionInfo, attemptNumber, isolationLevel)
          }
          tryCommit = false
        } catch {
          case _: FileAlreadyExistsException => attemptNumber += 1
        }
      }
      (commitVersion, updatedCurrentTransactionInfo, needsCheckpoint)
    }
  }


  /**
   * Commit `actions` using `attemptVersion` version number. Throws a FileAlreadyExistsException
   * if any conflicts are detected.
   *
   * @return if a checkpoint should be made once this commit is written
   */
  protected def doCommit(
      attemptVersion: Long,
      currentTransactionInfo: CurrentTransactionInfo,
      attemptNumber: Int,
      isolationLevel: IsolationLevel): Boolean = {
    val actions = currentTransactionInfo.finalActionsToCommit
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

    val fsWriteStartNano = System.nanoTime()
    val jsonActions = actions.map(_.json)
    deltaLog.store.write(
      deltaFile(deltaLog.logPath, attemptVersion),
      jsonActions.toIterator,
      overwrite = false,
      deltaLog.newDeltaHadoopConf())

    spark.sessionState.conf.setConf(
      DeltaSQLConf.DELTA_LAST_COMMIT_VERSION_IN_SESSION,
      Some(attemptVersion))

    commitEndNano = System.nanoTime()
    val postCommitSnapshot = deltaLog.update()
    val postCommitReconstructionTime = System.nanoTime()

    if (postCommitSnapshot.version < attemptVersion) {
      recordDeltaEvent(deltaLog, "delta.commit.inconsistentList", data = Map(
        "committedVersion" -> attemptVersion,
        "currentVersion" -> postCommitSnapshot.version
      ))
      throw DeltaErrors.invalidCommittedVersion(attemptVersion, postCommitSnapshot.version)
    }

    // Post stats
    // Here, we efficiently calculate various stats (number of each different action, number of
    // bytes per action, etc.) by iterating over all actions, case matching by type, and updating
    // variables. This is more efficient than a functional approach.
    var numAbsolutePaths = 0
    val distinctPartitions = new mutable.HashSet[Map[String, String]]
    var bytesNew: Long = 0L
    var numAdd: Int = 0
    var numRemove: Int = 0
    var numCdcFiles: Int = 0
    var cdcBytesNew: Long = 0L
    actions.foreach {
      case a: AddFile =>
        numAdd += 1
        if (a.pathAsUri.isAbsolute) numAbsolutePaths += 1
        distinctPartitions += a.partitionValues
        if (a.dataChange) bytesNew += a.size
      case r: RemoveFile =>
        numRemove += 1
      case c: AddCDCFile =>
        numCdcFiles += 1
        cdcBytesNew += c.size
      case _ =>
    }

    val needsCheckpoint = shouldCheckpoint(attemptVersion, postCommitSnapshot)
    val stats = CommitStats(
      startVersion = snapshot.version,
      commitVersion = attemptVersion,
      readVersion = postCommitSnapshot.version,
      txnDurationMs = NANOSECONDS.toMillis(commitEndNano - txnStartNano),
      commitDurationMs = NANOSECONDS.toMillis(commitEndNano - commitStartNano),
      fsWriteDurationMs = NANOSECONDS.toMillis(commitEndNano - fsWriteStartNano),
      stateReconstructionDurationMs =
        NANOSECONDS.toMillis(postCommitReconstructionTime - commitEndNano),
      numAdd = numAdd,
      numRemove = numRemove,
      bytesNew = bytesNew,
      numFilesTotal = postCommitSnapshot.numOfFiles,
      sizeInBytesTotal = postCommitSnapshot.sizeInBytes,
      numCdcFiles = numCdcFiles,
      cdcBytesNew = cdcBytesNew,
      protocol = postCommitSnapshot.protocol,
      commitSizeBytes = jsonActions.map(_.size).sum,
      checkpointSizeBytes = postCommitSnapshot.checkpointSizeInBytes(),
      totalCommitsSizeSinceLastCheckpoint = postCommitSnapshot.deltaFileSizeInBytes(),
      checkpointAttempt = needsCheckpoint,
      info = Option(commitInfo).map(_.copy(readVersion = None, isolationLevel = None)).orNull,
      newMetadata = newMetadata,
      numAbsolutePathsInAdd = numAbsolutePaths,
      numDistinctPartitionsInAdd = distinctPartitions.size,
      numPartitionColumnsInTable = postCommitSnapshot.metadata.partitionColumns.size,
      isolationLevel = isolationLevel.toString,
      txnId = Some(txnId))
    recordDeltaEvent(deltaLog, "delta.commit.stats", data = stats)

    needsCheckpoint
  }

  /**
   * Looks at actions that have happened since the txn started and checks for logical
   * conflicts with the read/writes. Resolve conflicts and returns a tuple representing
   * the commit version to attempt next and the commit summary which we need to commit.
   */
  protected def checkForConflicts(
      checkVersion: Long,
      currentTransactionInfo: CurrentTransactionInfo,
      attemptNumber: Int,
      commitIsolationLevel: IsolationLevel): (Long, CurrentTransactionInfo) = recordDeltaOperation(
        deltaLog,
        "delta.commit.retry.conflictCheck",
        tags = Map(TAG_LOG_STORE_CLASS -> deltaLog.store.getClass.getName)) {

    val nextAttemptVersion = getNextAttemptVersion(checkVersion)

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

    var updatedCurrentTransactionInfo = currentTransactionInfo
    (checkVersion until nextAttemptVersion).foreach { otherCommitVersion =>
      updatedCurrentTransactionInfo = checkForConflictsAgainstVersion(
        updatedCurrentTransactionInfo,
        otherCommitVersion,
        commitIsolationLevel)
      logInfo(s"$logPrefixStr No conflicts in version $otherCommitVersion, " +
        s"${clock.getTimeMillis() - commitAttemptStartTime} ms since start")
    }

    logInfo(s"$logPrefixStr No conflicts with versions [$checkVersion, $nextAttemptVersion) " +
      s"with current txn having $txnDetailsLogStr, " +
      s"${clock.getTimeMillis() - commitAttemptStartTime} ms since start")
    (nextAttemptVersion, updatedCurrentTransactionInfo)
  }

  protected def checkForConflictsAgainstVersion(
      currentTransactionInfo: CurrentTransactionInfo,
      otherCommitVersion: Long,
      commitIsolationLevel: IsolationLevel): CurrentTransactionInfo = {

    val conflictChecker = new ConflictChecker(
      spark,
      currentTransactionInfo,
      otherCommitVersion,
      commitIsolationLevel)
    conflictChecker.checkConflicts()
  }

  /** Returns the next attempt version given the last attempted version */
  protected def getNextAttemptVersion(previousAttemptVersion: Long): Long = {
    deltaLog.update().version + 1
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

  protected lazy val logPrefix: String = {
    def truncate(uuid: String): String = uuid.split("-").head
    s"[tableId=${truncate(snapshot.metadata.id)},txnId=${truncate(txnId)}] "
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
