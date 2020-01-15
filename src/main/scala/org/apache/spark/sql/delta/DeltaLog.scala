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

// scalastyle:off import.ordering.noEmptyLine
import java.io.{File, FileNotFoundException, IOException}
import java.util.concurrent.{Callable, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

import com.databricks.spark.util.TagDefinitions._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LogStoreProvider
import com.google.common.cache.{CacheBuilder, RemovalListener, RemovalNotification}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Cast, Expression, In, InSet, Literal}
import org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}

/**
 * Used to query the current state of the log as well as modify it by adding
 * new atomic collections of actions.
 *
 * Internally, this class implements an optimistic concurrency control
 * algorithm to handle multiple readers or writers. Any single read
 * is guaranteed to see a consistent snapshot of the table.
 */
class DeltaLog private(
    val logPath: Path,
    val dataPath: Path,
    val clock: Clock)
  extends Checkpoints
  with MetadataCleanup
  with LogStoreProvider
  with VerifyChecksum {

  import org.apache.spark.sql.delta.util.FileNames._


  private lazy implicit val _clock = clock

  @volatile private[delta] var asyncUpdateTask: Future[Unit] = _
  /** The timestamp when the last successful update action is finished. */
  @volatile private var lastUpdateTimestamp = -1L

  protected def spark = SparkSession.active

  /** Used to read and write physical log files and checkpoints. */
  val store = createLogStore(spark)
  /** Direct access to the underlying storage system. */
  private[delta] val fs = logPath.getFileSystem(spark.sessionState.newHadoopConf)

  /** Use ReentrantLock to allow us to call `lockInterruptibly` */
  private val deltaLogLock = new ReentrantLock()

  /** Delta History Manager containing version and commit history. */
  lazy val history = new DeltaHistoryManager(
    this, spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_HISTORY_PAR_SEARCH_THRESHOLD))

  /* --------------- *
   |  Configuration  |
   * --------------- */

  /** Returns the checkpoint interval for this log. Not transactional. */
  def checkpointInterval: Int = DeltaConfigs.CHECKPOINT_INTERVAL.fromMetaData(metadata)

  /**
   * The max lineage length of a Snapshot before Delta forces to build a Snapshot from scratch.
   * Delta will build a Snapshot on top of the previous one if it doesn't see a checkpoint.
   * However, there is a race condition that when two writers are writing at the same time,
   * a writer may fail to pick up checkpoints written by another one, and the lineage will grow
   * and finally cause StackOverflowError. Hence we have to force to build a Snapshot from scratch
   * when the lineage length is too large to avoid hitting StackOverflowError.
   */
  def maxSnapshotLineageLength: Int =
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_MAX_SNAPSHOT_LINEAGE_LENGTH)

  /** How long to keep around logically deleted files before physically deleting them. */
  private[delta] def tombstoneRetentionMillis: Long =
    DeltaConfigs.getMilliSeconds(DeltaConfigs.TOMBSTONE_RETENTION.fromMetaData(metadata))

  // TODO: There is a race here where files could get dropped when increasing the
  // retention interval...
  protected def metadata = if (snapshot == null) Metadata() else snapshot.metadata

  /**
   * Tombstones before this timestamp will be dropped from the state and the files can be
   * garbage collected.
   */
  def minFileRetentionTimestamp: Long = clock.getTimeMillis() - tombstoneRetentionMillis

  /**
   * Checks whether this table only accepts appends. If so it will throw an error in operations that
   * can remove data such as DELETE/UPDATE/MERGE.
   */
  def assertRemovable(): Unit = {
    if (DeltaConfigs.IS_APPEND_ONLY.fromMetaData(metadata)) {
      throw DeltaErrors.modifyAppendOnlyTableException
    }
  }

  /** The unique identifier for this table. */
  def tableId: String = metadata.id

  /* ------------------ *
   |  State Management  |
   * ------------------ */

  @volatile private var currentSnapshot: Snapshot = lastCheckpoint.map { c =>
    val checkpointFiles = c.parts
      .map(p => checkpointFileWithParts(logPath, c.version, p))
      .getOrElse(Seq(checkpointFileSingular(logPath, c.version)))
    val deltas = store.listFrom(deltaFile(logPath, c.version + 1))
      .filter(f => isDeltaFile(f.getPath))
      .toArray
    val deltaVersions = deltas.map(f => deltaVersion(f.getPath))
    verifyDeltaVersions(deltaVersions)
    val newVersion = deltaVersions.lastOption.getOrElse(c.version)
    logInfo(s"Loading version $newVersion starting from checkpoint ${c.version}")
    try {
      val deltaIndex = DeltaLogFileIndex(DeltaLog.COMMIT_FILE_FORMAT, deltas)
      val checkpointIndex = DeltaLogFileIndex(DeltaLog.CHECKPOINT_FILE_FORMAT, fs, checkpointFiles)
      val snapshot = new Snapshot(
        logPath,
        newVersion,
        None,
        checkpointIndex :: deltaIndex :: Nil,
        minFileRetentionTimestamp,
        this,
        // we don't want to make an additional RPC here to get commit timestamps when "deltas" is
        // empty. The next "update" call will take care of that if there are delta files.
        deltas.lastOption.map(_.getModificationTime).getOrElse(-1L))

      validateChecksum(snapshot)
      lastUpdateTimestamp = clock.getTimeMillis()
      snapshot
    } catch {
      case e: FileNotFoundException
          if Option(e.getMessage).exists(_.contains("parquet does not exist")) =>
        recordDeltaEvent(this, "delta.checkpoint.error.partial")
        throw DeltaErrors.missingPartFilesException(c, e)
      case e: AnalysisException if Option(e.getMessage).exists(_.contains("Path does not exist")) =>
        recordDeltaEvent(this, "delta.checkpoint.error.partial")
        throw DeltaErrors.missingPartFilesException(c, e)
    }
  }.getOrElse {
    new InitialSnapshot(logPath, this, Metadata())
  }

  if (currentSnapshot.version == -1) {
    // No checkpoint exists. Call "update" to load delta files.
    update()
  }

  /**
   * Verify the versions are contiguous.
   */
  private def verifyDeltaVersions(versions: Array[Long]): Unit = {
    // Turn this to a vector so that we can compare it with a range.
    val deltaVersions = versions.toVector
    if (deltaVersions.nonEmpty &&
      (deltaVersions.head to deltaVersions.last) != deltaVersions) {
      throw new IllegalStateException(s"versions ($deltaVersions) are not contiguous")
    }
  }

  /** Returns the current snapshot. Note this does not automatically `update()`. */
  def snapshot: Snapshot = currentSnapshot

  /**
   * Run `body` inside `deltaLogLock` lock using `lockInterruptibly` so that the thread can be
   * interrupted when waiting for the lock.
   */
  def lockInterruptibly[T](body: => T): T = {
    deltaLogLock.lockInterruptibly()
    try {
      body
    } finally {
      deltaLogLock.unlock()
    }
  }

  /** Checks if the snapshot of the table has surpassed our allowed staleness. */
  private def isSnapshotStale: Boolean = {
    val stalenessLimit = spark.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT)
    stalenessLimit == 0L || lastUpdateTimestamp < 0 ||
      clock.getTimeMillis() - lastUpdateTimestamp >= stalenessLimit
  }

  /**
   * Update ActionLog by applying the new delta files if any.
   *
   * @param stalenessAcceptable Whether we can accept working with a stale version of the table. If
   *                            the table has surpassed our staleness tolerance, we will update to
   *                            the latest state of the table synchronously. If staleness is
   *                            acceptable, and the table hasn't passed the staleness tolerance, we
   *                            will kick off a job in the background to update the table state,
   *                            and can return a stale snapshot in the meantime.
   */
  def update(stalenessAcceptable: Boolean = false): Snapshot = {
    val doAsync = stalenessAcceptable && !isSnapshotStale
    if (!doAsync) {
      lockInterruptibly {
        updateInternal(isAsync = false)
      }
    } else {
      if (asyncUpdateTask == null || asyncUpdateTask.isCompleted) {
        val jobGroup = spark.sparkContext.getLocalProperty(SparkContext.SPARK_JOB_GROUP_ID)
        asyncUpdateTask = Future[Unit] {
          spark.sparkContext.setLocalProperty("spark.scheduler.pool", "deltaStateUpdatePool")
          spark.sparkContext.setJobGroup(
            jobGroup,
            s"Updating state of Delta table at ${currentSnapshot.path}",
            interruptOnCancel = true)
          tryUpdate(isAsync = true)
        }(DeltaLog.deltaLogAsyncUpdateThreadPool)
      }
      currentSnapshot
    }
  }

  /**
   * Try to update ActionLog. If another thread is updating ActionLog, then this method returns
   * at once and return the current snapshot. The return snapshot may be stale.
   */
  def tryUpdate(isAsync: Boolean = false): Snapshot = {
    if (deltaLogLock.tryLock()) {
      try {
        updateInternal(isAsync)
      } finally {
        deltaLogLock.unlock()
      }
    } else {
      currentSnapshot
    }
  }

  /**
   * Queries the store for new delta files and applies them to the current state.
   * Note: the caller should hold `deltaLogLock` before calling this method.
   */
  private def updateInternal(isAsync: Boolean): Snapshot =
    recordDeltaOperation(this, "delta.log.update", Map(TAG_ASYNC -> isAsync.toString)) {
    withStatusCode("DELTA", "Updating the Delta table's state") {
      try {
        val newFiles = store
          // List from the current version since we want to get the checkpoint file for the current
          // version
          .listFrom(checkpointPrefix(logPath, math.max(currentSnapshot.version, 0L)))
          // Pick up checkpoint files not older than the current version and delta files newer than
          // the current version
          .filter { file =>
            isCheckpointFile(file.getPath) ||
              (isDeltaFile(file.getPath) && deltaVersion(file.getPath) > currentSnapshot.version)
        }.toArray

        val (checkpoints, deltas) = newFiles.partition(f => isCheckpointFile(f.getPath))
        if (deltas.isEmpty) {
          lastUpdateTimestamp = clock.getTimeMillis()
          return currentSnapshot
        }

        val deltaVersions = deltas.map(f => deltaVersion(f.getPath))
        verifyDeltaVersions(deltaVersions)
        val lastChkpoint = lastCheckpoint.map(CheckpointInstance.apply)
            .getOrElse(CheckpointInstance.MaxValue)
        val checkpointFiles = checkpoints.map(f => CheckpointInstance(f.getPath))
        val newCheckpoint = getLatestCompleteCheckpointFromList(checkpointFiles, lastChkpoint)
        val newSnapshot = if (newCheckpoint.isDefined) {
          // If there is a new checkpoint, start new lineage there.
          val newCheckpointVersion = newCheckpoint.get.version
          assert(
            newCheckpointVersion >= currentSnapshot.version,
            s"Attempting to load a checkpoint($newCheckpointVersion) " +
                s"older than current version (${currentSnapshot.version})")
          val newCheckpointFiles = newCheckpoint.get.getCorrespondingFiles(logPath)

          val newVersion = deltaVersions.last

          val deltaIndex = DeltaLogFileIndex(DeltaLog.COMMIT_FILE_FORMAT, deltas)
          val checkpointIndex =
            DeltaLogFileIndex(DeltaLog.CHECKPOINT_FILE_FORMAT, fs, newCheckpointFiles)

          logInfo(s"Loading version $newVersion starting from checkpoint $newCheckpointVersion")

          new Snapshot(
            logPath,
            newVersion,
            None,
            checkpointIndex :: deltaIndex :: Nil,
            minFileRetentionTimestamp,
            this,
            deltas.last.getModificationTime)
        } else {
          // If there is no new checkpoint, just apply the deltas to the existing state.
          assert(currentSnapshot.version + 1 == deltaVersions.head,
            s"versions in [${currentSnapshot.version + 1}, ${deltaVersions.head}) are missing")
          if (currentSnapshot.lineageLength >= maxSnapshotLineageLength) {
            // Load Snapshot from scratch to avoid StackOverflowError
            getSnapshotAt(deltaVersions.last, Some(deltas.last.getModificationTime))
          } else {
            val deltaIndex = DeltaLogFileIndex(DeltaLog.COMMIT_FILE_FORMAT, deltas)
            new Snapshot(
              logPath,
              deltaVersions.last,
              Some(currentSnapshot.state),
              deltaIndex :: Nil,
              minFileRetentionTimestamp,
              this,
              deltas.last.getModificationTime,
              lineageLength = currentSnapshot.lineageLength + 1)
          }
        }
        validateChecksum(newSnapshot)
        currentSnapshot.uncache()
        currentSnapshot = newSnapshot
      } catch {
        case f: FileNotFoundException =>
          val message = s"No delta log found for the Delta table at $logPath"
          logInfo(message)
          // When the state is empty, this is expected. The log will be lazily created when needed.
          // When the state is not empty, it's a real issue and we can't continue to execution.
          if (currentSnapshot.version != -1) {
            val e = new FileNotFoundException(message)
            e.setStackTrace(f.getStackTrace)
            throw e
          }
      }
      lastUpdateTimestamp = clock.getTimeMillis()
      currentSnapshot
    }
  }


  /* ------------------ *
   |  Delta Management  |
   * ------------------ */

  /**
   * Returns a new [[OptimisticTransaction]] that can be used to read the current state of the
   * log and then commit updates. The reads and updates will be checked for logical conflicts
   * with any concurrent writes to the log.
   *
   * Note that all reads in a transaction must go through the returned transaction object, and not
   * directly to the [[DeltaLog]] otherwise they will not be checked for conflicts.
   */
  def startTransaction(): OptimisticTransaction = {
    update()
    new OptimisticTransaction(this)
  }

  /**
   * Execute a piece of code within a new [[OptimisticTransaction]]. Reads/write sets will
   * be recorded for this table, and all other tables will be read
   * at a snapshot that is pinned on the first access.
   *
   * @note This uses thread-local variable to make the active transaction visible. So do not use
   *       multi-threaded code in the provided thunk.
   */
  def withNewTransaction[T](thunk: OptimisticTransaction => T): T = {
    try {
      update()
      val txn = new OptimisticTransaction(this)
      OptimisticTransaction.setActive(txn)
      thunk(txn)
    } finally {
      OptimisticTransaction.clearActive()
    }
  }


  /**
   * Upgrade the table's protocol version, by default to the maximum recognized reader and writer
   * versions in this DBR release.
   */
  def upgradeProtocol(newVersion: Protocol = Protocol()): Unit = {
    val currentVersion = snapshot.protocol
    if (newVersion.minReaderVersion < currentVersion.minReaderVersion ||
        newVersion.minWriterVersion < currentVersion.minWriterVersion) {
      throw new ProtocolDowngradeException(currentVersion, newVersion)
    } else if (newVersion.minReaderVersion == currentVersion.minReaderVersion &&
               newVersion.minWriterVersion == currentVersion.minWriterVersion) {
      logConsole(s"Table $dataPath is already at protocol version $newVersion.")
      return
    }

    val txn = startTransaction()
    try {
      SchemaUtils.checkColumnNameDuplication(txn.metadata.schema, "in the table schema")
    } catch {
      case e: AnalysisException =>
        throw new AnalysisException(
          e.getMessage + "\nPlease remove duplicate columns before you update your table.")
    }
    txn.commit(Seq(newVersion), DeltaOperations.UpgradeProtocol(newVersion))
    logConsole(s"Upgraded table at $dataPath to $newVersion.")
  }

  /**
   * Get all actions starting from "startVersion" (inclusive). If `startVersion` doesn't exist,
   * return an empty Iterator.
   */
  def getChanges(startVersion: Long): Iterator[(Long, Seq[Action])] = {
    val deltas = store.listFrom(deltaFile(logPath, startVersion))
      .filter(f => isDeltaFile(f.getPath))
    deltas.map { status =>
      val p = status.getPath
      val version = deltaVersion(p)
      (version, store.read(p).map(Action.fromJson))
    }
  }

  /* --------------------- *
   |  Protocol validation  |
   * --------------------- */

  private def oldProtocolMessage(protocol: Protocol): String =
    s"WARNING: The Delta Lake table at $dataPath has version " +
      s"${protocol.simpleString}, but the latest version is " +
      s"${Protocol().simpleString}. To take advantage of the latest features and bug fixes, " +
      "we recommend that you upgrade the table.\n" +
      "First update all clusters that use this table to the latest version of Databricks " +
      "Runtime, and then run the following command in a notebook:\n" +
      "'%scala com.databricks.delta.Delta.upgradeTable(\"" + s"$dataPath" + "\")'\n\n" +
      "For more information about Delta Lake table versions, see " +
      s"${DeltaErrors.baseDocsPath(spark)}/delta/versioning.html"

  /**
   * If the given `protocol` is older than that of the client.
   */
  private def isProtocolOld(protocol: Protocol): Boolean = protocol != null &&
    (Action.readerVersion > protocol.minReaderVersion ||
      Action.writerVersion > protocol.minWriterVersion)

  /**
   * Asserts that the client is up to date with the protocol and
   * allowed to read the table that is using the given `protocol`.
   */
  def protocolRead(protocol: Protocol): Unit = {
    if (protocol != null &&
        Action.readerVersion < protocol.minReaderVersion) {
      recordDeltaEvent(
        this,
        "delta.protocol.failure.read",
        data = Map(
          "clientVersion" -> Action.readerVersion,
          "minReaderVersion" -> protocol.minReaderVersion))
      throw new InvalidProtocolVersionException
    }

    if (isProtocolOld(protocol)) {
      recordDeltaEvent(this, "delta.protocol.warning")
      logConsole(oldProtocolMessage(protocol))
    }
  }

  /**
   * Asserts that the client is up to date with the protocol and
   * allowed to write to the table that is using the given `protocol`.
   */
  def protocolWrite(protocol: Protocol, logUpgradeMessage: Boolean = true): Unit = {
    if (protocol != null && Action.writerVersion < protocol.minWriterVersion) {
      recordDeltaEvent(
        this,
        "delta.protocol.failure.write",
        data = Map(
          "clientVersion" -> Action.writerVersion,
          "minWriterVersion" -> protocol.minWriterVersion))
      throw new InvalidProtocolVersionException
    }

    if (logUpgradeMessage && isProtocolOld(protocol)) {
      recordDeltaEvent(this, "delta.protocol.warning")
      logConsole(oldProtocolMessage(protocol))
    }
  }

  /* ------------------- *
   |  History Management |
   * ------------------- */

  /** Get the snapshot at `version`. */
  def getSnapshotAt(
      version: Long,
      commitTimestamp: Option[Long] = None,
      lastCheckpointHint: Option[CheckpointInstance] = None): Snapshot = {
    val current = snapshot
    if (current.version == version) {
      return current
    }

    // Do not use the hint if the version we're asking for is smaller than the last checkpoint hint
    val lastCheckpoint = lastCheckpointHint.collect { case ci if ci.version <= version => ci }
      .orElse(findLastCompleteCheckpoint(CheckpointInstance(version, None)))
    val lastCheckpointFiles = lastCheckpoint.map { c =>
      c.getCorrespondingFiles(logPath)
    }.toSeq.flatten
    val checkpointVersion = lastCheckpoint.map(_.version)
    if (checkpointVersion.isEmpty) {
      val versionZeroFile = deltaFile(logPath, 0L)
      val versionZeroFileExists = store.listFrom(versionZeroFile)
        .take(1)
        .exists(_.getPath.getName == versionZeroFile.getName)
      if (!versionZeroFileExists) {
        throw DeltaErrors.logFileNotFoundException(versionZeroFile, 0L, metadata)
      }
    }
    val startVersion = checkpointVersion.getOrElse(-1L) + 1
    // Listing the files may be more efficient than getting the file status for each file
    val deltaData = store.listFrom(deltaFile(logPath, startVersion))
      .filter(f => isDeltaFile(f.getPath))
      .takeWhile(f => deltaVersion(f.getPath) <= version)
      .toArray
    val deltaFileVersions = deltaData.map(f => deltaVersion(f.getPath))
    if (deltaFileVersions.nonEmpty) {
      // deltaFileVersions can be empty if we're loading a version for which a checkpoint exists
      verifyDeltaVersions(deltaFileVersions)
      require(deltaFileVersions.head == startVersion,
        s"Did not get the first delta file version: $startVersion to compute Snapshot")
      require(deltaFileVersions.last == version,
        s"Did not get the last delta file version: $version to compute Snapshot")
    }

    val deltaIndex = DeltaLogFileIndex(DeltaLog.COMMIT_FILE_FORMAT, deltaData)
    val checkpointIndex =
      DeltaLogFileIndex(DeltaLog.CHECKPOINT_FILE_FORMAT, fs, lastCheckpointFiles)

    new Snapshot(
      logPath,
      version,
      None,
      checkpointIndex :: deltaIndex :: Nil,
      minFileRetentionTimestamp,
      this,
      commitTimestamp.getOrElse(-1L))
  }

  /* ---------------------------------------- *
   |  Log Directory Management and Retention  |
   * ---------------------------------------- */

  def isValid(): Boolean = {
    val expectedExistingFile = deltaFile(logPath, currentSnapshot.version)
    try {
      store.listFrom(expectedExistingFile)
        .take(1)
        .exists(_.getPath.getName == expectedExistingFile.getName)
    } catch {
      case _: FileNotFoundException =>
        // Parent of expectedExistingFile doesn't exist
        false
    }
  }

  def isSameLogAs(otherLog: DeltaLog): Boolean = this.tableId == otherLog.tableId

  /** Creates the log directory if it does not exist. */
  def ensureLogDirectoryExist(): Unit = {
    if (!fs.exists(logPath)) {
      if (!fs.mkdirs(logPath)) {
        throw new IOException(s"Cannot create $logPath")
      }
    }
  }

  /* ------------  *
   |  Integration  |
   * ------------  */

  /**
   * Returns a [[org.apache.spark.sql.DataFrame]] containing the new files within the specified
   * version range.
   */
  def createDataFrame(
      snapshot: Snapshot,
      addFiles: Seq[AddFile],
      isStreaming: Boolean = false,
      actionTypeOpt: Option[String] = None): DataFrame = {
    val actionType = actionTypeOpt.getOrElse(if (isStreaming) "streaming" else "batch")
    val fileIndex = new TahoeBatchFileIndex(spark, actionType, addFiles, this, dataPath, snapshot)

    val relation = HadoopFsRelation(
      fileIndex,
      partitionSchema = snapshot.metadata.partitionSchema,
      dataSchema = snapshot.metadata.schema,
      bucketSpec = None,
      snapshot.fileFormat,
      snapshot.metadata.format.options)(spark)

    Dataset.ofRows(spark, LogicalRelation(relation, isStreaming = isStreaming))
  }

  /**
   * Returns a [[BaseRelation]] that contains all of the data present
   * in the table. This relation will be continually updated
   * as files are added or removed from the table. However, new [[BaseRelation]]
   * must be requested in order to see changes to the schema.
   */
  def createRelation(
      partitionFilters: Seq[Expression] = Nil,
      timeTravel: Option[DeltaTimeTravelSpec] = None): BaseRelation = {

    val versionToUse = timeTravel.map { tt =>
      val (version, accessType) = DeltaTableUtils.resolveTimeTravelVersion(
        spark.sessionState.conf, this, tt)
      val source = tt.creationSource.getOrElse("unknown")
      recordDeltaEvent(this, s"delta.timeTravel.$source", data = Map(
        "tableVersion" -> snapshot.version,
        "queriedVersion" -> version,
        "accessType" -> accessType
      ))
      version
    }

    /** Used to link the files present in the table into the query planner. */
    val fileIndex = TahoeLogFileIndex(spark, this, dataPath, partitionFilters, versionToUse)
    val snapshotToUse = versionToUse.map(getSnapshotAt(_)).getOrElse(snapshot)

    new HadoopFsRelation(
      fileIndex,
      partitionSchema = snapshotToUse.metadata.partitionSchema,
      dataSchema = snapshotToUse.metadata.schema,
      bucketSpec = None,
      snapshotToUse.fileFormat,
      snapshotToUse.metadata.format.options)(spark) with InsertableRelation {
      def insert(data: DataFrame, overwrite: Boolean): Unit = {
        val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
        WriteIntoDelta(
          deltaLog = DeltaLog.this,
          mode = mode,
          new DeltaOptions(Map.empty[String, String], spark.sessionState.conf),
          partitionColumns = Seq.empty,
          configuration = Map.empty,
          data = data).run(spark)
      }
    }
  }
}

object DeltaLog extends DeltaLogging {

  protected lazy val deltaLogAsyncUpdateThreadPool = {
    val tpe = ThreadUtils.newDaemonCachedThreadPool("delta-state-update", 8)
    ExecutionContext.fromExecutorService(tpe)
  }

  /**
   * We create only a single [[DeltaLog]] for any given path to avoid wasted work
   * in reconstructing the log.
   */
  private val deltaLogCache = {
    val builder = CacheBuilder.newBuilder()
      .expireAfterAccess(60, TimeUnit.MINUTES)
      .removalListener(new RemovalListener[Path, DeltaLog] {
        override def onRemoval(removalNotification: RemovalNotification[Path, DeltaLog]) = {
          val log = removalNotification.getValue
          try log.snapshot.uncache() catch {
            case _: java.lang.NullPointerException =>
              // Various layers will throw null pointer if the RDD is already gone.
          }
        }
      })
    sys.props.get("delta.log.cacheSize")
      .flatMap(v => Try(v.toLong).toOption)
      .foreach(builder.maximumSize)
    builder.build[Path, DeltaLog]()
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: String): DeltaLog = {
    apply(spark, new Path(dataPath, "_delta_log"), new SystemClock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: File): DeltaLog = {
    apply(spark, new Path(dataPath.getAbsolutePath, "_delta_log"), new SystemClock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: Path): DeltaLog = {
    apply(spark, new Path(dataPath, "_delta_log"), new SystemClock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: String, clock: Clock): DeltaLog = {
    apply(spark, new Path(dataPath, "_delta_log"), clock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: File, clock: Clock): DeltaLog = {
    apply(spark, new Path(dataPath.getAbsolutePath, "_delta_log"), clock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: Path, clock: Clock): DeltaLog = {
    apply(spark, new Path(dataPath, "_delta_log"), clock)
  }
  // TODO: Don't assume the data path here.
  def apply(spark: SparkSession, rawPath: Path, clock: Clock = new SystemClock): DeltaLog = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val fs = rawPath.getFileSystem(hadoopConf)
    val path = fs.makeQualified(rawPath)
    // The following cases will still create a new ActionLog even if there is a cached
    // ActionLog using a different format path:
    // - Different `scheme`
    // - Different `authority` (e.g., different user tokens in the path)
    // - Different mount point.
    val cached = try {
      deltaLogCache.get(path, new Callable[DeltaLog] {
        override def call(): DeltaLog = recordDeltaOperation(
            null, "delta.log.create", Map(TAG_TAHOE_PATH -> path.getParent.toString)) {
          AnalysisHelper.allowInvokingTransformsInAnalyzer {
            new DeltaLog(path, path.getParent, clock)
          }
        }
      })
    } catch {
      case e: com.google.common.util.concurrent.UncheckedExecutionException =>
        throw e.getCause
    }

    // Invalidate the cache if the reference is no longer valid as a result of the
    // log being deleted.
    if (cached.snapshot.version == -1 || cached.isValid) {
      cached
    } else {
      deltaLogCache.invalidate(path)
      apply(spark, path)
    }
  }

  /** Invalidate the cached DeltaLog object for the given `dataPath`. */
  def invalidateCache(spark: SparkSession, dataPath: Path): Unit = {
    try {
      val rawPath = new Path(dataPath, "_delta_log")
      val fs = rawPath.getFileSystem(spark.sessionState.newHadoopConf())
      val path = fs.makeQualified(rawPath)
      deltaLogCache.invalidate(path)
    } catch {
      case NonFatal(e) => logWarning(e.getMessage, e)
    }
  }

  def clearCache(): Unit = {
    deltaLogCache.invalidateAll()
  }

  /**
   * Filters the given [[Dataset]] by the given `partitionFilters`, returning those that match.
   * @param files The active files in the DeltaLog state, which contains the partition value
   *              information
   * @param partitionFilters Filters on the partition columns
   * @param partitionColumnPrefixes The path to the `partitionValues` column, if it's nested
   */
  def filterFileList(
      partitionSchema: StructType,
      files: DataFrame,
      partitionFilters: Seq[Expression],
      partitionColumnPrefixes: Seq[String] = Nil): DataFrame = {
    val rewrittenFilters = rewritePartitionFilters(
      partitionSchema,
      files.sparkSession.sessionState.conf.resolver,
      partitionFilters,
      partitionColumnPrefixes)
    val columnFilter = new Column(rewrittenFilters.reduceLeftOption(And).getOrElse(Literal(true)))
    files.filter(columnFilter)
  }

  /**
   * Rewrite the given `partitionFilters` to be used for filtering partition values.
   * We need to explicitly resolve the partitioning columns here because the partition columns
   * are stored as keys of a Map type instead of attributes in the AddFile schema (below) and thus
   * cannot be resolved automatically.
   *
   * @param partitionFilters Filters on the partition columns
   * @param partitionColumnPrefixes The path to the `partitionValues` column, if it's nested
   */
  def rewritePartitionFilters(
      partitionSchema: StructType,
      resolver: Resolver,
      partitionFilters: Seq[Expression],
      partitionColumnPrefixes: Seq[String] = Nil): Seq[Expression] = {
    partitionFilters.map(_.transformUp {
      case a: Attribute =>
        // If we have a special column name, e.g. `a.a`, then an UnresolvedAttribute returns
        // the column name as '`a.a`' instead of 'a.a', therefore we need to strip the backticks.
        val unquoted = a.name.stripPrefix("`").stripSuffix("`")
        val partitionCol = partitionSchema.find { field => resolver(field.name, unquoted) }
        partitionCol match {
          case Some(StructField(name, dataType, _, _)) =>
            Cast(
              UnresolvedAttribute(partitionColumnPrefixes ++ Seq("partitionValues", name)),
              dataType)
          case None =>
            // This should not be able to happen, but the case was present in the original code so
            // we kept it to be safe.
            log.error(s"Partition filter referenced column ${a.name} not in the partition schema")
            UnresolvedAttribute(partitionColumnPrefixes ++ Seq("partitionValues", a.name))
        }
    })
  }

  private lazy val COMMIT_FILE_FORMAT = new JsonFileFormat
  private lazy val CHECKPOINT_FILE_FORMAT = new ParquetFileFormat
}
