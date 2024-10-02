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
import scala.collection.mutable

import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.actions.Action.logSchema
import org.apache.spark.sql.delta.coordinatedcommits.{CommitCoordinatorClient, CoordinatedCommitsUsageLogs, CoordinatedCommitsUtils, TableCommitCoordinatorClient}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DataSkippingReader
import org.apache.spark.sql.delta.stats.DeltaStatsColumnSpec
import org.apache.spark.sql.delta.stats.StatisticsCollection
import org.apache.spark.sql.delta.util.DeltaCommitFileProvider
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.util.StateCache
import org.apache.spark.sql.util.ScalaExtensions._
import io.delta.storage.commit.CommitCoordinatorClient
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.internal.{MDC, MessageWithContext}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

/**
 * A description of a Delta [[Snapshot]], including basic information such its [[DeltaLog]]
 * metadata, protocol, and version.
 */
trait SnapshotDescriptor {
  def deltaLog: DeltaLog
  def version: Long
  def metadata: Metadata
  def protocol: Protocol

  def schema: StructType = metadata.schema

  protected[delta] def numOfFilesIfKnown: Option[Long]
  protected[delta] def sizeInBytesIfKnown: Option[Long]
}

/**
 * An immutable snapshot of the state of the log at some delta version. Internally
 * this class manages the replay of actions stored in checkpoint or delta files.
 *
 * After resolving any new actions, it caches the result and collects the
 * following basic information to the driver:
 *  - Protocol Version
 *  - Metadata
 *  - Transaction state
 *
 * @param inCommitTimestampOpt The in-commit-timestamp of the latest commit in milliseconds. Can
 *                  be set to None if
 *                   1. The timestamp has not been read yet - generally the case for cold tables.
 *                   2. Or the table has not been initialized, i.e. `version = -1`.
 *                   3. Or the table does not have [[InCommitTimestampTableFeature]] enabled.
 *
 */
class Snapshot(
    val path: Path,
    override val version: Long,
    val logSegment: LogSegment,
    override val deltaLog: DeltaLog,
    val checksumOpt: Option[VersionChecksum]
  )
  extends SnapshotDescriptor
  with SnapshotStateManager
  with StateCache
  with StatisticsCollection
  with DataSkippingReader
  with DeltaLogging {

  import Snapshot._
  import DeltaLogFileIndex.COMMIT_VERSION_COLUMN
  // For implicits which re-use Encoder:
  import org.apache.spark.sql.delta.implicits._

  protected def spark = SparkSession.active

  /** Snapshot to scan by the DeltaScanGenerator for metadata query optimizations */
  override val snapshotToScan: Snapshot = this

  override def columnMappingMode: DeltaColumnMappingMode = metadata.columnMappingMode

  /**
   * Returns the timestamp of the latest commit of this snapshot.
   * For an uninitialized snapshot, this returns -1.
   *
   * When InCommitTimestampTableFeature is enabled, the timestamp
   * is retrieved from the CommitInfo of the latest commit which
   * can result in an IO operation.
   */
  def timestamp: Long =
    getInCommitTimestampOpt.getOrElse(logSegment.lastCommitFileModificationTimestamp)

  /**
   * Returns the inCommitTimestamp if ICT is enabled, otherwise returns None.
   * This potentially triggers an IO operation to read the inCommitTimestamp.
   * This is a lazy val, so repeated calls will not trigger multiple IO operations.
   */
  protected lazy val getInCommitTimestampOpt: Option[Long] =
    Option.when(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(metadata)) {
      _reconstructedProtocolMetadataAndICT.inCommitTimestamp
        .getOrElse {
          val startTime = System.currentTimeMillis()
          var exception = Option.empty[Throwable]
          try {
            val commitInfoOpt = DeltaHistoryManager.getCommitInfoOpt(
              deltaLog.store,
              DeltaCommitFileProvider(this).deltaFile(version),
              deltaLog.newDeltaHadoopConf())
            CommitInfo.getRequiredInCommitTimestamp(commitInfoOpt, version.toString)
          } catch {
            case e: Throwable =>
              exception = Some(e)
              throw e
          } finally {
            recordDeltaEvent(
              deltaLog,
              "delta.inCommitTimestamp.read",
              data = Map(
                "version" -> version,
                "callSite" -> "Snapshot.getInCommitTimestampOpt",
                "checkpointVersion" -> logSegment.checkpointProvider.version,
                "durationMs" -> (System.currentTimeMillis() - startTime),
                "exceptionMessage" -> exception.map(_.getMessage).getOrElse(""),
                "exceptionStackTrace" -> exception.map(_.getStackTrace.mkString("\n")).getOrElse("")
              )
            )
          }
        }
    }


  private[delta] lazy val nonFileActions: Seq[Action] = {
    Seq(protocol, metadata) ++
      setTransactions ++
      domainMetadata
  }

  @volatile private[delta] var stateReconstructionTriggered = false

  /**
   * The last known backfilled version of this snapshot. This can be larger than the last
   * backfilled file in the snapshot's LogSegment so is separately tracked in this mutable
   * variable. The reason why this is needed is as follows:
   *
   * In general, we update a snapshot's LogSegment after a commit by appending the latest
   * commit file. This can be an unbackfilled commit. The next time we call update(), we
   * check, if we can reuse the post commit snapshot or if we need to create a new snapshot.
   * The update performs a listing and creates a new LogSegment and the criteria for
   * keeping or replacing the old snapshot is whether the old snapshot's LogSegment is equal
   * to the LogSegment created by the update() call (see getSnapshotForLogSegmentInternal).
   *
   * If an unbackfilled commit has been backfilled before update() is called, the new LogSegment
   * would contain the backfilled version of this commit and so the old and new LogSegments are
   * determined to be different and the snapshot is swapped. However, the snapshots are in fact
   * identical and so swapping the snapshot is not necessary and wold only lead to a loss of the
   * cached state of the old snapshot.
   *
   * To prevent this, we don't swap the snapshot in this case (see
   * LogSegment.lastMatchingBackfilledCommitIsEqual). This means that we'll continue to use
   * the old LogSegment, which contains the unbackfilled commit(s). To correctly keep track of
   * the fact that all commits in the LogSegment have indeed been backfilled, we keep the
   * last known backfilled version of the snapshot in this variable and update it each time
   * during LogSegment comparison. This allows callers to figure out whether this snapshot
   * indeed contains any unbackfilled commits or the LogSegment is just based on an older
   * version.
   */
  @volatile private var lastKnownBackfilledVersion: Long =
    logSegment.lastBackfilledVersionInSegment

  def getLastKnownBackfilledVersion: Long = lastKnownBackfilledVersion

  def updateLastKnownBackfilledVersion(newVersion: Long): Unit = {
    if (newVersion > this.version) {
      throw new IllegalStateException("Can't update the last known backfilled version " +
        "to a version greater than the snapshot's version.")
    }
    lastKnownBackfilledVersion = math.max(lastKnownBackfilledVersion, newVersion)
  }

  /**
   * Helper method to determine, whether this snapshot contains "actual" unbackfilled
   * commits. See [[Snapshot.lastKnownBackfilledVersion]] for more details on why a
   * LogSegment may contain unbackfilled commits, even though these files have already
   * been backfilled.
   */
  private[delta] def allCommitsBackfilled: Boolean = {
    lastKnownBackfilledVersion >= FileNames.getFileVersion(logSegment.deltas.last) &&
      // This should always be true because we synchronously backfill during checkpoint
      // creation and always create a new snapshot after that, which will force the
      // latest LogSegment to be used.
      lastKnownBackfilledVersion >= logSegment.checkpointProvider.version
  }

  /**
   * Use [[stateReconstruction]] to create a representation of the actions in this table.
   * Cache the resultant output.
   */
  private lazy val cachedState = recordFrameProfile("Delta", "snapshot.cachedState") {
    stateReconstructionTriggered = true
    cacheDS(stateReconstruction, s"Delta Table State #$version - $redactedPath")
  }

  /**
   * Given the list of files from `LogSegment`, create respective file indices to help create
   * a DataFrame and short-circuit the many file existence and partition schema inference checks
   * that exist in DataSource.resolveRelation().
   */
  protected[delta] lazy val deltaFileIndexOpt: Option[DeltaLogFileIndex] = {
    assertLogFilesBelongToTable(path, logSegment.deltas)
    DeltaLogFileIndex(DeltaLogFileIndex.COMMIT_FILE_FORMAT, logSegment.deltas)
  }

  protected lazy val fileIndices: Seq[DeltaLogFileIndex] = {
    val checkpointFileIndexes = checkpointProvider.allActionsFileIndexes()
    checkpointFileIndexes ++ deltaFileIndexOpt.toSeq
  }

  /**
   * Protocol, Metadata, and In-Commit Timestamp retrieved through
   * `protocolMetadataAndICTReconstruction` which skips a full state reconstruction.
   */
  case class ReconstructedProtocolMetadataAndICT(
      protocol: Protocol,
      metadata: Metadata,
      inCommitTimestamp: Option[Long])

  /**
   * Generate the protocol and metadata for this snapshot. This is usually cheaper than a
   * full state reconstruction, but still only compute it when necessary.
   */
  private lazy val _reconstructedProtocolMetadataAndICT: ReconstructedProtocolMetadataAndICT =
      {
    // Should be small. At most 'checkpointInterval' rows, unless new commits are coming
    // in before a checkpoint can be written
    var protocol: Protocol = null
    var metadata: Metadata = null
    var inCommitTimestamp: Option[Long] = None
    protocolMetadataAndICTReconstruction().foreach {
      case ReconstructedProtocolMetadataAndICT(p: Protocol, _, _) => protocol = p
      case ReconstructedProtocolMetadataAndICT(_, m: Metadata, _) => metadata = m
      case ReconstructedProtocolMetadataAndICT(_, _, ict: Option[Long]) => inCommitTimestamp = ict
    }

    if (protocol == null) {
      recordDeltaEvent(
        deltaLog,
        opType = "delta.assertions.missingAction",
        data = Map(
          "version" -> version.toString, "action" -> "Protocol", "source" -> "Snapshot"))
      throw DeltaErrors.actionNotFoundException("protocol", version)
    }

    if (metadata == null) {
      recordDeltaEvent(
        deltaLog,
        opType = "delta.assertions.missingAction",
        data = Map(
          "version" -> version.toString, "action" -> "Metadata", "source" -> "Snapshot"))
      throw DeltaErrors.actionNotFoundException("metadata", version)
    }

    ReconstructedProtocolMetadataAndICT(protocol, metadata, inCommitTimestamp)
  }

  /**
   * [[CommitCoordinatorClient]] for the given delta table as of this snapshot.
   * - This should not be None when a coordinator has been configured for this table. However, if
   *   the configured coordinator implementation has not been registered, this will be None. In such
   *   cases, the user will see potentially stale reads for the table. For strict enforcement of
   *   coordinated commits, the user can set the configuration
   *   [[DeltaSQLConf.COORDINATED_COMMITS_IGNORE_MISSING_COORDINATOR_IMPLEMENTATION]] to false.
   * - This must be None when coordinated commits is disabled.
   */
  val tableCommitCoordinatorClientOpt: Option[TableCommitCoordinatorClient] = {
    val failIfImplUnavailable =
      !spark.conf.get(DeltaSQLConf.COORDINATED_COMMITS_IGNORE_MISSING_COORDINATOR_IMPLEMENTATION)
    CoordinatedCommitsUtils.getTableCommitCoordinator(
      spark,
      deltaLog,
      this,
      failIfImplUnavailable
    )
  }

  /**
   * Returns the [[TableCommitCoordinatorClient]] that should be used for any type of mutation
   * operation on the table. This includes, data writes, backfills etc.
   * This method will throw an error if the configured coordinator could not be instantiated.
   * @return [[TableCommitCoordinatorClient]] if the table is configured for coordinated commits,
   *         None if the table is not configured for coordinated commits.
   */
  def getTableCommitCoordinatorForWrites: Option[TableCommitCoordinatorClient] = {
    val coordinatorOpt = tableCommitCoordinatorClientOpt
      val coordinatorName =
        DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.fromMetaData(metadata)
      if (coordinatorName.isDefined && coordinatorOpt.isEmpty) {
        recordDeltaEvent(
          deltaLog,
          CoordinatedCommitsUsageLogs.COMMIT_COORDINATOR_MISSING_IMPLEMENTATION_WRITE,
          data = Map(
            "commitCoordinatorName" -> coordinatorName.get,
            "readVersion" -> version.toString
          )
        )
        throw DeltaErrors.unsupportedWritesWithMissingCoordinators(coordinatorName.get)
      }
      coordinatorOpt
  }

  /** Number of columns to collect stats on for data skipping */
  override lazy val statsColumnSpec: DeltaStatsColumnSpec =
    StatisticsCollection.configuredDeltaStatsColumnSpec(metadata)

  /** Performs validations during initialization */
  protected def init(): Unit = {
    deltaLog.protocolRead(protocol)
    deltaLog.assertTableFeaturesMatchMetadata(protocol, metadata)
    SchemaUtils.recordUndefinedTypes(deltaLog, metadata.schema)
  }

  /** The current set of actions in this [[Snapshot]] as plain Rows */
  def stateDF: DataFrame = recordFrameProfile("Delta", "stateDF") {
    cachedState.getDF
  }

  /** The current set of actions in this [[Snapshot]] as a typed Dataset. */
  def stateDS: Dataset[SingleAction] = recordFrameProfile("Delta", "stateDS") {
    cachedState.getDS
  }

  private[delta] def allFilesViaStateReconstruction: Dataset[AddFile] = {
    stateDS.where("add IS NOT NULL").select(col("add").as[AddFile])
  }

  // Here we need to bypass the ACL checks for SELECT anonymous function permissions.
  /** All of the files present in this [[Snapshot]]. */
  def allFiles: Dataset[AddFile] = allFilesViaStateReconstruction

  /** All unexpired tombstones. */
  def tombstones: Dataset[RemoveFile] = {
    stateDS.where("remove IS NOT NULL").select(col("remove").as[RemoveFile])
  }

  def deltaFileSizeInBytes(): Long = deltaFileIndexOpt.map(_.sizeInBytes).getOrElse(0L)

  def checkpointSizeInBytes(): Long = checkpointProvider.effectiveCheckpointSizeInBytes()

  override def metadata: Metadata = _reconstructedProtocolMetadataAndICT.metadata

  override def protocol: Protocol = _reconstructedProtocolMetadataAndICT.protocol

  /**
   * Pulls the protocol and metadata of the table from the files that are used to compute the
   * Snapshot directly--without triggering a full state reconstruction. This is important, because
   * state reconstruction depends on protocol and metadata for correctness.
   * If the current table version does not have a checkpoint, this function will also return the
   * in-commit-timestamp of the latest commit if available.
   *
   * Also this method should only access methods defined in [[UninitializedCheckpointProvider]]
   * which are not present in [[CheckpointProvider]]. This is because initialization of
   * [[Snapshot.checkpointProvider]] depends on [[Snapshot.protocolMetadataAndICTReconstruction()]]
   * and so if [[Snapshot.protocolMetadataAndICTReconstruction()]] starts depending on
   * [[Snapshot.checkpointProvider]] then there will be cyclic dependency.
   */
  protected def protocolMetadataAndICTReconstruction():
      Array[ReconstructedProtocolMetadataAndICT] = {
    import implicits._

    val schemaToUse = Action.logSchema(Set("protocol", "metaData", "commitInfo"))
    val checkpointOpt = checkpointProvider.topLevelFileIndex.map { index =>
      deltaLog.loadIndex(index, schemaToUse)
        .withColumn(COMMIT_VERSION_COLUMN, lit(checkpointProvider.version))
    }
    (checkpointOpt ++ deltaFileIndexOpt.map(deltaLog.loadIndex(_, schemaToUse)).toSeq)
      .reduceOption(_.union(_)).getOrElse(emptyDF)
      .select("protocol", "metaData", "commitInfo.inCommitTimestamp", COMMIT_VERSION_COLUMN)
      .where("protocol.minReaderVersion is not null or metaData.id is not null " +
        s"or (commitInfo.inCommitTimestamp is not null and version = $version)")
      .as[(Protocol, Metadata, Option[Long], Long)]
      .collect()
      .sortBy(_._4)
      .map {
        case (p, m, ict, _) => ReconstructedProtocolMetadataAndICT(p, m, ict)
      }
  }

  // Reconstruct the state by applying deltas in order to the checkpoint.
  // We partition by path as it is likely the bulk of the data is add/remove.
  // Non-path based actions will be collocated to a single partition.
  protected def stateReconstruction: Dataset[SingleAction] = {
    recordFrameProfile("Delta", "snapshot.stateReconstruction") {
      // for serializability
      val localMinFileRetentionTimestamp = minFileRetentionTimestamp
      val localMinSetTransactionRetentionTimestamp = minSetTransactionRetentionTimestamp

      val canonicalPath = deltaLog.getCanonicalPathUdf()

      // Canonicalize the paths so we can repartition the actions correctly, but only rewrite the
      // add/remove actions themselves after partitioning and sorting are complete. Otherwise, the
      // optimizer can generate a really bad plan that re-evaluates _EVERY_ field of the rewritten
      // struct(...)  projection every time we touch _ANY_ field of the rewritten struct.
      //
      // NOTE: We sort by [[COMMIT_VERSION_COLUMN]] (provided by [[loadActions]]), to ensure that
      // actions are presented to InMemoryLogReplay in the ascending version order it expects.
      val ADD_PATH_CANONICAL_COL_NAME = "add_path_canonical"
      val REMOVE_PATH_CANONICAL_COL_NAME = "remove_path_canonical"
      loadActions
        .withColumn(ADD_PATH_CANONICAL_COL_NAME, when(
          col("add.path").isNotNull, canonicalPath(col("add.path"))))
        .withColumn(REMOVE_PATH_CANONICAL_COL_NAME, when(
          col("remove.path").isNotNull, canonicalPath(col("remove.path"))))
        .repartition(
          getNumPartitions,
          coalesce(col(ADD_PATH_CANONICAL_COL_NAME), col(REMOVE_PATH_CANONICAL_COL_NAME)))
        .sortWithinPartitions(COMMIT_VERSION_COLUMN)
        .withColumn("add", when(
          col("add.path").isNotNull,
          struct(
            col(ADD_PATH_CANONICAL_COL_NAME).as("path"),
            col("add.partitionValues"),
            col("add.size"),
            col("add.modificationTime"),
            col("add.dataChange"),
            col(ADD_STATS_TO_USE_COL_NAME).as("stats"),
            col("add.tags"),
            col("add.deletionVector"),
            col("add.baseRowId"),
            col("add.defaultRowCommitVersion"),
            col("add.clusteringProvider")
          )))
        .withColumn("remove", when(
          col("remove.path").isNotNull,
          col("remove").withField("path", col(REMOVE_PATH_CANONICAL_COL_NAME))))
        .as[SingleAction]
        .mapPartitions { iter =>
          val state: LogReplay =
            new InMemoryLogReplay(
              localMinFileRetentionTimestamp,
              localMinSetTransactionRetentionTimestamp)
          state.append(0, iter.map(_.unwrap))
          state.checkpoint.map(_.wrap)
        }
    }
  }

  /**
   * Loads the file indices into a DataFrame that can be used for LogReplay.
   *
   * In addition to the usual nested columns provided by the SingleAction schema, it should provide
   * two additional columns to simplify the log replay process: [[COMMIT_VERSION_COLUMN]] (which,
   * when sorted in ascending order, will order older actions before newer ones, as required by
   * [[InMemoryLogReplay]]); and [[ADD_STATS_TO_USE_COL_NAME]] (to handle certain combinations of
   * config settings for delta.checkpoint.writeStatsAsJson and delta.checkpoint.writeStatsAsStruct).
   */
  protected def loadActions: DataFrame = {
    fileIndices.map(deltaLog.loadIndex(_))
      .reduceOption(_.union(_)).getOrElse(emptyDF)
      .withColumn(ADD_STATS_TO_USE_COL_NAME, col("add.stats"))
  }

  /**
   * Tombstones before the [[minFileRetentionTimestamp]] timestamp will be dropped from the
   * checkpoint.
   */
  private[delta] def minFileRetentionTimestamp: Long = {
    deltaLog.clock.getTimeMillis() - DeltaLog.tombstoneRetentionMillis(metadata)
  }

  /**
   * [[SetTransaction]]s before [[minSetTransactionRetentionTimestamp]] will be considered expired
   * and dropped from the snapshot.
   */
  private[delta] def minSetTransactionRetentionTimestamp: Option[Long] = {
    DeltaLog.minSetTransactionRetentionInterval(metadata).map(deltaLog.clock.getTimeMillis() - _)
  }

  private[delta] def getNumPartitions: Int = {
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_SNAPSHOT_PARTITIONS)
      .getOrElse(Snapshot.defaultNumSnapshotPartitions)
  }

  /**
   * Computes all the information that is needed by the checksum for the current snapshot.
   * May kick off state reconstruction if needed by any of the underlying fields.
   * Note that it's safe to set txnId to none, since the snapshot doesn't always have a txn
   * attached. E.g. if a snapshot is created by reading a checkpoint, then no txnId is present.
   */
  def computeChecksum: VersionChecksum = VersionChecksum(
    txnId = None,
    tableSizeBytes = sizeInBytes,
    numFiles = numOfFiles,
    numMetadata = numOfMetadata,
    numProtocol = numOfProtocol,
    inCommitTimestampOpt = getInCommitTimestampOpt,
    setTransactions = checksumOpt.flatMap(_.setTransactions),
    domainMetadata = domainMetadatasIfKnown,
    metadata = metadata,
    protocol = protocol,
    histogramOpt = fileSizeHistogram,
    allFiles = checksumOpt.flatMap(_.allFiles))

  /** Returns the data schema of the table, used for reading stats */
  def tableSchema: StructType = metadata.dataSchema

  def outputTableStatsSchema: StructType = metadata.dataSchema

  def outputAttributeSchema: StructType = metadata.dataSchema

  /** Returns the schema of the columns written out to file (overridden in write path) */
  def dataSchema: StructType = metadata.dataSchema

  /** Return the set of properties of the table. */
  def getProperties: mutable.Map[String, String] = {
    val base = new mutable.LinkedHashMap[String, String]()
    metadata.configuration.foreach { case (k, v) =>
      if (k != "path") {
        base.put(k, v)
      }
    }
    base.put(Protocol.MIN_READER_VERSION_PROP, protocol.minReaderVersion.toString)
    base.put(Protocol.MIN_WRITER_VERSION_PROP, protocol.minWriterVersion.toString)
    if (protocol.supportsReaderFeatures || protocol.supportsWriterFeatures) {
      val features = protocol.readerAndWriterFeatureNames.map(name =>
        s"${TableFeatureProtocolUtils.FEATURE_PROP_PREFIX}$name" ->
          TableFeatureProtocolUtils.FEATURE_PROP_SUPPORTED)
      base ++ features.toSeq.sorted
    } else {
      base
    }
  }

  /** The [[CheckpointProvider]] for the underlying checkpoint */
  lazy val checkpointProvider: CheckpointProvider = logSegment.checkpointProvider match {
    case cp: CheckpointProvider => cp
    case uninitializedProvider: UninitializedCheckpointProvider =>
      CheckpointProvider(spark, this, checksumOpt, uninitializedProvider)
    case o => throw new IllegalStateException(s"Unknown checkpoint provider: ${o.getClass.getName}")
  }

  def redactedPath: String =
    Utils.redact(spark.sessionState.conf.stringRedactionPattern, path.toUri.toString)

  /**
   * Ensures that commit files are backfilled up to the current version in the snapshot.
   *
   * This method checks if there are any un-backfilled versions up to the current version and
   * triggers the backfilling process using the commit-coordinator. It verifies that the delta file
   * for the current version exists after the backfilling process.
   *
   * @throws IllegalStateException
   *   if the delta file for the current version is not found after backfilling.
   */
  def ensureCommitFilesBackfilled(tableIdentifierOpt: Option[TableIdentifier]): Unit = {
    val tableCommitCoordinatorClient = getTableCommitCoordinatorForWrites.getOrElse {
      return
    }
    val minUnbackfilledVersion = DeltaCommitFileProvider(this).minUnbackfilledVersion
    if (minUnbackfilledVersion <= version) {
      val hadoopConf = deltaLog.newDeltaHadoopConf()
      tableCommitCoordinatorClient.backfillToVersion(
        tableIdentifierOpt,
        version,
        lastKnownBackfilledVersion = Some(minUnbackfilledVersion - 1))
      val fs = deltaLog.logPath.getFileSystem(hadoopConf)
      val expectedBackfilledDeltaFile = FileNames.unsafeDeltaFile(deltaLog.logPath, version)
      if (!fs.exists(expectedBackfilledDeltaFile)) {
        throw new IllegalStateException("Backfilling of commit files failed. " +
          s"Expected delta file $expectedBackfilledDeltaFile not found.")
      }
    }
  }


  protected def emptyDF: DataFrame =
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], logSchema)


  def logInfo(msg: MessageWithContext): Unit = {
    super.logInfo(log"[tableId=${MDC(DeltaLogKeys.TABLE_ID, deltaLog.tableId)}] " + msg)
  }

  def logWarning(msg: MessageWithContext): Unit = {
    super.logWarning(log"[tableId=${MDC(DeltaLogKeys.TABLE_ID, deltaLog.tableId)}] " + msg)
  }

  def logWarning(msg: MessageWithContext, throwable: Throwable): Unit = {
    super.logWarning(log"[tableId=${MDC(DeltaLogKeys.TABLE_ID, deltaLog.tableId)}] " + msg,
      throwable)
  }

  def logError(msg: MessageWithContext): Unit = {
    super.logError(log"[tableId=${MDC(DeltaLogKeys.TABLE_ID, deltaLog.tableId)}] " + msg)
  }

  def logError(msg: MessageWithContext, throwable: Throwable): Unit = {
    super.logError(log"[tableId=${MDC(DeltaLogKeys.TABLE_ID, deltaLog.tableId)}] " + msg, throwable)
  }

  override def toString: String =
    s"${getClass.getSimpleName}(path=$path, version=$version, metadata=$metadata, " +
      s"logSegment=$logSegment, checksumOpt=$checksumOpt)"

  logInfo(log"Created snapshot ${MDC(DeltaLogKeys.SNAPSHOT, this)}")
  init()
}

object Snapshot extends DeltaLogging {

  // Used by [[loadActions]] and [[stateReconstruction]]
  val ADD_STATS_TO_USE_COL_NAME = "add_stats_to_use"

  private val defaultNumSnapshotPartitions: Int = 50

  /** Verifies that a set of delta or checkpoint files to be read actually belongs to this table. */
  private def assertLogFilesBelongToTable(logBasePath: Path, files: Seq[FileStatus]): Unit = {
    val logPath = new Path(logBasePath.toUri)
    val commitDirPath = FileNames.commitDirPath(logPath)
    files.map(_.getPath).foreach { filePath =>
      val commitParent = new Path(filePath.toUri).getParent
      if (commitParent != logPath && commitParent != commitDirPath) {
        // scalastyle:off throwerror
        throw new AssertionError(s"File ($filePath) doesn't belong in the " +
          s"transaction log at $logBasePath.")
        // scalastyle:on throwerror
      }
    }
  }
}

/**
 * An initial snapshot with only metadata specified. Useful for creating a DataFrame from an
 * existing parquet table during its conversion to delta.
 *
 * @param logPath the path to transaction log
 * @param deltaLog the delta log object
 * @param metadata the metadata of the table
 */
class InitialSnapshot(
    val logPath: Path,
    override val deltaLog: DeltaLog,
    override val metadata: Metadata)
  extends Snapshot(
    path = logPath,
    version = -1,
    logSegment = LogSegment.empty(logPath),
    deltaLog = deltaLog,
    checksumOpt = None
  ) {

  def this(logPath: Path, deltaLog: DeltaLog) = this(
    logPath,
    deltaLog,
    Metadata(
      configuration = DeltaConfigs.mergeGlobalConfigs(
        sqlConfs = SparkSession.active.sessionState.conf,
        tableConf = Map.empty,
        ignoreProtocolConfsOpt = Some(
          DeltaConfigs.ignoreProtocolDefaultsIsSet(
            sqlConfs = SparkSession.active.sessionState.conf,
            tableConf = deltaLog.allOptions))),
      createdTime = Some(System.currentTimeMillis())))

  override def stateDS: Dataset[SingleAction] = emptyDF.as[SingleAction]
  override def stateDF: DataFrame = emptyDF
  override protected lazy val computedState: SnapshotState = initialState(metadata)
  override def protocol: Protocol = computedState.protocol
  override protected lazy val getInCommitTimestampOpt: Option[Long] = None

  // The [[InitialSnapshot]] is not backed by any external commit-coordinator.
  override val tableCommitCoordinatorClientOpt: Option[TableCommitCoordinatorClient] = None

  // Commit 0 cannot be performed through a commit coordinator.
  override def getTableCommitCoordinatorForWrites: Option[TableCommitCoordinatorClient] = None

  override def timestamp: Long = -1L
}
