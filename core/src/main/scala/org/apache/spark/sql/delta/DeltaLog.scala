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
import java.io.File
import java.lang.ref.WeakReference
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try
import scala.util.control.NonFatal

import com.databricks.spark.util.TagDefinitions._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.{SchemaMergingUtils, SchemaUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LogStoreProvider
import com.google.common.cache.{CacheBuilder, RemovalNotification}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper
import org.apache.spark.sql.catalyst.util.FailFastMode
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util._

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
    val options: Map[String, String],
    val clock: Clock
  ) extends Checkpoints
  with MetadataCleanup
  with LogStoreProvider
  with SnapshotManagement
  with DeltaFileFormat
  with ReadChecksum {

  import org.apache.spark.sql.delta.util.FileNames._


  private lazy implicit val _clock = clock

  protected def spark = SparkSession.active

  /**
   * Keep a reference to `SparkContext` used to create `DeltaLog`. `DeltaLog` cannot be used when
   * `SparkContext` is stopped. We keep the reference so that we can check whether the cache is
   * still valid and drop invalid `DeltaLog`` objects.
   */
  private val sparkContext = new WeakReference(spark.sparkContext)

  /**
   * Returns the Hadoop [[Configuration]] object which can be used to access the file system. All
   * Delta code should use this method to create the Hadoop [[Configuration]] object, so that the
   * hadoop file system configurations specified in DataFrame options will come into effect.
   */
  // scalastyle:off deltahadoopconfiguration
  final def newDeltaHadoopConf(): Configuration =
    spark.sessionState.newHadoopConfWithOptions(options)
  // scalastyle:on deltahadoopconfiguration

  /** Used to read and write physical log files and checkpoints. */
  lazy val store = createLogStore(spark)

  /** Use ReentrantLock to allow us to call `lockInterruptibly` */
  protected val deltaLogLock = new ReentrantLock()

  /** Delta History Manager containing version and commit history. */
  lazy val history = new DeltaHistoryManager(
    this, spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_HISTORY_PAR_SEARCH_THRESHOLD))

  /* --------------- *
   |  Configuration  |
   * --------------- */

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

  // TODO: There is a race here where files could get dropped when increasing the
  // retention interval...
  protected def metadata = if (snapshot == null) Metadata() else snapshot.metadata

  /**
   * Checks whether this table only accepts appends. If so it will throw an error in operations that
   * can remove data such as DELETE/UPDATE/MERGE.
   */
  def assertRemovable(): Unit = {
    if (DeltaConfigs.IS_APPEND_ONLY.fromMetaData(metadata)) {
      throw DeltaErrors.modifyAppendOnlyTableException(metadata.name)
    }
  }

  /** The unique identifier for this table. */
  def tableId: String = metadata.id

  /**
   * Combines the tableId with the path of the table to ensure uniqueness. Normally `tableId`
   * should be globally unique, but nothing stops users from copying a Delta table directly to
   * a separate location, where the transaction log is copied directly, causing the tableIds to
   * match. When users mutate the copied table, and then try to perform some checks joining the
   * two tables, optimizations that depend on `tableId` alone may not be correct. Hence we use a
   * composite id.
   */
  private[delta] def compositeId: (String, Path) = tableId -> dataPath

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

  /**
   * Creates a [[LogicalRelation]] for a given [[DeltaLogFileIndex]], with all necessary file source
   * options taken from the Delta Log. All reads of Delta metadata files should use this method.
   */
  def indexToRelation(
      index: DeltaLogFileIndex,
      schema: StructType = Action.logSchema): LogicalRelation = {
    val formatSpecificOptions: Map[String, String] = index.format match {
      case DeltaLogFileIndex.COMMIT_FILE_FORMAT =>
        // Don't tolerate malformed JSON when parsing Delta log actions (default is PERMISSIVE)
        Map("mode" -> FailFastMode.name)
      case _ => Map.empty
    }
    // Delta should NEVER ignore missing or corrupt metadata files, because doing so can render the
    // entire table unusable. Hard-wire that into the file source options so the user can't override
    // it by setting spark.sql.files.ignoreCorruptFiles or spark.sql.files.ignoreMissingFiles.
    //
    // NOTE: This should ideally be [[FileSourceOptions.IGNORE_CORRUPT_FILES]] etc., but those
    // constants are only available since spark-3.4. By hard-coding the values here instead, we
    // preserve backward compatibility when compiling Delta against older spark versions (tho
    // obviously the desired protection would be missing in that case).
    val allOptions = options ++ formatSpecificOptions ++ Map(
      "ignoreCorruptFiles" -> "false",
      "ignoreMissingFiles" -> "false"
    )
    val fsRelation = HadoopFsRelation(
      index, index.partitionSchema, schema, None, index.format, allOptions)(spark)
    LogicalRelation(fsRelation)
  }

  /**
   * Load the data using the FileIndex. This allows us to skip many checks that add overhead, e.g.
   * file existence checks, partitioning schema inference.
   */
  def loadIndex(
      index: DeltaLogFileIndex,
      schema: StructType = Action.logSchema): DataFrame = {
    Dataset.ofRows(spark, indexToRelation(index, schema))
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
      val txn = startTransaction()
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
    if (newVersion.minReaderVersion == currentVersion.minReaderVersion &&
        newVersion.minWriterVersion == currentVersion.minWriterVersion) {
      logConsole(s"Table $dataPath is already at protocol version $newVersion.")
      return
    }

    val txn = startTransaction()
    try {
      SchemaMergingUtils.checkColumnNameDuplication(txn.metadata.schema, "in the table schema")
    } catch {
      case e: AnalysisException =>
        throw DeltaErrors.duplicateColumnsOnUpdateTable(e)
    }
    txn.commit(Seq(newVersion), DeltaOperations.UpgradeProtocol(newVersion))
    logConsole(s"Upgraded table at $dataPath to $newVersion.")
  }

  /**
   * Get all actions starting from "startVersion" (inclusive). If `startVersion` doesn't exist,
   * return an empty Iterator.
   */
  def getChanges(
      startVersion: Long,
      failOnDataLoss: Boolean = false): Iterator[(Long, Seq[Action])] = {
    val hadoopConf = newDeltaHadoopConf()
    val deltas = store.listFrom(deltaFile(logPath, startVersion), hadoopConf).filter(isDeltaFile)
    // Subtract 1 to ensure that we have the same check for the inclusive startVersion
    var lastSeenVersion = startVersion - 1
    deltas.map { status =>
      val p = status.getPath
      val version = deltaVersion(p)
      if (failOnDataLoss && version > lastSeenVersion + 1) {
        throw DeltaErrors.failOnDataLossException(lastSeenVersion + 1, version)
      }
      lastSeenVersion = version
      (version, store.read(p, hadoopConf).map(Action.fromJson))
    }
  }

  /**
   * Get access to all actions starting from "startVersion" (inclusive) via [[FileStatus]].
   * If `startVersion` doesn't exist, return an empty Iterator.
   */
  def getChangeLogFiles(
      startVersion: Long,
      failOnDataLoss: Boolean = false): Iterator[(Long, FileStatus)] = {
    val deltas = store.listFrom(deltaFile(logPath, startVersion), newDeltaHadoopConf())
      .filter(isDeltaFile)
    // Subtract 1 to ensure that we have the same check for the inclusive startVersion
    var lastSeenVersion = startVersion - 1
    deltas.map { status =>
      val version = deltaVersion(status)
      if (failOnDataLoss && version > lastSeenVersion + 1) {
        throw DeltaErrors.failOnDataLossException(lastSeenVersion + 1, version)
      }
      lastSeenVersion = version
      (version, status)
    }
  }

  /* --------------------- *
   |  Protocol validation  |
   * --------------------- */

  /**
   * Asserts that the client is up to date with the protocol and
   * allowed to read the table that is using the given `protocol`.
   */
  def protocolRead(protocol: Protocol): Unit = {
    val supportedReaderVersion =
      Action.supportedProtocolVersion(Some(spark.sessionState.conf)).minReaderVersion
    if (protocol != null && supportedReaderVersion < protocol.minReaderVersion) {
      recordDeltaEvent(
        this,
        "delta.protocol.failure.read",
        data = Map(
          "clientVersion" -> supportedReaderVersion,
          "minReaderVersion" -> protocol.minReaderVersion))
      throw new InvalidProtocolVersionException
    }
  }

  /**
   * Asserts that the client is up to date with the protocol and
   * allowed to write to the table that is using the given `protocol`.
   */
  def protocolWrite(protocol: Protocol, logUpgradeMessage: Boolean = true): Unit = {
    val supportedWriterVersion =
      Action.supportedProtocolVersion(Some(spark.sessionState.conf)).minWriterVersion
    if (protocol != null && supportedWriterVersion < protocol.minWriterVersion) {
      recordDeltaEvent(
        this,
        "delta.protocol.failure.write",
        data = Map(
          "clientVersion" -> supportedWriterVersion,
          "minWriterVersion" -> protocol.minWriterVersion))
      throw new InvalidProtocolVersionException
    }
  }

  /* ---------------------------------------- *
   |  Log Directory Management and Retention  |
   * ---------------------------------------- */

  /** Whether a Delta table exists at this directory. */
  def tableExists: Boolean = snapshot.version >= 0

  def isSameLogAs(otherLog: DeltaLog): Boolean = this.compositeId == otherLog.compositeId

  /** Creates the log directory if it does not exist. */
  def ensureLogDirectoryExist(): Unit = {
    val fs = logPath.getFileSystem(newDeltaHadoopConf())
    if (!fs.exists(logPath)) {
      if (!fs.mkdirs(logPath)) {
        throw DeltaErrors.cannotCreateLogPathException(logPath.toString)
      }
    }
  }

  /**
   * Create the log directory. Unlike `ensureLogDirectoryExist`, this method doesn't check whether
   * the log directory exists and it will ignore the return value of `mkdirs`.
   */
  def createLogDirectory(): Unit = {
    logPath.getFileSystem(newDeltaHadoopConf()).mkdirs(logPath)
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

    val hadoopOptions = snapshot.metadata.format.options ++ options

    val relation = HadoopFsRelation(
      fileIndex,
      partitionSchema =
        DeltaColumnMapping.dropColumnMappingMetadata(snapshot.metadata.partitionSchema),
      // We pass all table columns as `dataSchema` so that Spark will preserve the partition column
      // locations. Otherwise, for any partition columns not in `dataSchema`, Spark would just
      // append them to the end of `dataSchema`.
      dataSchema =
        DeltaColumnMapping.dropColumnMappingMetadata(
          ColumnWithDefaultExprUtils.removeDefaultExpressions(snapshot.metadata.schema)),
      bucketSpec = None,
      snapshot.deltaLog.fileFormat(snapshot.metadata),
      hadoopOptions)(spark)

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
      snapshotToUseOpt: Option[Snapshot] = None,
      isTimeTravelQuery: Boolean = false,
      cdcOptions: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty): BaseRelation = {

    /** Used to link the files present in the table into the query planner. */
    val snapshotToUse = snapshotToUseOpt.getOrElse(snapshot)
    if (snapshotToUse.version < 0) {
      // A negative version here means the dataPath is an empty directory. Read query should error
      // out in this case.
      throw DeltaErrors.pathNotExistsException(dataPath.toString)
    }

    // For CDC we have to return the relation that represents the change data instead of actual
    // data.
    if (!cdcOptions.isEmpty) {
      recordDeltaEvent(this, "delta.cdf.read", data = cdcOptions.asCaseSensitiveMap())
      return CDCReader.getCDCRelation(spark,
        this, snapshotToUse, partitionFilters, spark.sessionState.conf, cdcOptions)
    }

    val fileIndex = TahoeLogFileIndex(
      spark, this, dataPath, snapshotToUse, partitionFilters, isTimeTravelQuery)
    var bucketSpec: Option[BucketSpec] = None
    new HadoopFsRelation(
      fileIndex,
      partitionSchema = DeltaColumnMapping.dropColumnMappingMetadata(
        snapshotToUse.metadata.partitionSchema),
      // We pass all table columns as `dataSchema` so that Spark will preserve the partition column
      // locations. Otherwise, for any partition columns not in `dataSchema`, Spark would just
      // append them to the end of `dataSchema`
      dataSchema = DeltaColumnMapping.dropColumnMappingMetadata(
        ColumnWithDefaultExprUtils.removeDefaultExpressions(
          SchemaUtils.dropNullTypeColumns(snapshotToUse.metadata.schema))),
      bucketSpec = bucketSpec,
      fileFormat(snapshotToUse.metadata),
      // `metadata.format.options` is not set today. Even if we support it in future, we shouldn't
      // store any file system options since they may contain credentials. Hence, it will never
      // conflict with `DeltaLog.options`.
      snapshotToUse.metadata.format.options ++ options)(spark) with InsertableRelation {
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

  /**
   * The key type of `DeltaLog` cache. It's a pair of the canonicalized table path and the file
   * system options (options starting with "fs." or "dfs." prefix) passed into
   * `DataFrameReader/Writer`
   */
  private type DeltaLogCacheKey = (Path, Map[String, String])

  /**
   * We create only a single [[DeltaLog]] for any given `DeltaLogCacheKey` to avoid wasted work
   * in reconstructing the log.
   */
  private val deltaLogCache = {
    val builder = CacheBuilder.newBuilder()
      .expireAfterAccess(60, TimeUnit.MINUTES)
      .removalListener((removalNotification: RemovalNotification[DeltaLogCacheKey, DeltaLog]) => {
          val log = removalNotification.getValue
          try log.snapshot.uncache() catch {
            case _: java.lang.NullPointerException =>
            // Various layers will throw null pointer if the RDD is already gone.
          }
      })
    sys.props.get("delta.log.cacheSize")
      .flatMap(v => Try(v.toLong).toOption)
      .foreach(builder.maximumSize)
    builder.build[DeltaLogCacheKey, DeltaLog]()
  }


  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: String): DeltaLog = {
    apply(spark, new Path(dataPath, "_delta_log"), Map.empty, new SystemClock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: String, options: Map[String, String]): DeltaLog = {
    apply(spark, new Path(dataPath, "_delta_log"), options, new SystemClock)
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
  def forTable(spark: SparkSession, dataPath: Path, options: Map[String, String]): DeltaLog = {
    apply(spark, new Path(dataPath, "_delta_log"), options, new SystemClock)
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

  /** Helper for creating a log for the table. */
  def forTable(spark: SparkSession, tableName: TableIdentifier): DeltaLog = {
    forTable(spark, tableName, new SystemClock)
  }

  /** Helper for creating a log for the table. */
  def forTable(spark: SparkSession, table: CatalogTable): DeltaLog = {
    forTable(spark, table, new SystemClock)
  }

  /** Helper for creating a log for the table. */
  def forTable(spark: SparkSession, tableName: TableIdentifier, clock: Clock): DeltaLog = {
    if (DeltaTableIdentifier.isDeltaPath(spark, tableName)) {
      forTable(spark, new Path(tableName.table))
    } else {
      forTable(spark, spark.sessionState.catalog.getTableMetadata(tableName), clock)
    }
  }

  /** Helper for creating a log for the table. */
  def forTable(spark: SparkSession, table: CatalogTable, clock: Clock): DeltaLog = {
    val log = apply(spark, new Path(new Path(table.location), "_delta_log"), clock)
    log
  }

  /** Helper for creating a log for the table. */
  def forTable(spark: SparkSession, deltaTable: DeltaTableIdentifier): DeltaLog = {
    if (deltaTable.path.isDefined) {
      forTable(spark, deltaTable.path.get)
    } else {
      forTable(spark, deltaTable.table.get)
    }
  }

  private def apply(spark: SparkSession, rawPath: Path, clock: Clock = new SystemClock): DeltaLog =
    apply(spark, rawPath, Map.empty, clock)


  private def apply(
      spark: SparkSession,
      rawPath: Path,
      options: Map[String, String],
      clock: Clock
  ): DeltaLog = {
    val fileSystemOptions: Map[String, String] =
      if (spark.sessionState.conf.getConf(
          DeltaSQLConf.LOAD_FILE_SYSTEM_CONFIGS_FROM_DATAFRAME_OPTIONS)) {
        // We pick up only file system options so that we don't pass any parquet or json options to
        // the code that reads Delta transaction logs.
        options.filterKeys { k =>
          DeltaTableUtils.validDeltaTableHadoopPrefixes.exists(k.startsWith)
        }.toMap
      } else {
        Map.empty
      }
    // scalastyle:off deltahadoopconfiguration
    val hadoopConf = spark.sessionState.newHadoopConfWithOptions(fileSystemOptions)
    // scalastyle:on deltahadoopconfiguration
    var path = rawPath
    val fs = path.getFileSystem(hadoopConf)
    path = fs.makeQualified(path)
    def createDeltaLog(): DeltaLog = recordDeltaOperation(
      null,
      "delta.log.create",
      Map(TAG_TAHOE_PATH -> path.getParent.toString)) {
        AnalysisHelper.allowInvokingTransformsInAnalyzer {
          new DeltaLog(
            logPath = path,
            dataPath = path.getParent,
            options = fileSystemOptions,
            clock = clock
          )
        }
    }
    def getDeltaLogFromCache(): DeltaLog = {
      // The following cases will still create a new ActionLog even if there is a cached
      // ActionLog using a different format path:
      // - Different `scheme`
      // - Different `authority` (e.g., different user tokens in the path)
      // - Different mount point.
      try {
        deltaLogCache.get(path -> fileSystemOptions, () => createDeltaLog())
      } catch {
        case e: com.google.common.util.concurrent.UncheckedExecutionException =>
          throw e.getCause
      }
    }

    val deltaLog = getDeltaLogFromCache()
    if (Option(deltaLog.sparkContext.get).map(_.isStopped).getOrElse(true)) {
      // Invalid the cached `DeltaLog` and create a new one because the `SparkContext` of the cached
      // `DeltaLog` has been stopped.
      deltaLogCache.invalidate(path -> fileSystemOptions)
      getDeltaLogFromCache()
    } else {
      deltaLog
    }
  }

  /** Invalidate the cached DeltaLog object for the given `dataPath`. */
  def invalidateCache(spark: SparkSession, dataPath: Path): Unit = {
    try {
      val rawPath = new Path(dataPath, "_delta_log")
      // scalastyle:off deltahadoopconfiguration
      // This method cannot be called from DataFrameReader/Writer so it's safe to assume the user
      // has set the correct file system configurations in the session configs.
      val fs = rawPath.getFileSystem(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration
      val path = fs.makeQualified(rawPath)

      if (spark.sessionState.conf.getConf(
          DeltaSQLConf.LOAD_FILE_SYSTEM_CONFIGS_FROM_DATAFRAME_OPTIONS)) {
        // We rely on the fact that accessing the key set doesn't modify the entry access time. See
        // `CacheBuilder.expireAfterAccess`.
        val keysToBeRemoved = mutable.ArrayBuffer[DeltaLogCacheKey]()
        val iter = deltaLogCache.asMap().keySet().iterator()
        while (iter.hasNext) {
          val key = iter.next()
          if (key._1 == path) {
            keysToBeRemoved += key
          }
        }
        deltaLogCache.invalidateAll(keysToBeRemoved.asJava)
      } else {
        deltaLogCache.invalidate(path -> Map.empty)
      }
    } catch {
      case NonFatal(e) => logWarning(e.getMessage, e)
    }
  }

  def clearCache(): Unit = {
    deltaLogCache.invalidateAll()
  }

  /** Return the number of cached `DeltaLog`s. Exposing for testing */
  private[delta] def cacheSize: Long = {
    deltaLogCache.size()
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
    val expr = rewrittenFilters.reduceLeftOption(And).getOrElse(Literal.TrueLiteral)
    val columnFilter = new Column(expr)
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
          case Some(f: StructField) =>
            val name = DeltaColumnMapping.getPhysicalName(f)
            Cast(
              UnresolvedAttribute(partitionColumnPrefixes ++ Seq("partitionValues", name)),
              f.dataType)
          case None =>
            // This should not be able to happen, but the case was present in the original code so
            // we kept it to be safe.
            log.error(s"Partition filter referenced column ${a.name} not in the partition schema")
            UnresolvedAttribute(partitionColumnPrefixes ++ Seq("partitionValues", a.name))
        }
    })
  }

  /** How long to keep around SetTransaction actions before physically deleting them. */
  def minSetTransactionRetentionInterval(metadata: Metadata): Option[Long] = {
    DeltaConfigs.TRANSACTION_ID_RETENTION_DURATION
      .fromMetaData(metadata)
      .map(DeltaConfigs.getMilliSeconds)
  }
  /** How long to keep around logically deleted files before physically deleting them. */
  def tombstoneRetentionMillis(metadata: Metadata): Long = {
    DeltaConfigs.getMilliSeconds(DeltaConfigs.TOMBSTONE_RETENTION.fromMetaData(metadata))
  }
}
