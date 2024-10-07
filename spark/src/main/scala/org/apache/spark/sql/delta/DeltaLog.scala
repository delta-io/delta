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
import java.io.IOException
import java.lang.ref.WeakReference
import java.net.URI
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try
import scala.util.control.NonFatal

import com.databricks.spark.util.TagDefinitions._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.coordinatedcommits.CoordinatedCommitsUtils
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.{SchemaMergingUtils, SchemaUtils}
import org.apache.spark.sql.delta.sources._
import org.apache.spark.sql.delta.storage.LogStoreProvider
import org.apache.spark.sql.delta.util.FileNames
import com.google.common.cache.{Cache, CacheBuilder, RemovalNotification}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{FileSourceOptions, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStatistics, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper
import org.apache.spark.sql.catalyst.util.FailFastMode
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.internal.SQLConf
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
 *
 * @param logPath Path of the Delta log JSONs.
 * @param dataPath Path of the data files.
 * @param options Filesystem options filtered from `allOptions`.
 * @param allOptions All options provided by the user, for example via `df.write.option()`. This
 *                   includes but not limited to filesystem and table properties.
 * @param initialTableIdentifier Identifier of the table when the log is initialized.
 * @param clock Clock to be used when starting a new transaction.
 */
class DeltaLog private(
    val logPath: Path,
    val dataPath: Path,
    val options: Map[String, String],
    val allOptions: Map[String, String],
    val initialTableIdentifier: Option[TableIdentifier],
    val clock: Clock
  ) extends Checkpoints
  with MetadataCleanup
  with LogStoreProvider
  with SnapshotManagement
  with DeltaFileFormat
  with ProvidesUniFormConverters
  with ReadChecksum {

  import org.apache.spark.sql.delta.files.TahoeFileIndex
  import org.apache.spark.sql.delta.util.FileNames._

  /**
   * Path to sidecar directory.
   * This is intentionally kept `lazy val` as otherwise any other constructor codepaths in DeltaLog
   * (e.g. SnapshotManagement etc) will see it as null as they are executed before this line is
   * called.
   */
  lazy val sidecarDirPath: Path = FileNames.sidecarDirPath(logPath)


  protected def spark = SparkSession.active

  checkRequiredConfigurations()

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

  /** The unique identifier for this table. */
  def tableId: String = unsafeVolatileMetadata.id // safe because table id never changes

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
   * Creates a [[LogicalRelation]] for a given [[DeltaLogFileIndex]], with all necessary file source
   * options taken from the Delta Log. All reads of Delta metadata files should use this method.
   */
  def indexToRelation(
      index: DeltaLogFileIndex,
      schema: StructType = Action.logSchema): LogicalRelation = {
    DeltaLog.indexToRelation(spark, index, options, schema)
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
   * Returns a new [[OptimisticTransaction]] that can be used to read the current state of the log
   * and then commit updates. The reads and updates will be checked for logical conflicts with any
   * concurrent writes to the log, and post-commit hooks can be used to notify the table's catalog
   * of schema changes, etc.
   *
   * Note that all reads in a transaction must go through the returned transaction object, and not
   * directly to the [[DeltaLog]] otherwise they will not be checked for conflicts.
   *
   * @param catalogTableOpt The [[CatalogTable]] for the table this transaction updates. Passing
   * None asserts this is a path-based table with no catalog entry.
   *
   * @param snapshotOpt THe [[Snapshot]] this transaction should use, if not latest.
   */
  def startTransaction(
      catalogTableOpt: Option[CatalogTable],
      snapshotOpt: Option[Snapshot] = None): OptimisticTransaction = {
    TransactionExecutionObserver.getObserver.startingTransaction {
      new OptimisticTransaction(this, catalogTableOpt, snapshotOpt)
    }
  }

  /** Legacy/compat overload that does not require catalog table information. Avoid prod use. */
  @deprecated("Please use the CatalogTable overload instead", "3.0")
  def startTransaction(): OptimisticTransaction = {
    startTransaction(catalogTableOpt = None, snapshotOpt = None)
  }

  /**
   * Execute a piece of code within a new [[OptimisticTransaction]]. Reads/write sets will
   * be recorded for this table, and all other tables will be read
   * at a snapshot that is pinned on the first access.
   *
   * @param catalogTableOpt The [[CatalogTable]] for the table this transaction updates. Passing
   * None asserts this is a path-based table with no catalog entry.
   *
   * @param snapshotOpt THe [[Snapshot]] this transaction should use, if not latest.
   * @note This uses thread-local variable to make the active transaction visible. So do not use
   *       multi-threaded code in the provided thunk.
   */
  def withNewTransaction[T](
      catalogTableOpt: Option[CatalogTable],
      snapshotOpt: Option[Snapshot] = None)(
      thunk: OptimisticTransaction => T): T = {
    val txn = startTransaction(catalogTableOpt, snapshotOpt)
    OptimisticTransaction.setActive(txn)
    try {
      thunk(txn)
    } finally {
      OptimisticTransaction.clearActive()
    }
  }

  /** Legacy/compat overload that does not require catalog table information. Avoid prod use. */
  @deprecated("Please use the CatalogTable overload instead", "3.0")
  def withNewTransaction[T](thunk: OptimisticTransaction => T): T = {
    val txn = startTransaction()
    OptimisticTransaction.setActive(txn)
    try {
      thunk(txn)
    } finally {
      OptimisticTransaction.clearActive()
    }
  }


  /**
   * Upgrade the table's protocol version, by default to the maximum recognized reader and writer
   * versions in this Delta release. This method only upgrades protocol version, and will fail if
   * the new protocol version is not a superset of the original one used by the snapshot.
   */
  def upgradeProtocol(
      catalogTable: Option[CatalogTable],
      snapshot: Snapshot,
      newVersion: Protocol): Unit = {
    val currentVersion = snapshot.protocol
    if (newVersion == currentVersion) {
      logConsole(s"Table $dataPath is already at protocol version $newVersion.")
      return
    }
    if (!currentVersion.canUpgradeTo(newVersion)) {
      throw new ProtocolDowngradeException(currentVersion, newVersion)
    }

    val txn = startTransaction(catalogTable, Some(snapshot))
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
   * Callers are encouraged to use the other override which takes the endVersion if available to
   * avoid I/O and improve performance of this method.
   */
  def getChanges(
      startVersion: Long,
      failOnDataLoss: Boolean = false): Iterator[(Long, Seq[Action])] = {
    getChangeLogFiles(startVersion, failOnDataLoss).map { case (version, status) =>
      (version, store.read(status, newDeltaHadoopConf()).map(Action.fromJson(_)))
    }
  }

  private[sql] def getChanges(
      startVersion: Long,
      endVersion: Long,
      failOnDataLoss: Boolean): Iterator[(Long, Seq[Action])] = {
    getChangeLogFiles(startVersion, endVersion, failOnDataLoss).map { case (version, status) =>
      (version, store.read(status, newDeltaHadoopConf()).map(Action.fromJson(_)))
    }
  }

  private[sql] def getChangeLogFiles(
      startVersion: Long,
      endVersion: Long,
      failOnDataLoss: Boolean): Iterator[(Long, FileStatus)] = {
    implicit class IteratorWithStopAtHelper[T](underlying: Iterator[T]) {
      // This method is used to stop the iterator when the condition is met.
      def stopAt(stopAtFunc: (T) => Boolean): Iterator[T] = new Iterator[T] {
        var shouldStop = false

        override def hasNext: Boolean = !shouldStop && underlying.hasNext

        override def next(): T = {
          val v = underlying.next()
          shouldStop = stopAtFunc(v)
          v
        }
      }
    }

    getChangeLogFiles(startVersion, failOnDataLoss)
      // takeWhile always looks at one extra item, which can trigger unnecessary work. Instead, we
      // stop if we've seen the item we believe should be the last interesting item, without
      // examining the one that follows.
      .stopAt { case (version, _) => version >= endVersion }
      // The last element in this iterator may not be <= endVersion, so we need to filter it out.
      .takeWhile { case (version, _) => version <= endVersion }
  }

  /**
   * Get access to all actions starting from "startVersion" (inclusive) via [[FileStatus]].
   * If `startVersion` doesn't exist, return an empty Iterator.
   * Callers are encouraged to use the other override which takes the endVersion if available to
   * avoid I/O and improve performance of this method.
   */
  def getChangeLogFiles(
      startVersion: Long,
      failOnDataLoss: Boolean = false): Iterator[(Long, FileStatus)] = {
    val deltasWithVersion = CoordinatedCommitsUtils.commitFilesIterator(this, startVersion)
    // Subtract 1 to ensure that we have the same check for the inclusive startVersion
    var lastSeenVersion = startVersion - 1
    deltasWithVersion.map { case (status, version) =>
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
   * Asserts the highest protocol supported by this client is not less than what required by the
   * table for performing read or write operations. This ensures the client to support a
   * greater-or-equal protocol versions and recognizes/supports all features enabled by the table.
   *
   * The operation type to be checked is passed as a string in `readOrWrite`. Valid values are
   * `read` and `write`.
   */
  private def protocolCheck(tableProtocol: Protocol, readOrWrite: String): Unit = {
    val clientSupportedProtocol = Action.supportedProtocolVersion()
    // Depending on the operation, pull related protocol versions out of Protocol objects.
    // `getEnabledFeatures` is a pointer to pull reader/writer features out of a Protocol.
    val (clientSupportedVersions, tableRequiredVersion, getEnabledFeatures) = readOrWrite match {
      case "read" => (
        Action.supportedReaderVersionNumbers,
        tableProtocol.minReaderVersion,
        (f: Protocol) => f.readerFeatureNames)
      case "write" => (
        Action.supportedWriterVersionNumbers,
        tableProtocol.minWriterVersion,
        (f: Protocol) => f.writerFeatureNames)
      case _ =>
        throw new IllegalArgumentException("Table operation must be either `read` or `write`.")
    }

    // Check is complete when both the protocol version and all referenced features are supported.
    val clientSupportedFeatureNames = getEnabledFeatures(clientSupportedProtocol)
    val tableEnabledFeatureNames = getEnabledFeatures(tableProtocol)
    if (tableEnabledFeatureNames.subsetOf(clientSupportedFeatureNames) &&
      clientSupportedVersions.contains(tableRequiredVersion)) {
      return
    }

    // Otherwise, either the protocol version, or few features referenced by the table, is
    // unsupported.
    val clientUnsupportedFeatureNames =
      tableEnabledFeatureNames.diff(clientSupportedFeatureNames)
    // Prepare event log constants and the appropriate error message handler.
    val (opType, versionKey, unsupportedFeaturesException) = readOrWrite match {
      case "read" => (
          "delta.protocol.failure.read",
          "minReaderVersion",
          DeltaErrors.unsupportedReaderTableFeaturesInTableException _)
      case "write" => (
          "delta.protocol.failure.write",
          "minWriterVersion",
          DeltaErrors.unsupportedWriterTableFeaturesInTableException _)
    }
    recordDeltaEvent(
      this,
      opType,
      data = Map(
        "clientVersion" -> clientSupportedVersions.max,
        versionKey -> tableRequiredVersion,
        "clientFeatures" -> clientSupportedFeatureNames.mkString(","),
        "clientUnsupportedFeatures" -> clientUnsupportedFeatureNames.mkString(",")))
    if (!clientSupportedVersions.contains(tableRequiredVersion)) {
      throw new InvalidProtocolVersionException(
        dataPath.toString(),
        tableProtocol.minReaderVersion,
        tableProtocol.minWriterVersion,
        Action.supportedReaderVersionNumbers.toSeq,
        Action.supportedWriterVersionNumbers.toSeq)
    } else {
      throw unsupportedFeaturesException(dataPath.toString(), clientUnsupportedFeatureNames)
    }
  }

  /**
   * Asserts that the table's protocol enabled all features that are active in the metadata.
   *
   * A mismatch shouldn't happen when the table has gone through a proper write process because we
   * require all active features during writes. However, other clients may void this guarantee.
   */
  def assertTableFeaturesMatchMetadata(
      targetProtocol: Protocol,
      targetMetadata: Metadata): Unit = {
    if (!targetProtocol.supportsReaderFeatures && !targetProtocol.supportsWriterFeatures) return

    val protocolEnabledFeatures = targetProtocol.writerFeatureNames
      .flatMap(TableFeature.featureNameToFeature)
    val activeFeatures =
      Protocol.extractAutomaticallyEnabledFeatures(spark, targetMetadata, targetProtocol)
    val activeButNotEnabled = activeFeatures.diff(protocolEnabledFeatures)
    if (activeButNotEnabled.nonEmpty) {
      throw DeltaErrors.tableFeatureMismatchException(activeButNotEnabled.map(_.name))
    }
  }

  /**
   * Asserts that the client is up to date with the protocol and allowed to read the table that is
   * using the given `protocol`.
   */
  def protocolRead(protocol: Protocol): Unit = {
    protocolCheck(protocol, "read")
  }

  /**
   * Asserts that the client is up to date with the protocol and allowed to write to the table
   * that is using the given `protocol`.
   */
  def protocolWrite(protocol: Protocol): Unit = {
    protocolCheck(protocol, "write")
  }

  /* ---------------------------------------- *
   |  Log Directory Management and Retention  |
   * ---------------------------------------- */

  /**
   * Whether a Delta table exists at this directory.
   * It is okay to use the cached volatile snapshot here, since the worst case is that the table
   * has recently started existing which hasn't been picked up here. If so, any subsequent command
   * that updates the table will see the right value.
   */
  def tableExists: Boolean = unsafeVolatileSnapshot.version >= 0

  def isSameLogAs(otherLog: DeltaLog): Boolean = this.compositeId == otherLog.compositeId

  /** Creates the log directory and commit directory if it does not exist. */
  def createLogDirectoriesIfNotExists(): Unit = {
    val fs = logPath.getFileSystem(newDeltaHadoopConf())
    def createDirIfNotExists(path: Path): Unit = {
      // Optimistically attempt to create the directory first without checking its existence.
      // This is efficient because we're assuming it's more likely that the directory doesn't
      // exist and it saves an filesystem existence check in that case.
      val (success, mkdirsIOExceptionOpt) = try {
        // Return value of false should mean the directory already existed (not an error) but
        // we will verify below because we're paranoid about buggy FileSystem implementations.
        (fs.mkdirs(path), None)
      } catch {
        // A FileAlreadyExistsException is expected if a non-directory object exists but an explicit
        // check is needed because buggy Hadoop FileSystem.mkdir wrongly throws the exception even
        // on existing directories.
        case io: IOException =>
          val dirExists =
            try {
              fs.getFileStatus(path).isDirectory
            } catch {
              case NonFatal(_) => false
            }
          (dirExists, Some(io))
      }
      if (!success) {
        throw DeltaErrors.cannotCreateLogPathException(
          logPath = logPath.toString,
          cause = mkdirsIOExceptionOpt.orNull)
      }
    }
    createDirIfNotExists(FileNames.commitDirPath(logPath))
  }

  /* ------------  *
   |  Integration  |
   * ------------  */

  /**
   * Returns a [[org.apache.spark.sql.DataFrame]] containing the new files within the specified
   * version range.
   */
  def createDataFrame(
      snapshot: SnapshotDescriptor,
      addFiles: Seq[AddFile],
      isStreaming: Boolean = false,
      actionTypeOpt: Option[String] = None): DataFrame = {
    val actionType = actionTypeOpt.getOrElse(if (isStreaming) "streaming" else "batch")
    // It's ok to not pass down the partitionSchema to TahoeBatchFileIndex. Schema evolution will
    // ensure any partitionSchema changes will be captured, and upon restart, the new snapshot will
    // be initialized with the correct partition schema again.
    val fileIndex = new TahoeBatchFileIndex(spark, actionType, addFiles, this, dataPath, snapshot)
    // Drop null type columns from the relation's schema if it's not a streaming query until
    // null type columns are fully supported.
    val dropNullTypeColumnsFromSchema = if (isStreaming) {
      // Can force the legacy behavior(dropping nullType columns) with a flag.
      SQLConf.get.getConf(DeltaSQLConf.DELTA_STREAMING_CREATE_DATAFRAME_DROP_NULL_COLUMNS)
    } else {
      // Allow configurable behavior for non-streaming sources. This is used for testing.
      SQLConf.get.getConf(DeltaSQLConf.DELTA_CREATE_DATAFRAME_DROP_NULL_COLUMNS)
    }
    val relation = buildHadoopFsRelationWithFileIndex(
      snapshot,
      fileIndex,
      bucketSpec = None,
      dropNullTypeColumnsFromSchema = dropNullTypeColumnsFromSchema)
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
      catalogTableOpt: Option[CatalogTable] = None,
      isTimeTravelQuery: Boolean = false): BaseRelation = {

    /** Used to link the files present in the table into the query planner. */
    // TODO: If snapshotToUse is unspecified, get the correct snapshot from update()
    val snapshotToUse = snapshotToUseOpt.getOrElse(unsafeVolatileSnapshot)
    if (snapshotToUse.version < 0) {
      // A negative version here means the dataPath is an empty directory. Read query should error
      // out in this case.
      throw DeltaErrors.pathNotExistsException(dataPath.toString)
    }

    val fileIndex = TahoeLogFileIndex(
      spark, this, dataPath, snapshotToUse, partitionFilters, isTimeTravelQuery)
    var bucketSpec: Option[BucketSpec] = None

    val r = buildHadoopFsRelationWithFileIndex(snapshotToUse, fileIndex, bucketSpec = bucketSpec)
    new HadoopFsRelation(
      r.location,
      r.partitionSchema,
      r.dataSchema,
      r.bucketSpec,
      r.fileFormat,
      r.options
    )(spark) with InsertableRelation {
      def insert(data: DataFrame, overwrite: Boolean): Unit = {
        val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
        WriteIntoDelta(
          deltaLog = DeltaLog.this,
          mode = mode,
          new DeltaOptions(Map.empty[String, String], spark.sessionState.conf),
          partitionColumns = Seq.empty,
          configuration = Map.empty,
          data = data,
          catalogTableOpt = catalogTableOpt).run(spark)
      }
    }
  }

  def buildHadoopFsRelationWithFileIndex(
      snapshot: SnapshotDescriptor,
      fileIndex: TahoeFileIndex,
      bucketSpec: Option[BucketSpec],
      dropNullTypeColumnsFromSchema: Boolean = true): HadoopFsRelation = {
    val dataSchema = if (dropNullTypeColumnsFromSchema) {
      SchemaUtils.dropNullTypeColumns(snapshot.metadata.schema)
    } else {
      snapshot.metadata.schema
    }
    HadoopFsRelation(
      fileIndex,
      partitionSchema = DeltaColumnMapping.dropColumnMappingMetadata(
        snapshot.metadata.partitionSchema),
      // We pass all table columns as `dataSchema` so that Spark will preserve the partition
      // column locations. Otherwise, for any partition columns not in `dataSchema`, Spark would
      // just append them to the end of `dataSchema`.
      dataSchema = DeltaColumnMapping.dropColumnMappingMetadata(
        DeltaTableUtils.removeInternalMetadata(spark, dataSchema)
      ),
      bucketSpec = bucketSpec,
      fileFormat(snapshot.protocol, snapshot.metadata),
      // `metadata.format.options` is not set today. Even if we support it in future, we shouldn't
      // store any file system options since they may contain credentials. Hence, it will never
      // conflict with `DeltaLog.options`.
      snapshot.metadata.format.options ++ options)(spark)
  }

  /**
   * Verify the required Spark conf for delta
   * Throw `DeltaErrors.configureSparkSessionWithExtensionAndCatalog` exception if
   * `spark.sql.catalog.spark_catalog` config is missing. We do not check for
   * `spark.sql.extensions` because DeltaSparkSessionExtension can alternatively
   * be activated using the `.withExtension()` API. This check can be disabled
   * by setting DELTA_CHECK_REQUIRED_SPARK_CONF to false.
   */
  protected def checkRequiredConfigurations(): Unit = {
    if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_REQUIRED_SPARK_CONFS_CHECK)) {
      if (!spark.conf.contains(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key)) {
        throw DeltaErrors.configureSparkSessionWithExtensionAndCatalog(None)
      }
    }
  }

  /**
   * Returns a proper path canonicalization function for the current Delta log.
   *
   * If `runsOnExecutors` is true, the returned method will use a broadcast Hadoop Configuration
   * so that the method is suitable for execution on executors. Otherwise, the returned method
   * will use a local Hadoop Configuration and the method can only be executed on the driver.
   */
  private[delta] def getCanonicalPathFunction(runsOnExecutors: Boolean): String => String = {
    val hadoopConf = newDeltaHadoopConf()
    // Wrap `hadoopConf` with a method to delay the evaluation to run on executors.
    val getHadoopConf = if (runsOnExecutors) {
      val broadcastHadoopConf =
        spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
      () => broadcastHadoopConf.value.value
    } else {
      () => hadoopConf
    }

    new DeltaLog.CanonicalPathFunction(getHadoopConf)
  }

  /**
   * Returns a proper path canonicalization UDF for the current Delta log.
   *
   * If `runsOnExecutors` is true, the returned UDF will use a broadcast Hadoop Configuration.
   * Otherwise, the returned UDF will use a local Hadoop Configuration and the UDF can
   * only be executed on the driver.
   */
  private[delta] def getCanonicalPathUdf(runsOnExecutors: Boolean = true): UserDefinedFunction = {
    DeltaUDF.stringFromString(getCanonicalPathFunction(runsOnExecutors))
  }
}

object DeltaLog extends DeltaLogging {

  /**
   * The key type of `DeltaLog` cache. It's a pair of the canonicalized table path and the file
   * system options (options starting with "fs." or "dfs." prefix) passed into
   * `DataFrameReader/Writer`
   */
  private type DeltaLogCacheKey = (Path, Map[String, String])

  /** The name of the subdirectory that holds Delta metadata files */
  private[delta] val LOG_DIR_NAME = "_delta_log"

  private[delta] def logPathFor(dataPath: String): Path = logPathFor(new Path(dataPath))
  private[delta] def logPathFor(dataPath: Path): Path =
    DeltaTableUtils.safeConcatPaths(dataPath, LOG_DIR_NAME)

  /**
   * We create only a single [[DeltaLog]] for any given `DeltaLogCacheKey` to avoid wasted work
   * in reconstructing the log.
   */
  type CacheKey = (Path, Map[String, String])
  private[delta] def getOrCreateCache(conf: SQLConf):
      Cache[CacheKey, DeltaLog] = synchronized {
    deltaLogCache match {
      case Some(c) => c
      case None =>
        val builder = createCacheBuilder(conf)
          .removalListener(
              (removalNotification: RemovalNotification[DeltaLogCacheKey, DeltaLog]) => {
            val log = removalNotification.getValue
            // TODO: We should use ref-counting to uncache snapshots instead of a manual timed op
            try log.unsafeVolatileSnapshot.uncache() catch {
              case _: java.lang.NullPointerException =>
              // Various layers will throw null pointer if the RDD is already gone.
            }
          })
        deltaLogCache = Some(builder.build[CacheKey, DeltaLog]())
        deltaLogCache.get
    }
  }

  private var deltaLogCache: Option[Cache[CacheKey, DeltaLog]] = None

  /**
   * Helper to create delta log caches
   */
  private def createCacheBuilder(conf: SQLConf): CacheBuilder[AnyRef, AnyRef] = {
    val cacheRetention = conf.getConf(DeltaSQLConf.DELTA_LOG_CACHE_RETENTION_MINUTES)
    val cacheSize = conf
      .getConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE)
      .max(sys.props.get("delta.log.cacheSize").map(_.toLong).getOrElse(0L))

    CacheBuilder
      .newBuilder()
      .expireAfterAccess(cacheRetention, TimeUnit.MINUTES)
      .maximumSize(cacheSize)
  }


  /**
   * Creates a [[LogicalRelation]] for a given [[DeltaLogFileIndex]], with all necessary file source
   * options taken from the Delta Log. All reads of Delta metadata files should use this method.
   */
  def indexToRelation(
      spark: SparkSession,
      index: DeltaLogFileIndex,
      additionalOptions: Map[String, String],
      schema: StructType = Action.logSchema): LogicalRelation = {
    val formatSpecificOptions: Map[String, String] = index.format match {
      case DeltaLogFileIndex.COMMIT_FILE_FORMAT =>
        jsonCommitParseOption
      case _ => Map.empty
    }
    // Delta should NEVER ignore missing or corrupt metadata files, because doing so can render the
    // entire table unusable. Hard-wire that into the file source options so the user can't override
    // it by setting spark.sql.files.ignoreCorruptFiles or spark.sql.files.ignoreMissingFiles.
    val allOptions = additionalOptions ++ formatSpecificOptions ++ Map(
      FileSourceOptions.IGNORE_CORRUPT_FILES -> "false",
      FileSourceOptions.IGNORE_MISSING_FILES -> "false"
    )
    val fsRelation = HadoopFsRelation(
      index, index.partitionSchema, schema, None, index.format, allOptions)(spark)
    LogicalRelation(fsRelation)
  }

  // Don't tolerate malformed JSON when parsing Delta log actions (default is PERMISSIVE)
  val jsonCommitParseOption = Map("mode" -> FailFastMode.name)

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: String): DeltaLog = {
    apply(
      spark,
      logPathFor(dataPath),
      options = Map.empty,
      initialTableIdentifier = None,
      new SystemClock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: Path): DeltaLog = {
    apply(spark, logPathFor(dataPath), initialTableIdentifier = None, new SystemClock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: Path, options: Map[String, String]): DeltaLog = {
    apply(spark, logPathFor(dataPath), options, initialTableIdentifier = None, new SystemClock)
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(spark: SparkSession, dataPath: Path, clock: Clock): DeltaLog = {
    apply(spark, logPathFor(dataPath), initialTableIdentifier = None, clock)
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
    apply(spark, logPathFor(new Path(table.location)), Some(table.identifier), clock)
  }

  private def apply(
      spark: SparkSession,
      rawPath: Path,
      initialTableIdentifier: Option[TableIdentifier],
      clock: Clock): DeltaLog =
    apply(spark, rawPath, options = Map.empty, initialTableIdentifier, clock)


  /** Helper for getting a log, as well as the latest snapshot, of the table */
  def forTableWithSnapshot(spark: SparkSession, dataPath: String): (DeltaLog, Snapshot) =
    withFreshSnapshot { forTable(spark, new Path(dataPath), _) }

  /** Helper for getting a log, as well as the latest snapshot, of the table */
  def forTableWithSnapshot(spark: SparkSession, dataPath: Path): (DeltaLog, Snapshot) =
    withFreshSnapshot { forTable(spark, dataPath, _) }

  /** Helper for getting a log, as well as the latest snapshot, of the table */
  def forTableWithSnapshot(
      spark: SparkSession,
      tableName: TableIdentifier): (DeltaLog, Snapshot) =
    withFreshSnapshot { forTable(spark, tableName, _) }

  /** Helper for getting a log, as well as the latest snapshot, of the table */
  def forTableWithSnapshot(
      spark: SparkSession,
      dataPath: Path,
      options: Map[String, String]): (DeltaLog, Snapshot) =
    withFreshSnapshot {
      apply(spark, logPathFor(dataPath), options, initialTableIdentifier = None, _)
    }

  /**
   * Helper function to be used with the forTableWithSnapshot calls. Thunk is a
   * partially applied DeltaLog.forTable call, which we can then wrap around with a
   * snapshot update. We use the system clock to avoid back-to-back updates.
   */
  private[delta] def withFreshSnapshot(thunk: Clock => DeltaLog): (DeltaLog, Snapshot) = {
    val clock = new SystemClock
    val ts = clock.getTimeMillis()
    val deltaLog = thunk(clock)
    val snapshot = deltaLog.update(checkIfUpdatedSinceTs = Some(ts))
    (deltaLog, snapshot)
  }

  private def apply(
      spark: SparkSession,
      rawPath: Path,
      options: Map[String, String],
      initialTableIdentifier: Option[TableIdentifier],
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
    val fs = rawPath.getFileSystem(hadoopConf)
    val path = fs.makeQualified(rawPath)
    def createDeltaLog(): DeltaLog = recordDeltaOperation(
      null,
      "delta.log.create",
      Map(TAG_TAHOE_PATH -> path.getParent.toString)) {
        AnalysisHelper.allowInvokingTransformsInAnalyzer {
          new DeltaLog(
            logPath = path,
            dataPath = path.getParent,
            options = fileSystemOptions,
            allOptions = options,
            initialTableIdentifier = initialTableIdentifier,
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
        getOrCreateCache(spark.sessionState.conf)
          .get(path -> fileSystemOptions, () => {
            createDeltaLog()
          }
        )
      } catch {
        case e: com.google.common.util.concurrent.UncheckedExecutionException => throw e.getCause
        case e: java.util.concurrent.ExecutionException => throw e.getCause
      }
    }

    val deltaLog = getDeltaLogFromCache()
    if (Option(deltaLog.sparkContext.get).map(_.isStopped).getOrElse(true)) {
      // Invalid the cached `DeltaLog` and create a new one because the `SparkContext` of the cached
      // `DeltaLog` has been stopped.
      getOrCreateCache(spark.sessionState.conf).invalidate(path -> fileSystemOptions)
      getDeltaLogFromCache()
    } else {
      deltaLog
    }
  }

  /** Invalidate the cached DeltaLog object for the given `dataPath`. */
  def invalidateCache(spark: SparkSession, dataPath: Path): Unit = {
    try {
      val rawPath = logPathFor(dataPath)
      // scalastyle:off deltahadoopconfiguration
      // This method cannot be called from DataFrameReader/Writer so it's safe to assume the user
      // has set the correct file system configurations in the session configs.
      val fs = rawPath.getFileSystem(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration
      val path = fs.makeQualified(rawPath)

      val deltaLogCache = getOrCreateCache(spark.sessionState.conf)
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
    deltaLogCache.foreach(_.invalidateAll())
  }

  /** Unset the caches. Exposing for testing */
  private[delta] def unsetCache(): Unit = {
    synchronized {
      deltaLogCache = None
    }
  }

  /** Return the number of cached `DeltaLog`s. Exposing for testing */
  private[delta] def cacheSize: Long = {
    deltaLogCache.map(_.size()).getOrElse(0L)
  }

  /**
   * Filters the given [[Dataset]] by the given `partitionFilters`, returning those that match.
   * @param files The active files in the DeltaLog state, which contains the partition value
   *              information
   * @param partitionFilters Filters on the partition columns
   * @param partitionColumnPrefixes The path to the `partitionValues` column, if it's nested
   * @param shouldRewritePartitionFilters Whether to rewrite `partitionFilters` to be over the
   *                                      [[AddFile]] schema
   */
  def filterFileList(
      partitionSchema: StructType,
      files: DataFrame,
      partitionFilters: Seq[Expression],
      partitionColumnPrefixes: Seq[String] = Nil,
      shouldRewritePartitionFilters: Boolean = true): DataFrame = {

    val rewrittenFilters = if (shouldRewritePartitionFilters) {
      rewritePartitionFilters(
        partitionSchema,
        files.sparkSession.sessionState.conf.resolver,
        partitionFilters,
        partitionColumnPrefixes)
    } else {
      partitionFilters
    }
    val expr = rewrittenFilters.reduceLeftOption(And).getOrElse(Literal.TrueLiteral)
    val columnFilter = Column(expr)
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
    partitionFilters
      .map(_.transformUp {
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


  /**
   * Checks whether this table only accepts appends. If so it will throw an error in operations that
   * can remove data such as DELETE/UPDATE/MERGE.
   */
  def assertRemovable(snapshot: Snapshot): Unit = {
    val metadata = snapshot.metadata
    if (DeltaConfigs.IS_APPEND_ONLY.fromMetaData(metadata)) {
      throw DeltaErrors.modifyAppendOnlyTableException(metadata.name)
    }
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

  /** Get a function that canonicalizes a given `path`. */
  private[delta] class CanonicalPathFunction(getHadoopConf: () => Configuration)
      extends Function[String, String] with Serializable {
    // Mark it `@transient lazy val` so that de-serialization happens only once on every executor.
    @transient
    private lazy val fs = {
      // scalastyle:off FileSystemGet
      FileSystem.get(getHadoopConf())
      // scalastyle:on FileSystemGet
    }

    override def apply(path: String): String = {
      // scalastyle:off pathfromuri
      val hadoopPath = new Path(new URI(path))
      // scalastyle:on pathfromuri
      if (hadoopPath.isAbsoluteAndSchemeAuthorityNull) {
        fs.makeQualified(hadoopPath).toUri.toString
      } else {
        // return untouched if it is a relative path or is already fully qualified
        hadoopPath.toUri.toString
      }
    }
  }
}
