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

package org.apache.spark.sql.delta.icebergShaded

import java.util.concurrent.atomic.AtomicReference
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{CommittedTransaction, DeltaErrors, DeltaFileNotFoundException, DeltaFileProviderUtils, DeltaOperations, IcebergCompat, IcebergConstants, Snapshot, SnapshotDescriptor, UniversalFormat, UniversalFormatConverter}
import org.apache.spark.sql.delta.DeltaOperations.OPTIMIZE_OPERATION_NAME
import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, FileAction, RemoveFile}
import org.apache.spark.sql.delta.hooks.IcebergConverterHook
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path
import shadedForDelta.org.apache.iceberg.{Table => IcebergTable, TableProperties}
import shadedForDelta.org.apache.iceberg.exceptions.CommitFailedException
import shadedForDelta.org.apache.iceberg.hive.{HiveCatalog, HiveTableOperations}
import shadedForDelta.org.apache.iceberg.util.LocationUtil

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

object IcebergConverter {


  /**
   * Property to be set in translated Iceberg metadata files.
   * Indicates the delta commit version # that it corresponds to.
   */
  val DELTA_VERSION_PROPERTY = "delta-version"

  /**
   * Property to be set in translated Iceberg metadata files.
   * Indicates the timestamp (milliseconds) of the delta commit that it corresponds to.
   */
  val DELTA_TIMESTAMP_PROPERTY = "delta-timestamp"

  /**
   * Property to be set in translated Iceberg metadata files.
   * Indicates the base delta commit version # that the conversion started from
   */
  val BASE_DELTA_VERSION_PROPERTY = "base-delta-version"

  def getLastConvertedDeltaVersion(table: Option[IcebergTable]): Option[Long] =
    table.flatMap(_.properties().asScala.get(DELTA_VERSION_PROPERTY)).map(_.toLong)

  def getLastConvertedDeltaTimestamp(table: Option[IcebergTable]): Option[Long] =
    table.flatMap(_.properties().asScala.get(DELTA_TIMESTAMP_PROPERTY)).map(_.toLong)
}


/**
 * This class manages the transformation of delta snapshots into their Iceberg equivalent.
 */
class IcebergConverter(spark: SparkSession)
    extends UniversalFormatConverter(spark)
    with DeltaLogging {

  // Save an atomic reference of the snapshot being converted, and the txn that triggered
  // resulted in the specified snapshot
  protected val currentConversion =
    new AtomicReference[(Snapshot, CommittedTransaction)]()
  protected val standbyConversion =
    new AtomicReference[(Snapshot, CommittedTransaction)]()

  // Whether our async converter thread is active. We may already have an alive thread that is
  // about to shutdown, but in such cases this value should return false.
  @GuardedBy("asyncThreadLock")
  private var asyncConverterThreadActive: Boolean = false
  private val asyncThreadLock = new Object

  private[icebergShaded]var targetSnapshot: SnapshotDescriptor = _
  /**
   * Enqueue the specified snapshot to be converted to Iceberg. This will start an async
   * job to run the conversion, unless there already is an async conversion running for
   * this table. In that case, it will queue up the provided snapshot to be run after
   * the existing job completes.
   * Note that if there is another snapshot already queued, the previous snapshot will get
   * removed from the wait queue. Only one snapshot is queued at any point of time.
   *
   */
  override def enqueueSnapshotForConversion(
      snapshotToConvert: Snapshot,
      txn: CommittedTransaction): Unit = {
    if (!UniversalFormat.icebergEnabled(snapshotToConvert.metadata)) {
      return
    }
    val log = snapshotToConvert.deltaLog
    // Replace any previously queued snapshot
    val previouslyQueued = standbyConversion.getAndSet((snapshotToConvert, txn))
    asyncThreadLock.synchronized {
      if (!asyncConverterThreadActive) {
        val threadName = IcebergConverterHook.ASYNC_ICEBERG_CONVERTER_THREAD_NAME +
          s" [id=${snapshotToConvert.metadata.id}]"
        val asyncConverterThread: Thread = new Thread(threadName) {
          setDaemon(true)

          override def run(): Unit =
              try {
                var snapshotAndTxn = getNextSnapshot
                  while (snapshotAndTxn != null) {
                    val snapshotVal = snapshotAndTxn._1
                    val prevTxn = snapshotAndTxn._2
                    try {
                      logInfo(log"Converting Delta table [path=" +
                        log"${MDC(DeltaLogKeys.PATH, log.logPath)}, " +
                        log"tableId=${MDC(DeltaLogKeys.TABLE_ID, log.tableId)}, version=" +
                        log"${MDC(DeltaLogKeys.VERSION, snapshotVal.version)}] into Iceberg")
                      convertSnapshot(snapshotVal, prevTxn)
                    } catch {
                      case NonFatal(e) =>
                        logWarning(log"Error when writing Iceberg metadata asynchronously", e)
                        recordDeltaEvent(
                          log,
                          "delta.iceberg.conversion.async.error",
                          data = Map(
                            "exception" -> ExceptionUtils.getMessage(e),
                            "stackTrace" -> ExceptionUtils.getStackTrace(e)
                          )
                        )
                    }
                    currentConversion.set(null)
                    // Pick next snapshot to convert if there's a new one
                    snapshotAndTxn = getNextSnapshot
                  }
              } finally {
                // shuttingdown thread
                asyncThreadLock.synchronized {
                  asyncConverterThreadActive = false
                }
              }

          // Get a snapshot to convert from the icebergQueue. Sets the queue to null after.
          private def getNextSnapshot: (Snapshot, CommittedTransaction) =
            asyncThreadLock.synchronized {
              val potentialSnapshotAndTxn = standbyConversion.get()
              currentConversion.set(potentialSnapshotAndTxn)
              standbyConversion.compareAndSet(potentialSnapshotAndTxn, null)
              if (potentialSnapshotAndTxn == null) {
                asyncConverterThreadActive = false
              }
              potentialSnapshotAndTxn
            }
        }
        asyncConverterThread.start()
        asyncConverterThreadActive = true
      }
    }

    // If there already was a snapshot waiting to be converted, log that snapshot info.
    if (previouslyQueued != null) {
//      previouslyQueued._1.uncache()
      recordDeltaEvent(
        snapshotToConvert.deltaLog,
        "delta.iceberg.conversion.async.backlog",
        data = Map(
          "newVersion" -> snapshotToConvert.version,
          "replacedVersion" -> previouslyQueued._1.version)
      )
    }
  }

  /**
   * Convert the specified snapshot into Iceberg for the given catalogTable
   * @param snapshotToConvert the snapshot that needs to be converted to Iceberg
   * @param catalogTable the catalogTable this conversion targets.
   * @return Converted Delta version and commit timestamp
   */
  override def convertSnapshot(
      snapshotToConvert: Snapshot, catalogTable: CatalogTable): Option[(Long, Long)] = {
    try {
      convertSnapshotWithRetry(snapshotToConvert, None, catalogTable)
    } catch {
      case NonFatal(e) =>
        logError(log"Error when converting to Iceberg metadata", e)
        val (opType, baseTags) =
            ("delta.iceberg.conversion.error", Map.empty[String, String])

        recordDeltaEvent(
          snapshotToConvert.deltaLog,
          opType,
          data = baseTags ++ Map(
            "exception" -> ExceptionUtils.getMessage(e),
            "stackTrace" -> ExceptionUtils.getStackTrace(e)
          )
        )
        throw e
    }
  }

  /**
   * Convert the specified snapshot into Iceberg when performing an OptimisticTransaction
   * on a delta table.
   * @param snapshotToConvert the snapshot that needs to be converted to Iceberg
   * @param txn               the transaction that triggers the conversion. It must
   *                          contain the catalogTable this conversion targets.
   * @return Converted Delta version and commit timestamp
   */
  override def convertSnapshot(
      snapshotToConvert: Snapshot, txn: CommittedTransaction): Option[(Long, Long)] = {
    try {
      txn.catalogTable match {
        case Some(table) => convertSnapshotWithRetry(snapshotToConvert, Some(txn), table)
        case _ =>
          val msg = s"CatalogTable for table ${snapshotToConvert.deltaLog.tableId} " +
            s"is empty in txn. Skip iceberg conversion."
          throw DeltaErrors.universalFormatConversionFailedException(
            snapshotToConvert.version, "iceberg", msg)
      }
    } catch {
      case NonFatal(e) =>
        logError(log"Error when converting to Iceberg metadata", e)
        recordDeltaEvent(
          txn.deltaLog,
          "delta.iceberg.conversion.error",
          data = Map(
            "exception" -> ExceptionUtils.getMessage(e),
            "stackTrace" -> ExceptionUtils.getStackTrace(e)
          )
        )
        throw e
    }
  }

  /**
   *  Convert the specified snapshot into Iceberg with retry
   */
  private def convertSnapshotWithRetry(
      snapshotToConvert: Snapshot,
      txnOpt: Option[CommittedTransaction],
      catalogTable: CatalogTable,
      maxRetry: Int =
        spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_UNIFORM_ICEBERG_RETRY_TIMES)
  ): Option[(Long, Long)] = {
    var retryAttempt = 0
    while (retryAttempt < maxRetry) {
      try {
        return convertSnapshot(snapshotToConvert, txnOpt, catalogTable)
      } catch {
        case e: CommitFailedException if retryAttempt < maxRetry =>
          retryAttempt += 1
          val lastConvertedIcebergTable = loadIcebergTable(snapshotToConvert, catalogTable)
          val lastDeltaVersionConverted = IcebergConverter
            .getLastConvertedDeltaVersion(lastConvertedIcebergTable)
          val lastConvertedDeltaTimestamp = IcebergConverter
            .getLastConvertedDeltaTimestamp(lastConvertedIcebergTable)
          // Do not retry if the current or higher Delta version is already converted
          (lastDeltaVersionConverted, lastConvertedDeltaTimestamp) match {
            case (Some(version), Some(timestamp)) if version >= snapshotToConvert.version =>
              return Some(version, timestamp)
            case _ =>
              logWarning(s"CommitFailedException when converting to Iceberg metadata;" +
                s" retry count $retryAttempt", e)
          }
      }
    }
    throw new IllegalStateException("should not happen")
  }

  /**
   * Convert the specified snapshot into Iceberg.
   * NOTE: 1. This operation is blocking. Call [[enqueueSnapshotForConversion]] to run the
   *          operation asynchronously.
   *       2. This is the main entrance. All other with similar names are wrappers.
   * @param snapshotToConvert the snapshot that needs to be converted to Iceberg
   * @param txnOpt the OptimisticTransaction that created snapshotToConvert.
   *            Used as a hint to avoid recomputing old metadata.
   * @param catalogTable the catalogTable this conversion targets
   * @return Converted Delta version and commit timestamp
   */
  private def convertSnapshot(
      snapshotToConvert: Snapshot,
      txnOpt: Option[CommittedTransaction],
      catalogTable: CatalogTable): Option[(Long, Long)] =
      recordFrameProfile("Delta", "IcebergConverter.convertSnapshot") {

    targetSnapshot = snapshotToConvert
    val cleanedCatalogTable =
      cleanCatalogTableIfEnablingUniform(catalogTable, snapshotToConvert, txnOpt)
    val log = snapshotToConvert.deltaLog
    val lastConvertedIcebergTable = loadIcebergTable(snapshotToConvert, cleanedCatalogTable)
    val lastConvertedIcebergSnapshotId =
      lastConvertedIcebergTable.flatMap(it => Option(it.currentSnapshot())).map(_.snapshotId())
    val lastDeltaVersionConverted = IcebergConverter
      .getLastConvertedDeltaVersion(lastConvertedIcebergTable)
    val maxCommitsToConvert =
      spark.sessionState.conf.getConf(DeltaSQLConf.ICEBERG_MAX_COMMITS_TO_CONVERT)
    // Conversion is up-to-date
    if (lastDeltaVersionConverted.contains(snapshotToConvert.version)) {
      return None
    }
    val conversionStartTime = System.currentTimeMillis()

    val prevConvertedSnapshotOpt = (lastDeltaVersionConverted, txnOpt) match {
      // The provided Snapshot is the last converted Snapshot
      case (Some(version), Some(txn)) if version == txn.readSnapshot.version =>
        Some(txn.readSnapshot)
      // Some snapshots are pending conversion since last conversion
      case (Some(version), _) if snapshotToConvert.version - version <= maxCommitsToConvert =>
        try {
          Some(log.getSnapshotAt(version, catalogTableOpt = Some(catalogTable)))
        } catch {
          // If we can't load the file since the last time Iceberg was converted, it's likely that
          // the commit file expired. Treat this like a new Iceberg table conversion.
          case _: DeltaFileNotFoundException => None
        }
      // Never converted before
      case _ => None
    }

    val tableOp = (lastDeltaVersionConverted, prevConvertedSnapshotOpt) match {
      case (Some(_), Some(_)) => WRITE_TABLE
      case (Some(_), None) => REPLACE_TABLE
      case (None, None) => CREATE_TABLE
    }

    UniversalFormat.enforceSupportInCatalog(cleanedCatalogTable, snapshotToConvert.metadata) match {
      case Some(updatedTable) => spark.sessionState.catalog.alterTable(updatedTable)
      case _ =>
    }

    val icebergTxn = new IcebergConversionTransaction(
      spark, cleanedCatalogTable, log.newDeltaHadoopConf(), snapshotToConvert, tableOp,
      lastConvertedIcebergSnapshotId, lastDeltaVersionConverted)

    val convertedCommits: Seq[Option[CommitInfo]] = prevConvertedSnapshotOpt match {
      case Some(prevSnapshot) =>
        // Read the actions directly from the delta json files.
        // TODO: Run this as a spark job on executors
        val deltaFiles = DeltaFileProviderUtils.getDeltaFilesInVersionRange(
          spark = spark,
          deltaLog = log,
          startVersion = prevSnapshot.version + 1,
          endVersion = snapshotToConvert.version,
          catalogTableOpt = Some(catalogTable))

        recordDeltaEvent(
          snapshotToConvert.deltaLog,
          "delta.iceberg.conversion.deltaCommitRange",
          data = Map(
            "fromVersion" -> (prevSnapshot.version + 1),
            "toVersion" -> snapshotToConvert.version,
            "numDeltaFiles" -> deltaFiles.length
          )
        )

        val actionsToConvert = DeltaFileProviderUtils.parallelReadAndParseDeltaFilesAsIterator(
          log, spark, deltaFiles)
        var deltaVersion = prevSnapshot.version
        val commitInfos = actionsToConvert.map { actionsIter =>
          try {
            deltaVersion += 1
            runIcebergConversionForActions(
              icebergTxn,
              actionsIter.map(Action.fromJson).toSeq,
              prevConvertedSnapshotOpt,
              deltaVersion)
          } finally {
            actionsIter.close()
          }
        }
        // If the metadata hasn't changed, this will no-op.
        icebergTxn.updateTableMetadata(prevSnapshot.metadata)
        commitInfos
      case None =>
        // If we don't have a snapshot of the last converted version, get all the AddFiles
        // (via state reconstruction).
        // Batch is always disabled but we still want to reuse the event for conversion
        recordDeltaEvent(
          snapshotToConvert.deltaLog,
          "delta.iceberg.conversion.batch",
          data = Map(
            "version" -> snapshotToConvert.version,
            "numOfFiles" -> snapshotToConvert.numOfFiles,
            "actionBatchSize" -> -1, // This param is ignored as batch is deprecated
            "numOfPartitions" -> 1
          )
        )
        runIcebergConversionForActions(
          icebergTxn,
          snapshotToConvert.allFiles.toLocalIterator().asScala.toSeq,
          None,
          snapshotToConvert.version)

        // Always attempt to update table metadata (schema/properties) for REPLACE_TABLE
        if (tableOp == REPLACE_TABLE) {
          icebergTxn.updateTableMetadata(snapshotToConvert.metadata)
        }
        Nil
    }


    // OPTIMIZE will trigger snapshot expiration for iceberg table
    val OPR_TRIGGER_EXPIRE = Set(DeltaOperations.OPTIMIZE_OPERATION_NAME)
    val needsExpireSnapshot =
      OPR_TRIGGER_EXPIRE.intersect(convertedCommits.flatten.map(_.operation).toSet).nonEmpty
    if (needsExpireSnapshot) {
      logInfo(log"Committing iceberg snapshot expiration for uniform table " +
        log"[path = ${MDC(DeltaLogKeys.PATH, log.logPath)}] tableId=" +
        log"${MDC(DeltaLogKeys.TABLE_ID, log.tableId)}]")
      expireIcebergSnapshot(snapshotToConvert, icebergTxn)
    }

    icebergTxn.commit()
    validateIcebergCommit(snapshotToConvert, cleanedCatalogTable)

    recordDeltaEvent(
      snapshotToConvert.deltaLog,
      "delta.iceberg.conversion",
      data = Map(
        "deltaVersion" -> snapshotToConvert.version,
        "compatVersion" -> IcebergCompat.getEnabledVersion(snapshotToConvert.metadata)
          .getOrElse(0),
        "elapsedTimeMs" -> (System.currentTimeMillis() - conversionStartTime)
      )
    )

    Some(snapshotToConvert.version, snapshotToConvert.timestamp)
  }

  /**
   * Helper function to execute and commit Iceberg snapshot expiry
   * @param snapshotToConvert the Delta snapshot that needs to be converted to Iceberg
   * @param icebergTxn the IcebergConversionTransaction created in convertSnapshot, used
   *                   to create a table object and expiration helper
   */
  private def expireIcebergSnapshot(
      snapshotToConvert: Snapshot,
      icebergTxn: IcebergConversionTransaction): Unit = {
    val expireSnapshotHelper = icebergTxn.getExpireSnapshotHelper()
    val table = icebergTxn.txn.table()
    val tableLocation = LocationUtil.stripTrailingSlash(table.location)
    val defaultWriteMetadataLocation = s"$tableLocation/metadata"
    val writeMetadataLocation = LocationUtil.stripTrailingSlash(
      table.properties().getOrDefault(
        TableProperties.WRITE_METADATA_LOCATION, defaultWriteMetadataLocation))

    if (snapshotToConvert.path.toString == writeMetadataLocation) {
      // Don't attempt any file cleanup in the edge-case configuration
      // that the data location (in Uniform the table root location)
      // is the same as the Iceberg metadata location
      expireSnapshotHelper.cleanExpiredFiles(false)
    } else {
      expireSnapshotHelper.deleteWith(path => {
        if (path.startsWith(writeMetadataLocation)) {
          table.io().deleteFile(path)
        }
      })
    }

    expireSnapshotHelper.commit(snapshotToConvert.version)
  }

  // This is for newly enabling uniform table to
  // start a new history line for iceberg metadata
  // so that if a uniform table is corrupted,
  // user can unset and re-enable to unblock
  private def cleanCatalogTableIfEnablingUniform(
      table: CatalogTable,
      snapshotToConvert: Snapshot,
      txnOpt: Option[CommittedTransaction]): CatalogTable = {
    val disabledIceberg = txnOpt.map(txn =>
      !UniversalFormat.icebergEnabled(txn.readSnapshot.metadata)
    ).getOrElse(!UniversalFormat.icebergEnabled(table.properties))
    val enablingUniform =
      disabledIceberg && UniversalFormat.icebergEnabled(snapshotToConvert.metadata)
    if (enablingUniform) {
      clearDeltaUniformMetadata(table)
    } else {
      table
    }
  }

  protected def clearDeltaUniformMetadata(table: CatalogTable): CatalogTable = {
    val metadata_key = IcebergConstants.ICEBERG_TBLPROP_METADATA_LOCATION
    if (table.properties.contains(metadata_key)) {
      val cleanedCatalogTable = table.copy(properties = table.properties
        - metadata_key
        - IcebergConverter.DELTA_VERSION_PROPERTY
        - IcebergConverter.DELTA_TIMESTAMP_PROPERTY
      )
      spark.sessionState.catalog.alterTable(cleanedCatalogTable)
      cleanedCatalogTable
    } else {
      table
    }
  }

  override def loadLastDeltaVersionConverted(
      snapshot: Snapshot, catalogTable: CatalogTable): Option[Long] =
    recordFrameProfile("Delta", "IcebergConverter.loadLastDeltaVersionConverted") {
      IcebergConverter.getLastConvertedDeltaVersion(loadIcebergTable(snapshot, catalogTable))
    }

  protected def loadIcebergTable(
      snapshot: Snapshot, catalogTable: CatalogTable): Option[IcebergTable] = {
    recordFrameProfile("Delta", "IcebergConverter.loadLastConvertedIcebergTable") {
      val hiveCatalog = IcebergTransactionUtils
        .createHiveCatalog(snapshot.deltaLog.newDeltaHadoopConf())
      val icebergTableId = IcebergTransactionUtils
        .convertSparkTableIdentifierToIcebergHive(catalogTable.identifier)
      if (hiveCatalog.tableExists(icebergTableId)) {
        Some(hiveCatalog.loadTable(icebergTableId))
      } else {
        None
      }
    }
  }
  /**
   * Commit the set of changes into an Iceberg snapshot. Each call to this function will
   * build exactly one Iceberg Snapshot.
   *
   * We determine what type of [[IcebergConversionTransaction.TransactionHelper]] to use
   * (and what type of Iceberg snapshot to create) based on the types of actions and
   * whether they contain data change. An [[UnsupportedOperationException]] will be
   * thrown for cases not listed in the table below. It means the combination of actions are
   * not recognized/supported. IcebergConverter will do a re-try with REPLACE TABLE, which
   * collects all valid data files from the target Delta snapshot and commit to Iceberg.
   *
   * Some Delta operations are known to contain only AddFiles(dataChange=false), intended to
   * replace/overwrite existing AddFiles. They rely on Delta's dedup in state reconstruction
   * and cannot be action-to-action translated to Iceberg, which lacks dedup abilities.
   * We create corresponding RemoveFile entries for the AddFiles so these operations can be
   * properly translated into [[RewriteFiles]] in Iceberg. These operations are marked as
   * [[needAutoRewrite]] in the code and the table below.
   *
   * The following table demonstrates how to choose the appropriate TransactionHelper.
   * The conditions can overlap and should be checked in order.
   * +-------------------+---------------+---------------------+--------------------+
   * |  Type of actions  |  Data Change  |   TransactionHelper | Example / Note     |
   * +-------------------+---------------+---------------------+--------------------+
   * |  Create table     |  Any          |   AppendHelper      |  Note 1            |
   * +-------------------+---------------+---------------------+--------------------+
   * |                   |  All          |   AppendHelper      |  INSERT            |
   * |  Add only         +---------------+---------------------+--------------------+
   * |                   |  None         |   needAutoRewrite   |  Note 2            |
   * |                   |               |   else              |                    |
   * |                   |               |       NullHelper    |  Add Tag           |
   * |                   +---------------+---------------------+--------------------+
   * |                   |  Some         |   Unsupported       |  (unknown)         |
   * +-------------------+---------------+---------------------+--------------------+
   * |  Remove only      |  Any          |   RemoveHelper      |  DELETE            |
   * +-------------------+---------------+---------------------+--------------------+
   * |                   |  All          |   OverwriteHelper   |  UPDATE            |
   * |  Add + Remove     +---------------+---------------------+--------------------+
   * |                   |  None         |   RewriteHelper     |  OPTIMIZE          |
   * |                   +---------------+---------------------+--------------------+
   * |                   |  Some         |   Unsupported       |  (unknown)         |
   * +-------------------+---------------+---------------------+--------------------+
   * Note:
   * 1. We assume a Create/Replace table operation will only contain AddFiles.
   * 2. DV is allowed but ignored as known operations (ComputeStats) do not touch DV.
   */
  private[delta] def runIcebergConversionForActions(
      icebergTxn: IcebergConversionTransaction,
      actionsToCommit: Seq[Action],
      prevSnapshotOpt: Option[SnapshotDescriptor],
      deltaVersion: Long): Option[CommitInfo] = {

    var commitInfo: Option[CommitInfo] = None
    var addFiles: Seq[AddFile] = Nil
    var removeFiles: Seq[RemoveFile] = Nil
    // Determining what txnHelper to use for this group of Actions requires a full-scan
    // of [[actionsToCommit]], which is not too expensive as the actions are already in-memory.

    val txnHelper = prevSnapshotOpt match {
      // Having no previous Snapshot implies that the table is either being created or replaced.
      // This guarantees that the actions are fetched via [[Snapshot.allFiles]] and are unique.
      case None =>
        addFiles = actionsToCommit.asInstanceOf[Seq[AddFile]]
        if (addFiles.isEmpty) {
          icebergTxn.getNullHelper
        } else if (addFiles.exists(_.deletionVector != null)) {
          throw new UnsupportedOperationException("Deletion Vector is not supported")
        } else {
          icebergTxn.getAppendOnlyHelper
        }
      case Some(_) =>
        val addBuffer = new ArrayBuffer[AddFile]()
        val removeBuffer = new ArrayBuffer[RemoveFile]()
        // Scan the actions to collect info needed to determine which txnHelper to use
        object DataChange extends Enumeration {
          val Empty = Value(0, "Empty")
          val None = Value(1, "None")
          val All = Value(2, "All")
          val Some = Value(3, "Some")
        }
        var dataChangeBits = 0
        var hasDv: Boolean = false
        val autoRewriteOprs = Set("COMPUTE STATS")
        var needAutoRewrite = false

        actionsToCommit.foreach {
          case file: FileAction =>
            addBuffer ++= Option(file.wrap.add)
            removeBuffer ++= Option(file.wrap.remove)
            dataChangeBits |= (1 << (if (file.dataChange) 1 else 0))
            hasDv |= file.deletionVector != null
          case c: CommitInfo =>
            commitInfo = Some(c)
            needAutoRewrite = autoRewriteOprs.contains(c.operation)
          case _ => // Ignore other actions
        }
        addFiles = addBuffer.toSeq
        removeFiles = removeBuffer.toSeq
        val dataChange = DataChange(dataChangeBits)

        (addFiles.nonEmpty, removeFiles.nonEmpty, dataChange) match {
          case (true, false, DataChange.All) if !hasDv =>
            icebergTxn.getAppendOnlyHelper
          case (true, false, DataChange.None) =>
            if (!needAutoRewrite) {
              icebergTxn.getNullHelper // Ignore
            } else {
              // Create RemoveFiles to refresh these AddFiles without data change
              removeFiles = addBuffer.map(_.removeWithTimestamp(dataChange = false)).toSeq
              icebergTxn.getRewriteHelper
            }
          case (false, true, _) =>
            icebergTxn.getRemoveOnlyHelper
          case (true, true, DataChange.All) if !hasDv =>
            icebergTxn.getOverwriteHelper
          case (true, true, DataChange.None) if !hasDv =>
            icebergTxn.getRewriteHelper
          case (false, false, _) =>
            icebergTxn.getNullHelper
          case _ =>
            recordDeltaEvent(
              targetSnapshot.deltaLog,
              "delta.iceberg.conversion.unsupportedActions",
              data = Map(
                "version" -> targetSnapshot.version,
                "commitInfo" -> commitInfo.map(_.operation).getOrElse(""),
                "hasAdd" -> addFiles.nonEmpty.toString,
                "hasRemove" -> removeFiles.nonEmpty.toString,
                "dataChange" -> dataChange.toString,
                "hasDv" -> hasDv.toString
              )
            )
            throw new UnsupportedOperationException(
              "Unsupported combination of actions for incremental conversion.")
        }
    }
    recordDeltaEvent(
      targetSnapshot.deltaLog,
      "delta.iceberg.conversion.convertActions",
      data = Map(
        "version" -> targetSnapshot.version,
        "commitInfo" -> commitInfo.map(_.operation).getOrElse(""),
        "txnHelper" -> txnHelper.getClass.getSimpleName
      )
    )

    removeFiles.foreach(txnHelper.add)
    addFiles.foreach(txnHelper.add)
    // Make sure the next snapshot sequence number is deltaVersion
    txnHelper.commit(deltaVersion)

    commitInfo
  }

  /**
   * Validate the Iceberg conversion by comparing the number of files and size in bytes
   * between the converted Iceberg table and the Delta table.
   * TODO: throw exception and proactively abort conversion transaction
   */
  private def validateIcebergCommit(snapshotToConvert: Snapshot, catalogTable: CatalogTable) = {
    val table = loadIcebergTable(snapshotToConvert, catalogTable)
    val lastConvertedDeltaVersion = IcebergConverter.getLastConvertedDeltaVersion(table)
    table.map {t =>
      if (lastConvertedDeltaVersion.contains(snapshotToConvert.version) &&
          t.currentSnapshot() != null) {
        val icebergNumOfFiles = t.currentSnapshot().summary().asScala
          .getOrElse("total-data-files", "-1").toLong
        val icebergTotalBytes = t.currentSnapshot().summary().asScala
          .getOrElse("total-files-size", "-1").toLong

        if (icebergNumOfFiles != snapshotToConvert.numOfFiles ||
            icebergTotalBytes != snapshotToConvert.sizeInBytes) {
          recordDeltaEvent(
            snapshotToConvert.deltaLog, "delta.iceberg.conversion.mismatch",
            data = Map(
              "lastConvertedDeltaVersion" -> snapshotToConvert.version,
              "numOfFiles" -> snapshotToConvert.numOfFiles,
              "icebergNumOfFiles" -> icebergNumOfFiles,
              "sizeInBytes" -> snapshotToConvert.sizeInBytes,
              "icebergTotalBytes" -> icebergTotalBytes
            )
          )
        }
      }
    }
  }
}
