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

// scalastyle:off import.ordering.noEmptyLine
import java.net.URI

import scala.collection.mutable

import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.actions.Action.logSchema
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.StateCache
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{SerializableConfiguration, Utils}

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
 * @param timestamp The timestamp of the latest commit in milliseconds. Can also be set to -1 if the
 *                  timestamp of the commit is unknown or the table has not been initialized, i.e.
 *                  `version = -1`.
 *
 */
class Snapshot(
    val path: Path,
    val version: Long,
    val logSegment: LogSegment,
    val minFileRetentionTimestamp: Long,
    val deltaLog: DeltaLog,
    val timestamp: Long,
    val checksumOpt: Option[VersionChecksum])
  extends StateCache
  with PartitionFiltering
  with DeltaFileFormat
  with DeltaLogging {

  import Snapshot._
  // For implicits which re-use Encoder:
  import SingleAction._

  protected def spark = SparkSession.active


  protected def getNumPartitions: Int = {
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_SNAPSHOT_PARTITIONS)
      .getOrElse(Snapshot.defaultNumSnapshotPartitions)
  }

  /** Performs validations during initialization */
  protected def init(): Unit = {
    deltaLog.protocolRead(protocol)
  }

  // Reconstruct the state by applying deltas in order to the checkpoint.
  // We partition by path as it is likely the bulk of the data is add/remove.
  // Non-path based actions will be collocated to a single partition.
  private def stateReconstruction: Dataset[SingleAction] = {
    val implicits = spark.implicits
    import implicits._

    val time = minFileRetentionTimestamp
    val hadoopConf = spark.sparkContext.broadcast(
      new SerializableConfiguration(spark.sessionState.newHadoopConf()))
    val logPath = path.toUri // for serializability
    var wrapPath = false

    loadActions.mapPartitions { actions =>
        val hdpConf = hadoopConf.value.value
        actions.flatMap(canonicalizePath(_, hdpConf, wrapPath))
      }
      .withColumn("file", assertLogBelongsToTable(logPath)(input_file_name()))
      .repartition(getNumPartitions, coalesce($"add.path", $"remove.path"))
      .sortWithinPartitions("file")
      .as[SingleAction]
      .mapPartitions { iter =>
        val state = new InMemoryLogReplay(time)
        state.append(0, iter.map(_.unwrap))
        state.checkpoint.map(_.wrap)
      }
  }

  def redactedPath: String =
    Utils.redact(spark.sessionState.conf.stringRedactionPattern, path.toUri.toString)

  private lazy val cachedState =
    cacheDS(stateReconstruction, s"Delta Table State #$version - $redactedPath")

  /** The current set of actions in this [[Snapshot]]. */
  def state: Dataset[SingleAction] = cachedState.getDS

  /** Helper method to log missing actions when state reconstruction checks are not enabled */
  protected def logMissingActionWarning(action: String): Unit = {
    logWarning(
      s"""
         |Found no $action in computed state, setting it to defaults. State reconstruction
         |validation was turned off. To turn it back on set
         |${DeltaSQLConf.DELTA_STATE_RECONSTRUCTION_VALIDATION_ENABLED.key} to "true"
        """.stripMargin)
  }

  /**
   * Computes some statistics around the transaction log, therefore on the actions made on this
   * Delta table.
   */
  protected lazy val computedState: State = {
    withStatusCode("DELTA", s"Compute snapshot for version: $version") {
      val implicits = spark.implicits
      import implicits._
      var _computedState = state.select(
        last($"protocol", ignoreNulls = true) as "protocol",
        last($"metaData", ignoreNulls = true) as "metadata",
        collect_set($"txn") as "setTransactions",
        // sum may return null for empty data set.
        coalesce(sum($"add.size"), lit(0L)) as "sizeInBytes",
        count($"add") as "numOfFiles",
        count($"metaData") as "numOfMetadata",
        count($"protocol") as "numOfProtocol",
        count($"remove") as "numOfRemoves",
        count($"txn") as "numOfSetTransactions"
      ).as[State](stateEncoder).first()
      val stateReconstructionCheck = spark.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_STATE_RECONSTRUCTION_VALIDATION_ENABLED)
      if (_computedState.protocol == null) {
        recordDeltaEvent(
          deltaLog,
          opType = "delta.assertions.missingAction",
          data = Map("version" -> version.toString, "action" -> "Protocol", "source" -> "Snapshot"))
        if (stateReconstructionCheck) {
          throw DeltaErrors.actionNotFoundException("protocol", version)
        }
      }
      if (_computedState.metadata == null) {
        recordDeltaEvent(
          deltaLog,
          opType = "delta.assertions.missingAction",
          data = Map("version" -> version.toString, "action" -> "Metadata", "source" -> "Metadata"))
        if (stateReconstructionCheck) {
          throw DeltaErrors.actionNotFoundException("metadata", version)
        }
        logMissingActionWarning("metadata")
        _computedState = _computedState.copy(metadata = Metadata())
      }
      _computedState
    }
  }

  def protocol: Protocol = computedState.protocol
  def metadata: Metadata = computedState.metadata
  def setTransactions: Seq[SetTransaction] = computedState.setTransactions
  def sizeInBytes: Long = computedState.sizeInBytes
  def numOfFiles: Long = computedState.numOfFiles
  def numOfMetadata: Long = computedState.numOfMetadata
  def numOfProtocol: Long = computedState.numOfProtocol
  def numOfRemoves: Long = computedState.numOfRemoves
  def numOfSetTransactions: Long = computedState.numOfSetTransactions

  /** A map to look up transaction version by appId. */
  lazy val transactions: Map[String, Long] = setTransactions.map(t => t.appId -> t.version).toMap

  // Here we need to bypass the ACL checks for SELECT anonymous function permissions.
  /** All of the files present in this [[Snapshot]]. */
  def allFiles: Dataset[AddFile] = {
    val implicits = spark.implicits
    import implicits._
    state.where("add IS NOT NULL").select($"add".as[AddFile])
  }

  /** All unexpired tombstones. */
  def tombstones: Dataset[RemoveFile] = {
    val implicits = spark.implicits
    import implicits._
    state.where("remove IS NOT NULL").select($"remove".as[RemoveFile])
  }

  /** Returns the schema of the table. */
  def schema: StructType = metadata.schema

  /** Returns the data schema of the table, the schema of the columns written out to file. */
  def dataSchema: StructType = metadata.dataSchema

  /** Number of columns to collect stats on for data skipping */
  lazy val numIndexedCols: Int = DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.fromMetaData(metadata)

  /** Return the set of properties of the table. */
  def getProperties: mutable.HashMap[String, String] = {
    val base = new mutable.HashMap[String, String]()
    metadata.configuration.foreach { case (k, v) =>
      if (k != "path") {
        base.put(k, v)
      }
    }
    base.put(Protocol.MIN_READER_VERSION_PROP, protocol.minReaderVersion.toString)
    base.put(Protocol.MIN_WRITER_VERSION_PROP, protocol.minWriterVersion.toString)
    base
  }

  // Given the list of files from `LogSegment`, create respective file indices to help create
  // a DataFrame and short-circuit the many file existence and partition schema inference checks
  // that exist in DataSource.resolveRelation().
  protected lazy val deltaFileIndexOpt: Option[DeltaLogFileIndex] = {
    DeltaLogFileIndex(DeltaLogFileIndex.COMMIT_FILE_FORMAT, logSegment.deltas)
  }

  protected lazy val checkpointFileIndexOpt: Option[DeltaLogFileIndex] = {
    DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT, logSegment.checkpoint)
  }

  protected lazy val fileIndices: Seq[DeltaLogFileIndex] = {
    checkpointFileIndexOpt.toSeq ++ deltaFileIndexOpt.toSeq
  }

  /** Creates a LogicalRelation with the given schema from a DeltaLogFileIndex. */
  protected def indexToRelation(
      index: DeltaLogFileIndex,
      schema: StructType = logSchema): LogicalRelation = {
    val fsRelation = HadoopFsRelation(
      index,
      index.partitionSchema,
      schema,
      None,
      index.format,
      Map.empty[String, String])(spark)
    LogicalRelation(fsRelation)
  }

  /**
   * Loads the file indices into a Dataset that can be used for LogReplay.
   */
  protected def loadActions: Dataset[SingleAction] = {
    val dfs = fileIndices.map { index => Dataset[SingleAction](spark, indexToRelation(index)) }
    dfs.reduceOption(_.union(_)).getOrElse(emptyActions)
  }

  protected def emptyActions: Dataset[SingleAction] =
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], logSchema).as[SingleAction]


  override def logInfo(msg: => String): Unit = {
    super.logInfo(s"[tableId=${deltaLog.tableId}] " + msg)
  }

  override def logWarning(msg: => String): Unit = {
    super.logWarning(s"[tableId=${deltaLog.tableId}] " + msg)
  }

  override def logWarning(msg: => String, throwable: Throwable): Unit = {
    super.logWarning(s"[tableId=${deltaLog.tableId}] " + msg, throwable)
  }

  override def logError(msg: => String): Unit = {
    super.logError(s"[tableId=${deltaLog.tableId}] " + msg)
  }

  override def logError(msg: => String, throwable: Throwable): Unit = {
    super.logError(s"[tableId=${deltaLog.tableId}] " + msg, throwable)
 }

  override def toString: String =
    s"${getClass.getSimpleName}(path=$path, version=$version, metadata=$metadata, " +
      s"logSegment=$logSegment, checksumOpt=$checksumOpt)"

  logInfo(s"Created snapshot $this")
  init()
}

object Snapshot extends DeltaLogging {

  private val defaultNumSnapshotPartitions: Int = 50

  private def canonicalizePath(
      action: SingleAction,
      hdpConf: Configuration,
      wrapPath: Boolean): Option[SingleAction] = {
    action.unwrap match {
      case add: AddFile =>
        Some(add.copy(path = canonicalizePath(add.path, hdpConf)).wrap)
      case rm: RemoveFile =>
        Some(rm.copy(path = canonicalizePath(rm.path, hdpConf)).wrap)
      case other if other == null => None
      case other => Some(other.wrap)
    }
  }


  /** Canonicalize the paths for Actions */
  private[delta] def canonicalizePath(path: String, hadoopConf: Configuration): String = {
    val hadoopPath = new Path(new URI(path))
    if (hadoopPath.isAbsoluteAndSchemeAuthorityNull) {
      // scalastyle:off FileSystemGet
      val fs = FileSystem.get(hadoopConf)
      // scalastyle:on FileSystemGet
      fs.makeQualified(hadoopPath).toUri.toString
    } else {
      // return untouched if it is a relative path or is already fully qualified
      hadoopPath.toUri.toString
    }
  }

  /**
   * Make sure that the delta file we're reading belongs to this table. Cached snapshots from
   * the previous states will contain empty strings as the file name.
   */
  private def assertLogBelongsToTable(logBasePath: URI): UserDefinedFunction = {
    udf((filePath: String) => {
      if (filePath.isEmpty || new Path(new URI(filePath)).getParent == new Path(logBasePath)) {
        filePath
      } else {
        // scalastyle:off throwerror
        throw new AssertionError(s"File ($filePath) doesn't belong in the " +
          s"transaction log at $logBasePath. Please contact Databricks Support.")
        // scalastyle:on throwerror
      }
    })
  }

  /**
   * Metrics and metadata computed around the Delta table
   * @param protocol The protocol version of the Delta table
   * @param metadata The metadata of the table
   * @param setTransactions The streaming queries writing to this table
   * @param sizeInBytes The total size of the table (of active files, not including tombstones)
   * @param numOfFiles The number of files in this table
   * @param numOfMetadata The number of metadata actions in the state. Should be 1
   * @param numOfProtocol The number of protocol actions in the state. Should be 1
   * @param numOfRemoves The number of tombstones in the state
   * @param numOfSetTransactions Number of streams writing to this table
   */
  case class State(
      protocol: Protocol,
      metadata: Metadata,
      setTransactions: Seq[SetTransaction],
      sizeInBytes: Long,
      numOfFiles: Long,
      numOfMetadata: Long,
      numOfProtocol: Long,
      numOfRemoves: Long,
      numOfSetTransactions: Long)

  private[this] lazy val _stateEncoder: ExpressionEncoder[State] = try {
    ExpressionEncoder[State]()
  } catch {
    case e: Throwable =>
      logError(e.getMessage, e)
      throw e
  }


  implicit private def stateEncoder: Encoder[State] = {
    _stateEncoder.copy()
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
  extends Snapshot(logPath, -1, LogSegment.empty(logPath), -1, deltaLog, -1, None) {

  def this(logPath: Path, deltaLog: DeltaLog) = this(
    logPath,
    deltaLog,
    Metadata(configuration = DeltaConfigs.mergeGlobalConfigs(
      SparkSession.active.sessionState.conf, Map.empty))
  )

  override def state: Dataset[SingleAction] = emptyActions
  override protected lazy val computedState: Snapshot.State = {
    val protocol = Protocol.forNewTable(spark, metadata)
    Snapshot.State(protocol, metadata, Nil, 0L, 0L, 1L, 1L, 0L, 0L)
  }
}
