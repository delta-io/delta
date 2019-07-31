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
import java.net.URI

import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.actions.Action.logSchema
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.StateCache
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * An immutable snapshot of the state of the log at some delta version. Internally
 * this class manages the replay of actions stored in checkpoint or delta files,
 * given an optional starting snapshot.
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
 */
class Snapshot(
    val path: Path,
    val version: Long,
    previousSnapshot: Option[Dataset[SingleAction]],
    files: Seq[Path],
    val minFileRetentionTimestamp: Long,
    val deltaLog: DeltaLog,
    val timestamp: Long,
    val lineageLength: Int = 1)
  extends StateCache
  with PartitionFiltering
  with DeltaFileFormat
  with DeltaLogging {

  import Snapshot._
  // For implicits which re-use Encoder:
  import SingleAction._

  protected def spark = SparkSession.active


  // Reconstruct the state by applying deltas in order to the checkpoint.
  // We partition by path as it is likely the bulk of the data is add/remove.
  // Non-path based actions will be collocated to a single partition.
  private val stateReconstruction = {
    val implicits = spark.implicits
    import implicits._

    val numPartitions = spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_SNAPSHOT_PARTITIONS)

    val checkpointData = previousSnapshot.getOrElse(emptyActions)
    val deltaData = load(files)
    val allActions = checkpointData.union(deltaData)
    val time = minFileRetentionTimestamp
    val hadoopConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())
    val logPath = path.toUri // for serializability

    allActions.as[SingleAction]
      .mapPartitions { actions =>
        val hdpConf = hadoopConf.value
        actions.flatMap {
          _.unwrap match {
            case add: AddFile => Some(add.copy(path = canonicalizePath(add.path, hdpConf)).wrap)
            case rm: RemoveFile => Some(rm.copy(path = canonicalizePath(rm.path, hdpConf)).wrap)
            case other if other == null => None
            case other => Some(other.wrap)
          }
        }
      }
      .withColumn("file", assertLogBelongsToTable(logPath)(input_file_name()))
      .repartition(numPartitions, coalesce($"add.path", $"remove.path"))
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

  private val cachedState =
    cacheDS(stateReconstruction, "Delta Table State #$version - $redactedPath")

  /** The current set of actions in this [[Snapshot]]. */
  def state: Dataset[SingleAction] = cachedState.getDS

  // Force materialization of the cache and collect the basics to the driver for fast access.
  // Here we need to bypass the ACL checks for SELECT anonymous function permissions.
  val State(protocol, metadata, setTransactions, sizeInBytes, numOfFiles, numOfMetadata,
      numOfProtocol, numOfRemoves, numOfSetTransactions) = {
    val implicits = spark.implicits
    import implicits._
    state.select(
        coalesce(last($"protocol", ignoreNulls = true), defaultProtocol()) as "protocol",
        coalesce(last($"metaData", ignoreNulls = true), emptyMetadata()) as "metadata",
        collect_set($"txn") as "setTransactions",
        // sum may return null for empty data set.
        coalesce(sum($"add.size"), lit(0L)) as "sizeInBytes",
        count($"add") as "numOfFiles",
        count($"metaData") as "numOfMetadata",
        count($"protocol") as "numOfProtocol",
        count($"remove") as "numOfRemoves",
        count($"txn") as "numOfSetTransactions")
      .as[State](stateEncoder)}
      .first

  deltaLog.protocolRead(protocol)

  /** A map to look up transaction version by appId. */
  lazy val transactions = setTransactions.map(t => t.appId -> t.version).toMap

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
  val numIndexedCols: Int = DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.fromMetaData(metadata)

  /**
   * Load the transaction logs from paths. The files here may have different file formats and the
   * file format can be extracted from the file extensions.
   *
   * Here we are reading the transaction log, and we need to bypass the ACL checks
   * for SELECT any file permissions.
   */
  private def load(paths: Seq[Path]): Dataset[SingleAction] = {
    val pathAndFormats = paths.map(_.toString).map(path => path -> path.split("\\.").last)
    pathAndFormats.groupBy(_._2).map { case (format, paths) =>
      spark.read.format(format).schema(logSchema).load(paths.map(_._1): _*).as[SingleAction]
    }.reduceOption(_.union(_)).getOrElse(emptyActions)
  }

  private def emptyActions =
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], logSchema).as[SingleAction]
}

object Snapshot extends DeltaLogging {
  private lazy val emptyMetadata = udf(() => Metadata())
  private lazy val defaultProtocol = udf(() => Protocol())

  /** Canonicalize the paths for Actions */
  private def canonicalizePath(path: String, hadoopConf: Configuration): String = {
    val hadoopPath = new Path(new URI(path))
    if (hadoopPath.isAbsoluteAndSchemeAuthorityNull) {
      val fs = FileSystem.get(hadoopConf)
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

  private case class State(
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

  implicit private def stateEncoder: ExpressionEncoder[State] = _stateEncoder.copy()
}

/**
 * An initial snapshot with only metadata specified. Useful for creating a DataFrame from an
 * existing parquet table during its conversion to delta.
 * @param logPath the path to transaction log
 * @param deltaLog the delta log object
 * @param metadata the metadata of the table
 */
class InitialSnapshot(
    val logPath: Path,
    override val deltaLog: DeltaLog,
    override val metadata: Metadata)
  extends Snapshot(logPath, -1, None, Nil, -1, deltaLog, -1)
