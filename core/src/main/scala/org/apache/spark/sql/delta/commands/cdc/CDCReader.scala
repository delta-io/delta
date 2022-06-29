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

package org.apache.spark.sql.delta.commands.cdc

import java.sql.Timestamp

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaErrors, DeltaHistoryManager, DeltaLog, DeltaOperations, DeltaParquetFileFormat, DeltaTableUtils, DeltaTimeTravelSpec, NoMapping, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, AddCDCFile, AddFile, CommitInfo, FileAction, Metadata, RemoveFile}
import org.apache.spark.sql.delta.files.{CdcAddFileIndex, TahoeChangeFileIndex, TahoeFileIndex, TahoeRemoveFileIndex}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSource, DeltaSQLConf}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{LongType, StringType, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * The API that allows reading Change data between two versions of a table.
 *
 * The basic abstraction here is the CDC type column defined by [[CDCReader.CDC_TYPE_COLUMN_NAME]].
 * When CDC is enabled, our writer will treat this column as a special partition column even though
 * it's not part of the table. Writers should generate a query that has two types of rows in it:
 * the main data in partition CDC_TYPE_NOT_CDC and the CDC data with the appropriate CDC type value.
 *
 * [[org.apache.spark.sql.delta.files.DelayedCommitProtocol]]
 * does special handling for this column, dispatching the main data to its normal location while the
 * CDC data is sent to [[AddCDCFile]] entries.
 */
object CDCReader extends DeltaLogging {
  // Definitions for the CDC type column. Delta writers will write data with a non-null value for
  // this column into [[AddCDCFile]] actions separate from the main table, and the CDC reader will
  // read this column to determine what type of change it was.
  val CDC_TYPE_COLUMN_NAME = "_change_type" // emitted from data
  val CDC_COMMIT_VERSION = "_commit_version" // inferred by reader
  val CDC_COMMIT_TIMESTAMP = "_commit_timestamp" // inferred by reader
  val CDC_TYPE_DELETE = "delete"
  val CDC_TYPE_INSERT = "insert"
  val CDC_TYPE_UPDATE_PREIMAGE = "update_preimage"
  val CDC_TYPE_UPDATE_POSTIMAGE = "update_postimage"

  // A special sentinel value indicating rows which are part of the main table rather than change
  // data. Delta writers will partition rows with this value away from the CDC data and
  // write them as normal to the main table.
  // Note that we specifically avoid using `null` here, because partition values of `null` are in
  // some scenarios mapped to a special string for Hive compatibility.
  val CDC_TYPE_NOT_CDC: String = null

  // The virtual column name used for dividing CDC data from main table data. Delta writers should
  // permit this column through even though it's not part of the main table, and the
  // [[DelayedCommitProtocol]] will apply some special handling, ensuring there's only a
  // subfolder with __is_cdc = true and writing data with __is_cdc = false to the base location
  // as it would with CDC output off.
  // This is a bit redundant with CDC_TYPE_COL, but partitioning directly on the type would mean
  // that CDC of each type is partitioned away separately, exacerbating small file problems.
  val CDC_PARTITION_COL = "__is_cdc" // emitted by data

  // The top-level folder within the Delta table containing change data. This folder may contain
  // partitions within itself.
  val CDC_LOCATION = "_change_data"

  // CDC specific columns in data written by operations
  val CDC_COLUMNS_IN_DATA = Seq(CDC_PARTITION_COL, CDC_TYPE_COLUMN_NAME)

  /**
   * Given timestamp or version this method returns the corresponding version for that timestamp
   * or the version itself.
   */
  def getVersionForCDC(
      spark: SparkSession,
      deltaLog: DeltaLog,
      conf: SQLConf,
      options: CaseInsensitiveStringMap,
      versionKey: String,
      timestampKey: String): Option[Long] = {
    if (options.containsKey(versionKey)) {
      Some(options.get(versionKey).toLong)
    } else if (options.containsKey(timestampKey)) {
      val ts = options.get(timestampKey)
      val spec = DeltaTimeTravelSpec(Some(Literal(ts)), None, Some("cdcReader"))
      val allowOutOfRange = conf.getConf(DeltaSQLConf.DELTA_CDF_ALLOW_OUT_OF_RANGE_TIMESTAMP)
      if (timestampKey == DeltaDataSource.CDC_START_TIMESTAMP_KEY) {
        // For the starting timestamp we need to find a version after the provided timestamp
        // we can use the same semantics as streaming.
        val resolvedVersion = DeltaSource.getStartingVersionFromTimestamp(
          spark,
          deltaLog,
          spec.getTimestamp(spark.sessionState.conf),
          allowOutOfRange
        )
        Some(resolvedVersion)
      } else {
        // For ending timestamp the version should be before the provided timestamp.
        val resolvedVersion = DeltaTableUtils.resolveTimeTravelVersion(
          conf,
          deltaLog,
          spec,
          allowOutOfRange
        )
        Some(resolvedVersion._1)
      }
    } else {
      None
    }
  }

  /**
   * Get a Relation that represents change data between two snapshots of the table.
   */
  def getCDCRelation(
      spark: SparkSession,
      deltaLog: DeltaLog,
      snapshotToUse: Snapshot,
      partitionFilters: Seq[Expression],
      conf: SQLConf,
      options: CaseInsensitiveStringMap): BaseRelation = {

    val startingVersion = getVersionForCDC(
      spark,
      deltaLog,
      conf,
      options,
      DeltaDataSource.CDC_START_VERSION_KEY,
      DeltaDataSource.CDC_START_TIMESTAMP_KEY
    )

    if (startingVersion.isEmpty) {
      throw DeltaErrors.noStartVersionForCDC()
    }

    // add a version check here that is cheap instead of after trying to list a large version
    // that doesn't exist
    if (startingVersion.get > snapshotToUse.version) {
      val allowOutOfRange = conf.getConf(DeltaSQLConf.DELTA_CDF_ALLOW_OUT_OF_RANGE_TIMESTAMP)
      // LS-129: return an empty relation if start version passed in is beyond latest commit version
      if (allowOutOfRange) {
        return new DeltaCDFRelation(
          cdcReadSchema(snapshotToUse.metadata.schema),
          spark.sqlContext,
          deltaLog,
          None,
          None
        ) {
          override def buildScan(
              requiredColumns: Array[String],
              filters: Array[Filter]): RDD[Row] = {
            sqlContext.sparkSession.sparkContext.emptyRDD[Row]
          }
        }
      }
      throw DeltaErrors.startVersionAfterLatestVersion(
        startingVersion.get, snapshotToUse.version)
    }

    val endingVersion = getVersionForCDC(
      spark,
      deltaLog,
      conf,
      options,
      DeltaDataSource.CDC_END_VERSION_KEY,
      DeltaDataSource.CDC_END_TIMESTAMP_KEY
    )

    if (endingVersion.exists(_ < startingVersion.get)) {
      throw DeltaErrors.endBeforeStartVersionInCDC(startingVersion.get, endingVersion.get)
    }

    logInfo(s"startingVersion: $startingVersion, endingVersion: $endingVersion")

    DeltaCDFRelation(
      cdcReadSchema(snapshotToUse.metadata.schema),
      spark.sqlContext,
      deltaLog,
      startingVersion,
      endingVersion
    )
  }

  /**
   * A special BaseRelation wrapper for CDF reads.
   */
  case class DeltaCDFRelation(
      schema: StructType,
      sqlContext: SQLContext,
      deltaLog: DeltaLog,
      startingVersion: Option[Long],
      endingVersion: Option[Long]) extends BaseRelation with PrunedFilteredScan {

    override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
      val df = changesToBatchDF(
        deltaLog,
        startingVersion.get,
        endingVersion.getOrElse {
          // If no ending version was specified, use the latest version as of scan building time.
          // Note that this line won't be invoked (and thus we won't incur the update() cost)
          // when endingVersion is present.
          deltaLog.update().version
        },
        sqlContext.sparkSession)

      df.select(requiredColumns.map(SchemaUtils.fieldNameToColumn): _*).rdd
    }
  }

  /**
   * For a sequence of changes(AddFile, RemoveFile, AddCDCFile) create a DataFrame that represents
   * that captured change data between start and end inclusive.
   *
   * Builds the DataFrame using the following logic: Per each change of type (Long, Seq[Action]) in
   * `changes`, iterates over the actions and handles two cases.
   * - If there are any CDC actions, then we ignore the AddFile and RemoveFile actions in that
   *   version and create an AddCDCFile instead.
   * - If there are no CDC actions, then we must infer the CDC data from the AddFile and RemoveFile
   *   actions, taking only those with `dataChange = true`.
   *
   * These buffers of AddFile, RemoveFile, and AddCDCFile actions are then used to create
   * corresponding FileIndexes (e.g. [[TahoeChangeFileIndex]]), where each is suited to use the
   * given action type to read CDC data. These FileIndexes are then unioned to produce the final
   * DataFrame.
   *
   * @param deltaLog - DeltaLog for the table for which we are creating a cdc dataFrame
   * @param start - startingVersion of the changes
   * @param end - endingVersion of the changes
   * @param changes - changes is an iterator of all FileActions for a particular commit version.
   * @param isStreaming - indicates whether the DataFrame returned is a streaming DataFrame
   * @param spark - SparkSession
   * @return CDCInfo which contains the DataFrame of the changes as well as the statistics
   *         related to the changes
   */
  def changesToDF(
      deltaLog: DeltaLog,
      start: Long,
      end: Long,
      changes: Iterator[(Long, Seq[Action])],
      spark: SparkSession,
      isStreaming: Boolean = false): CDCVersionDiffInfo = {

    if (end < start) {
      throw DeltaErrors.endBeforeStartVersionInCDC(start, end)
    }

    val snapshot = deltaLog.snapshot

    // If the table has column mapping enabled, throw an error. With column mapping, certain schema
    // changes are possible (rename a column or drop a column) which don't work well with CDF.
    if (snapshot.metadata.columnMappingMode != NoMapping) {
      throw DeltaErrors.blockCdfAndColumnMappingReads()
    }

    // A map from change version to associated commit timestamp.
    val timestampsByVersion: Map[Long, Timestamp] =
      getTimestampsByVersion(deltaLog, start, end, spark)

    val changeFiles = ListBuffer[CDCDataSpec[AddCDCFile]]()
    val addFiles = ListBuffer[CDCDataSpec[AddFile]]()
    val removeFiles = ListBuffer[CDCDataSpec[RemoveFile]]()
    if (!isCDCEnabledOnTable(deltaLog.getSnapshotAt(start).metadata)) {
      throw DeltaErrors.changeDataNotRecordedException(start, start, end)
    }

    var totalBytes = 0L
    var totalFiles = 0L

    changes.foreach {
      case (v, actions) =>
        // Check whether CDC was newly disabled in this version. (We should have already checked
        // that it's enabled for the starting version, so checking this for each version
        // incrementally is sufficient to ensure that it's enabled for the entire range.)
        val cdcDisabled = actions.exists {
          case m: Metadata => !isCDCEnabledOnTable(m)
          case _ => false
        }

        if (cdcDisabled) {
          throw DeltaErrors.changeDataNotRecordedException(v, start, end)
        }

        // Set up buffers for all action types to avoid multiple passes.
        val cdcActions = ListBuffer[AddCDCFile]()
        val addActions = ListBuffer[AddFile]()
        val removeActions = ListBuffer[RemoveFile]()
        val ts = timestampsByVersion.get(v).orNull

        // Note that the CommitInfo is *not* guaranteed to be generated in 100% of cases.
        // We are using it only for a hotfix-safe mitigation/defense-in-depth - the value
        // extracted here cannot be relied on for correctness.
        var commitInfo: Option[CommitInfo] = None
        actions.foreach {
          case c: AddCDCFile =>
            cdcActions.append(c)
            totalFiles += 1L
            totalBytes += c.size
          case a: AddFile =>
            addActions.append(a)
            totalFiles += 1L
            totalBytes += a.size
          case r: RemoveFile =>
            removeActions.append(r)
            totalFiles += 1L
            totalBytes += r.size.getOrElse(0L)
          case i: CommitInfo => commitInfo = Some(i)
          case _ => // do nothing
        }

        // If there are CDC actions, we read them exclusively.
        if (cdcActions.nonEmpty) {
          changeFiles.append(CDCDataSpec(v, ts, cdcActions.toSeq))
        } else {
          // MERGE will sometimes rewrite files in a way which *could* have changed data
          // (so dataChange = true) but did not actually do so (so no CDC will be produced).
          // In this case the correct CDC output is empty - we shouldn't serve it from
          // those files.
          // This should be handled within the command, but as a hotfix-safe fix, we check the
          // metrics. If the command reported 0 rows inserted, updated, or deleted, then CDC
          // shouldn't be produced.
          val isMerge = commitInfo.isDefined &&
            commitInfo.get.operation == DeltaOperations.Merge(None, Nil, Nil).name
          val knownToHaveNoChangedRows = {
            val metrics = commitInfo.flatMap(_.operationMetrics).getOrElse(Map.empty)
            // Note that if any metrics are missing, this condition will be false and we won't skip.
            // Unfortunately there are no predefined constants for these metric values.
            Seq("numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted").forall {
              metrics.get(_).contains("0")
            }
          }
          if (isMerge && knownToHaveNoChangedRows) {
            // This was introduced for a hotfix, so we're mirroring the existing logic as closely
            // as possible - it'd likely be safe to just return an empty dataframe here.
            addFiles.append(CDCDataSpec(v, ts, Nil))
            removeFiles.append(CDCDataSpec(v, ts, Nil))
          } else {
            // Otherwise, we take the AddFile and RemoveFile actions with dataChange = true and
            // infer CDC from them.
            val addActions = actions.collect { case a: AddFile if a.dataChange => a }
            val removeActions = actions.collect { case r: RemoveFile if r.dataChange => r }
            addFiles.append(CDCDataSpec(v, ts, addActions))
            removeFiles.append(CDCDataSpec(v, ts, removeActions))
          }
        }
    }

    val dfs = ListBuffer[DataFrame]()
    if (changeFiles.nonEmpty) {
      dfs.append(scanIndex(
        spark,
        new TahoeChangeFileIndex(spark, changeFiles.toSeq, deltaLog, deltaLog.dataPath, snapshot),
        snapshot.metadata,
        isStreaming))
    }

    if (addFiles.nonEmpty) {
      dfs.append(scanIndex(
        spark,
        new CdcAddFileIndex(spark, addFiles.toSeq, deltaLog, deltaLog.dataPath, snapshot),
        snapshot.metadata,
        isStreaming))
    }

    if (removeFiles.nonEmpty) {
      dfs.append(scanIndex(
        spark,
        new TahoeRemoveFileIndex(spark, removeFiles.toSeq, deltaLog, deltaLog.dataPath, snapshot),
        snapshot.metadata,
        isStreaming))
    }

    CDCVersionDiffInfo(dfs.reduce((df1, df2) => df1.unionAll(df2)), totalFiles, totalBytes)
  }

  /**
   * Builds a map from commit versions to associated commit timestamps.
   * @param start  start commit version
   * @param end  end commit version
   */
  def getTimestampsByVersion(
      deltaLog: DeltaLog,
      start: Long,
      end: Long,
      spark: SparkSession): Map[Long, Timestamp] = {
    // Correct timestamp values are only available through DeltaHistoryManager.getCommits(). Commit
    // info timestamps are wrong, and file modification times are wrong because they need to be
    // monotonized first. This just performs a list (we don't read the contents of the files in
    // getCommits()) so the performance overhead is minimal.
    val monotonizationStart =
      math.max(start - DeltaHistoryManager.POTENTIALLY_UNMONOTONIZED_TIMESTAMPS, 0)
    val commits = DeltaHistoryManager.getCommits(
      deltaLog.store,
      deltaLog.logPath,
      monotonizationStart,
      Some(end + 1),
      deltaLog.newDeltaHadoopConf())

    // Note that the timestamps come from filesystem modification timestamps, so they're
    // milliseconds since epoch and we don't need to deal with timezones.
    commits.map(f => (f.version -> new Timestamp(f.timestamp))).toMap
  }

  /**
   * Get the block of change data from start to end Delta log versions (both sides inclusive).
   * The returned DataFrame has isStreaming set to false.
   */
  def changesToBatchDF(
      deltaLog: DeltaLog,
      start: Long,
      end: Long,
      spark: SparkSession): DataFrame = {

    val itr = deltaLog.getChanges(start)
    changesToDF(deltaLog, start, end, itr.takeWhile(_._1 <= end), spark, isStreaming = false)
      .fileChangeDf
  }

  /**
   * Build a dataframe from the specified file index. We can't use a DataFrame scan directly on the
   * file names because that scan wouldn't include partition columns.
   */
  private def scanIndex(
      spark: SparkSession,
      index: TahoeFileIndex,
      metadata: Metadata,
      isStreaming: Boolean = false): DataFrame = {
    val relation = HadoopFsRelation(
      index,
      index.partitionSchema,
      cdcReadSchema(metadata.schema),
      bucketSpec = None,
      new DeltaParquetFileFormat(metadata.columnMappingMode, metadata.schema),
      options = index.deltaLog.options)(spark)
    val plan = LogicalRelation(relation, isStreaming = isStreaming)
    Dataset.ofRows(spark, plan)
  }

  /**
   * Append CDC metadata columns to the provided schema.
   */
  def cdcReadSchema(deltaSchema: StructType): StructType = {
    deltaSchema
      .add(CDC_TYPE_COLUMN_NAME, StringType)
      .add(CDC_COMMIT_VERSION, LongType)
      .add(CDC_COMMIT_TIMESTAMP, TimestampType)
  }

  /**
   * Based on the read options passed it indicates whether the read was a cdc read or not.
   */
  def isCDCRead(options: CaseInsensitiveStringMap): Boolean = {
    val cdcEnabled = options.containsKey(DeltaDataSource.CDC_ENABLED_KEY) &&
      options.get(DeltaDataSource.CDC_ENABLED_KEY) == "true"

    val cdcLegacyConfEnabled = options.containsKey(DeltaDataSource.CDC_ENABLED_KEY_LEGACY) &&
      options.get(DeltaDataSource.CDC_ENABLED_KEY_LEGACY) == "true"

    cdcEnabled || cdcLegacyConfEnabled
  }

  /**
   * Determine if the metadata provided has cdc enabled or not.
   */
  def isCDCEnabledOnTable(metadata: Metadata): Boolean = {
    DeltaConfigs.CHANGE_DATA_FEED.fromMetaData(metadata)
  }

  case class CDCDataSpec[T <: FileAction](version: Long, timestamp: Timestamp, actions: Seq[T])

  /**
   * Represents the changes between some start and end version of a Delta table
   * @param fileChangeDf contains all of the file changes (AddFile, RemoveFile, AddCDCFile)
   * @param numFiles the number of AddFile + RemoveFile + AddCDCFiles that are in the df
   * @param numBytes the total size of the AddFile + RemoveFile + AddCDCFiles that are in the df
   */
  case class CDCVersionDiffInfo(fileChangeDf: DataFrame, numFiles: Long, numBytes: Long)
}
