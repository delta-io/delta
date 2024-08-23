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

import scala.collection.mutable.{ListBuffer, Map => MutableMap}
import scala.util.Try

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.files.{CdcAddFileIndex, TahoeChangeFileIndex, TahoeFileIndexWithSnapshotDescriptor, TahoeRemoveFileIndex}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSource, DeltaSourceUtils, DeltaSQLConf}
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.util.ScalaExtensions.OptionExt

import org.apache.spark.internal.MDC
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.ColumnImplicitsShim._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.execution.LogicalRDD
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
object CDCReader extends CDCReaderImpl
{
  // Definitions for the CDC type column. Delta writers will write data with a non-null value for
  // this column into [[AddCDCFile]] actions separate from the main table, and the CDC reader will
  // read this column to determine what type of change it was.
  val CDC_TYPE_COLUMN_NAME = "_change_type" // emitted from data
  val CDC_COMMIT_VERSION = "_commit_version" // inferred by reader
  val CDC_COMMIT_TIMESTAMP = "_commit_timestamp" // inferred by reader
  val CDC_TYPE_DELETE_STRING = "delete"
  val CDC_TYPE_DELETE = Literal(CDC_TYPE_DELETE_STRING)
  val CDC_TYPE_INSERT = "insert"
  val CDC_TYPE_UPDATE_PREIMAGE = "update_preimage"
  val CDC_TYPE_UPDATE_POSTIMAGE = "update_postimage"

  /**
   * Append CDC metadata columns to the provided schema.
   */
  def cdcAttributes: Seq[Attribute] = Seq(
    AttributeReference(CDC_TYPE_COLUMN_NAME, StringType)(),
    AttributeReference(CDC_COMMIT_VERSION, LongType)(),
    AttributeReference(CDC_COMMIT_TIMESTAMP, TimestampType)())

  // A special sentinel value indicating rows which are part of the main table rather than change
  // data. Delta writers will partition rows with this value away from the CDC data and
  // write them as normal to the main table.
  // Note that we specifically avoid using `null` here, because partition values of `null` are in
  // some scenarios mapped to a special string for Hive compatibility.
  val CDC_TYPE_NOT_CDC: Literal = Literal(null, StringType)

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

  // A snapshot coupled with a schema mode that user specified
  case class SnapshotWithSchemaMode(snapshot: Snapshot, schemaMode: DeltaBatchCDFSchemaMode)

  /**
   * A special BaseRelation wrapper for CDF reads.
   */
  case class DeltaCDFRelation(
      snapshotWithSchemaMode: SnapshotWithSchemaMode,
      sqlContext: SQLContext,
      startingVersion: Option[Long],
      endingVersion: Option[Long]) extends BaseRelation with PrunedFilteredScan {

    private val deltaLog = snapshotWithSchemaMode.snapshot.deltaLog

    private lazy val latestVersionOfTableDuringAnalysis: Long = deltaLog.update().version

    /**
     * There may be a slight divergence here in terms of what schema is in the latest data vs what
     * schema we have captured during analysis, but this is an inherent limitation of Spark.
     *
     * However, if there are schema changes between analysis and execution, since we froze this
     * schema, our schema incompatibility checks will kick in during the scan so we will always
     * be safe - Although it is a notable caveat that user should be aware of because the CDC query
     * may break.
     */
    private lazy val endingVersionForBatchSchema: Long = endingVersion.map { v =>
      // As defined in the method doc, if ending version is greater than the latest version, we will
      // just use the latest version to find the schema.
      latestVersionOfTableDuringAnalysis min v
    }.getOrElse {
      // Or if endingVersion is not specified, we just use the latest schema.
      latestVersionOfTableDuringAnalysis
    }

    // The final snapshot whose schema is going to be used as this CDF relation's schema
    private val snapshotForBatchSchema: Snapshot = snapshotWithSchemaMode.schemaMode match {
      case BatchCDFSchemaEndVersion =>
        // Fetch the ending version and its schema
        deltaLog.getSnapshotAt(endingVersionForBatchSchema)
      case _ =>
        // Apply the default, either latest generated by DeltaTableV2 or specified by Time-travel
        // options.
        snapshotWithSchemaMode.snapshot
    }

    override val schema: StructType = cdcReadSchema(snapshotForBatchSchema.metadata.schema)

    override def unhandledFilters(filters: Array[Filter]): Array[Filter] = Array.empty

    override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
      val df = changesToBatchDF(
        deltaLog,
        startingVersion.get,
        // The actual ending version we should scan until during execution, as it might have changed
        endingVersion.getOrElse {
          deltaLog.update().version
        },
        sqlContext.sparkSession,
        readSchemaSnapshot = Some(snapshotForBatchSchema))

      val filter = Column(DeltaSourceUtils.translateFilters(filters))
      val projections = requiredColumns.map(SchemaUtils.fieldNameToColumn)
      df.filter(filter).select(projections: _*).rdd
    }
  }

  case class CDCDataSpec[T <: FileAction](
      version: Long,
      timestamp: Timestamp,
      actions: Seq[T],
      commitInfo: Option[CommitInfo]) {
    def this(
        tableVersion: TableVersion,
        actions: Seq[T],
        commitInfo: Option[CommitInfo]) = {
      this(
        tableVersion.version,
        tableVersion.timestamp,
        actions,
        commitInfo)
    }
  }

  /** A version number of a Delta table, with the version's timestamp. */
  case class TableVersion(version: Long, timestamp: Timestamp) {
    def this(wp: FilePathWithTableVersion) = this(wp.version, wp.timestamp)
  }

  /** Path of a file of a Delta table, together with it's origin table version & timestamp. */
  case class FilePathWithTableVersion(
      path: String,
      commitInfo: Option[CommitInfo],
      version: Long,
      timestamp: Timestamp)
}

trait CDCReaderImpl extends DeltaLogging {

  import org.apache.spark.sql.delta.commands.cdc.CDCReader._

  /**
   * Given timestamp or version, this method returns the corresponding version for that timestamp
   * or the version itself, as well as how the return version is obtained: by `version` or
   * `timestamp`.
   */
  private def getVersionForCDC(
      spark: SparkSession,
      deltaLog: DeltaLog,
      conf: SQLConf,
      options: CaseInsensitiveStringMap,
      versionKey: String,
      timestampKey: String): Option[ResolvedCDFVersion] = {
    if (options.containsKey(versionKey)) {
      Some(ResolvedCDFVersion(options.get(versionKey).toLong, timestamp = None))
    } else if (options.containsKey(timestampKey)) {
      val ts = options.get(timestampKey)
      val spec = DeltaTimeTravelSpec(Some(Literal(ts)), None, Some("cdcReader"))
      val timestamp = spec.getTimestamp(spark.sessionState.conf)
      val allowOutOfRange = conf.getConf(DeltaSQLConf.DELTA_CDF_ALLOW_OUT_OF_RANGE_TIMESTAMP)
      val resolvedVersion = if (timestampKey == DeltaDataSource.CDC_START_TIMESTAMP_KEY) {
        // For the starting timestamp we need to find a version after the provided timestamp
        // we can use the same semantics as streaming.
        DeltaSource.getStartingVersionFromTimestamp(spark, deltaLog, timestamp, allowOutOfRange)
      } else {
        // For ending timestamp the version should be before the provided timestamp.
        DeltaTableUtils.resolveTimeTravelVersion(conf, deltaLog, spec, allowOutOfRange)._1
      }
      Some(ResolvedCDFVersion(resolvedVersion, Some(timestamp)))
    } else {
      None
    }
  }

  /**
   * Get the batch cdf schema mode for a table, considering whether it has column mapping enabled
   * or not.
   */
  def getBatchSchemaModeForTable(
      spark: SparkSession,
      columnMappingEnabled: Boolean): DeltaBatchCDFSchemaMode = {
    if (columnMappingEnabled) {
      // Tables with column-mapping enabled can specify which schema version to use with this
      // config.
      DeltaBatchCDFSchemaMode(spark.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_CDF_DEFAULT_SCHEMA_MODE_FOR_COLUMN_MAPPING_TABLE))
    } else {
      // Non column-mapping table uses the current default, which is typically `legacy` - usually
      // the latest schema is used, but it can depend on time-travel arguments as well.
      BatchCDFSchemaLegacy
    }
  }

  /**
   * Get a Relation that represents change data between two snapshots of the table.
   *
   * @param spark Spark session
   * @param snapshotToUse Snapshot to use to provide read schema and version
   * @param isTimeTravelQuery Whether this CDC scan is used in conjunction with time-travel args
   * @param conf SQL conf
   * @param options CDC specific options
   */
  def getCDCRelation(
      spark: SparkSession,
      snapshotToUse: Snapshot,
      isTimeTravelQuery: Boolean,
      conf: SQLConf,
      options: CaseInsensitiveStringMap): BaseRelation = {

    val startingVersion = getVersionForCDC(
      spark,
      snapshotToUse.deltaLog,
      conf,
      options,
      DeltaDataSource.CDC_START_VERSION_KEY,
      DeltaDataSource.CDC_START_TIMESTAMP_KEY).getOrElse {
      throw DeltaErrors.noStartVersionForCDC()
    }

    val endingVersionOpt = getVersionForCDC(
      spark,
      snapshotToUse.deltaLog,
      conf,
      options,
      DeltaDataSource.CDC_END_VERSION_KEY,
      DeltaDataSource.CDC_END_TIMESTAMP_KEY
    )

    verifyStartingVersion(spark, snapshotToUse, conf, startingVersion) match {
      case Some(toReturn) =>
        return toReturn
      case None =>
    }

    verifyEndingVersion(spark, snapshotToUse, startingVersion, endingVersionOpt) match {
      case Some(toReturn) =>
        return toReturn
      case None =>
    }

    logInfo(
      log"startingVersion: ${MDC(DeltaLogKeys.START_VERSION, startingVersion.version)}, " +
      log"endingVersion: ${MDC(DeltaLogKeys.END_VERSION, endingVersionOpt.map(_.version))}")

    val startingSnapshot = snapshotToUse.deltaLog.getSnapshotAt(startingVersion.version)
    val columnMappingEnabledAtStartingVersion =
      startingSnapshot.metadata.columnMappingMode != NoMapping

    val columnMappingEnabledAtEndVersion = endingVersionOpt.exists { endingVersion =>
      // End version could be after the snapshot to use version, in which case it might not exist.
      if (endingVersion.version > snapshotToUse.version) {
        false
      } else {
        val endingSnapshot = snapshotToUse.deltaLog.getSnapshotAt(endingVersion.version)
        endingSnapshot.metadata.columnMappingMode != NoMapping &&
          endingVersion.version <= snapshotToUse.version
      }
    }

    val columnMappingEnabledAtSnapshotToUseVersion =
      snapshotToUse.metadata.columnMappingMode != NoMapping

    // Special handling for tables with column mapping mode enabled in any of the versions.
    val columnMappingEnabled = columnMappingEnabledAtSnapshotToUseVersion ||
      columnMappingEnabledAtEndVersion || columnMappingEnabledAtStartingVersion
    val schemaMode = getBatchSchemaModeForTable(spark, columnMappingEnabled = columnMappingEnabled)

    // Non-legacy schema mode options cannot be used with time-travel because the schema to use
    // will be confusing.
    if (isTimeTravelQuery && schemaMode != BatchCDFSchemaLegacy) {
      throw DeltaErrors.illegalDeltaOptionException(
        DeltaSQLConf.DELTA_CDF_DEFAULT_SCHEMA_MODE_FOR_COLUMN_MAPPING_TABLE.key,
        schemaMode.name,
        s"${DeltaSQLConf.DELTA_CDF_DEFAULT_SCHEMA_MODE_FOR_COLUMN_MAPPING_TABLE.key} " +
          s"cannot be used with time travel options.")
    }
    DeltaCDFRelation(
      SnapshotWithSchemaMode(snapshotToUse, schemaMode),
      spark.sqlContext,
      Some(startingVersion.version),
      endingVersionOpt.map(_.version))
  }

  private def verifyStartingVersion(
      spark: SparkSession,
      snapshotToUse: Snapshot,
      conf: SQLConf,
      startingVersion: ResolvedCDFVersion): Option[BaseRelation] = {
    // add a version check here that is cheap instead of after trying to list a large version
    // that doesn't exist
    if (startingVersion.version > snapshotToUse.version) {
      val allowOutOfRange = conf.getConf(DeltaSQLConf.DELTA_CDF_ALLOW_OUT_OF_RANGE_TIMESTAMP)
      // LS-129: return an empty relation if start version passed in is beyond latest commit version
      if (allowOutOfRange) {
        return Some(emptyCDFRelation(spark, snapshotToUse, BatchCDFSchemaLegacy))
      }
      throw DeltaErrors.startVersionAfterLatestVersion(
        startingVersion.version, snapshotToUse.version)
    }
    None
  }

  private def verifyEndingVersion(
      spark: SparkSession,
      snapshotToUse: Snapshot,
      startingVersion: ResolvedCDFVersion,
      endingVersionOpt: Option[ResolvedCDFVersion]): Option[BaseRelation] = {
    // Given two timestamps, there is a case when both of them lay closely between two versions:
    // version:          4                                                 5
    //          ---------|-------------------------------------------------|--------
    //                           ^ start timestamp        ^ end timestamp
    // In this case the starting version will be 5 and ending version will be 4. We must not
    // throw `endBeforeStartVersionInCDC` but return empty result.
    endingVersionOpt.foreach { endingVersion =>
      if (startingVersion.resolvedByTimestamp && endingVersion.resolvedByTimestamp) {
        // The next `if` is true when end is less than start but no commit is in between.
        // We need to capture such a case and throw early.
        if (startingVersion.timestamp.get.after(endingVersion.timestamp.get)) {
          throw DeltaErrors.endBeforeStartVersionInCDC(
            startingVersion.version,
            endingVersion.version)
        }
        if (endingVersion.version == startingVersion.version - 1) {
          return Some(emptyCDFRelation(spark, snapshotToUse, BatchCDFSchemaLegacy))
        }
      }
      if (endingVersionOpt.exists(_.version < startingVersion.version)) {
        throw DeltaErrors.endBeforeStartVersionInCDC(
          startingVersion.version,
          endingVersionOpt.get.version)
      }
    }
    None
  }

  private def emptyCDFRelation(
      spark: SparkSession,
      snapshot: Snapshot,
      schemaMode: DeltaBatchCDFSchemaMode) = {
    new DeltaCDFRelation(
      SnapshotWithSchemaMode(snapshot, schemaMode),
      spark.sqlContext,
      startingVersion = None,
      endingVersion = None) {
      override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
        sqlContext.sparkSession.sparkContext.emptyRDD[Row]
    }
  }

  /**
   * Function to check if file actions should be skipped for no-op merges based on
   * CommitInfo metrics.
   * MERGE will sometimes rewrite files in a way which *could* have changed data
   * (so dataChange = true) but did not actually do so (so no CDC will be produced).
   * In this case the correct CDC output is empty - we shouldn't serve it from
   * those files. This should be handled within the command, but as a hotfix-safe fix, we check
   * the metrics. If the command reported 0 rows inserted, updated, or deleted, then CDC
   * shouldn't be produced.
   */
  def shouldSkipFileActionsInCommit(commitInfo: CommitInfo): Boolean = {
    val isMerge = commitInfo.operation == DeltaOperations.OP_MERGE
    val knownToHaveNoChangedRows = {
      val metrics = commitInfo.operationMetrics.getOrElse(Map.empty)
      // Note that if any metrics are missing, this condition will be false and we won't skip.
      // Unfortunately there are no predefined constants for these metric values.
      Seq("numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted").forall {
        metrics.get(_).contains("0")
      }
    }
    isMerge && knownToHaveNoChangedRows
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
   * @param readSchemaSnapshot - Snapshot for the table for which we are creating a CDF
   *                             Dataframe, the schema of the snapshot is expected to be
   *                             the change DF's schema. We have already adjusted this
   *                             snapshot with the schema mode if there's any. We don't use
   *                             its data actually.
   * @param start - startingVersion of the changes
   * @param end - endingVersion of the changes
   * @param changes - changes is an iterator of all FileActions for a particular commit version.
   * @param spark - SparkSession
   * @param isStreaming - indicates whether the DataFrame returned is a streaming DataFrame
   * @param useCoarseGrainedCDC - ignores checks related to CDC being disabled in any of the
   *         versions and computes CDC entirely from AddFiles/RemoveFiles (ignoring
   *         AddCDCFile actions)
   * @param startVersionSnapshot - The snapshot of the starting version.
   * @return CDCInfo which contains the DataFrame of the changes as well as the statistics
   *         related to the changes
   */
  def changesToDF(
      readSchemaSnapshot: SnapshotDescriptor,
      start: Long,
      end: Long,
      changes: Iterator[(Long, Seq[Action])],
      spark: SparkSession,
      isStreaming: Boolean = false,
      useCoarseGrainedCDC: Boolean = false,
      startVersionSnapshot: Option[SnapshotDescriptor] = None): CDCVersionDiffInfo = {
    val deltaLog = readSchemaSnapshot.deltaLog

    if (end < start) {
      throw DeltaErrors.endBeforeStartVersionInCDC(start, end)
    }

    // A map from change version to associated commit timestamp.
    val timestampsByVersion: Map[Long, Timestamp] =
      getTimestampsByVersion(deltaLog, start, end, spark)

    val changeFiles = ListBuffer[CDCDataSpec[AddCDCFile]]()
    val addFiles = ListBuffer[CDCDataSpec[AddFile]]()
    val removeFiles = ListBuffer[CDCDataSpec[RemoveFile]]()

    val startVersionMetadata = startVersionSnapshot.map(_.metadata).getOrElse {
      deltaLog.getSnapshotAt(start).metadata
    }
    if (!useCoarseGrainedCDC && !isCDCEnabledOnTable(startVersionMetadata, spark)) {
      throw DeltaErrors.changeDataNotRecordedException(start, start, end)
    }

    // Check schema read-compatibility
    val allowUnsafeBatchReadOnIncompatibleSchemaChanges =
      spark.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_CDF_UNSAFE_BATCH_READ_ON_INCOMPATIBLE_SCHEMA_CHANGES)

    if (allowUnsafeBatchReadOnIncompatibleSchemaChanges) {
      recordDeltaEvent(deltaLog, "delta.unsafe.cdf.readOnColumnMappingSchemaChanges")
    }

    val shouldCheckSchemaToBlockBatchRead =
      !isStreaming && !allowUnsafeBatchReadOnIncompatibleSchemaChanges
    /**
     * Check metadata (which may contain schema change)'s read compatibility with read schema.
     */
    def checkBatchCdfReadSchemaIncompatibility(
        metadata: Metadata, metadataVer: Long, isSchemaChange: Boolean): Unit = {
      // We do not check for any incompatibility if the global "I don't care" flag is turned on
      if (shouldCheckSchemaToBlockBatchRead) {
        // Column mapping incompatibilities
        val compatible = {
          // For column mapping schema change, the order matters because we don't want to treat
          // an ADD COLUMN as an inverse DROP COLUMN.
          if (metadataVer <= readSchemaSnapshot.version) {
            DeltaColumnMapping.hasNoColumnMappingSchemaChanges(
              newMetadata = readSchemaSnapshot.metadata, oldMetadata = metadata)
          } else {
            DeltaColumnMapping.hasNoColumnMappingSchemaChanges(
              newMetadata = metadata, oldMetadata = readSchemaSnapshot.metadata)
          }
        } && {
          // Other standard read incompatibilities
          if (metadataVer <= readSchemaSnapshot.version) {
            // If the metadata is before the read schema version, make sure:
            // a) metadata schema is a part of the read schema, i.e. only ADD COLUMN can evolve
            //    metadata schema into read schema
            // b) data type for common fields remain the same
            // c) metadata schema should not contain field that is nullable=true but the read schema
            //    is nullable=false.
            SchemaUtils.isReadCompatible(
              existingSchema = metadata.schema,
              readSchema = readSchemaSnapshot.schema,
              forbidTightenNullability = true)
          } else {
            // If the metadata is POST the read schema version, which can happen due to time-travel
            // or simply a divergence between analyzed version and the actual latest
            // version during scan, we will make sure the other way around:
            // a) the metadata must be a super set of the read schema, i.e. only ADD COLUMN can
            //    evolve read schema into metadata schema
            // b) data type for common fields remain the same
            // c) read schema should not contain field that is nullable=false but the metadata
            //    schema has nullable=true.
            SchemaUtils.isReadCompatible(
              existingSchema = readSchemaSnapshot.schema,
              readSchema = metadata.schema,
              forbidTightenNullability = false)
          }
        }

        if (!compatible) {
          throw DeltaErrors.blockBatchCdfReadWithIncompatibleSchemaChange(
            start, end,
            // The consistent read schema
            readSchemaSnapshot.metadata.schema, readSchemaSnapshot.version,
            // The conflicting schema or schema change version
            metadataVer,
            isSchemaChange
          )
        }
      }
    }

    var totalBytes = 0L
    var totalFiles = 0L

    changes.foreach {
      case (v, actions) =>
        // Check whether CDC was newly disabled in this version. (We should have already checked
        // that it's enabled for the starting version, so checking this for each version
        // incrementally is sufficient to ensure that it's enabled for the entire range.)
        val cdcDisabled = actions.exists {
          case m: Metadata => !isCDCEnabledOnTable(m, spark)
          case _ => false
        }

        if (cdcDisabled && !useCoarseGrainedCDC) {
          throw DeltaErrors.changeDataNotRecordedException(v, start, end)
        }

        // Check all intermediary metadata schema changes, this guarantees that there will be no
        // read-incompatible schema changes across the querying range.
        // Note that we don't have to check the schema change if it's at the start version, because:
        // 1. If it's an initialization, e.g. CREATE AS SELECT, we don't have to consider this
        //    as a schema change and report weird error messages.
        // 2. If it's indeed a schema change, as we won't be reading any data prior to it that
        //    falls back to the previous (possibly incorrect) schema, we will be safe. Also if there
        //    are any data file residing in the same commit, it will follow the new schema as well.
        if (v > start) {
          actions.collect { case a: Metadata => a }.foreach { metadata =>
            // Verify with start snapshot to check for any read-incompatible changes
            // This also detects the corner case in that there's only one schema change between
            // start and end, which looks exactly like the end schema.
            checkBatchCdfReadSchemaIncompatibility(metadata, v, isSchemaChange = true)
          }
        }

        // Set up buffers for all action types to avoid multiple passes.
        val cdcActions = ListBuffer[AddCDCFile]()
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
            totalFiles += 1L
            totalBytes += a.size
          case r: RemoveFile =>
            totalFiles += 1L
            totalBytes += r.size.getOrElse(0L)
          case i: CommitInfo => commitInfo = Some(i)
          case _ => // do nothing
        }

        // If there are CDC actions, we read them exclusively if we should not use the
        // Add and RemoveFiles.
        if (cdcActions.nonEmpty && !useCoarseGrainedCDC) {
          changeFiles.append(CDCDataSpec(v, ts, cdcActions.toSeq, commitInfo))
        } else {
          val shouldSkipIndexedFile = commitInfo.exists(CDCReader.shouldSkipFileActionsInCommit)
          if (shouldSkipIndexedFile) {
            // This was introduced for a hotfix, so we're mirroring the existing logic as closely
            // as possible - it'd likely be safe to just return an empty dataframe here.
            addFiles.append(CDCDataSpec(v, ts, Nil, commitInfo))
            removeFiles.append(CDCDataSpec(v, ts, Nil, commitInfo))
          } else {
            // Otherwise, we take the AddFile and RemoveFile actions with dataChange = true and
            // infer CDC from them.
            val addActions = actions.collect { case a: AddFile if a.dataChange => a }
            val removeActions = actions.collect { case r: RemoveFile if r.dataChange => r }
            addFiles.append(
              CDCDataSpec(
                version = v,
                timestamp = ts,
                actions = addActions,
                commitInfo = commitInfo)
            )
            removeFiles.append(
              CDCDataSpec(
                version = v,
                timestamp = ts,
                actions = removeActions,
                commitInfo = commitInfo)
            )
          }
        }
    }

    // Verify the final read schema with the start snapshot version once again
    // This is needed to:
    // 1. Handle the case in that there are no read-incompatible schema change with the range, BUT
    //    the latest schema may still be incompatible as it COULD be arbitrary.
    // 2. Similarly, handle the corner case when there are no read-incompatible schema change with
    //    the range, BUT time-travel is used so the read schema could also be arbitrary.
    // It is sufficient to just verify with the start version schema because we have already
    // verified that all data being queried is read-compatible with start schema.
    checkBatchCdfReadSchemaIncompatibility(startVersionMetadata, start, isSchemaChange = false)

    val dfs = ListBuffer[DataFrame]()
    if (changeFiles.nonEmpty) {
      dfs.append(scanIndex(
        spark,
        new TahoeChangeFileIndex(
          spark, changeFiles.toSeq, deltaLog, deltaLog.dataPath, readSchemaSnapshot),
        isStreaming))
    }

    val deletedAndAddedRows = getDeletedAndAddedRows(
      addFiles.toSeq, removeFiles.toSeq, deltaLog,
      readSchemaSnapshot, isStreaming, spark)
    dfs.append(deletedAndAddedRows: _*)

    val readSchema = cdcReadSchema(readSchemaSnapshot.metadata.schema)
    // build an empty DS. This DS retains the table schema and the isStreaming property
    // NOTE: We need to manually set the stats to 0 otherwise we will use default stats of INT_MAX,
    // which causes lots of optimizations to be applied wrong.
    val emptyRdd = LogicalRDD(
      toAttributes(readSchema),
      spark.sparkContext.emptyRDD[InternalRow],
      isStreaming = isStreaming
    )(spark.sqlContext.sparkSession, Some(Statistics(0, Some(0))))
    val emptyDf =
      Dataset.ofRows(spark.sqlContext.sparkSession, emptyRdd)

    CDCVersionDiffInfo(
      (emptyDf +: dfs).reduce((df1, df2) => df1.union(
        df2
      )),
      totalFiles,
      totalBytes)
  }

  /**
   * Generate CDC rows by looking at added and removed files, together with Deletion Vectors they
   * may have.
   *
   * When DV is used, the same file can be removed then added in the same version, and the only
   * difference is the assigned DVs. The base method does not consider DVs in this case, thus will
   * produce CDC that *all* rows in file being removed then *some* re-added. The correct answer,
   * however, is to compare two DVs and apply the diff to the file to get removed and re-added rows.
   *
   * Currently it is always the case that in the log "remove" comes first, followed by "add" --
   * which means that the file stays alive with a new DV. There's another possibility, though not
   * make many senses, that a file is "added" to log then "removed" in the same version. If this
   * becomes possible in future, we have to reconstruct the timeline considering the order of
   * actions rather than simply matching files by path.
   */
  protected def getDeletedAndAddedRows(
      addFileSpecs: Seq[CDCDataSpec[AddFile]],
      removeFileSpecs: Seq[CDCDataSpec[RemoveFile]],
      deltaLog: DeltaLog,
      snapshot: SnapshotDescriptor,
      isStreaming: Boolean,
      spark: SparkSession): Seq[DataFrame] = {
    // Transform inputs to maps indexed by version and path and map each version to a CommitInfo
    // object.
    val versionToCommitInfo = MutableMap.empty[Long, CommitInfo]
    val addFilesMap = addFileSpecs.flatMap { spec =>
      spec.commitInfo.ifDefined { ci => versionToCommitInfo(spec.version) = ci }
      spec.actions.map { action =>
        val key =
          FilePathWithTableVersion(action.path, spec.commitInfo, spec.version, spec.timestamp)
        key -> action
      }
    }.toMap
    val removeFilesMap = removeFileSpecs.flatMap { spec =>
      spec.commitInfo.ifDefined { ci => versionToCommitInfo(spec.version) = ci }
      spec.actions.map { action =>
        val key =
          FilePathWithTableVersion(action.path, spec.commitInfo, spec.version, spec.timestamp)
        key -> action
      }
    }.toMap

    val finalAddFiles = MutableMap[TableVersion, ListBuffer[AddFile]]()
    val finalRemoveFiles = MutableMap[TableVersion, ListBuffer[RemoveFile]]()

    // If a path is only being added, then scan it normally as inserted rows
    (addFilesMap.keySet -- removeFilesMap.keySet).foreach { addKey =>
      finalAddFiles
        .getOrElseUpdate(new TableVersion(addKey), ListBuffer())
        .append(addFilesMap(addKey))
    }

    // If a path is only being removed, then scan it normally as removed rows
    (removeFilesMap.keySet -- addFilesMap.keySet).foreach { removeKey =>
      finalRemoveFiles
        .getOrElseUpdate(new TableVersion(removeKey), ListBuffer())
        .append(removeFilesMap(removeKey))
    }

    // Convert maps back into Seq[CDCDataSpec] and feed it into a single scan. This will greatly
    // reduce the number of tasks.
    val finalAddFilesSpecs = buildCDCDataSpecSeq(finalAddFiles, versionToCommitInfo)
    val finalRemoveFilesSpecs = buildCDCDataSpecSeq(finalRemoveFiles, versionToCommitInfo)

    val dfAddsAndRemoves = ListBuffer[DataFrame]()

    if (finalAddFilesSpecs.nonEmpty) {
      dfAddsAndRemoves.append(
        scanIndex(
          spark,
          new CdcAddFileIndex(spark, finalAddFilesSpecs, deltaLog, deltaLog.dataPath, snapshot),
          isStreaming))
    }

    if (finalRemoveFilesSpecs.nonEmpty) {
      dfAddsAndRemoves.append(
        scanIndex(
          spark,
          new TahoeRemoveFileIndex(
            spark,
            finalRemoveFilesSpecs,
            deltaLog,
            deltaLog.dataPath,
            snapshot),
          isStreaming))
    }

    val dfGeneratedDvScanActions = processDeletionVectorActions(
      addFilesMap,
      removeFilesMap,
      versionToCommitInfo.toMap,
      deltaLog,
      snapshot,
      isStreaming,
      spark)

    dfAddsAndRemoves.toSeq ++ dfGeneratedDvScanActions
  }

  def processDeletionVectorActions(
      addFilesMap: Map[FilePathWithTableVersion, AddFile],
      removeFilesMap: Map[FilePathWithTableVersion, RemoveFile],
      versionToCommitInfo: Map[Long, CommitInfo],
      deltaLog: DeltaLog,
      snapshot: SnapshotDescriptor,
      isStreaming: Boolean,
      spark: SparkSession): Seq[DataFrame] = {
    val finalReplaceAddFiles = MutableMap[TableVersion, ListBuffer[AddFile]]()
    val finalReplaceRemoveFiles = MutableMap[TableVersion, ListBuffer[RemoveFile]]()

    val dvStore = DeletionVectorStore.createInstance(deltaLog.newDeltaHadoopConf())
    (addFilesMap.keySet intersect removeFilesMap.keySet).foreach { key =>
      val add = addFilesMap(key)
      val remove = removeFilesMap(key)
      val generatedActions = generateFileActionsWithInlineDv(add, remove, dvStore, deltaLog)
      generatedActions.foreach {
        case action: AddFile =>
          finalReplaceAddFiles
            .getOrElseUpdate(new TableVersion(key), ListBuffer())
            .append(action)
        case action: RemoveFile =>
          finalReplaceRemoveFiles
            .getOrElseUpdate(new TableVersion(key), ListBuffer())
            .append(action)
        case _ =>
          throw new Exception("Expecting AddFile or RemoveFile.")
      }
    }

    // We have to build one scan for each version because DVs attached to actions will be
    // broadcasted in [[ScanWithDeletionVectors.createBroadcastDVMap]] which is not version-aware.
    // Here, one file can have different row index filters in different versions.
    val dfs = ListBuffer[DataFrame]()
    // Scan for masked rows as change_type = "insert",
    // see explanation in [[generateFileActionsWithInlineDv]].
    finalReplaceAddFiles.foreach { case (tableVersion, addFiles) =>
      val commitInfo = versionToCommitInfo.get(tableVersion.version)
      dfs.append(
        scanIndex(
          spark,
          new CdcAddFileIndex(
            spark,
            Seq(new CDCDataSpec(tableVersion, addFiles.toSeq, commitInfo)),
            deltaLog,
            deltaLog.dataPath,
            snapshot,
            rowIndexFilters =
              Some(fileActionsToIfNotContainedRowIndexFilters(addFiles.toSeq))),
          isStreaming))
    }

    // Scan for masked rows as change_type = "delete",
    // see explanation in [[generateFileActionsWithInlineDv]].
    finalReplaceRemoveFiles.foreach { case (tableVersion, removeFiles) =>
      val commitInfo = versionToCommitInfo.get(tableVersion.version)
      dfs.append(
        scanIndex(
          spark,
          new TahoeRemoveFileIndex(
            spark,
            Seq(new CDCDataSpec(tableVersion, removeFiles.toSeq, commitInfo)),
            deltaLog,
            deltaLog.dataPath,
            snapshot,
            rowIndexFilters =
              Some(fileActionsToIfNotContainedRowIndexFilters(removeFiles.toSeq))),
          isStreaming))
    }

    dfs.toSeq
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
   *
   * @param readSchemaSnapshot The snapshot with the desired schema that will be used to
   *                           serve this CDF batch. It is usually passed upstream from
   *                           e.g. DeltaTableV2 as an effort to stablize the schema used for the
   *                           batch DF. We don't actually use its data.
   *                           If not set, it will fallback to the legacy behavior of using
   *                           whatever deltaLog.unsafeVolatileSnapshot is. This should be
   *                           avoided in production.
   */
  def changesToBatchDF(
      deltaLog: DeltaLog,
      start: Long,
      end: Long,
      spark: SparkSession,
      readSchemaSnapshot: Option[Snapshot] = None,
      useCoarseGrainedCDC: Boolean = false,
      startVersionSnapshot: Option[SnapshotDescriptor] = None): DataFrame = {

    val changesWithinRange = deltaLog.getChanges(start, end, failOnDataLoss = false)
    changesToDF(
      readSchemaSnapshot.getOrElse(deltaLog.unsafeVolatileSnapshot),
      start,
      end,
      changesWithinRange,
      spark,
      isStreaming = false,
      useCoarseGrainedCDC = useCoarseGrainedCDC,
      startVersionSnapshot = startVersionSnapshot)
      .fileChangeDf
  }

  /**
   * Build a dataframe from the specified file index. We can't use a DataFrame scan directly on the
   * file names because that scan wouldn't include partition columns.
   *
   * It can optionally take a customReadSchema for the dataframe generated.
   */
  protected def scanIndex(
      spark: SparkSession,
      index: TahoeFileIndexWithSnapshotDescriptor,
      isStreaming: Boolean = false): DataFrame = {

    val relation = HadoopFsRelation(
      location = index,
      partitionSchema = index.partitionSchema,
      dataSchema = cdcReadSchema(index.schema),
      bucketSpec = None,
      new DeltaParquetFileFormat(index.protocol, index.metadata, isCDCRead = true),
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
    // Consistent with DeltaOptions.readChangeFeed,
    // but CDCReader use CaseInsensitiveStringMap vs. CaseInsensitiveMap used by DataFrameReader.
    def toBoolean(input: String, name: String): Boolean = {
      Try(input.toBoolean).toOption.getOrElse {
        throw DeltaErrors.illegalDeltaOptionException(name, input, "must be 'true' or 'false'")
      }
    }

    val cdcEnabled = options.containsKey(DeltaDataSource.CDC_ENABLED_KEY) &&
      toBoolean(options.get(DeltaDataSource.CDC_ENABLED_KEY), DeltaDataSource.CDC_ENABLED_KEY)

    val cdcLegacyConfEnabled = options.containsKey(DeltaDataSource.CDC_ENABLED_KEY_LEGACY) &&
      toBoolean(
        options.get(DeltaDataSource.CDC_ENABLED_KEY_LEGACY), DeltaDataSource.CDC_ENABLED_KEY_LEGACY)

    cdcEnabled || cdcLegacyConfEnabled
  }

  /**
   * Determine if the metadata provided has cdc enabled or not.
   */
  def isCDCEnabledOnTable(metadata: Metadata, spark: SparkSession): Boolean = {
    ChangeDataFeedTableFeature.metadataRequiresFeatureToBeEnabled(
      protocol = Protocol(), metadata, spark)
  }

  /**
   * Given `add` and `remove` actions of the same file, manipulate DVs to get rows that are deleted
   * and re-added from `add` to `remove`.
   *
   * @return One or more [[AddFile]] and [[RemoveFile]], corresponding to CDC change_type "insert"
   *         and "delete". Rows masked by inline DVs are changed rows.
   */
  private def generateFileActionsWithInlineDv(
      add: AddFile,
      remove: RemoveFile,
      dvStore: DeletionVectorStore,
      deltaLog: DeltaLog): Seq[FileAction] = {

    val removeDvOpt = Option(remove.deletionVector)
    val addDvOpt = Option(add.deletionVector)

    val newActions = ListBuffer[FileAction]()

    // Four cases:
    // 1) Remove without DV, add without DV:
    //    Not possible. This case has been handled before.
    // 2) Remove without DV, add with DV1:
    //    Rows masked by DV1 are deleted.
    // 3) Remove with DV1, add without DV:
    //    Rows masked by DV1 are added. May happen when restoring a table.
    // 4) Remove with DV1, add with DV2:
    //   a) Rows masked by DV2 but not DV1 are deleted.
    //   b) Rows masked by DV1 but not DV2 are re-added. May happen when restoring a table.
    (removeDvOpt, addDvOpt) match {
      case (None, None) =>
        throw new Exception("Expecting one or both of add and remove contain DV.")
      case (None, Some(addDv)) =>
        newActions += remove.copy(deletionVector = addDv)
      case (Some(removeDv), None) =>
        newActions += add.copy(deletionVector = removeDv)
      case (Some(removeDv), Some(addDv)) =>
        val removeBitmap = dvStore.read(removeDv, deltaLog.dataPath)
        val addBitmap = dvStore.read(addDv, deltaLog.dataPath)

        // Case 4a
        val finalRemovedRowsBitmap = getDeletionVectorsDiff(addBitmap, removeBitmap)
        // Case 4b
        val finalReAddedRowsBitmap = getDeletionVectorsDiff(removeBitmap, addBitmap)

        val finalRemovedRowsDv = DeletionVectorDescriptor.inlineInLog(
          finalRemovedRowsBitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable),
          finalRemovedRowsBitmap.cardinality)
        val finalReAddedRowsDv = DeletionVectorDescriptor.inlineInLog(
          finalReAddedRowsBitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable),
          finalReAddedRowsBitmap.cardinality)

        newActions += remove.copy(deletionVector = finalRemovedRowsDv)
        newActions += add.copy(deletionVector = finalReAddedRowsDv)
    }

    newActions.toSeq
  }

  /**
   * Return a map of file paths to IfNotContained row index filters, to keep only the marked rows.
   */
  private def fileActionsToIfNotContainedRowIndexFilters(
      actions: Seq[FileAction]): Map[String, RowIndexFilterType] = {
    actions.map(f => f.path -> RowIndexFilterType.IF_NOT_CONTAINED).toMap
  }

  /**
   * Get a new [[RoaringBitmapArray]] copy storing values that are in `left` but not in `right`.
   */
  private def getDeletionVectorsDiff(
      left: RoaringBitmapArray,
      right: RoaringBitmapArray): RoaringBitmapArray = {
    val leftCopy = left.copy()
    leftCopy.diff(right)
    leftCopy
  }

  private def buildCDCDataSpecSeq[T <: FileAction](
      actionsByVersion: MutableMap[TableVersion, ListBuffer[T]],
      versionToCommitInfo: MutableMap[Long, CommitInfo]
  ): Seq[CDCDataSpec[T]] = actionsByVersion.map { case (fileVersion, addFiles) =>
    val commitInfo = versionToCommitInfo.get(fileVersion.version)
    new CDCDataSpec(fileVersion, addFiles.toSeq, commitInfo)
  }.toSeq

  /**
   * Represents the changes between some start and end version of a Delta table
   * @param fileChangeDf contains all of the file changes (AddFile, RemoveFile, AddCDCFile)
   * @param numFiles the number of AddFile + RemoveFile + AddCDCFiles that are in the df
   * @param numBytes the total size of the AddFile + RemoveFile + AddCDCFiles that are in the df
   */
  case class CDCVersionDiffInfo(fileChangeDf: DataFrame, numFiles: Long, numBytes: Long)

  /**
   * Represents a Delta log version, and how the version is determined.
   * @param version the determined version.
   * @param timestamp the commit timestamp of the determined version. Will be filled when the
   *                  version is determined by timestamp.
   */
  private case class ResolvedCDFVersion(version: Long, timestamp: Option[Timestamp]) {
    /** Whether this version is resolved by timestamp. */
    def resolvedByTimestamp: Boolean = timestamp.isDefined
  }
}
