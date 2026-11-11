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

package org.apache.spark.sql.delta.amt

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.concurrent.duration.Duration

import org.apache.spark.sql.delta.{RowIndexFilter, RowIndexFilterProvider}
import org.apache.spark.sql.delta.{CheckpointPolicy, CheckpointProvider, DataFrameUtils, DeltaLog, DeltaLogFileIndex, Snapshot, UninitializedCheckpointProvider}
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.DeltaLogFileIndex.COMMIT_VERSION_COLUMN
import org.apache.spark.sql.delta.SnapshotManagement.checkpointThreadPool
import org.apache.spark.sql.delta.actions.{Action, AddFile, BackReference, Checkpoint, ContentRoot, FileAction, Metadata, Protocol, RemoveFile, SingleAction}
import org.apache.spark.sql.delta.actions.FileAction.UniqueFileActionTuple
import org.apache.spark.sql.delta.deletionvectors.ManifestBitmap
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.DeltaEncoder
import org.apache.spark.sql.delta.util.threads.NonFateSharingFuture
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.execution.datasources.FileFormat.{FILE_PATH, METADATA_NAME}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, concat, element_at, lit, raise_error, struct, sum, udf, when}
import org.apache.spark.sql.types.{IntegerType, LongType, MapType, StringType, StructField, StructType}

/**
 * Leaf pointers and optional cached root rows. The cached schema is the root Parquet read schema,
 * based on [[AMTSingleAction.persistedSchema]]. Rows contain DATA and DATA_MANIFEST entries as
 * [[InternalRow]]s, with partition and content_stats kept as typed structs when present.
 */
private[delta] class AMTRootInfo(
    val leaves: Seq[DataManifestEntry],
    private val cachedRows: Option[(StructType, Seq[InternalRow])]) {
  def hasCachedRows: Boolean = cachedRows.isDefined

  // Build a fresh plan for the caller's session over the collected rows.
  def toDF(spark: SparkSession): Option[DataFrame] = cachedRows.map { case (schema, rows) =>
    DataFrameUtils.ofRows(spark, LocalRelation(toAttributes(schema), rows))
  }
}

/** Common checkpoint information available before and after the AMT root has been loaded. */
trait AMTCheckpointProviderBase
  extends UninitializedCheckpointProvider
  with DeltaLogging {
  /** The version of the manifest commit that wrote this checkpoint. */
  def manifestCommitVersion: Long

  /** The inline action describing this AMT checkpoint. */
  def checkpointAction: Checkpoint

  /** The table data path used to resolve manifest paths. */
  def tableRoot: Path

  override def version: Long = checkpointAction.version

  /** The number of leaf manifests referenced by the root. */
  private[delta] def numLeaves: Long

  override def topLevelFiles: Seq[FileStatus] = {
    val contentRoot = checkpointAction.contentRoot
    Seq(contentRoot.toFileStatus(tableRoot))
  }

  // Protocol and Metadata are carried inline on the Checkpoint action (not in the AMT), so convert
  // them to a DataFrame with the required schema without waiting for the root-manifest read.
  override def loadProtocolMetadataActions(
      spark: SparkSession, deltaLog: DeltaLog): Option[DataFrame] = {
    import org.apache.spark.sql.delta.implicits._
    val rows = Seq(
      SingleAction(protocol = checkpointAction.protocol),
      SingleAction(metaData = checkpointAction.metaData))
    val df = spark.createDataset(rows).toDF()
      .select(Snapshot.pAndMQuerySchema.fieldNames.toIndexedSeq.map(col): _*)
      .withColumn(COMMIT_VERSION_COLUMN, lit(version))
    Some(df)
  }
}

/**
 * A [[CheckpointProvider]] backed by an AMT (Adaptive Metadata Tree) manifest tree.
 *
 * This provider is only for inline manifest-commit checkpoints, and this is not intended for
 * standalone checkpoint which also refers to an AMT.
 *
 * @param manifestCommitVersion The version of the manifest commit that wrote this checkpoint.
 * @param checkpointAction The inline-emitted Checkpoint action this tree was committed with;
 *                         carries the version, contentRoot, and inline non-file state.
 * @param leaves           The root's `DATA_MANIFEST` pointer entries, one per reachable leaf.
 *                         Each entry's `location` is stored table-root-relative; use
 *                         [[liveLeafManifestAbsolutePaths]] to resolve it against the table root.
 * @param tableRoot        The table's data path.
 */
sealed abstract class AMTCheckpointProvider(
    override val manifestCommitVersion: Long,
    override val checkpointAction: Checkpoint,
    val leaves: Seq[DataManifestEntry],
    override val tableRoot: Path)
  extends AMTCheckpointProviderBase
  with AMTCheckpointProviderImpl
{
  override private[delta] def numLeaves: Long =
    checkpointAction.contentRoot.numLeaves.getOrElse(leaves.size.toLong)
}

/**
 * An AMT checkpoint whose root-manifest read starts immediately and completes asynchronously.
 */
final class UninitializedAMTCheckpointProvider(
    val deltaLog: DeltaLog,
    override val checkpointAction: Checkpoint,
    override val manifestCommitVersion: Long)
  extends AMTCheckpointProviderBase {

  override val tableRoot: Path = deltaLog.dataPath

  private[delta] val nonFateSharingCheckpointReadFuture
      : NonFateSharingFuture[AMTRootInfo] =
    checkpointThreadPool.submitNonFateSharing { spark: SparkSession =>
      AMTCheckpointProvider.loadRootFromCheckpoint(
        spark, deltaLog, checkpointAction, manifestCommitVersion)
    }

  override private[delta] def numLeaves: Long =
    checkpointAction.contentRoot.numLeaves.getOrElse {
      // ContentRoot.tags normally provides numLeaves without waiting. Only when that information
      // is absent from the tags must we wait for the existing prefetch future to derive the count.
      nonFateSharingCheckpointReadFuture.get(Duration.Inf).leaves.size.toLong
    }
}

/**
 * A complete-provider facade which waits for the prefetched AMT root only when an API requiring
 * the complete checkpoint is first used.
 */
abstract class LazyCompleteAMTCheckpointProvider(
    uninitializedCheckpointProvider: UninitializedAMTCheckpointProvider)
  extends AMTCheckpointProviderBase
  with CheckpointProvider {

  override val manifestCommitVersion: Long =
    uninitializedCheckpointProvider.manifestCommitVersion
  override val checkpointAction: Checkpoint = uninitializedCheckpointProvider.checkpointAction
  override val tableRoot: Path = uninitializedCheckpointProvider.tableRoot

  protected def createCheckpointProvider(): AMTCheckpointProvider

  lazy val underlyingCheckpointProvider: AMTCheckpointProvider = createCheckpointProvider()

  override private[delta] def numLeaves: Long = uninitializedCheckpointProvider.numLeaves

  override def effectiveCheckpointSizeInBytes(): Long =
    underlyingCheckpointProvider.effectiveCheckpointSizeInBytes()

  override def checkpointPolicyForLogging: Option[CheckpointPolicy.Policy] =
    underlyingCheckpointProvider.checkpointPolicyForLogging

  override def createCompatibilityCheckpoint(
      spark: SparkSession,
      deltaLog: DeltaLog,
      logPath: Path,
      hadoopConf: Configuration): Unit =
    underlyingCheckpointProvider.createCompatibilityCheckpoint(
      spark, deltaLog, logPath, hadoopConf)

  override def loadActionsForStateReconstruction(
      spark: SparkSession, deltaLog: DeltaLog): Option[DataFrame] =
    underlyingCheckpointProvider.loadActionsForStateReconstruction(spark, deltaLog)

}

/**
 * An [[AMTCheckpointProvider]] whose state reconstruction decodes every manifest entry into an
 * [[AMTCheckpointProvider.AMTDataEntryExtended]] and converts it to an `AddFile` on the executors.
 * The default, kept as the fallback for [[ColumnarAMTCheckpointProvider]].
 */
final class PerRowAMTCheckpointProvider(
    manifestCommitVersion: Long,
    checkpointAction: Checkpoint,
    leaves: Seq[DataManifestEntry],
    tableRoot: Path)
  extends AMTCheckpointProvider(manifestCommitVersion, checkpointAction, leaves, tableRoot) {

  override protected def rootLiveAddSingleActions(
      spark: SparkSession, deltaLog: DeltaLog): Dataset[SingleAction] = {
    val index = DeltaLogFileIndex(
      DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET, Array(rootFile))
    val entries = liveDataEntries(deltaLog, index)
    toAddSingleActions(entries, dataEntryResolverProvider(parentLeaves = Seq.empty))
  }

  override protected def leafLiveAddSingleActions(deltaLog: DeltaLog): Dataset[SingleAction] =
    toAddSingleActions(
      loadLeafDataEntriesWithRowIdPrefix(deltaLog, liveLeavesWithManifestFilters),
      dataEntryResolverProvider(parentLeaves = liveLeaves))

  override private[amt] def readLiveRootDataEntries(
      spark: SparkSession, deltaLog: DeltaLog): Seq[AddFile] =
    AMTCheckpointProvider.readLiveRootDataEntries(deltaLog, checkpointAction)
}

/**
 * An [[AMTCheckpointProvider]] whose state reconstruction builds each `AddFile` field as a column
 * over the persisted manifest rows (see [[addFileColumn]]). Enabled by
 * [[DeltaSQLConf.AMT_COLUMNAR_STATE_RECONSTRUCTION_ENABLED]].
 *
 * @param rootInfo The root's leaf pointers and optional prefetched rows.
 */
final class ColumnarAMTCheckpointProvider(
    manifestCommitVersion: Long,
    checkpointAction: Checkpoint,
    tableRoot: Path,
    private val rootInfo: AMTRootInfo)
  extends AMTCheckpointProvider(
    manifestCommitVersion, checkpointAction, rootInfo.leaves, tableRoot) {

  override protected def rootLiveAddSingleActions(
      spark: SparkSession, deltaLog: DeltaLog): Dataset[SingleAction] = {
    // Reuse the prefetched root if possible.
    val entries = rootInfo.toDF(spark).map { root =>
      // The shared back-reference expression requires a non-null pos, unused for root entries.
      root.withColumn("pos", lit(0))
    }.getOrElse {
      val index = DeltaLogFileIndex(new AMTParquetFileFormat, Array(rootFile))
      loadPersistedEntries(deltaLog, index)
    }
    toAddSingleActions(
      withRootEntryContext(liveEntries(dataEntries(entries))),
      baseRowId = col("tracking.first_row_id"))
  }

  override protected def leafLiveAddSingleActions(deltaLog: DeltaLog): Dataset[SingleAction] =
    toAddSingleActions(
      leafEntriesWithRowIdPrefix(deltaLog, liveLeavesWithManifestFilters),
      baseRowId = inheritedBaseRowId)

  /** Reuses prefetched root for incremental writes as well as snapshot state reconstruction. */
  override private[amt] def readLiveRootDataEntries(
      spark: SparkSession, deltaLog: DeltaLog): Seq[AddFile] =
    rootLiveAddSingleActions(spark, deltaLog).collect().toSeq.map(_.add)

  /**
   * Like [[loadLeafDataEntriesWithRowIdPrefix]], but returns the persisted rows of the entries,
   * with their entry context (see [[withLeafEntryContext]]), rather than decoded ones.
   */
  private def leafEntriesWithRowIdPrefix(
      deltaLog: DeltaLog,
      leavesWithRowIndexFilters: Seq[(DataManifestEntry, Option[RowIndexFilterProvider])])
      : DataFrame = {
    val index = leafFileIndex(leavesWithRowIndexFilters)
    val withPrefix =
      dataEntries(loadPersistedEntries(deltaLog, index, includeRowIndexFilterMarker = true))
        .withColumn(PRECEDING_RECORDS_COLUMN, precedingNullRowIdRecords())
    withLeafEntryContext(
      filterOutRowIndexFilterEntries(liveEntries(withPrefix)),
      leavesWithRowIndexFilters.map { case (leaf, _) => leaf })
  }

  /** The DATA entries of persisted manifest rows. */
  private def dataEntries(entries: DataFrame): DataFrame =
    entries.where(col("content_type") === lit(AMTSingleAction.ContentType.Type.Data))

  /** The entries of persisted manifest rows whose status makes them part of the table. */
  private def liveEntries(entries: DataFrame): DataFrame =
    entries.where(col("tracking.status").isin(Tracking.Status.liveEntryStatuses.toSeq: _*))

  /**
   * The persisted rows of the manifests in `index`, read with the checkpoint's persisted schema,
   * with their manifest (`leafPath`) and their position in it (`pos`). The optional
   * row-index-filter marker is carried along so callers can apply it after computing any row-ID
   * prefix.
   */
  private def loadPersistedEntries(
      deltaLog: DeltaLog,
      index: DeltaLogFileIndex,
      includeRowIndexFilterMarker: Boolean = false): DataFrame = {
    val persistedSchema =
      AMTSingleAction.persistedSchema(checkpointAction.metaData, checkpointAction.protocol)
    val readSchema =
      if (includeRowIndexFilterMarker) rowIndexFilterReadSchema(persistedSchema)
      else persistedSchema
    val markerColumns =
      if (includeRowIndexFilterMarker) Seq(rowIndexFilterMarkerColumn)
      else Seq.empty
    deltaLog.loadIndex(index, readSchema)
      .select(
        (persistedSchema.fieldNames.toIndexedSeq.map(col) :+
          col(s"$METADATA_NAME.$FILE_PATH").as("leafPath") :+
          col(s"$METADATA_NAME.${ParquetFileFormat.ROW_INDEX}").as("pos")) ++ markerColumns: _*)
  }

  /**
   * Adds the entry context of root entry rows, which the AddFile columns read instead of the
   * manifest a row was read from: root entries have no parent, so they keep their own tracking and
   * carry no back reference.
   */
  private def withRootEntryContext(entries: DataFrame): DataFrame =
    entries.select(col("*") +: Seq(
      lit(true).as(IS_ROOT_ENTRY_COLUMN),
      lit(null).cast(StringType).as(MANIFEST_LOCATION_COLUMN),
      lit(null).cast(LongType).as(PARENT_SNAPSHOT_ID_COLUMN),
      lit(null).cast(LongType).as(PARENT_FILE_SEQUENCE_NUMBER_COLUMN),
      lit(null).cast(LongType).as(PARENT_FIRST_ROW_ID_COLUMN)): _*)

  /**
   * Adds the entry context of entry rows read from `leaves`: the location of the leaf a row was
   * read from and the tracking its entries inherit from the leaf's root pointer.
   */
  private def withLeafEntryContext(
      entries: DataFrame, leaves: Seq[DataManifestEntry]): DataFrame = {
    val tableRootString = tableRoot.toString
    val contextType = StructType(Seq(
      StructField(MANIFEST_LOCATION_COLUMN, StringType),
      StructField(PARENT_SNAPSHOT_ID_COLUMN, LongType),
      StructField(PARENT_FILE_SEQUENCE_NUMBER_COLUMN, LongType),
      StructField(PARENT_FIRST_ROW_ID_COLUMN, LongType)))
    val contextByManifestPath = leaves.map { leaf =>
      val leafPath = leaf.getAbsolutePath(tableRoot)
      val parent = InheritableTracking(leaf)
      SparkPath.fromPath(leafPath).urlEncoded -> Row(
        AMTUtils.relativizeLocation(tableRootString, leafPath.toString),
        parent.snapshot_id,
        parent.file_sequence_number,
        parent.first_row_id)
    }.toMap
    val context = element_at(
      Column(Literal.create(contextByManifestPath, MapType(StringType, contextType))),
      col("leafPath"))
    entries.select(col("*") +: lit(false).as(IS_ROOT_ENTRY_COLUMN) +:
      contextType.fieldNames.toIndexedSeq.map(name => context.getField(name).as(name)): _*)
  }

  /**
   * Like the conversion above, but converts live DATA entry rows, with their entry context, as
   * columns.
   *
   * @param baseRowId The entry's first row ID; leaf entries may inherit it from their parent.
   */
  private def toAddSingleActions(entries: DataFrame, baseRowId: Column): Dataset[SingleAction] = {
    import org.apache.spark.sql.delta.implicits._
    addFileEntries(entries, baseRowId).select(Action.logSchema.fields.toIndexedSeq.map { field =>
      (if (field.name == "add") col("add") else lit(null).cast(field.dataType)).as(field.name)
    }: _*).as[SingleAction]
  }

  /**
   * `entries` with the AddFile of each in an `add` column (see [[addFileStruct]]), failing the read
   * if an entry is malformed.
   *
   * @param baseRowId The entry's first row ID; leaf entries may inherit it from their parent.
   */
  private def addFileEntries(entries: DataFrame, baseRowId: Column): DataFrame =
    entries.withColumn("add", addFileStruct(entries.sparkSession, baseRowId))

  /**
   * The AddFile of a DATA entry row, failing the read if the row is malformed. The row must
   * carry its entry context (see [[withRootEntryContext]] and [[withLeafEntryContext]]).
   *
   * @param baseRowId The entry's first row ID; leaf entries may inherit it from their parent.
   */
  private def addFileStruct(spark: SparkSession, baseRowId: Column): Column = {
    val fields = struct(Action.addFileSchema.fields.toIndexedSeq.map { field =>
      addFileColumn(spark, field, baseRowId).as(field.name)
    }: _*)
    // Casting the whole struct keeps its type fixed when the optimizer drops a no-op field cast,
    // which would otherwise expose the manifest column's Parquet field-id metadata in the type.
    entryChecks.foldRight(fields) { case ((malformed, error), checked) =>
      when(malformed, error).otherwise(checked)
    }.cast(Action.addFileSchema)
  }

  /**
   * The checks of a live DATA entry row that no AddFile field depends on, as conditions on a
   * malformed row paired with the error they raise. Only state reconstruction runs them; callers
   * that build only some fields with [[addFileColumn]] get just the checks of those fields.
   */
  private def entryChecks: Seq[(Column, Column)] = Seq(
    // Scoped to DATA rows: filter CSE may evaluate this before the DATA filter, on root pointers.
    (col("content_type") === lit(AMTSingleAction.ContentType.Type.Data) &&
      col("manifest_info").isNotNull) ->
      ColumnarAMTCheckpointProvider.malformedEntry(
        lit("manifest_info must be null for a DATA entry.")),
    // No AddFile field uses `sequence_number`, but `Tracking.resolve` requires it like the rest.
    (!col(IS_ROOT_ENTRY_COLUMN) && col("tracking.sequence_number").isNull &&
      col("tracking.status") =!= lit(Tracking.Status.Added)) ->
      missingTrackingField("sequence_number"))

  /**
   * The AddFile field `field` of a DATA entry row, as [[DataEntry.toAddFile]] derives it,
   * failing the read if the entry row is malformed in a way the field depends on. The row must
   * carry its entry context (see [[withRootEntryContext]] and [[withLeafEntryContext]]).
   *
   * @param baseRowId The entry's first row ID; leaf entries may inherit it from their parent.
   */
  private[amt] def addFileColumn(
      spark: SparkSession, field: StructField, baseRowId: Column): Column = {
    val metadata = checkpointAction.metaData
    val value = field.name match {
      case "path" => ColumnarAMTCheckpointProvider.encodePath(col("location"))
      case "partitionValues" =>
        coalesce(
          AMTPartitionValues.toStringMap(metadata.partitionSchema),
          Column(Literal.create(Map.empty[String, String], MapType(StringType, StringType))))
      case "size" => col("file_size_in_bytes")
      case "modificationTime" => lit(0L)
      // Must not be null: V2 data skipping drops rows with a null `dataChange`.
      case "dataChange" => lit(false)
      case "stats" =>
        AMTContentStats.addFileStatsJson(spark, metadata, checkpointAction.protocol)
      case "tags" => col("tags")
      case "deletionVector" =>
        ColumnarAMTCheckpointProvider.toDescriptor(tableRoot)(col("deletion_vector"))
      case "baseRowId" => baseRowId
      case "defaultRowCommitVersion" =>
        inheritedTrackingField(
          "file_sequence_number", col(PARENT_FILE_SEQUENCE_NUMBER_COLUMN))
      case "backReference" => backReference
      case AMTPassthrough.FIELD_NAME => passthrough
      case "snapshotId" =>
        inheritedTrackingField("snapshot_id", col(PARENT_SNAPSHOT_ID_COLUMN))
      case "modifiedSnapshotId" => col("tracking.modified_snapshot_id")
      // AMT entries carry neither; no fallback, so a new AddFile field fails here until mapped.
      case "clusteringProvider" => lit(null)
    }
    value.cast(field.dataType)
  }

  /**
   * The tracking field `name` of a DATA entry row, as [[Tracking.resolve]] resolves it: an `ADDED`
   * leaf entry that leaves it null inherits `inherited`; any other leaf entry must materialize it.
   * Root entries have no parent and keep the persisted value.
   */
  private def inheritedTrackingField(name: String, inherited: Column): Column = {
    val persisted = col("tracking." + name)
    when(persisted.isNotNull || col(IS_ROOT_ENTRY_COLUMN), persisted)
      .when(col("tracking.status") === lit(Tracking.Status.Added), inherited)
      .otherwise(missingTrackingField(name))
  }

  private def missingTrackingField(name: String): Column =
    ColumnarAMTCheckpointProvider.malformedEntry(
      lit("tracking." + name + " is null but the entry's status is "), statusName,
      lit(". Only ADDED entries inherit " + name + " from the root DATA_MANIFEST entry; " +
        "every other status must materialize it."))

  /** The `first_row_id` of a leaf entry row, inheriting its parent's plus the row-ID prefix. */
  private def inheritedBaseRowId: Column =
    inheritedTrackingField(
      "first_row_id", col(PARENT_FIRST_ROW_ID_COLUMN) + col(PRECEDING_RECORDS_COLUMN))

  private def statusName: Column = {
    val names = Tracking.Status.all.map(status => status -> Tracking.Status.nameOf(status)).toMap
    coalesce(
      element_at(Column(Literal.create(names, MapType(IntegerType, StringType))),
        col("tracking.status")),
      col("tracking.status").cast(StringType))
  }

  /** The (leaf, position) of a leaf-resident entry; root-resident entries have none. */
  private def backReference: Column =
    when(!col(IS_ROOT_ENTRY_COLUMN), struct(
      col(MANIFEST_LOCATION_COLUMN).as("manifest"),
      col("pos").cast(IntegerType).as("pos")))

  /**
   * The AMT-only DataEntry fields AddFile carries through; null when none is set. Like
   * [[AMTPassthrough.fromDataEntry]], it only supports Parquet data files.
   */
  private def passthrough: Column = {
    val fields = AMTPassthrough.STRUCT_TYPE.fieldNames.toIndexedSeq
    val fileFormat = col("file_format")
    when(fileFormat =!= lit(AMTSingleAction.FileFormatParquet),
      ColumnarAMTCheckpointProvider.malformedEntry(
        lit("amtPassthrough only supports parquet. got "), fileFormat, lit(".")))
      .when(fields.map(name => col(name).isNotNull).reduce(_ || _),
        struct(fields.map(name => col(name).as(name)): _*))
  }

  // The entry context of an entry row (see [[withRootEntryContext]] and [[withLeafEntryContext]]).
  private val IS_ROOT_ENTRY_COLUMN: String = "isRootEntry"
  private val MANIFEST_LOCATION_COLUMN: String = "manifestLocation"
  private val PARENT_SNAPSHOT_ID_COLUMN: String = "parentSnapshotId"
  private val PARENT_FILE_SEQUENCE_NUMBER_COLUMN: String = "parentFileSequenceNumber"
  private val PARENT_FIRST_ROW_ID_COLUMN: String = "parentFirstRowId"
}

object ColumnarAMTCheckpointProvider {
  /** RFC-2396 encoding of an AMT `location`, as [[DataEntry.toAddFile]] does. */
  private[amt] val encodePath =
    udf((location: String) => SparkPath.fromPathString(location).urlEncoded)

  /** Fails the read of an entry row with `reason`, naming the entry's data file. */
  private[amt] def malformedEntry(reason: Column*): Column =
    raise_error(concat(
      Seq(lit("Malformed AMT data entry '"), col("location"), lit("': ")) ++ reason: _*))

  /** An AMT deletion vector struct -> Delta's descriptor, as [[DeletionVector.toDescriptor]]. */
  private[amt] def toDescriptor(tableRoot: Path) = udf { (dv: Row) =>
    Option(dv).map { dv =>
      DeletionVector.toDescriptor(
        DeletionVector(
          location = dv.getAs[String]("location"),
          offset = dv.getAs[Long]("offset"),
          size_in_bytes = dv.getAs[Long]("size_in_bytes"),
          cardinality = dv.getAs[Long]("cardinality")),
        tableRoot)
    }.orNull
  }
}

trait AMTCheckpointProviderImpl extends CheckpointProvider {
  self: AMTCheckpointProvider =>

  import AMTCheckpointProvider.{AMTDataEntryExtended, DataEntryResolverProvider}

  /** The table version the manifest tree describes. */
  def checkpointVersion: Long = checkpointAction.version

  /** Pointer to the root manifest parquet. */
  private def contentRoot: ContentRoot = checkpointAction.contentRoot

  /** Absolute [[Path]] to the root manifest parquet, resolved against the table root. */
  private val rootManifestAbsolutePath: Path = contentRoot.getAbsolutePath(tableRoot)

  /** The live leaf pointers which must have all the live [[DataEntry]]s. */
  protected lazy val liveLeaves: Seq[DataManifestEntry] =
    leaves.filter(l => Tracking.Status.liveEntryStatuses.contains(l.tracking.status))

  /** Absolute [[Path]]s to the live leaf manifest parquet files, resolved against the root. */
  lazy val liveLeafManifestAbsolutePaths: Seq[Path] = liveLeaves.map(_.getAbsolutePath(tableRoot))

  /** The root manifest as a [[FileStatus]]. */
  protected lazy val rootFile: FileStatus = contentRoot.toFileStatus(tableRoot)

  override def effectiveCheckpointSizeInBytes(): Long =
    contentRoot.sizeInBytes + liveLeaves.map(_.file_size_in_bytes).sum

  override def checkpointPolicyForLogging: Option[CheckpointPolicy.Policy] = None

  override def loadActionsForStateReconstruction(
      spark: SparkSession, deltaLog: DeltaLog): Option[DataFrame] = {
    val df = allActions(spark, deltaLog).toDF()
      .withColumn(COMMIT_VERSION_COLUMN, lit(version))
      .withColumn(Snapshot.ADD_STATS_TO_USE_COL_NAME, col("add.stats"))
    Some(df)
  }
  /**
   * The full action set of this checkpoint as a distributed [[Dataset]] of [[SingleAction]]: the
   * live file `AddFile`s reconstructed from the AMT(root + leaves), unioned with the inline
   * non-content actions (protocol, metadata, domain metadata, txns) built on the driver.
   */
  private def allActions(spark: SparkSession, deltaLog: DeltaLog): Dataset[SingleAction] = {
    import org.apache.spark.sql.delta.implicits._
    val nonFileActions = spark.createDataset(nonContentSingleActions)
    liveAddSingleActions(spark, deltaLog).union(nonFileActions)
  }

  /** The inline, non-content actions carried directly on the [[Checkpoint]] action. */
  private def nonContentSingleActions: Seq[SingleAction] =
    Seq(
      SingleAction(protocol = checkpointAction.protocol),
      SingleAction(metaData = checkpointAction.metaData)) ++
    checkpointAction.domainMetadata.map(dm => SingleAction(domainMetadata = dm)) ++
    checkpointAction.txns.map(txn => SingleAction(txn = txn))

  /**
   * Reconstructs the live-file AddFile actions from the AMT as a [[Dataset]].
   */
  private def liveAddSingleActions(
      spark: SparkSession, deltaLog: DeltaLog): Dataset[SingleAction] = {
    val root = rootLiveAddSingleActions(spark, deltaLog)
    if (liveLeaves.isEmpty) root else root.union(leafLiveAddSingleActions(deltaLog))
  }

  /**
   * Reconstructs root-resident AddFiles. Root entries have no parent and therefore no inheritance.
   */
  protected def rootLiveAddSingleActions(
      spark: SparkSession, deltaLog: DeltaLog): Dataset[SingleAction]

  /** Reconstructs the AddFiles of every live leaf, taking care of inheritance. */
  protected def leafLiveAddSingleActions(deltaLog: DeltaLog): Dataset[SingleAction]

  /** The live AddFiles that the root holds, for the incremental writer. */
  private[amt] def readLiveRootDataEntries(
      spark: SparkSession, deltaLog: DeltaLog): Seq[AddFile]

  /** The live leaves, each with the row-index filter of its manifest DV, if it has one. */
  protected def liveLeavesWithManifestFilters
      : Seq[(DataManifestEntry, Option[RowIndexFilterProvider])] =
    liveLeaves.map { leaf =>
      leaf -> leaf.manifestDV.map { case (dvBytes, _) =>
        AMTUtils.deserializeMdv(dvBytes).toRowIndexFilterProvider(tableRoot)
      }
    }

  /**
   * Reads all live DATA entries of the given leaves, computes their row-ID prefix over each
   * unfiltered leaf, then applies status filtering and each leaf's row-index filter, if any. A
   * leaf without a filter is read unfiltered. The returned entries retain the computed prefix.
   */
  protected def loadLeafDataEntriesWithRowIdPrefix(
      deltaLog: DeltaLog,
      leavesWithRowIndexFilters: Seq[(DataManifestEntry, Option[RowIndexFilterProvider])])
      : Dataset[AMTDataEntryExtended] = {
    val index = leafFileIndex(leavesWithRowIndexFilters)
    val withPrefix = loadEntriesWithExtendedMetadata(
        deltaLog, index, checkpointAction.metaData, checkpointAction.protocol,
        includeRowIndexFilterMarker = true)
      .where(col("entry.content_type") === lit(AMTSingleAction.ContentType.Type.Data))
      .withColumn(PRECEDING_RECORDS_COLUMN, precedingNullRowIdRecords(fieldPrefix = "entry."))
      .where(col("entry.tracking.status").isin(Tracking.Status.liveEntryStatuses.toSeq: _*))
    asExtendedEntries(filterOutRowIndexFilterEntries(withPrefix))
  }

  /**
   * Reads visible DATA entries when no row-ID inheritance is needed.
   */
  protected def liveDataEntries(
      deltaLog: DeltaLog, index: DeltaLogFileIndex): Dataset[AMTDataEntryExtended] =
    asExtendedEntries(
      loadEntriesWithExtendedMetadata(
          deltaLog, index, checkpointAction.metaData, checkpointAction.protocol)
        .where(col("entry.content_type") === lit(AMTSingleAction.ContentType.Type.Data))
        .where(col("entry.tracking.status").isin(Tracking.Status.liveEntryStatuses.toSeq: _*))
        .withColumn(PRECEDING_RECORDS_COLUMN, lit(0L)))

  /** Builds an index over the given leaves that applies each leaf's row-index filter, if any. */
  protected def leafFileIndex(
      leavesWithRowIndexFilters: Seq[(DataManifestEntry, Option[RowIndexFilterProvider])])
      : DeltaLogFileIndex = {
    val files =
      leavesWithRowIndexFilters.map { case (leaf, _) => leaf.toFileStatus(tableRoot) }.toArray
    val perFileRowIndexFilters: Map[String, RowIndexFilterProvider] =
      leavesWithRowIndexFilters.collect { case (leaf, Some(provider)) =>
        leaf.getAbsolutePath(tableRoot).toString -> provider
      }.toMap
    DeltaLogFileIndex(new AMTParquetFileFormat, files, perFileRowIndexFilters)
  }

  /** Extends the Parquet read schema with the keep/drop marker. */
  protected def rowIndexFilterReadSchema(persistedSchema: StructType): StructType =
    persistedSchema.add(AMTParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD)

  /** Name of the keep/drop marker carried through manifest decoding. */
  protected def rowIndexFilterMarkerColumnName: String =
    AMTParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME

  /** Selects the keep/drop marker from the Parquet scan. */
  protected def rowIndexFilterMarkerColumn: Column =
    col(rowIndexFilterMarkerColumnName)

  /** Keeps the rows the row-index filter does not mark deleted, then removes the marker column. */
  protected def filterOutRowIndexFilterEntries(dataFrame: DataFrame): DataFrame =
    dataFrame
      .where(col(AMTParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME) ===
        lit(RowIndexFilter.KEEP_ROW_VALUE))
      .drop(AMTParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME)

  /**
   * Converts manifest entries into the `AddFile` actions of the reconstructed state. Each leaf
   * entry carries a back reference to its position in its leaf and has its inheritance resolved.
   *
   * @param entries The live DATA entries to convert, read from the root or from leaves, each with
   *                its read location and `first_row_id` prefix sum.
   * @param resolverProvider The [[DataEntryResolverProvider]] for the leaves `entries` were read
   *                         from.
   */
  protected def toAddSingleActions(
      entries: Dataset[AMTDataEntryExtended],
      resolverProvider: DataEntryResolverProvider): Dataset[SingleAction] = {
    import org.apache.spark.sql.delta.implicits._
    entries.mapPartitions { iter =>
      val resolver = resolverProvider.createResolver()
      iter.map { entryWithLoc =>
        val (add, leafLocation) = resolver.resolve(entryWithLoc)
        SingleAction(add = add.copy(
          backReference = leafLocation.map(BackReference(_, entryWithLoc.pos.toInt))))
      }
    }
  }

  private def asExtendedEntries(df: DataFrame): Dataset[AMTDataEntryExtended] = {
    implicit val encoder: Encoder[AMTDataEntryExtended] =
      AMTCheckpointProvider.amtDataEntryExtendedEncoder
    df.as[AMTDataEntryExtended]
  }

  /** Name of the [[AMTDataEntryExtended.precedingNullFirstRowIdRecords]] column. */
  protected val PRECEDING_RECORDS_COLUMN: String = "precedingNullFirstRowIdRecords"

  /**
   * The `first_row_id` prefix sum of a manifest row: the total `record_count` of the entries that
   * precede it in the same manifest, have `ADDED` status, and carry a null `first_row_id`.
   *
   * @param fieldPrefix The prefix of the row's manifest fields: `entry.` for rows decoded by
   *                    [[loadEntriesWithExtendedMetadata]], none for persisted rows.
   */
  protected def precedingNullRowIdRecords(fieldPrefix: String = ""): Column = {
    val precedingRowsOfManifest = Window
      .partitionBy(col("leafPath"))
      .orderBy(col("pos"))
      .rowsBetween(Window.unboundedPreceding, -1)
    val contribution = when(
      col(fieldPrefix + "tracking.status") === lit(Tracking.Status.Added) &&
        col(fieldPrefix + "tracking.first_row_id").isNull,
      col(fieldPrefix + "record_count")).otherwise(lit(0L))
    // The frame is empty for the first row of a manifest, where the sum is null and the offset 0.
    coalesce(sum(contribution).over(precedingRowsOfManifest), lit(0L))
  }

  /**
   * Reconstructs the AddFile and RemoveFile actions of the single commit this manifest tree
   * describes. The AMT does not persist `dataChange` or when a file was removed, so callers supply
   * the commit-level `dataChange` and the commit timestamp, which becomes every reconstructed
   * RemoveFile's `deletionTimestamp`. The actions come in no particular order.
   */
  private[delta] def reconstructFileActions(
      deltaLog: DeltaLog,
      dataChange: Boolean,
      commitTimestamp: Long): Seq[FileAction] =
    mdvDerivedChangeActions(deltaLog, dataChange, commitTimestamp)
      .foldLeft(trackingDerivedChangeActions(deltaLog, dataChange, commitTimestamp))(_ union _)
      .collect().toSeq
      .map(_.unwrap.asInstanceOf[FileAction])

  /**
   * Change files derived from tracking statuses from both root and new leaves, stamped with the
   * commit-level `dataChange` and `commitTimestamp` (see [[reconstructFileActions]]).
   */
  private def trackingDerivedChangeActions(
      deltaLog: DeltaLog,
      dataChange: Boolean,
      commitTimestamp: Long): Dataset[SingleAction] = {
    import org.apache.spark.sql.delta.implicits._
    // Only Added leaves are considered here.
    // Existing leaves with MDV changes are handled by mdvDerivedChangeActions.
    // Existing leaves removed entirely will be a subset of mdvDerivedChangeActions.
    val addedLeaves = leaves.filter { leaf =>
      leaf.tracking.status == Tracking.Status.Added && !holdsOnlyExistingEntries(leaf)
    }
    val manifestIndex = DeltaLogFileIndex(
      DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET,
      (rootFile +: addedLeaves.map(_.toFileStatus(tableRoot))).toArray)

    val resolverProvider = dataEntryResolverProvider(addedLeaves)
    // Only leaf entries inherit a row ID, so skip the prefix's shuffle when there is no leaf.
    val precedingRecords =
      if (addedLeaves.isEmpty) lit(0L) else precedingNullRowIdRecords(fieldPrefix = "entry.")

    asExtendedEntries(
      loadEntriesWithExtendedMetadata(
          deltaLog, manifestIndex, checkpointAction.metaData, checkpointAction.protocol)
        .where(col("entry.content_type") === lit(AMTSingleAction.ContentType.Type.Data))
        // The row-ID prefix must see every entry of a leaf, so compute it before status filtering.
        .withColumn(PRECEDING_RECORDS_COLUMN, precedingRecords)
        .where(col("entry.tracking.status").isin(Tracking.Status.changeEntryStatuses.toSeq: _*)))
      .mapPartitions { entries =>
        val resolver = resolverProvider.createResolver()
        entries.map { entryWithLoc =>
          val (add, _) = resolver.resolve(entryWithLoc)
          val status = entryWithLoc.entry.unwrap.asInstanceOf[DataEntry].tracking.status
          if (Tracking.Status.liveEntryStatuses.contains(status)) {
            SingleAction(add = add.copy(dataChange = dataChange))
          } else {
            SingleAction(remove = add.removeWithTimestamp(commitTimestamp, dataChange))
          }
        }
      }
  }

  /**
   * The change files coming from MDV-marked positions in the leaves, stamped with the commit-level
   * `dataChange` and `commitTimestamp` (see [[reconstructFileActions]]).
   */
  private def mdvDerivedChangeActions(
      deltaLog: DeltaLog,
      dataChange: Boolean,
      commitTimestamp: Long): Option[Dataset[SingleAction]] = {
    import org.apache.spark.sql.delta.implicits._
    // These are the leaves that contain MDV-masked entries.
    // By definition these leaves cannot have ADDED statuses.
    val touchedLeaves = leaves.filter(_.changedPositionsBitmaps.nonEmpty)
    assert(touchedLeaves.forall(_.tracking.status != Tracking.Status.Added))
    if (touchedLeaves.isEmpty) return None

    val leavesWithFilters = touchedLeaves.map { leaf =>
      leaf -> Some(ManifestBitmap.toRowIndexFilterProvider(
        leaf.changedPositionsBitmaps, tableRoot, inverse = true))
    }
    val resolverProvider = dataEntryResolverProvider(touchedLeaves)
    Some(loadLeafDataEntriesWithRowIdPrefix(deltaLog, leavesWithFilters)
      .mapPartitions { entries =>
        val resolver = resolverProvider.createResolver()
        entries.map { entryWithLoc =>
          val (add, _) = resolver.resolve(entryWithLoc)
          SingleAction(remove = add.removeWithTimestamp(commitTimestamp, dataChange))
        }
      })
  }

  /**
   * Whether every entry of `leaf` is EXISTING, per its `manifest_info` counts. Such a leaf holds
   * only spilled files and carries no data change.
   */
  private def holdsOnlyExistingEntries(leaf: DataManifestEntry): Boolean = {
    val info = leaf.manifest_info
    AMTUtils.invariantCheckWithLogging(
      checkInvariant = info.totalFilesCount == leaf.record_count,
      opTypeSuffix = AMTUsageLogs.ALERT_INCONSISTENT_MANIFEST_INFO_FILE_COUNTS,
      message = s"Leaf ${leaf.location} has record_count ${leaf.record_count}, but its " +
        s"manifest_info file counts sum to ${info.totalFilesCount}.",
      data = Map(
        "leafLocation" -> leaf.location,
        "recordCount" -> leaf.record_count,
        "addedFilesCount" -> info.added_files_count,
        "existingFilesCount" -> info.existing_files_count,
        "deletedFilesCount" -> info.deleted_files_count,
        "replacedFilesCount" -> info.replaced_files_count,
        "modifiedFilesCount" -> info.modified_files_count))
    info.existing_files_count == leaf.record_count
  }

  /**
   * Test-only invariant: verify the AMT back references carried by the current proposed commit's
   * file actions.
   *
   * On an AMT-backed table a leaf-resident file's AddFile / RemoveFile carries a [[BackReference]]
   * to the (leaf manifest, row position) its entry occupies in the tree, so a later commit can mask
   * or supersede that leaf slot; a root-resident file carries none. `committedActions` are the
   * current proposed commit's actions; they are checked against the live set of the AMT checkpoint
   * this (pre-commit) snapshot is backed by, keyed by (path, dv id):
   *   - a file whose (path, dv) is live in the AMT checkpoint leaf must carry the back reference;
   *   - a file whose (path, dv) is absent from the AMT checkpoint leaf must carry none -- a net-new
   *     file, or the re-added copy of a same-path replace (re-added under a new dv).
   * A (path, dv) that an intermediate commit (landed after the AMT checkpoint but before this one)
   * already superseded is relaxed: this commit's later add/remove of it may omit backreference.
   *
   * Example: the AMT checkpoint is at version 10 and commits 11/12/13 sit on top of it while this
   * commit is 14. File f1 lives at leaf-1 / pos-1 in the checkpoint. If commit 12 (say an
   * ANALYZE TABLE COMPUTE STATS) already re-committed f1 -- carrying its back reference at that
   * point -- then f1's add/remove in commit 14 need not carry a back reference.
   */
  private[delta] def verifyCommitBackReferences(
      spark: SparkSession,
      deltaLog: DeltaLog,
      catalogTableOpt: Option[CatalogTable],
      committedActions: Seq[Action]): Unit = {
    // Key by (path, dv) so a same-path replace is handled: the removed (path, oldDv) is checked
    // against the AMT, while the re-added (path, newDv) is a distinct key absent from the tree.
    val committedFiles = committedActions.collect {
      case a: AddFile =>
        a.toUniqueFileActionTuple(tableRoot, useObjectIdentity = true) -> a.backReference
      case r: RemoveFile =>
        r.toUniqueFileActionTuple(tableRoot, useObjectIdentity = true) -> r.backReference
    }
    if (committedFiles.isEmpty) return

    val expectedKeyToBackreferenceMap =
        liveAddSingleActions(spark, deltaLog).collect()
        .map { singleAction =>
          singleAction.add.toUniqueFileActionTuple(tableRoot, useObjectIdentity = true) ->
            singleAction.add.backReference
        }
        .toMap

    // Keys an intermediate commit (after this AMT) already re-committed. The first superseding
    // add/remove must carry a back reference; a 2nd superseding one of the same key need not.
    val intermediateCommittedKeys =
      deltaLog.getChanges(checkpointVersion + 1, catalogTableOpt).flatMap(_._2).collect {
        case a: AddFile => a.toUniqueFileActionTuple(tableRoot, useObjectIdentity = true)
        case r: RemoveFile => r.toUniqueFileActionTuple(tableRoot, useObjectIdentity = true)
      }.toSet

    committedFiles.foreach { case (key, actual) =>
      expectedKeyToBackreferenceMap.get(key) match {
        case Some(expected)
            if actual != expected && !(intermediateCommittedKeys.contains(key) && actual.isEmpty) =>
          throw new IllegalStateException(
            s"AMT back reference for file '${key.fileURI}' does not match the AMT. " +
            s"Expected $expected but the committed action carried $actual.")
        case None if actual.isDefined =>
          throw new IllegalStateException(
            s"File '${key.fileURI}' carries a back reference $actual but is not present in " +
            "the AMT tree, so it must not carry one.")
        case _ => // Matching, omitted after a window supersession, or absent+empty: as expected.
      }
    }
  }

  private def getBackreferencesForFilePaths(
      spark: SparkSession,
      deltaLog: DeltaLog,
      filePaths: Set[String]): Map[UniqueFileActionTuple, Option[BackReference]] =
    liveAddSingleActions(spark, deltaLog)
      .where(col("add.path").isin(filePaths.toSeq: _*))
      .collect()
      .map { singleAction =>
        singleAction.add.toUniqueFileActionTuple(tableRoot, useObjectIdentity = true) ->
          singleAction.add.backReference
      }
      .toMap

  /**
   * Re-derives file actions' back references to match this (the latest) tree.
   * The actions which have backreferences corresponding to leaves which
   * hasn't changed (Same old leaf is still present with the new tree and MDV
   * also hasn't changed) doesn't need to be re-calculated as the old
   * backreferences are still valid.
   */
  private[delta] def reStampBackReferences(
      spark: SparkSession,
      deltaLog: DeltaLog,
      actions: Seq[Action]): AMTCheckpointProvider.ReStampedBackReferences = {
    def existingBackReference(action: Action): Option[BackReference] = action match {
      case a: AddFile => a.backReference
      case r: RemoveFile => r.backReference
      case _ => None
    }
    def isFileAction(action: Action): Boolean = action match {
      case _: AddFile | _: RemoveFile => true
      case _ => false
    }
    // An `isMasked(pos)` test over each referenced leaf's manifest DV (a leaf with no DV masks
    // nothing), keyed by the leaf `location` a back reference's `manifest` holds. Only the leaves
    // some back reference points at are deserialized.
    val referencedManifests =
      actions.iterator.flatMap(existingBackReference).map(_.manifest).toSet
    val isMaskedByLocation: Map[String, Int => Boolean] = liveLeaves.iterator
      .filter(leaf => referencedManifests.contains(leaf.location))
      .map { leaf =>
        val isMasked: Int => Boolean = leaf.manifestDV match {
          case Some((dvBytes, _)) =>
            val mdv = AMTUtils.deserializeMdv(dvBytes)
            pos => mdv.contains(pos)
          case None => _ => false
        }
        leaf.location -> isMasked
      }.toMap
    // A back reference is still valid iff its leaf is present in this tree and its position there
    // is not MDV-masked. If an action has no back reference against the older tree, we recalculate
    // it too, as the entry might have spilled from the root to a new leaf in the latest tree.
    def stillValid(backReference: Option[BackReference]): Boolean =
      backReference.exists { case BackReference(manifest, pos) =>
        isMaskedByLocation.get(manifest).exists(isMasked => !isMasked(pos))
      }
    def needsReStamp(action: Action): Boolean = action match {
      case a: AddFile => !stillValid(a.backReference)
      case r: RemoveFile => !stillValid(r.backReference)
      case _ => false
    }
    if (!actions.exists(needsReStamp)) {
      return AMTCheckpointProvider.ReStampedBackReferences(
        actions,
        numActionsReusingBackref = actions.count(isFileAction),
        numActionsRegeneratingBackref = 0)
    }
    val pathsToReStamp: Set[String] = actions.iterator.collect {
      case a: AddFile if needsReStamp(a) => a.path
      case r: RemoveFile if needsReStamp(r) => r.path
    }.toSet
    val keyToBackRef = getBackreferencesForFilePaths(spark, deltaLog, pathsToReStamp)
    def backRefFor(action: FileAction): Option[BackReference] =
      keyToBackRef.getOrElse(
        action.toUniqueFileActionTuple(tableRoot, useObjectIdentity = true), None)
    val restamped = actions.map {
      case a: AddFile if needsReStamp(a) => a.copy(backReference = backRefFor(a))
      case r: RemoveFile if needsReStamp(r) => r.copy(backReference = backRefFor(r))
      case other => other
    }
    AMTCheckpointProvider.ReStampedBackReferences(
      restamped,
      numActionsReusingBackref = actions.count(a => isFileAction(a) && !needsReStamp(a)),
      numActionsRegeneratingBackref = actions.count(needsReStamp))
  }

  /**
   * Like [[AMTCheckpointProvider.loadEntries]], but also captures each row's physical location.
   * The optional row-index-filter marker is carried through manifest decoding so callers can apply
   * it after computing any row-ID prefix.
   */
  private def loadEntriesWithExtendedMetadata(
      deltaLog: DeltaLog,
      index: DeltaLogFileIndex,
      metadata: Metadata,
      protocol: Protocol,
      includeRowIndexFilterMarker: Boolean = false): DataFrame = {
    import org.apache.spark.sql.delta.implicits._
    val persistedSchema = AMTSingleAction.persistedSchema(metadata, protocol)
    val readSchema =
      if (includeRowIndexFilterMarker) rowIndexFilterReadSchema(persistedSchema)
      else persistedSchema
    val markerColumns =
      if (includeRowIndexFilterMarker) Seq(rowIndexFilterMarkerColumn)
      else Seq.empty
    val persisted = deltaLog.loadIndex(index, readSchema)
      .select(
        (persistedSchema.fieldNames.toIndexedSeq.map(col) :+
          col(s"$METADATA_NAME.$FILE_PATH").as("leafPath") :+
          col(s"$METADATA_NAME.${ParquetFileFormat.ROW_INDEX}").as("pos")) ++ markerColumns: _*)
    val decodedMarkerColumns =
      if (includeRowIndexFilterMarker) Seq(col(rowIndexFilterMarkerColumnName))
      else Seq.empty
    val withPartition = AMTPartitionValues.forRead(persisted, metadata.partitionSchema)
    AMTContentStats.forRead(withPartition, metadata, protocol)
      .select(
        (Seq(
          struct(amtSingleActionEncoder.schema.fieldNames.toIndexedSeq.map(col): _*).as("entry"),
          col("leafPath"),
          col("pos")) ++ decodedMarkerColumns): _*)
  }

  /**
   * Builds the [[DataEntryResolverProvider]] for DATA entries read from the root or from
   * `parentLeaves`.
   *
   * @param parentLeaves The leaf pointers the scanned leaf entries were read from.
   */
  protected def dataEntryResolverProvider(
      parentLeaves: Seq[DataManifestEntry]): DataEntryResolverProvider = {
    val tableRootString = tableRoot.toString
    DataEntryResolverProvider(
      tableRoot = tableRoot,
      urlEncodedRootManifestPath = SparkPath.fromPath(rootManifestAbsolutePath).urlEncoded,
      parentTrackingByLeafLocation = parentLeaves.map { leaf =>
        AMTUtils.relativizeLocation(tableRootString, leaf.getAbsolutePath(tableRoot).toString) ->
          InheritableTracking(leaf)
      }.toMap)
  }
}

object AMTCheckpointProvider extends DeltaLogging {

  /**
   * @param actions                       the actions with re-derived back references where needed.
   * @param numActionsReusingBackref      file actions whose existing back reference stayed valid.
   * @param numActionsRegeneratingBackref file actions whose back reference was re-derived.
   */
  case class ReStampedBackReferences(
      actions: Seq[Action],
      numActionsReusingBackref: Int,
      numActionsRegeneratingBackref: Int)

  /**
   * The identity a file action reconstructed by [[AMTCheckpointProvider.reconstructFileActions]]
   * is matched to a committed one by.
   *
   * @param file  The file's (path, deletion vector) tuple, using deletion vector object identity.
   * @param isAdd Whether the action is an [[AddFile]] rather than a [[RemoveFile]].
   */
  private[delta] case class FileActionKey(file: UniqueFileActionTuple, isAdd: Boolean)

  private[delta] object FileActionKey {
    def apply(action: FileAction, tableRoot: Path): FileActionKey =
      FileActionKey(
        action.toUniqueFileActionTuple(tableRoot, useObjectIdentity = true),
        isAdd = action.isInstanceOf[AddFile])
  }

  /**
   * An [[AMTSingleAction]] entry paired with its physical read location in its manifest parquet.
   *
   * @param entry    The manifest content entry.
   * @param leafPath The URL-encoded absolute path of the manifest parquet the entry was read from
   *                 (Spark's `_metadata.file_path`).
   * @param pos      The 0-based position of the entry inside the manifest (Spark's
   *                 `_metadata.row_index`).
   * @param precedingNullFirstRowIdRecords The entry's `first_row_id` prefix sum.
   */
  case class AMTDataEntryExtended(
      entry: AMTSingleAction,
      leafPath: String,
      pos: Long,
      precedingNullFirstRowIdRecords: Long)

  private[amt] lazy val amtDataEntryExtendedEncoder: Encoder[AMTDataEntryExtended] =
    new DeltaEncoder[AMTDataEntryExtended].get

  /**
   * Everything needed to convert [[AMTDataEntryExtended]] DataEntries read from the root or from
   * leaves into [[AddFile]]s with their tracking resolved.
   *
   * @param tableRoot                    The table's data path.
   * @param urlEncodedRootManifestPath   The URL-encoded absolute path of the root manifest, which
   *                                     tells root entries apart from leaf entries.
   * @param parentTrackingByLeafLocation The inheritable tracking of each leaf the entries were read
   *                                     from, keyed by the leaf's `location` relativized against
   *                                     `tableRoot`.
   */
  private[amt] case class DataEntryResolverProvider(
      tableRoot: Path,
      urlEncodedRootManifestPath: String,
      parentTrackingByLeafLocation: Map[String, InheritableTracking]) {

    /** Creates the [[DataEntryResolver]] for one partition. Call it on the executor. */
    def createResolver(): DataEntryResolver = new DataEntryResolver(this)
  }

  /**
   * Resolves the DataEntries of one partition (see [[DataEntryResolverProvider]]). All entries of
   * a leaf share its path, so each distinct leaf path is relativized once per partition rather than
   * once per entry.
   */
  private[amt] final class DataEntryResolver(provider: DataEntryResolverProvider) {
    private val tableRootString = provider.tableRoot.toString

    /** Cache for each URL-encoded leaf path to (its raw relative path, its tracking). */
    private val leafByUrlEncodedPath = mutable.HashMap.empty[String, (String, InheritableTracking)]

    /** The uncoded relative path and parent tracking of the leaf at `urlEncodedPath`. */
    private def getOrCacheLeaf(urlEncodedPath: String): (String, InheritableTracking) =
      leafByUrlEncodedPath.getOrElseUpdate(urlEncodedPath, {
        val leafUncodedRelPath = AMTUtils.relativizeLocation(
          tableRootString, SparkPath.fromUrlString(urlEncodedPath).toPath.toString)
        (leafUncodedRelPath, provider.parentTrackingByLeafLocation(leafUncodedRelPath))
      })

    /**
     * Converts `entryWithLoc` into an [[AddFile]] with its tracking resolved. It also returns the
     * uncoded relative path of the leaf the entry was read from, or None for a root entry.
     */
    def resolve(entryWithLoc: AMTDataEntryExtended): (AddFile, Option[String]) =
      entryWithLoc.entry.unwrap match {
        case data: DataEntry =>
          if (entryWithLoc.leafPath == provider.urlEncodedRootManifestPath) {
            // No inheritence for root entries.
            (data.toAddFile(provider.tableRoot), None)
          } else {
            // Resolve inheritence for leaf entries.
            val (leafUncodedRelPath, parentTracking) = getOrCacheLeaf(
              entryWithLoc.leafPath
            )
            val resolvedTracking = Tracking.resolve(
              childTracking = data.tracking,
              parentTracking = parentTracking,
              prefixSumRecordCountForNullFirstRowId = entryWithLoc.precedingNullFirstRowIdRecords,
              childEntryLocationForLogging = data.location)
            (
              data.copy(tracking = resolvedTracking).toAddFile(provider.tableRoot),
              Some(leafUncodedRelPath)
            )
          }
        case other => throw new IllegalStateException(
          s"Expected a DATA entry after filtering, got ${other.getClass.getSimpleName}.")
      }
  }

  /**
   * A provider over the given tree: a [[ColumnarAMTCheckpointProvider]] when
   * [[DeltaSQLConf.AMT_COLUMNAR_STATE_RECONSTRUCTION_ENABLED]] is set in the given session, a
   * [[PerRowAMTCheckpointProvider]] otherwise.
   */
  def apply(
      spark: SparkSession,
      manifestCommitVersion: Long,
      checkpointAction: Checkpoint,
      leaves: Seq[DataManifestEntry],
      tableRoot: Path): AMTCheckpointProvider =
    apply(
      spark,
      manifestCommitVersion,
      checkpointAction,
      new AMTRootInfo(leaves, cachedRows = None),
      tableRoot)

  def apply(
      spark: SparkSession,
      manifestCommitVersion: Long,
      checkpointAction: Checkpoint,
      rootInfo: AMTRootInfo,
      tableRoot: Path): AMTCheckpointProvider = {
    val columnar = spark.sessionState.conf
      .getConf(DeltaSQLConf.AMT_COLUMNAR_STATE_RECONSTRUCTION_ENABLED)
    if (columnar) {
      new ColumnarAMTCheckpointProvider(
        manifestCommitVersion, checkpointAction, tableRoot, rootInfo)
    } else {
      new PerRowAMTCheckpointProvider(
        manifestCommitVersion, checkpointAction, rootInfo.leaves, tableRoot)
    }
  }

  /**
   * Builds a provider from an emitted [[Checkpoint]] action by reading the leaf pointers out of the
   * root manifest parquet.
   *
   * @param deltaLog   The table's DeltaLog, used to read the root via `loadIndex` (which bypasses
   *                   the path-based Delta format check the root file under the table root would
   *                   otherwise trip).
   * @param checkpoint The inline-emitted checkpoint action carrying the `contentRoot`.
   */
  def fromCheckpoint(
      spark: SparkSession,
      deltaLog: DeltaLog,
      checkpoint: Checkpoint,
      manifestCommitVersion: Long): AMTCheckpointProvider = {
    val rootInfo = loadRootFromCheckpoint(spark, deltaLog, checkpoint, manifestCommitVersion)
    AMTCheckpointProvider(
      spark = spark,
      manifestCommitVersion = manifestCommitVersion,
      checkpointAction = checkpoint,
      rootInfo = rootInfo,
      tableRoot = deltaLog.dataPath)
  }

  private[delta] def loadRootFromCheckpoint(
      spark: SparkSession,
      deltaLog: DeltaLog,
      checkpoint: Checkpoint,
      manifestCommitVersion: Long): AMTRootInfo = {
    val startNanos = System.nanoTime()
    val tableRoot = deltaLog.dataPath
    val rootFile = checkpoint.contentRoot.toFileStatus(tableRoot)
    val index =
      DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET, Array(rootFile))
    val root = {
      val persistedSchema =
        AMTSingleAction.persistedSchema(checkpoint.metaData, checkpoint.protocol)
      // Preserve null entry kinds so discovery can reject them.
      val readSchema = StructType(persistedSchema.map { field =>
        if (field.name == "content_type") field.copy(nullable = true) else field
      })
      val persisted = deltaLog.loadIndex(index, readSchema)
      val maxNumEntries = spark.sessionState.conf
        .getConf(DeltaSQLConf.AMT_ROOT_CACHE_MAX_NUM_ENTRIES)
      // Decide before collecting rows so large roots only return leaf pointers to the driver.
      val cacheRoot = maxNumEntries > 0 &&
        Snapshot.getParquetFileSchemaAndRowCount(spark, deltaLog, rootFile)._2 < maxNumEntries
      val cachedRows = if (cacheRoot) {
        Some((persisted.schema, persisted.queryExecution.executedPlan.executeCollect().toSeq))
      } else {
        None
      }
      val rootEntries = cachedRows.map { case (schema, rows) =>
        DataFrameUtils.ofRows(spark, LocalRelation(toAttributes(schema), rows))
      }.getOrElse(persisted)
      // Decode pointers and reject unsupported entry kinds without decoding DATA rows.
      val pointers = rootEntries.where(
        col("content_type").isNull ||
          col("content_type") =!= lit(AMTSingleAction.ContentType.Type.Data))
      val decodedPointers = decodeEntries(pointers, checkpoint.metaData, checkpoint.protocol)
      val pointersToCollect = if (cacheRoot) {
        // Evaluate these projections locally even when the general optimizer disables this rule.
        DataFrameUtils
          .ofRows(spark, ConvertToLocalRelation(decodedPointers.queryExecution.analyzed))
          .as[AMTSingleAction](decodedPointers.encoder)
      } else {
        decodedPointers
      }
      val leaves = pointersToCollect
        .collect().toSeq.map(_.unwrap.asInstanceOf[DataManifestEntry])
      new AMTRootInfo(leaves, cachedRows)
    }
    recordDeltaEvent(
      deltaLog,
      opType = AMTUsageLogs.CHECKPOINT_PROVIDER_INITIALIZE_FROM_CHECKPOINT_ACTION,
      data = Map(
        "durationMs" -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos),
        "manifestCommitVersion" -> manifestCommitVersion,
        "contentRootVersion" -> checkpoint.contentRoot.version,
        "checkpointVersion" -> checkpoint.version,
        "numLeaves" -> root.leaves.size.toLong,
        "contentRootSizeInBytes" -> checkpoint.contentRoot.sizeInBytes)
    )
    root
  }

  /**
   * Builds a provider from an [[AMTWriteResult]] without reading the root manifest from storage.
   */
  def fromWriteResult(
      spark: SparkSession,
      deltaLog: DeltaLog,
      writeResult: AMTWriteResult,
      manifestCommitVersion: Long): AMTCheckpointProvider = {
    AMTCheckpointProvider(
      spark = spark,
      manifestCommitVersion = manifestCommitVersion,
      checkpointAction = writeResult.checkpoint,
      leaves = writeResult.leaves,
      tableRoot = deltaLog.dataPath)
  }

  /** Reads the AMT root and returns the live [[DataEntry]]s tracked by root. */
  private[amt] def readLiveRootDataEntries(
      deltaLog: DeltaLog,
      checkpoint: Checkpoint): Seq[AddFile] = {
    val tableRoot = deltaLog.dataPath
    val rootFile = checkpoint.contentRoot.toFileStatus(tableRoot)
    val index =
      DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET, Array(rootFile))
    loadEntries(deltaLog, index, checkpoint.metaData, checkpoint.protocol).collect()
      .toSeq
      .filter(_.content_type == AMTSingleAction.ContentType.Type.Data)
      .map(_.unwrap.asInstanceOf[DataEntry])
      .filter(e => Tracking.Status.liveEntryStatuses.contains(e.tracking.status))
      .map(_.toAddFile(tableRoot))
  }

  /**
   * Reads AMT manifest parquet files (root or leaves) into a [[Dataset]] of
   * [[AMTSingleAction]].
   */
  private def loadEntries(
      deltaLog: DeltaLog,
      index: DeltaLogFileIndex,
      metadata: Metadata,
      protocol: Protocol): Dataset[AMTSingleAction] = {
    val persistedSchema =
      AMTSingleAction.persistedSchema(metadata, protocol)
    decodeEntries(deltaLog.loadIndex(index, persistedSchema), metadata, protocol)
  }

  /** Decodes persisted rows after callers have selected the entries they need. */
  private def decodeEntries(
      persisted: DataFrame,
      metadata: Metadata,
      protocol: Protocol): Dataset[AMTSingleAction] = {
    import org.apache.spark.sql.delta.implicits._
    val withPartition = AMTPartitionValues.forRead(persisted, metadata.partitionSchema)
    AMTContentStats.forRead(withPartition, metadata, protocol)
      .as[AMTSingleAction]
  }

}
