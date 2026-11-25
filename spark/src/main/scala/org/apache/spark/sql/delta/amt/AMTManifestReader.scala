/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import org.apache.spark.sql.delta.{RowIndexFilter, RowIndexFilterProvider}
import org.apache.spark.sql.delta.{DeltaLog, DeltaLogFileIndex}
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.actions.{Action, Metadata, Protocol, SingleAction}
import org.apache.spark.sql.delta.deletionvectors.ManifestBitmap
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{KnownNotNull, KnownNullable, Literal}
import org.apache.spark.sql.execution.datasources.FileFormat.{FILE_PATH, METADATA_NAME}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, concat, element_at, lit, raise_error, struct, sum, udf, when}
import org.apache.spark.sql.types.{IntegerType, LongType, MapType, StringType, StructField, StructType}

/**
 * Reads the AMT manifests, roots and leaves, of the trees of a table that share a metadata and a
 * protocol, which shape the manifest schema. An [[AMTCheckpointProvider]] reads the changes of its
 * own tree through one, and its state reconstruction uses the manifest reads of one. Each reader
 * states whether it is compatible with multiple trees, that is, whether one call can read the
 * manifests of several trees.
 *
 * @param tableRoot The table's data path.
 * @param metadata  The metadata of the trees whose manifests are read.
 * @param protocol  The protocol of the trees whose manifests are read.
 */
final class AMTManifestReader(
    val tableRoot: Path,
    val metadata: Metadata,
    val protocol: Protocol)
  extends AMTManifestReaderImpl
{
}

trait AMTManifestReaderImpl {
  self: AMTManifestReader =>

  import AMTManifestReader._

  /**
   * The persisted rows of the DATA entries of `roots`, of every status, with their entry context
   * (see [[withRootEntryContext]]). Compatible with multiple trees, but a row carries only its
   * manifest path and position, not the tree that lists it.
   */
  private[amt] def rootEntries(deltaLog: DeltaLog, roots: Seq[FileStatus]): DataFrame = {
    val index = DeltaLogFileIndex(new AMTParquetFileFormat, roots.toArray)
    withRootEntryContext(dataEntries(loadPersistedEntries(deltaLog, index)))
  }

  /**
   * The persisted rows of the DATA entries of the given leaves, of every status, with their
   * `first_row_id` prefix sum over each unfiltered leaf and their entry context (see
   * [[withLeafEntryContext]]). Each leaf's row-index filter, if any, applies after the prefix sum;
   * callers filter by status. Compatible with multiple trees, as long as no leaf is listed twice:
   * the row-index filters are keyed by leaf path, and the prefix is computed per leaf path.
   */
  private[amt] def leafEntriesWithRowIdPrefix(
      spark: SparkSession,
      deltaLog: DeltaLog,
      leavesWithRowIndexFilters: Seq[(DataManifestEntry, Option[RowIndexFilterProvider])])
      : DataFrame =
    leafEntriesWithRowIdPrefix(
      deltaLog, leavesWithRowIndexFilters, leafFileIndex(spark, leavesWithRowIndexFilters))

  private def leafEntriesWithRowIdPrefix(
      deltaLog: DeltaLog,
      leavesWithRowIndexFilters: Seq[(DataManifestEntry, Option[RowIndexFilterProvider])],
      index: DeltaLogFileIndex): DataFrame = {
    val withPrefix =
      dataEntries(loadPersistedEntries(deltaLog, index, includeRowIndexFilterMarker = true))
        .withColumn(PRECEDING_RECORDS_COLUMN, precedingNullRowIdRecords())
    withLeafEntryContext(
      filterOutRowIndexFilterEntries(withPrefix),
      leavesWithRowIndexFilters.map { case (leaf, _) => leaf })
  }

  /**
   * The persisted rows of the manifests in `index`, read with the checkpoint's persisted schema,
   * with their manifest (`leafPath`) and their position in it (`pos`). The optional
   * row-index-filter marker is carried along so callers can apply it after computing any row-ID
   * prefix. Compatible with multiple trees, but a row carries only its manifest path and
   * position, not the tree that lists it.
   */
  private[amt] def loadPersistedEntries(
      deltaLog: DeltaLog,
      index: DeltaLogFileIndex,
      includeRowIndexFilterMarker: Boolean = false): DataFrame = {
    val persistedSchema = AMTSingleAction.persistedSchema(metadata, protocol)
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

  /** The DATA entries of persisted manifest rows. */
  private[amt] def dataEntries(entries: DataFrame): DataFrame =
    entries.where(col("content_type") === lit(AMTSingleAction.ContentType.Type.Data))

  /** The entries of persisted manifest rows whose status is one of `statuses`. */
  private[amt] def withStatuses(entries: DataFrame, statuses: Set[Int]): DataFrame =
    entries.where(col("tracking.status").isin(statuses.toSeq: _*))

  /** Builds an index over the given leaves that applies each leaf's row-index filter, if any. */
  private[amt] def leafFileIndex(
      spark: SparkSession,
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
  private[amt] def rowIndexFilterReadSchema(persistedSchema: StructType): StructType =
    persistedSchema.add(AMTParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD)

  /** Name of the keep/drop marker carried through manifest decoding. */
  private[amt] def rowIndexFilterMarkerColumnName: String =
    AMTParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME

  /** Selects the keep/drop marker from the Parquet scan. */
  private[amt] def rowIndexFilterMarkerColumn: Column =
    col(rowIndexFilterMarkerColumnName)

  /** Keeps the rows the row-index filter does not mark deleted, then removes the marker column. */
  private[amt] def filterOutRowIndexFilterEntries(dataFrame: DataFrame): DataFrame =
    dataFrame
      .where(col(AMTParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME) ===
        lit(RowIndexFilter.KEEP_ROW_VALUE))
      .drop(AMTParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME)

  /**
   * The `first_row_id` prefix sum of a manifest row: the total `record_count` of the entries that
   * precede it in the same manifest, have `ADDED` status, and carry a null `first_row_id`.
   */
  private[amt] def precedingNullRowIdRecords(): Column = {
    val precedingRowsOfManifest = Window
      .partitionBy(col("leafPath"))
      .orderBy(col("pos"))
      .rowsBetween(Window.unboundedPreceding, -1)
    val contribution = when(
      col("tracking.status") === lit(Tracking.Status.Added) &&
        col("tracking.first_row_id").isNull,
      col("record_count")).otherwise(lit(0L))
    // The frame is empty for the first row of a manifest, where the sum is null and the offset 0.
    coalesce(sum(contribution).over(precedingRowsOfManifest), lit(0L))
  }

  /**
   * Adds the entry context of root entry rows, which the AddFile columns read instead of the
   * manifest a row was read from: root entries have no parent, so they keep their own tracking and
   * carry no back reference.
   */
  private[amt] def withRootEntryContext(entries: DataFrame): DataFrame =
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
  private[amt] def withLeafEntryContext(
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
    // The leaf scan reads exactly `leaves`, so a miss is a bug. Fail the read rather than leave
    // the inherited fields null.
    val context = coalesce(
      element_at(
        Column(Literal.create(contextByManifestPath, MapType(StringType, contextType))),
        col("leafPath")),
      AMTManifestReader.malformedEntry(
        lit("no leaf manifest matches "), col("leafPath"), lit(".")))
    entries.select(col("*") +: lit(false).as(IS_ROOT_ENTRY_COLUMN) +:
      contextType.fieldNames.toIndexedSeq.map(name => context.getField(name).as(name)): _*)
  }

  /**
   * `entries` with the AddFile of each in an `add` column (see [[addFileStruct]]), failing the read
   * if an entry is malformed.
   *
   * @param baseRowId The entry's first row ID; leaf entries may inherit it from their parent.
   */
  private[amt] def addFileEntries(entries: DataFrame, baseRowId: Column): DataFrame =
    entries.withColumn("add", addFileStruct(entries.sparkSession, baseRowId))

  /** The `first_row_id` of a leaf entry row, inheriting its parent's plus the row-ID prefix. */
  private[amt] def inheritedBaseRowId: Column =
    inheritedTrackingField(
      "first_row_id", col(PARENT_FIRST_ROW_ID_COLUMN) + col(PRECEDING_RECORDS_COLUMN))

  /**
   * The AddFile of a DATA entry row, failing the read if the row is malformed. The row must
   * carry its entry context (see [[withRootEntryContext]] and [[withLeafEntryContext]]).
   *
   * @param baseRowId The entry's first row ID; leaf entries may inherit it from their parent.
   */
  private def addFileStruct(spark: SparkSession, baseRowId: Column): Column = {
    val checks = entryChecks
    struct(Action.addFileSchema.fields.toIndexedSeq.map { field =>
      // Each field carries its own checks, so the optimizer can drop the fields a query does
      // not read.
      val checked = checks.foldRight(addFileColumn(spark, field, baseRowId)) {
        case ((malformed, error), value) => when(malformed, error).otherwise(value)
      }
      // Each field sets its own nullability and metadata instead of a cast of the whole struct to
      // the AddFile schema. The metadata keeps the Parquet field ids of the manifest column out of
      // the type when the optimizer drops a no-op field cast.
      val normalized =
        if (field.nullable) KnownNullable(checked.expr) else KnownNotNull(checked.expr)
      Column(normalized).as(field.name, field.metadata)
    }: _*)
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
      AMTManifestReader.malformedEntry(
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
    val value = field.name match {
      case "path" => AMTManifestReader.encodePath(col("location"))
      case "partitionValues" =>
        coalesce(
          AMTPartitionValues.toStringMap(metadata.partitionSchema),
          Column(Literal.create(Map.empty[String, String], MapType(StringType, StringType))))
      case "size" => col("file_size_in_bytes")
      case "modificationTime" => lit(0L)
      // Must not be null: V2 data skipping drops rows with a null `dataChange`.
      case "dataChange" => lit(false)
      case "stats" =>
        AMTContentStats.addFileStatsJson(spark, metadata, protocol)
      case "tags" => col("tags")
      case "deletionVector" => deletionVectorColumn(col("deletion_vector"))
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

  /** An AMT deletion vector struct -> Delta's descriptor, as [[DeletionVector.toDescriptor]]. */
  protected def deletionVectorColumn(deletionVector: Column): Column =
    AMTManifestReader.toDescriptor(tableRoot)(deletionVector)

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
    AMTManifestReader.malformedEntry(
      lit("tracking." + name + " is null but the entry's status is "), statusName,
      lit(". Only ADDED entries inherit " + name + " from the root DATA_MANIFEST entry; " +
        "every other status must materialize it."))

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
      AMTManifestReader.malformedEntry(
        lit("amtPassthrough only supports parquet. got "), fileFormat, lit(".")))
      .when(fields.map(name => col(name).isNotNull).reduce(_ || _),
        struct(fields.map(name => col(name).as(name)): _*))
  }

  ////////////////////////////////
  // Change file reconstruction //
  ////////////////////////////////

  /**
   * The changed files that `root`, `addedLeaves` and the changed positions of
   * `changedPositionsLeaves` record, as [[projectChangeFiles]] selects them, in no particular
   * order. `root` and the leaves must come from one tree, which has this reader's metadata and
   * protocol. Not compatible with multiple trees: it reads one root.
   */
  private[amt] def loadChangeFiles(
      spark: SparkSession,
      deltaLog: DeltaLog,
      root: FileStatus,
      addedLeaves: Seq[DataManifestEntry],
      changedPositionsLeaves: Seq[DataManifestEntry]): DataFrame =
    (trackingDerivedChangeFiles(deltaLog, root, addedLeaves) +:
      mdvDerivedChangeFiles(spark, deltaLog, changedPositionsLeaves).toSeq)
      .reduce(_ union _)

  /**
   * Change files derived from the tracking statuses of `root` and of `addedLeaves`, the leaves the
   * commit wrote, as the changed files [[projectChangeFiles]] selects. Not compatible with
   * multiple trees: it reads one root.
   */
  private def trackingDerivedChangeFiles(
      deltaLog: DeltaLog,
      root: FileStatus,
      addedLeaves: Seq[DataManifestEntry]): DataFrame = {
    val changeStatuses = Tracking.Status.changeEntryStatuses
    val removed = !col("tracking.status").isin(Tracking.Status.liveEntryStatuses.toSeq: _*)
    val rootChanges = projectChangeFiles(
      withStatuses(rootEntries(deltaLog, Seq(root)), changeStatuses),
      baseRowId = col("tracking.first_row_id"),
      removed = removed)
    if (addedLeaves.isEmpty) {
      rootChanges
    } else {
      // TODO(v4amt): Reuse the manifest layout for tracking-derived change reconstruction too.
      val leavesWithFilters = addedLeaves.map(_ -> None)
      val leafIndex = DeltaLogFileIndex(
        new AMTParquetFileFormat, addedLeaves.map(_.toFileStatus(tableRoot)).toArray)
      rootChanges.union(projectChangeFiles(
        withStatuses(
          leafEntriesWithRowIdPrefix(deltaLog, leavesWithFilters, leafIndex), changeStatuses),
        baseRowId = inheritedBaseRowId,
        removed = removed))
    }
  }

  /**
   * The files at the MDV-marked positions of `touchedLeaves`, all removed by the commit, as the
   * changed files [[projectChangeFiles]] selects.
   */
  private def mdvDerivedChangeFiles(
      spark: SparkSession,
      deltaLog: DeltaLog,
      touchedLeaves: Seq[DataManifestEntry]): Option[DataFrame] = {
    if (touchedLeaves.isEmpty) return None
    val leavesWithFilters = touchedLeaves.map { leaf =>
      leaf -> Some(ManifestBitmap.toRowIndexFilterProvider(
        leaf.changedPositionsBitmaps, tableRoot, inverse = true))
    }
    // A leaf read at its changed positions yields the superseded entries there, which are still
    // live in that leaf.
    val entries = withStatuses(
      leafEntriesWithRowIdPrefix(spark, deltaLog, leavesWithFilters),
      Tracking.Status.liveEntryStatuses)
    Some(projectChangeFiles(entries, baseRowId = inheritedBaseRowId, removed = lit(true)))
  }

  /**
   * The changed files of `entries`: the manifest path (`leafPath`) and position (`pos`) of each
   * entry, which order them, its AddFile (see [[addFileEntries]]), and whether the commit
   * `removed` the file rather than added it.
   *
   * @param baseRowId The entry's first row ID; leaf entries may inherit it from their parent.
   */
  private def projectChangeFiles(
      entries: DataFrame, baseRowId: Column, removed: Column): DataFrame =
    addFileEntries(entries, baseRowId)
      .select(col("leafPath"), col("pos"), col("add"), removed.as(REMOVED_COLUMN))

  /**
   * The AddFile and RemoveFile actions of `changeFiles`, which [[loadChangeFiles]] reads, stamped
   * with the commit-level `dataChange` and `commitTimestamp` (see
   * [[AMTCheckpointProvider.reconstructFileActions]]). If `ordered`, they are sorted by the
   * manifest path and position of their entries.
   */
  private[amt] def toChangeActions(
      changeFiles: DataFrame,
      dataChange: Boolean,
      commitTimestamp: Long,
      ordered: Boolean): Dataset[SingleAction] = {
    import org.apache.spark.sql.delta.implicits._
    (if (ordered) changeFiles.orderBy(col("leafPath"), col("pos")) else changeFiles)
      .select(Action.logSchema.fields.toIndexedSeq.map { field =>
        (field.name match {
          case "add" => when(!col(REMOVED_COLUMN), stampedAddFileStruct(lit(dataChange)))
          case "remove" =>
            when(col(REMOVED_COLUMN), removeFileStruct(lit(commitTimestamp), lit(dataChange)))
          case _ => lit(null).cast(field.dataType)
        }).as(field.name)
      }: _*)
      .as[SingleAction]
  }

  /**
   * The add column with dataChange and backReference stamped, so it's ready to be converted to a
   * Scala AddFile.
   */
  private def stampedAddFileStruct(dataChange: Column): Column =
    col("add")
      .withField("dataChange", dataChange)
      .withField("backReference", lit(null).cast(Action.addFileSchema("backReference").dataType))

  /**
   * The remove column built from the add column as [[AddFile.removeWithTimestamp]] does, with
   * deletionTimestamp, dataChange and backReference stamped, so it's ready to be converted to a
   * Scala RemoveFile.
   */
  private def removeFileStruct(timestamp: Column, dataChange: Column): Column = {
    val removeFileSchema = Action.logSchema("remove").dataType.asInstanceOf[StructType]
    struct(removeFileSchema.fields.toIndexedSeq.map { field =>
      removeFileColumn(field, timestamp, dataChange).cast(field.dataType).as(field.name)
    }: _*)
  }

  /**
   * The RemoveFile field `field` of [[removeFileStruct]]. No fallback, so a RemoveFile field that
   * AddFile does not have fails here until it is mapped.
   */
  private[amt] def removeFileColumn(
      field: StructField, timestamp: Column, dataChange: Column): Column = field.name match {
    case "deletionTimestamp" => timestamp
    case "dataChange" => dataChange
    case "extendedFileMetadata" => lit(true)
    case "snapshotId" | "backReference" => lit(null)
    case name if Action.addFileSchema.fieldNames.contains(name) => col("add").getField(name)
  }
}

object AMTManifestReader {
  /** Name of the column with an entry's `first_row_id` prefix sum. */
  private[amt] val PRECEDING_RECORDS_COLUMN: String = "precedingNullFirstRowIdRecords"

  // The entry context of an entry row (see [[withRootEntryContext]] and [[withLeafEntryContext]]).
  private[amt] val IS_ROOT_ENTRY_COLUMN: String = "isRootEntry"
  private[amt] val MANIFEST_LOCATION_COLUMN: String = "manifestLocation"
  private[amt] val PARENT_SNAPSHOT_ID_COLUMN: String = "parentSnapshotId"
  private[amt] val PARENT_FILE_SEQUENCE_NUMBER_COLUMN: String = "parentFileSequenceNumber"
  private[amt] val PARENT_FIRST_ROW_ID_COLUMN: String = "parentFirstRowId"

  /**
   * Whether the commit removed a changed file rather than added it (see [[projectChangeFiles]]).
   */
  private[amt] val REMOVED_COLUMN: String = "_amt_removed"

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
