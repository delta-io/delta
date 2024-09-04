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
import org.apache.spark.sql.delta.DeltaOperationMetrics.MetricsTransformer
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeIntoClause
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Exhaustive list of operations that can be performed on a Delta table. These operations are
 * tracked as the first line in delta logs, and power `DESCRIBE HISTORY` for Delta tables.
 */
object DeltaOperations {

  /**
   * An operation that can be performed on a Delta table.
   * @param name The name of the operation.
   */
  sealed abstract class Operation(val name: String) {
    def parameters: Map[String, Any]

    lazy val jsonEncodedValues: Map[String, String] =
      parameters.mapValues(JsonUtils.toJson(_)).toMap

    val operationMetrics: Set[String] = Set()

    def transformMetrics(metrics: Map[String, SQLMetric]): Map[String, String] = {
      metrics.filterKeys( s =>
        operationMetrics.contains(s)
      ).mapValues(_.value.toString).toMap
    }

    val userMetadata: Option[String] = None

    /** Whether this operation changes data */
    def changesData: Boolean = false

    /**
     * Manually transform the deletion vector metrics, because they are not part of
     * `operationMetrics` and are filtered out by the super.transformMetrics() call.
     */
    def transformDeletionVectorMetrics(
        allMetrics: Map[String, SQLMetric],
        dvMetrics: Map[String, MetricsTransformer] = DeltaOperationMetrics.DELETION_VECTORS)
    : Map[String, String] = {
      dvMetrics.flatMap { case (metric, transformer) =>
        transformer.transformToString(metric, allMetrics)
      }
    }

    /**
     * A transaction that commits AddFile actions with deletionVector should have column stats that
     * are not tight bounds. An exception to this is ComputeStats operation, which recomputes stats
     * on these files, and the new stats are tight bounds. Some other operations that merely take an
     * existing AddFile action and commit a copy of it, not changing the deletionVector or stats,
     * can then also recommit AddFile with deletionVector and tight bound stats that were recomputed
     * before.
     *
     * An operation for which this can happen, and there is no way that it could be committing
     * new deletion vectors, should set this to false to bypass this check.
     * All other operations should set this to true, so that this is validated during commit.
     *
     * This is abstract to force the implementers of all operations to think about this setting.
     * All operations should add a comment justifying this setting.
     * Any operation that sets this to false should add a test in TightBoundsSuite.
     */
    def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean

  }

  abstract class OperationWithPredicates(name: String, val predicates: Seq[Expression])
      extends Operation(name) {
    private val predicateString = JsonUtils.toJson(predicatesToString(predicates))
    override def parameters: Map[String, Any] = Map("predicate" -> predicateString)
  }

  /** Recorded during batch inserts. Predicates can be provided for overwrites. */
  case class Write(
      mode: SaveMode,
      partitionBy: Option[Seq[String]] = None,
      predicate: Option[String] = None,
      override val userMetadata: Option[String] = None
  ) extends Operation("WRITE") {
    override val parameters: Map[String, Any] = Map("mode" -> mode.name()
    ) ++
      partitionBy.map("partitionBy" -> JsonUtils.toJson(_)) ++
      predicate.map("predicate" -> _)

    val replaceWhereMetricsEnabled = SparkSession.active.conf.get(
      DeltaSQLConf.REPLACEWHERE_METRICS_ENABLED)

    override def transformMetrics(metrics: Map[String, SQLMetric]): Map[String, String] = {
      // Need special handling for replaceWhere as it is implemented as a Write + Delete.
      if (predicate.nonEmpty && replaceWhereMetricsEnabled) {
        var strMetrics = super.transformMetrics(metrics)
        // find the case where deletedRows are not captured
        if (strMetrics.get("numDeletedRows").exists(_ == "0") &&
          strMetrics.get("numRemovedFiles").exists(_ != "0")) {
          // identify when row level metrics are unavailable. This will happen when the entire
          // table or partition are deleted.
          strMetrics -= "numDeletedRows"
          strMetrics -= "numCopiedRows"
          strMetrics -= "numAddedFiles"
        }

        // in the case when stats are not collected we need to remove all row based metrics
        // If the DF provided to replaceWhere is an empty DataFrame and we don't have stats
        // we won't return row level metrics.
        if (strMetrics.get("numOutputRows").exists(_ == "0") &&
            strMetrics.get("numFiles").exists(_ != 0)) {
          strMetrics -= "numDeletedRows"
          strMetrics -= "numOutputRows"
          strMetrics -= "numCopiedRows"
        }

        strMetrics
      } else {
        super.transformMetrics(metrics)
      }
    }

    override val operationMetrics: Set[String] = if (predicate.isEmpty ||
        !replaceWhereMetricsEnabled) {
      DeltaOperationMetrics.WRITE
    } else {
      // Need special handling for replaceWhere as rows/files are deleted as well.
      DeltaOperationMetrics.WRITE_REPLACE_WHERE
    }
    override def changesData: Boolean = true

    // This operation shouldn't be introducing AddFile actions with DVs and tight bounds stats.
    // DVs can be introduced by the replaceWhere operation.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  case class RemoveColumnMapping(
      override val userMetadata: Option[String] = None) extends Operation("REMOVE COLUMN MAPPING") {
    override def parameters: Map[String, Any] = Map()

    override val operationMetrics: Set[String] = DeltaOperationMetrics.REMOVE_COLUMN_MAPPING

    // This operation shouldn't be introducing AddFile actions at all. This check should be trivial.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  /** Recorded during streaming inserts. */
  case class StreamingUpdate(
      outputMode: OutputMode,
      queryId: String,
      epochId: Long,
      override val userMetadata: Option[String] = None
  ) extends Operation("STREAMING UPDATE") {
    override val parameters: Map[String, Any] =
      Map("outputMode" -> outputMode.toString, "queryId" -> queryId, "epochId" -> epochId.toString
      )
    override val operationMetrics: Set[String] = DeltaOperationMetrics.STREAMING_UPDATE
    override def changesData: Boolean = true

    // This operation shouldn't be introducing AddFile actions with DVs and tight bounds stats.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }
  /** Recorded while deleting certain partitions. */
  case class Delete(predicate: Seq[Expression])
      extends OperationWithPredicates("DELETE", predicate) {
    override val operationMetrics: Set[String] = DeltaOperationMetrics.DELETE

    override def transformMetrics(metrics: Map[String, SQLMetric]): Map[String, String] = {
      var strMetrics = super.transformMetrics(metrics)
      // find the case where deletedRows are not captured
      if (strMetrics("numDeletedRows") == "0" && strMetrics("numRemovedFiles") != "0") {
        // identify when row level metrics are unavailable. This will happen when the entire
        // table or partition are deleted.
        strMetrics -= "numDeletedRows"
        strMetrics -= "numCopiedRows"
        strMetrics -= "numAddedFiles"
      }

      val dvMetrics = transformDeletionVectorMetrics(metrics)
      strMetrics ++ dvMetrics
    }
    override def changesData: Boolean = true

    // This operation shouldn't be introducing AddFile actions with DVs and tight bounds stats.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }
  /** Recorded when truncating the table. */
  case class Truncate() extends Operation("TRUNCATE") {
    override val parameters: Map[String, Any] = Map.empty
    override val operationMetrics: Set[String] = DeltaOperationMetrics.TRUNCATE
    override def changesData: Boolean = true

    // This operation shouldn't be introducing AddFile actions at all. This check should be trivial.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  /** Recorded when converting a table into a Delta table. */
  case class Convert(
      numFiles: Long,
      partitionBy: Seq[String],
      collectStats: Boolean,
      catalogTable: Option[String],
      sourceFormat: Option[String]) extends Operation("CONVERT") {
    override val parameters: Map[String, Any] = Map(
      "numFiles" -> numFiles,
      "partitionedBy" -> JsonUtils.toJson(partitionBy),
      "collectStats" -> collectStats) ++
        catalogTable.map("catalogTable" -> _) ++
        sourceFormat.map("sourceFormat" -> _)
    override val operationMetrics: Set[String] = DeltaOperationMetrics.CONVERT
    override def changesData: Boolean = true

    // This operation shouldn't be introducing AddFile actions with DVs and non-tight bounds stats.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  /** Represents the predicates and action type (insert, update, delete) for a Merge clause */
  case class MergePredicate(
      predicate: Option[String],
      actionType: String)

  object MergePredicate {
    def apply(mergeClause: DeltaMergeIntoClause): MergePredicate = {
      MergePredicate(
        predicate = mergeClause.condition.map(_.simpleString(SQLConf.get.maxToStringFields)),
        mergeClause.clauseType.toLowerCase())
    }
  }

  /**
   * Recorded when a merge operation is committed to the table.
   *
   * `updatePredicate`, `deletePredicate`, and `insertPredicate` are DEPRECATED.
   * Only use `predicate`, `matchedPredicates`, `notMatchedPredicates` and
   * `notMatchedBySourcePredicates` to record the merge.
   */
  val OP_MERGE = "MERGE"
  case class Merge(
      predicate: Option[Expression],
      updatePredicate: Option[String],
      deletePredicate: Option[String],
      insertPredicate: Option[String],
      matchedPredicates: Seq[MergePredicate],
      notMatchedPredicates: Seq[MergePredicate],
      notMatchedBySourcePredicates: Seq[MergePredicate]
  )
    extends OperationWithPredicates(OP_MERGE, predicate.toSeq) {

    override val parameters: Map[String, Any] = {
      super.parameters ++
        updatePredicate.map("updatePredicate" -> _).toMap ++
        deletePredicate.map("deletePredicate" -> _).toMap ++
        insertPredicate.map("insertPredicate" -> _).toMap +
        ("matchedPredicates" -> JsonUtils.toJson(matchedPredicates)) +
        ("notMatchedPredicates" -> JsonUtils.toJson(notMatchedPredicates)) +
        ("notMatchedBySourcePredicates" -> JsonUtils.toJson(notMatchedBySourcePredicates))
    }
    override val operationMetrics: Set[String] = DeltaOperationMetrics.MERGE

    override def transformMetrics(metrics: Map[String, SQLMetric]): Map[String, String] = {

      var strMetrics = super.transformMetrics(metrics)

      // We have to recalculate "numOutputRows" to avoid counting CDC rows
      if (metrics.contains("numTargetRowsInserted") &&
          metrics.contains("numTargetRowsUpdated") &&
          metrics.contains("numTargetRowsCopied")) {
        val actualNumOutputRows = metrics("numTargetRowsInserted").value +
          metrics("numTargetRowsUpdated").value +
          metrics("numTargetRowsCopied").value
        strMetrics += "numOutputRows" -> actualNumOutputRows.toString
      }

      val dvMetrics = transformDeletionVectorMetrics(
        metrics, dvMetrics = DeltaOperationMetrics.MERGE_DELETION_VECTORS)
      strMetrics ++= dvMetrics

      strMetrics
    }

    override def changesData: Boolean = true

    // This operation shouldn't be introducing AddFile actions with DVs and non-tight bounds stats.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  object Merge {
    /** constructor to provide default values for deprecated fields */
    def apply(
        predicate: Option[Expression],
        matchedPredicates: Seq[MergePredicate],
        notMatchedPredicates: Seq[MergePredicate],
        notMatchedBySourcePredicates: Seq[MergePredicate]
    ): Merge = Merge(
          predicate,
          updatePredicate = None,
          deletePredicate = None,
          insertPredicate = None,
          matchedPredicates,
          notMatchedPredicates,
          notMatchedBySourcePredicates
    )
  }

  /** Recorded when an update operation is committed to the table. */
  case class Update(predicate: Option[Expression])
      extends OperationWithPredicates("UPDATE", predicate.toSeq) {
    override val operationMetrics: Set[String] = DeltaOperationMetrics.UPDATE

    override def changesData: Boolean = true

    override def transformMetrics(metrics: Map[String, SQLMetric]): Map[String, String] = {
      val dvMetrics = transformDeletionVectorMetrics(metrics)
      super.transformMetrics(metrics) ++ dvMetrics
    }

    // This operation shouldn't be introducing AddFile actions with DVs and non-tight bounds stats.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }
  /** Recorded when the table is created. */
  case class CreateTable(
      metadata: Metadata,
      isManaged: Boolean,
      asSelect: Boolean = false,
      clusterBy: Option[Seq[String]] = None
  ) extends Operation("CREATE TABLE" + s"${if (asSelect) " AS SELECT" else ""}") {
    override val parameters: Map[String, Any] = Map(
      "isManaged" -> isManaged.toString,
      "description" -> Option(metadata.description),
      "partitionBy" -> JsonUtils.toJson(metadata.partitionColumns),
      CLUSTERING_PARAMETER_KEY -> JsonUtils.toJson(clusterBy.getOrElse(Seq.empty)),
      "properties" -> JsonUtils.toJson(metadata.configuration)
    )
    override val operationMetrics: Set[String] = if (!asSelect) {
      Set()
    } else {
      DeltaOperationMetrics.WRITE
    }
    override def changesData: Boolean = asSelect

    // This operation shouldn't be introducing AddFile actions with DVs and non-tight bounds stats.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }
  /** Recorded when the table is replaced. */
  case class ReplaceTable(
      metadata: Metadata,
      isManaged: Boolean,
      orCreate: Boolean,
      asSelect: Boolean = false,
      override val userMetadata: Option[String] = None,
      clusterBy: Option[Seq[String]] = None
  ) extends Operation(s"${if (orCreate) "CREATE OR " else ""}REPLACE TABLE" +
      s"${if (asSelect) " AS SELECT" else ""}") {
    override val parameters: Map[String, Any] = Map(
      "isManaged" -> isManaged.toString,
      "description" -> Option(metadata.description),
      "partitionBy" -> JsonUtils.toJson(metadata.partitionColumns),
      CLUSTERING_PARAMETER_KEY -> JsonUtils.toJson(clusterBy.getOrElse(Seq.empty)),
      "properties" -> JsonUtils.toJson(metadata.configuration)
  )
    override val operationMetrics: Set[String] = if (!asSelect) {
      Set()
    } else {
      DeltaOperationMetrics.WRITE
    }
    override def changesData: Boolean = true

    // This operation shouldn't be introducing AddFile actions with DVs and non-tight bounds stats.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }
  /** Recorded when the table properties are set. */
  val OP_SET_TBLPROPERTIES = "SET TBLPROPERTIES"
  case class SetTableProperties(
      properties: Map[String, String]) extends Operation(OP_SET_TBLPROPERTIES) {
    override val parameters: Map[String, Any] = Map("properties" -> JsonUtils.toJson(properties))

    // This operation shouldn't be introducing AddFile actions at all. This check should be trivial.
    // Note: This operation may trigger additional actions and additional commits. For example
    // RowTrackingBackfill. These are separate transactions, and this check is performed separately.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }
  /** Recorded when the table properties are unset. */
  case class UnsetTableProperties(
      propKeys: Seq[String],
      ifExists: Boolean) extends Operation("UNSET TBLPROPERTIES") {
    override val parameters: Map[String, Any] = Map(
      "properties" -> JsonUtils.toJson(propKeys),
      "ifExists" -> ifExists)

    // This operation shouldn't be introducing AddFile actions at all. This check should be trivial.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }
  /** Recorded when dropping a table feature. */
  case class DropTableFeature(
      featureName: String,
      truncateHistory: Boolean) extends Operation("DROP FEATURE") {
    override val parameters: Map[String, Any] = Map(
      "featureName" -> featureName,
      "truncateHistory" -> truncateHistory)

    // This operation shouldn't be introducing AddFile actions at all. This check should be trivial.
    // Note: this operation may trigger additional actions and additional commits. These would be
    // separate transactions, and this check is performed separately.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }
  /** Recorded when columns are added. */
  case class AddColumns(
      colsToAdd: Seq[QualifiedColTypeWithPositionForLog]) extends Operation("ADD COLUMNS") {

    override val parameters: Map[String, Any] = Map(
      "columns" -> JsonUtils.toJson(colsToAdd.map {
        case QualifiedColTypeWithPositionForLog(columnPath, column, colPosition) =>
          Map(
            "column" -> structFieldToMap(columnPath, column)
          ) ++ colPosition.map("position" -> _.toString)
      }))

    // This operation shouldn't be introducing AddFile actions at all. This check should be trivial.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  /** Recorded when columns are dropped. */
  val OP_DROP_COLUMN = "DROP COLUMNS"
  case class DropColumns(
    colsToDrop: Seq[Seq[String]]) extends Operation(OP_DROP_COLUMN) {

    override val parameters: Map[String, Any] = Map(
      "columns" -> JsonUtils.toJson(colsToDrop.map(UnresolvedAttribute(_).name)))

    // This operation shouldn't be introducing AddFile actions at all. This check should be trivial.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  /** Recorded when column is renamed */
  val OP_RENAME_COLUMN = "RENAME COLUMN"
  case class RenameColumn(oldColumnPath: Seq[String], newColumnPath: Seq[String])
    extends Operation(OP_RENAME_COLUMN) {
    override val parameters: Map[String, Any] = Map(
      "oldColumnPath" -> UnresolvedAttribute(oldColumnPath).name,
      "newColumnPath" -> UnresolvedAttribute(newColumnPath).name
    )

    // This operation shouldn't be introducing AddFile actions at all. This check should be trivial.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  /** Recorded when columns are changed. */
  case class ChangeColumn(
      columnPath: Seq[String],
      columnName: String,
      newColumn: StructField,
      colPosition: Option[String]) extends Operation("CHANGE COLUMN") {

    override val parameters: Map[String, Any] = Map(
      "column" -> JsonUtils.toJson(structFieldToMap(columnPath, newColumn))
    ) ++ colPosition.map("position" -> _)

    // This operation shouldn't be introducing AddFile actions at all. This check should be trivial.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }
  /** Recorded when columns are replaced. */
  case class ReplaceColumns(
      columns: Seq[StructField]) extends Operation("REPLACE COLUMNS") {

    override val parameters: Map[String, Any] = Map(
      "columns" -> JsonUtils.toJson(columns.map(structFieldToMap(Seq.empty, _))))

    // This operation shouldn't be introducing AddFile actions at all. This check should be trivial.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  case class UpgradeProtocol(newProtocol: Protocol) extends Operation("UPGRADE PROTOCOL") {
    override val parameters: Map[String, Any] = Map("newProtocol" -> JsonUtils.toJson(Map(
      "minReaderVersion" -> newProtocol.minReaderVersion,
      "minWriterVersion" -> newProtocol.minWriterVersion,
      "readerFeatures" -> newProtocol.readerFeatures,
      "writerFeatures" -> newProtocol.writerFeatures
    )))

    // This operation shouldn't be introducing AddFile actions at all. This check should be trivial.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  object ManualUpdate extends Operation("Manual Update") {
    override val parameters: Map[String, Any] = Map.empty

    // Unsafe manual update disables checks.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = false
  }

  /** A commit without any actions. Could be used to force creation of new checkpoints. */
  object EmptyCommit extends Operation("Empty Commit") {
    override val parameters: Map[String, Any] = Map.empty

    // This operation shouldn't be introducing AddFile actions at all. This check should be trivial.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  case class UpdateColumnMetadata(
      operationName: String,
      columns: Seq[(Seq[String], StructField)])
    extends Operation(operationName) {
    override val parameters: Map[String, Any] = {
      Map("columns" -> JsonUtils.toJson(columns.map {
        case (path, field) => structFieldToMap(path, field)
      }))
    }

    // This operation shouldn't be introducing AddFile actions at all. This check should be trivial.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  case class UpdateSchema(oldSchema: StructType, newSchema: StructType)
      extends Operation("UPDATE SCHEMA") {
    override val parameters: Map[String, Any] = Map(
      "oldSchema" -> JsonUtils.toJson(oldSchema),
      "newSchema" -> JsonUtils.toJson(newSchema))

    // This operation shouldn't be introducing AddFile actions at all. This check should be trivial.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  case class AddConstraint(
      constraintName: String, expr: String) extends Operation("ADD CONSTRAINT") {
    override val parameters: Map[String, Any] = Map("name" -> constraintName, "expr" -> expr)

    // This operation shouldn't be introducing AddFile actions at all. This check should be trivial.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  case class DropConstraint(
      constraintName: String, expr: Option[String]) extends Operation("DROP CONSTRAINT") {
    override val parameters: Map[String, Any] = {
      expr.map { e =>
        Map("name" -> constraintName, "expr" -> e, "existed" -> "true")
      }.getOrElse {
        Map("name" -> constraintName, "existed" -> "false")
      }
    }

    // This operation shouldn't be introducing AddFile actions at all. This check should be trivial.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  /** Recorded when recomputing stats on the table. */
  case class ComputeStats(predicate: Seq[Expression])
      extends OperationWithPredicates("COMPUTE STATS", predicate) {

    // ComputeStats operation commits AddFiles with recomputed stats which are always tight bounds,
    // even when DVs are present. This check should be disabled.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = false
  }

  /** Recorded when restoring a Delta table to an older version. */
  val OP_RESTORE = "RESTORE"
  case class Restore(
      version: Option[Long],
      timestamp: Option[String]) extends Operation(OP_RESTORE) {
    override val parameters: Map[String, Any] = Map(
      "version" -> version,
      "timestamp" -> timestamp)
    override def changesData: Boolean = true

    override val operationMetrics: Set[String] = DeltaOperationMetrics.RESTORE

    // Restore operation commits AddFiles with files, DVs and stats from the version it restores to.
    // It can happen that tight bound stats were recomputed before by ComputeStats.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = false
  }

  sealed abstract class OptimizeOrReorg(override val name: String, predicates: Seq[Expression])
    extends OperationWithPredicates(name, predicates)

  /** operation name for ROW TRACKING BACKFILL command */
  val ROW_TRACKING_BACKFILL_OPERATION_NAME = "ROW TRACKING BACKFILL"

  /** parameter key to indicate whether it's an Auto Compaction */
  val AUTO_COMPACTION_PARAMETER_KEY = "auto"

  /** operation name for REORG command */
  val REORG_OPERATION_NAME = "REORG"
  /** operation name for OPTIMIZE command */
  val OPTIMIZE_OPERATION_NAME = "OPTIMIZE"
  /** parameter key to indicate which columns to z-order by */
  val ZORDER_PARAMETER_KEY = "zOrderBy"
  /** parameter key to indicate clustering columns */
  val CLUSTERING_PARAMETER_KEY = "clusterBy"

  /** Recorded when optimizing the table. */
  case class Optimize(
      predicate: Seq[Expression],
      zOrderBy: Seq[String] = Seq.empty,
      auto: Boolean = false,
      clusterBy: Option[Seq[String]] = None
  ) extends OptimizeOrReorg(OPTIMIZE_OPERATION_NAME, predicate) {
    override val parameters: Map[String, Any] = super.parameters ++ Map(
      // When clustering columns are specified, set the zOrderBy key to empty.
      ZORDER_PARAMETER_KEY -> JsonUtils.toJson(if (clusterBy.isEmpty) zOrderBy else Seq.empty),
      CLUSTERING_PARAMETER_KEY -> JsonUtils.toJson(clusterBy.getOrElse(Seq.empty)),
      AUTO_COMPACTION_PARAMETER_KEY -> auto
    )

    override val operationMetrics: Set[String] = DeltaOperationMetrics.OPTIMIZE

    // This operation shouldn't be introducing AddFile actions with DVs and tight bounds stats.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  /** Recorded when cloning a Delta table into a new location. */
  val OP_CLONE = "CLONE"
  case class Clone(
      source: String,
      sourceVersion: Long
  ) extends Operation(OP_CLONE) {
    override val parameters: Map[String, Any] = Map(
      "source" -> source,
      "sourceVersion" -> sourceVersion
    )
    override def changesData: Boolean = true
    override val operationMetrics: Set[String] = DeltaOperationMetrics.CLONE

    // Clone operation commits AddFiles with files, DVs and stats copied over from the source table.
    // It can happen that tight bound stats were recomputed before by ComputeStats.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = false
  }

  /**
   * @param retentionCheckEnabled - whether retention check was enabled for this run of vacuum.
   * @param specifiedRetentionMillis - specified retention interval
   * @param defaultRetentionMillis - default retention period for the table
   */
  case class VacuumStart(
      retentionCheckEnabled: Boolean,
      specifiedRetentionMillis: Option[Long],
      defaultRetentionMillis: Long) extends Operation("VACUUM START") {
    override val parameters: Map[String, Any] = Map(
      "retentionCheckEnabled" -> retentionCheckEnabled,
      "defaultRetentionMillis" -> defaultRetentionMillis
    ) ++ specifiedRetentionMillis.map("specifiedRetentionMillis" -> _)

    override val operationMetrics: Set[String] = DeltaOperationMetrics.VACUUM_START

    // This operation shouldn't be introducing AddFile actions at all. This check should be trivial.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  /**
   * @param status - whether the vacuum operation was successful; either "COMPLETED" or "FAILED"
   */
  case class VacuumEnd(status: String) extends Operation(s"VACUUM END") {
    override val parameters: Map[String, Any] = Map(
      "status" -> status
    )

    override val operationMetrics: Set[String] = DeltaOperationMetrics.VACUUM_END

    // This operation shouldn't be introducing AddFile actions at all. This check should be trivial.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  /** Recorded when running REORG on the table. */
  case class Reorg(
      predicate: Seq[Expression],
      applyPurge: Boolean = true) extends OptimizeOrReorg(REORG_OPERATION_NAME, predicate) {
    override val parameters: Map[String, Any] = super.parameters ++ Map(
      "applyPurge" -> applyPurge
    )

    override val operationMetrics: Set[String] = DeltaOperationMetrics.OPTIMIZE

    // This operation shouldn't be introducing AddFile actions with DVs and tight bounds stats.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  /** Recorded when clustering columns are changed on clustered tables. */
  case class ClusterBy(
      oldClusteringColumns: String,
      newClusteringColumns: String) extends Operation("CLUSTER BY") {
    override val parameters: Map[String, Any] = Map(
      "oldClusteringColumns" -> oldClusteringColumns,
      "newClusteringColumns" -> newClusteringColumns)

    // This operation shouldn't be introducing AddFile actions at all. This check should be trivial.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  /** Recorded when we backfill a Delta table's existing AddFiles with row tracking data. */
  case class RowTrackingBackfill(
      batchId: Int = 0) extends Operation(ROW_TRACKING_BACKFILL_OPERATION_NAME) {
    override val parameters: Map[String, Any] = Map(
      "batchId" -> JsonUtils.toJson(batchId)
    )

    // RowTrackingBackfill operation commits AddFiles with files, DVs and stats copied over.
    // It can happen that tight bound stats were recomputed before by ComputeStats.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = false
  }

  private def structFieldToMap(colPath: Seq[String], field: StructField): Map[String, Any] = {
    Map(
      "name" -> UnresolvedAttribute(colPath :+ field.name).name,
      "type" -> field.dataType.typeName,
      "nullable" -> field.nullable,
      "metadata" -> JsonUtils.mapper.readValue[Map[String, Any]](field.metadata.json)
    )
  }

  /**
   * Qualified column type with position. We define a copy of the type here to avoid depending on
   * the parser output classes in our logging.
   */
  case class QualifiedColTypeWithPositionForLog(
     columnPath: Seq[String],
     column: StructField,
     colPosition: Option[String])

  /** Dummy operation only for testing with arbitrary operation names */
  case class TestOperation(operationName: String = "TEST") extends Operation(operationName) {
    override val parameters: Map[String, Any] = Map.empty

    // Perform the check for testing.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }

  /**
   * Helper method to convert a sequence of command predicates in the form of an
   * [[Expression]]s to a sequence of Strings so be stored in the commit info.
   */
  def predicatesToString(predicates: Seq[Expression]): Seq[String] = {
    val maxToStringFields = SQLConf.get.maxToStringFields
    predicates.map(_.simpleString(maxToStringFields))
  }

  /** Recorded when the table properties are set. */
  private val OP_UPGRADE_UNIFORM_BY_REORG = "REORG TABLE UPGRADE UNIFORM"

  /**
   * recorded when upgrading a table set uniform properties by REORG TABLE ... UPGRADE UNIFORM
   */
  case class UpgradeUniformProperties(properties: Map[String, String]) extends Operation(
      OP_UPGRADE_UNIFORM_BY_REORG) {
    override val parameters: Map[String, Any] = Map("properties" -> JsonUtils.toJson(properties))

    // This operation shouldn't be introducing AddFile actions with DVs and tight bounds stats.
    override def checkAddFileWithDeletionVectorStatsAreNotTightBounds: Boolean = true
  }
}

private[delta] object DeltaOperationMetrics {
  val WRITE = Set(
    "numFiles", // number of files written
    "numOutputBytes", // size in bytes of the written contents
    "numOutputRows" // number of rows written
  )

  val REMOVE_COLUMN_MAPPING: Set[String] = Set(
    "numRewrittenFiles",
    "numOutputBytes",
    "numRemovedBytes",
    "numCopiedRows",
    "numDeletionVectorsRemoved"
  )

  val STREAMING_UPDATE = Set(
    "numAddedFiles", // number of files added
    "numRemovedFiles", // number of files removed
    "numOutputRows", // number of rows written
    "numOutputBytes" // number of output writes
  )

  val DELETE = Set(
    "numAddedFiles", // number of files added
    "numRemovedFiles", // number of files removed
    "numDeletionVectorsAdded", // number of deletion vectors added
    "numDeletionVectorsRemoved", // number of deletion vectors removed
    "numDeletionVectorsUpdated", // number of deletion vectors updated
    "numAddedChangeFiles", // number of CDC files
    "numDeletedRows", // number of rows removed
    "numCopiedRows", // number of rows copied in the process of deleting files
    "executionTimeMs", // time taken to execute the entire operation
    "scanTimeMs", // time taken to scan the files for matches
    "rewriteTimeMs", // time taken to rewrite the matched files
    "numRemovedBytes", // number of bytes removed
    "numAddedBytes" // number of bytes added
  )

  val WRITE_REPLACE_WHERE = Set(
    "numFiles", // number of files written
    "numOutputBytes", // size in bytes of the written
    "numOutputRows", // number of rows written
    "numRemovedFiles", // number of files removed
    "numAddedChangeFiles", // number of CDC files
    "numDeletedRows", // number of rows removed
    "numCopiedRows", // number of rows copied in the process of deleting files
    "numRemovedBytes" // number of bytes removed
  )

  val WRITE_REPLACE_WHERE_PARTITIONS = Set(
    "numFiles", // number of files written
    "numOutputBytes", // size in bytes of the written contents
    "numOutputRows", // number of rows written
    "numAddedChangeFiles", // number of CDC files
    "numRemovedFiles", // number of files removed
    // Records below only exist when DELTA_DML_METRICS_FROM_METADATA is enabled
    "numCopiedRows", // number of rows copied
    "numDeletedRows", // number of rows deleted
    "numRemovedBytes" // number of bytes removed
  )

  /**
   * Deleting the entire table or partition will record row level metrics when
   * DELTA_DML_METRICS_FROM_METADATA is enabled
   * * DELETE_PARTITIONS is used only in test to verify specific delete cases.
   */
  val DELETE_PARTITIONS = Set(
    "numRemovedFiles", // number of files removed
    "numAddedChangeFiles", // number of CDC files generated - generally 0 in this case
    "numDeletionVectorsAdded", // number of deletion vectors added
    "numDeletionVectorsRemoved", // number of deletion vectors removed
    "numDeletionVectorsUpdated", // number of deletion vectors updated
    "executionTimeMs", // time taken to execute the entire operation
    "scanTimeMs", // time taken to scan the files for matches
    "rewriteTimeMs", // time taken to rewrite the matched files
    // Records below only exist when DELTA_DML_METRICS_FROM_METADATA is enabled
    "numCopiedRows", // number of rows copied
    "numDeletedRows", // number of rows deleted
    "numAddedFiles", // number of files added
    "numRemovedBytes", // number of bytes removed
    "numAddedBytes" // number of bytes added
  )


  trait MetricsTransformer {
    /**
     * Produce the output metric `metricName`, given all available metrics.
     *
     * If one or more input metrics are missing, the output metrics may be skipped by
     * returning `None`.
     */
    def transform(
        metricName: String,
        allMetrics: Map[String, SQLMetric]): Option[(String, Long)]

    def transformToString(
        metricName: String,
        allMetrics: Map[String, SQLMetric]): Option[(String, String)] = {
      this.transform(metricName, allMetrics).map { case (name, metric) =>
        name -> metric.toString
      }
    }
  }

  /** Pass metric on unaltered. */
  final object PassMetric extends MetricsTransformer {
    override def transform(
        metricName: String,
        allMetrics: Map[String, SQLMetric]): Option[(String, Long)] =
      allMetrics.get(metricName).map(metric => metricName -> metric.value)
  }

  /**
   * Produce a new metric by summing up the values of `inputMetrics`.
   *
   * Treats missing metrics at 0.
   */
  final case class SumMetrics(inputMetrics: String*)
    extends MetricsTransformer {

    override def transform(
        metricName: String,
        allMetrics: Map[String, SQLMetric]): Option[(String, Long)] = {
      var atLeastOneMetricExists = false
      val total = inputMetrics.map { name =>
        val metricValueOpt = allMetrics.get(name)
        atLeastOneMetricExists |= metricValueOpt.isDefined
        metricValueOpt.map(_.value).getOrElse(0L)
      }.sum
      if (atLeastOneMetricExists) {
        Some(metricName -> total)
      } else {
        None
      }
    }
  }

  val DELETION_VECTORS: Map[String, MetricsTransformer] = Map(
    // Adding "numDeletionVectorsUpdated" here makes the values line up with how
    // "numFilesAdded"/"numFilesRemoved" behave.
    "numDeletionVectorsAdded" -> SumMetrics("numDeletionVectorsAdded", "numDeletionVectorsUpdated"),
    "numDeletionVectorsRemoved" ->
      SumMetrics("numDeletionVectorsRemoved", "numDeletionVectorsUpdated")
  )

  // The same as [[DELETION_VECTORS]] but with the "Target" prefix that is used by MERGE.
  val MERGE_DELETION_VECTORS = Map(
    // Adding "numDeletionVectorsUpdated" here makes the values line up with how
    // "numFilesAdded"/"numFilesRemoved" behave.
    "numTargetDeletionVectorsAdded" ->
      SumMetrics("numTargetDeletionVectorsAdded", "numTargetDeletionVectorsUpdated"),
    "numTargetDeletionVectorsRemoved" ->
      SumMetrics("numTargetDeletionVectorsRemoved", "numTargetDeletionVectorsUpdated")
  )

  val TRUNCATE = Set(
    "numRemovedFiles", // number of files removed
    "executionTimeMs" // time taken to execute the entire operation
  )

  val CONVERT = Set(
    "numConvertedFiles" // number of parquet files that have been converted.
  )

  val MERGE = Set(
    "numSourceRows", // number of rows in the source dataframe
    "numTargetRowsInserted", // number of rows inserted into the target table.
    "numTargetRowsUpdated", // number of rows updated in the target table.
    "numTargetRowsMatchedUpdated", // number of rows updated by a matched clause.
    // number of rows updated by a not matched by source clause.
    "numTargetRowsNotMatchedBySourceUpdated",
    "numTargetRowsDeleted", // number of rows deleted in the target table.
    "numTargetRowsMatchedDeleted", // number of rows deleted by a matched clause.
    // number of rows deleted by a not matched by source clause.
    "numTargetRowsNotMatchedBySourceDeleted",
    "numTargetRowsCopied", // number of target rows copied
    "numTargetBytesAdded", // number of target bytes added
    "numTargetBytesRemoved", // number of target bytes removed
    "numOutputRows", // total number of rows written out
    "numTargetFilesAdded", // num files added to the sink(target)
    "numTargetFilesRemoved", // number of files removed from the sink(target)
    "numTargetChangeFilesAdded", // number of CDC files
    "executionTimeMs",  // time taken to execute the entire operation
    "materializeSourceTimeMs", // time taken to materialize source (or determine it's not needed)
    "scanTimeMs", // time taken to scan the files for matches
    "rewriteTimeMs", // time taken to rewrite the matched files
    "numTargetDeletionVectorsAdded", // number of deletion vectors added
    "numTargetDeletionVectorsRemoved", // number of deletion vectors removed
    "numTargetDeletionVectorsUpdated" // number of deletion vectors updated
  )

  val UPDATE = Set(
    "numAddedFiles", // number of files added
    "numRemovedFiles", // number of files removed
    "numAddedChangeFiles", // number of CDC files
    "numDeletionVectorsAdded", // number of deletion vectors added
    "numDeletionVectorsRemoved", // number of deletion vectors removed
    "numDeletionVectorsUpdated", // number of deletion vectors updated
    "numUpdatedRows", // number of rows updated
    "numCopiedRows", // number of rows just copied over in the process of updating files.
    "executionTimeMs",  // time taken to execute the entire operation
    "scanTimeMs", // time taken to scan the files for matches
    "rewriteTimeMs", // time taken to rewrite the matched files
    "numRemovedBytes", // number of bytes removed
    "numAddedBytes" // number of bytes added
  )

  val OPTIMIZE = Set(
    "numAddedFiles", // number of data files added
    "numRemovedFiles", // number of data files removed
    "numAddedBytes", // number of data bytes added by optimize
    "numRemovedBytes", // number of data bytes removed by optimize
    "minFileSize", // the size of the smallest file
    "p25FileSize", // the size of the 25th percentile file
    "p50FileSize", // the median file size
    "p75FileSize", // the 75th percentile of the file sizes
    "maxFileSize", // the size of the largest file
    "numDeletionVectorsRemoved" // number of deletion vectors removed by optimize
  )

  val RESTORE = Set(
    "tableSizeAfterRestore", // table size in bytes after restore
    "numOfFilesAfterRestore", // number of files in the table after restore
    "numRemovedFiles", // number of files removed by the restore operation
    "numRestoredFiles", // number of files that were added as a result of the restore
    "removedFilesSize", // size in bytes of files removed by the restore
    "restoredFilesSize" // size in bytes of files added by the restore
  )

  val CLONE = Set(
    "sourceTableSize", // size in bytes of source table at version
    "sourceNumOfFiles", // number of files in source table at version
    "numRemovedFiles", // number of files removed from target table if delta table was replaced
    "numCopiedFiles", // number of files that were cloned - 0 for shallow tables
    "removedFilesSize", // size in bytes of files removed from an existing Delta table if one exists
    "copiedFilesSize" // size of files copied - 0 for shallow tables
  )

  val VACUUM_START = Set(
    "numFilesToDelete", // number of files that will be deleted by vacuum
    "sizeOfDataToDelete" // total size in bytes of files that will be deleted by vacuum
  )

  val VACUUM_END = Set(
    "numDeletedFiles", // number of files deleted by vacuum
    "numVacuumedDirectories" // number of directories vacuumed
  )

}
