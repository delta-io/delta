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

package org.apache.spark.sql.delta.commands

// scalastyle:off import.ordering.noEmptyLine
import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.skipping.clustering.ClusteredTableUtils
import org.apache.spark.sql.delta.skipping.clustering.ClusteringColumnInfo
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.backfill.RowTrackingBackfillCommand
import org.apache.spark.sql.delta.commands.columnmapping.RemoveColumnMappingCommand
import org.apache.spark.sql.delta.constraints.{CharVarcharConstraint, Constraints}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.schema.{SchemaMergingUtils, SchemaUtils}
import org.apache.spark.sql.delta.schema.SchemaUtils.transformSchema
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.StatisticsCollection
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.MDC
import org.apache.spark.sql.{AnalysisException, Column, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, IgnoreCachedData, QualifiedColType}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, SparkCharVarcharUtils}
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.catalog.TableChange.{After, ColumnPosition, First}
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types._

/**
 * A super trait for alter table commands that modify Delta tables.
 */
trait AlterDeltaTableCommand extends DeltaCommand {

  def table: DeltaTableV2

  protected def startTransaction(): OptimisticTransaction = {
    // WARNING: It's not safe to use startTransactionWithInitialSnapshot here. Some commands call
    // this method more than once, and some commands can be created with a stale table.
    val txn = table.startTransaction()
    if (txn.readVersion == -1) {
      throw DeltaErrors.notADeltaTableException(table.name())
    }
    txn
  }

  /**
   * Check if the column to change has any dependent expressions:
   *   - generated column expressions
   *   - check constraints
   */
  protected def checkDependentExpressions(
      sparkSession: SparkSession,
      columnParts: Seq[String],
      newMetadata: actions.Metadata,
      protocol: Protocol): Unit = {
    if (!sparkSession.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_ALTER_TABLE_CHANGE_COLUMN_CHECK_EXPRESSIONS)) {
      return
    }
    // check if the column to change is referenced by check constraints
    val dependentConstraints =
      Constraints.findDependentConstraints(sparkSession, columnParts, newMetadata)
    if (dependentConstraints.nonEmpty) {
      throw DeltaErrors.foundViolatingConstraintsForColumnChange(
        UnresolvedAttribute(columnParts).name, dependentConstraints)
    }
    // check if the column to change is referenced by any generated columns
    val dependentGenCols = SchemaUtils.findDependentGeneratedColumns(
      sparkSession, columnParts, protocol, newMetadata.schema)
    if (dependentGenCols.nonEmpty) {
      throw DeltaErrors.foundViolatingGeneratedColumnsForColumnChange(
        UnresolvedAttribute(columnParts).name, dependentGenCols)
    }
  }
}

/**
 * A command that sets Delta table configuration.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table1 SET TBLPROPERTIES ('key1' = 'val1', 'key2' = 'val2', ...);
 * }}}
 */
case class AlterTableSetPropertiesDeltaCommand(
    table: DeltaTableV2,
    configuration: Map[String, String])
  extends LeafRunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = table.deltaLog

    val rowTrackingPropertyKey = DeltaConfigs.ROW_TRACKING_ENABLED.key
    val enableRowTracking = configuration.keySet.contains(rowTrackingPropertyKey) &&
      configuration(rowTrackingPropertyKey).toBoolean

    if (enableRowTracking) {
      // If we're enabling row tracking on an existing table, we need to complete a backfill process
      // prior to updating the table metadata.
      RowTrackingBackfillCommand(
        deltaLog,
        nameOfTriggeringOperation = DeltaOperations.OP_SET_TBLPROPERTIES,
        table.catalogTable).run(sparkSession)
    }
    val columnMappingPropertyKey = DeltaConfigs.COLUMN_MAPPING_MODE.key
    val disableColumnMapping = configuration.get(columnMappingPropertyKey).contains("none")
    val columnMappingRemovalAllowed = sparkSession.sessionState.conf.getConf(
      DeltaSQLConf.ALLOW_COLUMN_MAPPING_REMOVAL)
    if (disableColumnMapping && columnMappingRemovalAllowed) {
      RemoveColumnMappingCommand(deltaLog, table.catalogTable)
        .run(sparkSession, removeColumnMappingTableProperty = false)
      // Not changing anything else, so we can return early.
      if (configuration.size == 1) {
        return Seq.empty[Row]
      }
    }
    recordDeltaOperation(deltaLog, "delta.ddl.alter.setProperties") {
      val txn = startTransaction()

      val metadata = txn.metadata
      val filteredConfs = configuration.filterKeys {
        case k if k.toLowerCase(Locale.ROOT).startsWith("delta.constraints.") =>
          throw DeltaErrors.useAddConstraints
        case k if k == TableCatalog.PROP_LOCATION =>
          throw DeltaErrors.useSetLocation()
        case k if k == TableCatalog.PROP_COMMENT =>
          false
        case k if k == TableCatalog.PROP_PROVIDER =>
          throw DeltaErrors.cannotChangeProvider()
        case k if k == TableFeatureProtocolUtils.propertyKey(ClusteringTableFeature) =>
          throw DeltaErrors.alterTableSetClusteringTableFeatureException(
            ClusteringTableFeature.name)
        case k if k == ClusteredTableUtils.PROP_CLUSTERING_COLUMNS =>
          throw DeltaErrors.cannotModifyTableProperty(k)
        case _ =>
          true
      }.toMap

      val newMetadata = metadata.copy(
        description = configuration.getOrElse(TableCatalog.PROP_COMMENT, metadata.description),
        configuration = metadata.configuration ++ filteredConfs)

      txn.updateMetadata(newMetadata)

      // Tag if the metadata update is _only_ for enabling row tracking. This allows for
      // an optimization where we can safely not fail concurrent txns from the metadata update.
      var tags = Map.empty[String, String]
      if (enableRowTracking && configuration.size == 1) {
        tags += (DeltaCommitTag.RowTrackingEnablementOnlyTag.key -> "true")
      }
      txn.commit(Nil, DeltaOperations.SetTableProperties(configuration), tags)

      Seq.empty[Row]
    }
  }
}

/**
 * A command that unsets Delta table configuration.
 * If ifExists is false, each individual key will be checked if it exists or not, it's a
 * one-by-one operation, not an all or nothing check. Otherwise, non-existent keys will be ignored.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table1 UNSET TBLPROPERTIES [IF EXISTS] ('key1', 'key2', ...);
 * }}}
 */
case class AlterTableUnsetPropertiesDeltaCommand(
    table: DeltaTableV2,
    propKeys: Seq[String],
    ifExists: Boolean)
  extends LeafRunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = table.deltaLog
    val columnMappingPropertyKey = DeltaConfigs.COLUMN_MAPPING_MODE.key
    val disableColumnMapping = propKeys.contains(columnMappingPropertyKey)
    val columnMappingRemovalAllowed = sparkSession.sessionState.conf.getConf(
      DeltaSQLConf.ALLOW_COLUMN_MAPPING_REMOVAL)
    if (disableColumnMapping && columnMappingRemovalAllowed) {
      RemoveColumnMappingCommand(deltaLog, table.catalogTable)
        .run(sparkSession, removeColumnMappingTableProperty = true)
      if (propKeys.size == 1) {
        // Not unsetting anything else, so we can return early.
        return Seq.empty[Row]
      }
    }
    recordDeltaOperation(deltaLog, "delta.ddl.alter.unsetProperties") {
      val txn = startTransaction()
      val metadata = txn.metadata

      val normalizedKeys = DeltaConfigs.normalizeConfigKeys(propKeys)
      if (!ifExists) {
        normalizedKeys.foreach { k =>
          if (!metadata.configuration.contains(k)) {
            throw DeltaErrors.unsetNonExistentProperty(k, table.name())
          }
        }
      }

      val newConfiguration = metadata.configuration.filterNot {
        case (key, _) => normalizedKeys.contains(key)
      }
      val description = if (normalizedKeys.contains(TableCatalog.PROP_COMMENT)) null else {
        metadata.description
      }
      val newMetadata = metadata.copy(
        description = description,
        configuration = newConfiguration)
      txn.updateMetadata(newMetadata)
      txn.commit(Nil, DeltaOperations.UnsetTableProperties(normalizedKeys, ifExists))

      Seq.empty[Row]
    }
  }
}

/**
 * A command that removes an existing feature from the table. The feature needs to implement the
 * [[RemovableFeature]] trait.
 *
 * The syntax of the command is:
 * {{{
 *   ALTER TABLE t DROP FEATURE f [TRUNCATE HISTORY]
 * }}}
 *
 * The operation consists of two stages (see [[RemovableFeature]]):
 *  1) preDowngradeCommand. This command is responsible for removing any data and metadata
 *     related to the feature.
 *  2) Protocol downgrade. Removes the feature from the current version's protocol.
 *     During this stage we also validate whether all traces of the feature-to-be-removed are gone.
 *
 *  For removing writer features the 2 steps above are sufficient. However, for removing
 *  reader+writer features we also need to ensure the history does not contain any traces of the
 *  removed feature. The user journey is the following:
 *
 *  1) The user runs the remove feature command which removes any traces of the feature from
 *     the latest version. The removal command throws a message that there was partial success
 *     and the retention period must pass before a protocol downgrade is possible.
 *  2) The user runs again the command after the retention period is over. The command checks the
 *     current state again and the history. If everything is clean, it proceeds with the protocol
 *     downgrade. The TRUNCATE HISTORY option may be used here to automatically set
 *     the log retention period to a minimum of 24 hours before clearing the logs. The minimum
 *     value is based on the expected duration of the longest running transaction. This is the
 *     lowest retention period we can set without endangering concurrent transactions.
 *     If transactions do run for longer than this period while this command is run, then this
 *     can lead to data corruption.
 *
 *  Note, legacy features can be removed as well. When removing a legacy feature from a legacy
 *  protocol, if the result cannot be represented with a legacy representation we use the
 *  table features representation. For example, removing Invariants from (1, 3) results to
 *  (1, 7, None, [AppendOnly, CheckConstraints]). Adding back Invariants to the protocol is
 *  normalized back to (1, 3). This allows to easily transitions back and forth between legacy
 *  protocols and table feature protocols.
 */
case class AlterTableDropFeatureDeltaCommand(
    table: DeltaTableV2,
    featureName: String,
    truncateHistory: Boolean = false)
  extends LeafRunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  def createEmptyCommitAndCheckpoint(snapshotRefreshStartTime: Long): Unit = {
    val log = table.deltaLog
    val snapshot = log.update(checkIfUpdatedSinceTs = Some(snapshotRefreshStartTime))
    val emptyCommitTS = System.nanoTime()
    log.startTransaction(table.catalogTable, Some(snapshot))
      .commit(Nil, DeltaOperations.EmptyCommit)
    log.checkpoint(log.update(checkIfUpdatedSinceTs = Some(emptyCommitTS)))
  }

  def truncateHistoryLogRetentionMillis(txn: OptimisticTransaction): Option[Long] = {
    if (!truncateHistory) return None

    val truncateHistoryLogRetention = DeltaConfigs
      .TABLE_FEATURE_DROP_TRUNCATE_HISTORY_LOG_RETENTION
      .fromMetaData(txn.metadata)

    Some(DeltaConfigs.getMilliSeconds(truncateHistoryLogRetention))
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = table.deltaLog
    recordDeltaOperation(deltaLog, "delta.ddl.alter.dropFeature") {
      val removableFeature = TableFeature.featureNameToFeature(featureName) match {
        case Some(feature: RemovableFeature) => feature
        case Some(_) => throw DeltaErrors.dropTableFeatureNonRemovableFeature(featureName)
        case None => throw DeltaErrors.dropTableFeatureFeatureNotSupportedByClient(featureName)
      }

      // Check whether the protocol contains the feature in either the writer features list or
      // the reader+writer features list. Note, protocol needs to denormalized to allow dropping
      // features from legacy protocols.
      val protocol = table.initialSnapshot.protocol
      val protocolContainsFeatureName =
        protocol.implicitlyAndExplicitlySupportedFeatures.map(_.name).contains(featureName)
      if (!protocolContainsFeatureName) {
        throw DeltaErrors.dropTableFeatureFeatureNotSupportedByProtocol(featureName)
      }

      if (truncateHistory && !removableFeature.requiresHistoryTruncation) {
        throw DeltaErrors.tableFeatureDropHistoryTruncationNotAllowed()
      }

      // Validate that the `removableFeature` is not a dependency of any other feature that is
      // enabled on the table.
      dependentFeatureCheck(removableFeature, protocol)

      // The removableFeature.preDowngradeCommand needs to adhere to the following requirements:
      //
      // a) Bring the table to a state the validation passes.
      // b) To not allow concurrent commands to alter the table in a way the validation does not
      //    pass. This can be done by first disabling the relevant metadata property.
      // c) Undoing (b) should cause the preDowngrade command to fail.
      //
      // Note, for features that cannot be disabled we solely rely for correctness on
      // validateRemoval.
      val requiresHistoryValidation = removableFeature.requiresHistoryTruncation
      val startTimeNs = System.nanoTime()
      val preDowngradeMadeChanges =
        removableFeature.preDowngradeCommand(table).removeFeatureTracesIfNeeded()
      if (requiresHistoryValidation) {
        // Generate a checkpoint after the cleanup that is based on commits that do not use
        // the feature. This intends to help slow-moving tables to qualify for history truncation
        // asap. The checkpoint is based on a new commit to avoid creating a checkpoint
        // on a commit that still contains traces of the removed feature.
        // Note, the checkpoint is created in both executions of DROP FEATURE command.
        createEmptyCommitAndCheckpoint(startTimeNs)

        // If the pre-downgrade command made changes, then the table's historical versions
        // certainly still contain traces of the feature. We don't have to run an expensive
        // explicit check, but instead we fail straight away.
        if (preDowngradeMadeChanges) {
          throw DeltaErrors.dropTableFeatureWaitForRetentionPeriod(
            featureName, table.initialSnapshot.metadata)
        }
      }

      val txn = table.startTransaction()
      val snapshot = txn.snapshot

      // Verify whether all requirements hold before performing the protocol downgrade.
      // If any concurrent transactions interfere with the protocol downgrade txn we
      // revalidate the requirements against the snapshot of the winning txn.
      if (!removableFeature.validateRemoval(snapshot)) {
        throw DeltaErrors.dropTableFeatureConflictRevalidationFailed()
      }

      // For reader+writer features, before downgrading the protocol we need to ensure there are no
      // traces of the feature in past versions. If traces are found, the user is advised to wait
      // until the retention period is over. This is a slow operation.
      // Note, if this txn conflicts, we check all winning commits for traces of the feature.
      // Therefore, we do not need to check again for historical versions during conflict
      // resolution.
      if (requiresHistoryValidation) {
        // Clean up expired logs before checking history. This also makes sure there is no
        // concurrent metadataCleanup during findEarliestReliableCheckpoint. Note, this
        // cleanUpExpiredLogs call truncates the cutoff at a minute granularity.
        deltaLog.cleanUpExpiredLogs(
          snapshot,
          truncateHistoryLogRetentionMillis(txn),
          TruncationGranularity.MINUTE)

        val historyContainsFeature = removableFeature.historyContainsFeature(
          spark = sparkSession,
          downgradeTxnReadSnapshot = snapshot)
        if (historyContainsFeature) {
          throw DeltaErrors.dropTableFeatureHistoricalVersionsExist(featureName, snapshot.metadata)
        }
      }

      txn.updateProtocol(txn.protocol.denormalized.removeFeature(removableFeature))
      txn.commit(Nil, DeltaOperations.DropTableFeature(featureName, truncateHistory))
      Nil
    }
  }

  /**
   * Checks if the `removableFeature` is a requirement for some other feature that is enabled on the
   * table. In such a scenario, we need to fail the drop feature command. The dependent features
   * needs to be dropped first before this `removableFeature` can be removed.
   */
  private def dependentFeatureCheck(
      removableFeature: TableFeature,
      protocol: Protocol): Unit = {
    val dependentFeatures = TableFeature.getDependentFeatures(removableFeature)
    if (dependentFeatures.nonEmpty) {
      val dependentFeaturesInProtocol = dependentFeatures.filter(protocol.isFeatureSupported)
      if (dependentFeaturesInProtocol.nonEmpty) {
        val dependentFeatureNames = dependentFeaturesInProtocol.map(_.name)
        throw DeltaErrors.dropTableFeatureFailedBecauseOfDependentFeatures(
          removableFeature.name, dependentFeatureNames.toSeq)
      }
    }
  }
}

/**
 * A command that add columns to a Delta table.
 * The syntax of using this command in SQL is:
 * {{{
 *   ALTER TABLE table_identifier
 *   ADD COLUMNS (col_name data_type [COMMENT col_comment], ...);
 * }}}
*/
case class AlterTableAddColumnsDeltaCommand(
    table: DeltaTableV2,
    colsToAddWithPosition: Seq[QualifiedColType])
  extends LeafRunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = table.deltaLog
    recordDeltaOperation(deltaLog, "delta.ddl.alter.addColumns") {
      val txn = startTransaction()

      if (SchemaUtils.filterRecursively(
            StructType(colsToAddWithPosition.map {
              case QualifiedColTypeWithPosition(_, column, _) => column
            }), true)(!_.nullable).nonEmpty) {
        throw DeltaErrors.operationNotSupportedException("NOT NULL in ALTER TABLE ADD COLUMNS")
      }

      // TODO: remove this after auto cache refresh is merged.
      table.tableIdentifier.foreach { identifier =>
        try sparkSession.catalog.uncacheTable(identifier) catch {
          case NonFatal(e) =>
            log.warn(s"Exception when attempting to uncache table $identifier", e)
        }
      }

      val metadata = txn.metadata
      val oldSchema = metadata.schema

      val resolver = sparkSession.sessionState.conf.resolver
      val newSchema = colsToAddWithPosition.foldLeft(oldSchema) {
        case (schema, QualifiedColTypeWithPosition(columnPath, column, None)) =>
          val parentPosition = SchemaUtils.findColumnPosition(columnPath, schema, resolver)
          val insertPosition = SchemaUtils.getNestedTypeFromPosition(schema, parentPosition) match {
            case s: StructType => s.size
            case other =>
               throw DeltaErrors.addColumnParentNotStructException(column, other)
          }
          SchemaUtils.addColumn(schema, column, parentPosition :+ insertPosition)
        case (schema, QualifiedColTypeWithPosition(columnPath, column, Some(_: First))) =>
          val parentPosition = SchemaUtils.findColumnPosition(columnPath, schema, resolver)
          SchemaUtils.addColumn(schema, column, parentPosition :+ 0)
        case (schema,
        QualifiedColTypeWithPosition(columnPath, column, Some(after: After))) =>
          val prevPosition =
            SchemaUtils.findColumnPosition(columnPath :+ after.column, schema, resolver)
          val position = prevPosition.init :+ (prevPosition.last + 1)
          SchemaUtils.addColumn(schema, column, position)
      }

      SchemaMergingUtils.checkColumnNameDuplication(newSchema, "in adding columns")
      SchemaUtils.checkSchemaFieldNames(newSchema, metadata.columnMappingMode)

      val newMetadata = metadata.copy(schemaString = newSchema.json)
      txn.updateMetadata(newMetadata)
      txn.commit(Nil, DeltaOperations.AddColumns(
        colsToAddWithPosition.map {
          case QualifiedColTypeWithPosition(path, col, colPosition) =>
            DeltaOperations.QualifiedColTypeWithPositionForLog(
              path, col, colPosition.map(_.toString))
        }))

      Seq.empty[Row]
    }
  }

  object QualifiedColTypeWithPosition {

    private def toV2Position(input: Any): ColumnPosition = {
      input.asInstanceOf[org.apache.spark.sql.catalyst.analysis.FieldPosition].position
    }

    def unapply(
        col: QualifiedColType): Option[(Seq[String], StructField, Option[ColumnPosition])] = {
      val builder = new MetadataBuilder
      col.comment.foreach(builder.putString("comment", _))

      val field = StructField(col.name.last, col.dataType, col.nullable, builder.build())

      col.default.map { value =>
        Some((col.name.init, field.withCurrentDefaultValue(value), col.position.map(toV2Position)))
      }.getOrElse {
        Some((col.name.init, field, col.position.map(toV2Position)))
      }
    }
  }
}

/**
 * A command that drop columns from a Delta table.
 * The syntax of using this command in SQL is:
 * {{{
 *   ALTER TABLE table_identifier
 *   DROP COLUMN(S) (col_name_1, col_name_2, ...);
 * }}}
 */
case class AlterTableDropColumnsDeltaCommand(
    table: DeltaTableV2,
    columnsToDrop: Seq[Seq[String]])
  extends LeafRunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (!sparkSession.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_ALTER_TABLE_DROP_COLUMN_ENABLED)) {
      // this featue is still behind the flag and not ready for release.
      throw DeltaErrors.dropColumnNotSupported(suggestUpgrade = false)
    }
    val deltaLog = table.deltaLog
    recordDeltaOperation(deltaLog, "delta.ddl.alter.dropColumns") {
      val txn = startTransaction()
      val metadata = txn.metadata
      if (txn.metadata.columnMappingMode == NoMapping) {
        throw DeltaErrors.dropColumnNotSupported(suggestUpgrade = true)
      }
      val newSchema = columnsToDrop.foldLeft(metadata.schema) { case (schema, columnPath) =>
        val parentPosition =
          SchemaUtils.findColumnPosition(
            columnPath, schema, sparkSession.sessionState.conf.resolver)
        SchemaUtils.dropColumn(schema, parentPosition)._1
      }

      // in case any of the dropped column is partition columns
      val droppedColumnSet = columnsToDrop.map(UnresolvedAttribute(_).name).toSet
      val droppingPartitionCols = metadata.partitionColumns.filter(droppedColumnSet.contains(_))
      if (droppingPartitionCols.nonEmpty) {
        throw DeltaErrors.dropPartitionColumnNotSupported(droppingPartitionCols)
      }
      // Disallow dropping clustering columns.
      val clusteringCols = ClusteringColumnInfo.extractLogicalNames(txn.snapshot)
      val droppingClusteringCols = clusteringCols.filter(droppedColumnSet.contains(_))
      if (droppingClusteringCols.nonEmpty) {
        throw DeltaErrors.dropClusteringColumnNotSupported(droppingClusteringCols)
      }
      // Updates the delta statistics column list by removing the dropped columns from it.
      val newConfiguration = metadata.configuration ++
        StatisticsCollection.dropDeltaStatsColumns(metadata, columnsToDrop)
      val newMetadata = metadata.copy(
        schemaString = newSchema.json,
        configuration = newConfiguration
      )
      columnsToDrop.foreach { columnParts =>
        checkDependentExpressions(sparkSession, columnParts, newMetadata, txn.protocol)
      }

      txn.updateMetadata(newMetadata)
      txn.commit(Nil, DeltaOperations.DropColumns(columnsToDrop))

      Seq.empty[Row]
    }
  }
}

/**
 * A command to change the column for a Delta table, support changing the comment of a column and
 * reordering columns.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   ALTER TABLE table_identifier
 *   CHANGE [COLUMN] column_old_name column_new_name column_dataType [COMMENT column_comment]
 *   [FIRST | AFTER column_name];
 * }}}
 */
case class AlterTableChangeColumnDeltaCommand(
    table: DeltaTableV2,
    columnPath: Seq[String],
    columnName: String,
    newColumn: StructField,
    colPosition: Option[ColumnPosition],
    syncIdentity: Boolean)
  extends LeafRunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = table.deltaLog
    recordDeltaOperation(deltaLog, "delta.ddl.alter.changeColumns") {
      val txn = startTransaction()
      val metadata = txn.metadata
      val bypassCharVarcharToStringFix =
        sparkSession.conf.get(DeltaSQLConf.DELTA_BYPASS_CHARVARCHAR_TO_STRING_FIX)
      val oldSchema = if (bypassCharVarcharToStringFix) {
          metadata.schema
        } else {
          SchemaUtils.getRawSchemaWithoutCharVarcharMetadata(metadata.schema)
        }
      val resolver = sparkSession.sessionState.conf.resolver

      // Verify that the columnName provided actually exists in the schema
      SchemaUtils.findColumnPosition(columnPath :+ columnName, oldSchema, resolver)

      val transformedSchema = transformSchema(oldSchema, Some(columnName)) {
        case (`columnPath`, struct @ StructType(fields), _) =>
          val oldColumn = struct(columnName)

          // Analyzer already validates the char/varchar type change of ALTER COLUMN in
          // `CheckAnalysis.checkAlterTableCommand`. We should normalize char/varchar type
          // to string type first, then apply Delta-specific checks.
          val oldColumnForVerification = if (bypassCharVarcharToStringFix) {
            oldColumn
          } else {
            CharVarcharUtils.replaceCharVarcharWithStringInSchema(StructType(Seq(oldColumn))).head
          }
          verifyColumnChange(sparkSession, oldColumnForVerification, resolver, txn)

          val newField = {
            if (syncIdentity) {
              assert(oldColumn == newColumn)
              val df = txn.snapshot.deltaLog.createDataFrame(txn.snapshot, txn.filterFiles())
              val field = IdentityColumn.syncIdentity(newColumn, df)
              txn.setSyncIdentity()
              txn.readWholeTable()
              field
            } else {
              // Take the name, comment, nullability and data type from newField
              // It's crucial to keep the old column's metadata, which may contain column mapping
              // metadata.
              var result = newColumn.getComment().map(oldColumn.withComment).getOrElse(oldColumn)
              // Apply the current default value as well, if any.
              result = newColumn.getCurrentDefaultValue() match {
                case Some(newDefaultValue) => result.withCurrentDefaultValue(newDefaultValue)
                case None => result.clearCurrentDefaultValue()
              }

              result
                .copy(
                  name = newColumn.name,
                  dataType =
                    SchemaUtils.changeDataType(oldColumn.dataType, newColumn.dataType, resolver),
                  nullable = newColumn.nullable)
            }
          }

          // Replace existing field with new field
          val newFieldList = fields.map { field =>
            if (DeltaColumnMapping.getPhysicalName(field) ==
              DeltaColumnMapping.getPhysicalName(newField)) {
              newField
            } else field
          }

          // Reorder new field to correct position if necessary
          StructType(colPosition.map { position =>
            reorderFieldList(struct, newFieldList, newField, position, resolver)
          }.getOrElse(newFieldList.toSeq))

        case (`columnPath`, m: MapType, _) if columnName == "key" =>
          val originalField = StructField(columnName, m.keyType, nullable = false)
          verifyMapArrayChange(sparkSession, originalField, resolver, txn)
          m.copy(keyType = SchemaUtils.changeDataType(m.keyType, newColumn.dataType, resolver))

        case (`columnPath`, m: MapType, _) if columnName == "value" =>
          val originalField = StructField(columnName, m.valueType, nullable = m.valueContainsNull)
          verifyMapArrayChange(sparkSession, originalField, resolver, txn)
          m.copy(valueType = SchemaUtils.changeDataType(m.valueType, newColumn.dataType, resolver))

        case (`columnPath`, a: ArrayType, _) if columnName == "element" =>
          val originalField = StructField(columnName, a.elementType, nullable = a.containsNull)
          verifyMapArrayChange(sparkSession, originalField, resolver, txn)
          a.copy(elementType =
            SchemaUtils.changeDataType(a.elementType, newColumn.dataType, resolver))

        case (_, other @ (_: StructType | _: ArrayType | _: MapType), _) => other
      }
      val newSchema = if (bypassCharVarcharToStringFix) {
        transformedSchema
      } else {
        // Fields with type CHAR/VARCHAR had been converted from
        // (StringType, metadata = 'VARCHAR(n)') into (VARCHAR(n), metadata = '')
        // so that CHAR/VARCHAR to String conversion can be handled correctly. We should
        // invert this conversion so that downstream operations are not affected.
        CharVarcharUtils.replaceCharVarcharWithStringInSchema(transformedSchema)
      }

      // update `partitionColumns` if the changed column is a partition column
      val newPartitionColumns = if (columnPath.isEmpty) {
        metadata.partitionColumns.map { partCol =>
          if (partCol == columnName) newColumn.name else partCol
        }
      } else metadata.partitionColumns

      val oldColumnPath = columnPath :+ columnName
      val newColumnPath = columnPath :+ newColumn.name
      // Rename the column in the delta statistics columns configuration, if present.
      val newConfiguration = metadata.configuration ++
        StatisticsCollection.renameDeltaStatsColumn(metadata, oldColumnPath, newColumnPath)

      val newSchemaWithTypeWideningMetadata =
        TypeWideningMetadata.addTypeWideningMetadata(txn, schema = newSchema, oldSchema = oldSchema)

      val newMetadata = metadata.copy(
        schemaString = newSchemaWithTypeWideningMetadata.json,
        partitionColumns = newPartitionColumns,
        configuration = newConfiguration
      )

      if (newColumn.name != columnName) {
        // need to validate the changes if the column is renamed
        checkDependentExpressions(
          sparkSession, columnPath :+ columnName, newMetadata, txn.protocol)
      }


      txn.updateMetadata(newMetadata)

      if (newColumn.name != columnName) {
        // record column rename separately
        txn.commit(Nil, DeltaOperations.RenameColumn(oldColumnPath, newColumnPath))
      } else {
        txn.commit(Nil, DeltaOperations.ChangeColumn(
          columnPath, columnName, newColumn, colPosition.map(_.toString)))
      }

      Seq.empty[Row]
    }
  }

  /**
   * Reorder the given fieldList to place `field` at the given `position` in `fieldList`
   *
   * @param struct The initial StructType with the original field at its original position
   * @param fieldList List of fields with the changed field in the original position
   * @param field The field that is to be added
   * @param position Position where the field is to be placed
   * @return Returns a new list of fields with the changed field in the new position
   */
  private def reorderFieldList(
      struct: StructType,
      fieldList: Array[StructField],
      field: StructField,
      position: ColumnPosition,
      resolver: Resolver): Seq[StructField] = {
    val startIndex = struct.fieldIndex(columnName)
    val filtered = fieldList.filterNot(_.name == columnName)
    val newFieldList = position match {
      case _: First =>
        field +: filtered

      case after: After if after.column() == columnName =>
        filtered.slice(0, startIndex)++
          Seq(field) ++
          filtered.slice(startIndex, filtered.length)

      case after: After =>
        val endIndex = filtered.indexWhere(i => resolver(i.name, after.column()))
        if (endIndex < 0) {
          throw DeltaErrors.columnNotInSchemaException(after.column(), struct)
        }

        filtered.slice(0, endIndex + 1) ++
          Seq(field) ++
          filtered.slice(endIndex + 1, filtered.length)
    }
    newFieldList.toSeq
  }

  /**
   * Given two columns, verify whether replacing the original column with the new column is a valid
   * operation.
   *
   * Note that this requires a full table scan in the case of SET NOT NULL to verify that all
   * existing values are valid.
   *
   * @param originalField The existing column
   */
  private def verifyColumnChange(
      spark: SparkSession,
      originalField: StructField,
      resolver: Resolver,
      txn: OptimisticTransaction): Unit = {

    originalField.dataType match {
      case same if same == newColumn.dataType =>
      // just changing comment or position so this is fine
      case s: StructType if s != newColumn.dataType =>
        val fieldName = UnresolvedAttribute(columnPath :+ columnName).name
        throw DeltaErrors.cannotUpdateStructField(table.name(), fieldName)
      case m: MapType if m != newColumn.dataType =>
        val fieldName = UnresolvedAttribute(columnPath :+ columnName).name
        throw DeltaErrors.cannotUpdateMapField(table.name(), fieldName)
      case a: ArrayType if a != newColumn.dataType =>
        val fieldName = UnresolvedAttribute(columnPath :+ columnName).name
        throw DeltaErrors.cannotUpdateArrayField(table.name(), fieldName)
      case _: AtomicType =>
      // update is okay
      case o =>
        throw DeltaErrors.cannotUpdateOtherField(table.name(), o)
    }

    // Analyzer already validates the char/varchar type change of ALTER COLUMN in
    // `CheckAnalysis.checkAlterTableCommand`. We should normalize char/varchar type to string type
    // first (original data type is already normalized as we store char/varchar as string type with
    // special metadata in the Delta log), then apply Delta-specific checks.
    val newType = CharVarcharUtils.replaceCharVarcharWithString(newColumn.dataType)
    if (SchemaUtils.canChangeDataType(
        originalField.dataType,
        newType,
        resolver,
        txn.metadata.columnMappingMode,
        columnPath :+ originalField.name,
        allowTypeWidening = TypeWidening.isEnabled(txn.protocol, txn.metadata)
      ).nonEmpty) {
      throw DeltaErrors.alterTableChangeColumnException(
        fieldPath = UnresolvedAttribute(columnPath :+ originalField.name).name,
        oldField = originalField,
        newField = newColumn
      )
    }

    if (columnName != newColumn.name) {
      if (txn.metadata.columnMappingMode == NoMapping) {
        throw DeltaErrors.columnRenameNotSupported
      }
    }

    if (originalField.dataType != newType) {
      checkDependentExpressions(
        spark, columnPath :+ columnName, txn.metadata, txn.protocol)
    }

    if (originalField.nullable && !newColumn.nullable) {
      throw DeltaErrors.alterTableChangeColumnException(
        fieldPath = UnresolvedAttribute(columnPath :+ originalField.name).name,
        oldField = originalField,
        newField = newColumn
      )
    }
  }

  /**
   * Verify whether replacing the original map key/value or array element with a new data type is a
   * valid operation.
   *
   * @param originalField the original map key/value or array element to update.
   */
  private def verifyMapArrayChange(spark: SparkSession, originalField: StructField,
      resolver: Resolver, txn: OptimisticTransaction): Unit = {
    // Map key/value and array element can't have comments.
    if (newColumn.getComment().nonEmpty) {
      throw DeltaErrors.addCommentToMapArrayException(
        fieldPath = UnresolvedAttribute(columnPath :+ columnName).name
      )
    }
    // Changing the nullability of map key/value or array element isn't supported.
    if (originalField.nullable != newColumn.nullable) {
      throw DeltaErrors.alterTableChangeColumnException(
        fieldPath = UnresolvedAttribute(columnPath :+ originalField.name).name,
        oldField = originalField,
        newField = newColumn
      )
    }
    verifyColumnChange(spark, originalField, resolver, txn)
  }
}

/**
 * A command to replace columns for a Delta table, support changing the comment of a column,
 * reordering columns, and loosening nullabilities.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   ALTER TABLE table_identifier REPLACE COLUMNS (col_spec[, col_spec ...]);
 * }}}
 */
case class AlterTableReplaceColumnsDeltaCommand(
    table: DeltaTableV2,
    columns: Seq[StructField])
  extends LeafRunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    recordDeltaOperation(table.deltaLog, "delta.ddl.alter.replaceColumns") {
      val txn = startTransaction()

      val metadata = txn.metadata
      val existingSchema = metadata.schema

      if (ColumnWithDefaultExprUtils.hasIdentityColumn(table.initialSnapshot.schema)) {
        throw DeltaErrors.identityColumnReplaceColumnsNotSupported()
      }

      val resolver = sparkSession.sessionState.conf.resolver
      val changingSchema = StructType(columns)

      SchemaUtils.canChangeDataType(
        existingSchema,
        changingSchema,
        resolver,
        txn.metadata.columnMappingMode,
        allowTypeWidening = TypeWidening.isEnabled(txn.protocol, txn.metadata),
        failOnAmbiguousChanges = true
      ).foreach { operation =>
        throw DeltaErrors.alterTableReplaceColumnsException(
          existingSchema, changingSchema, operation)
      }

      val newSchema = SchemaUtils.changeDataType(existingSchema, changingSchema, resolver)
        .asInstanceOf[StructType]

      SchemaMergingUtils.checkColumnNameDuplication(newSchema, "in replacing columns")
      SchemaUtils.checkSchemaFieldNames(newSchema, metadata.columnMappingMode)

      val newSchemaWithTypeWideningMetadata = TypeWideningMetadata.addTypeWideningMetadata(
        txn,
        schema = newSchema,
        oldSchema = existingSchema
      )

      val newMetadata = metadata.copy(schemaString = newSchemaWithTypeWideningMetadata.json)
      txn.updateMetadata(newMetadata)
      txn.commit(Nil, DeltaOperations.ReplaceColumns(columns))

      Nil
    }
  }
}

/**
 * A command to change the location of a Delta table. Effectively, this only changes the symlink
 * in the Hive MetaStore from one Delta table to another.
 *
 * This command errors out if the new location is not a Delta table. By default, the new Delta
 * table must have the same schema as the old table, but we have a SQL conf that allows users
 * to bypass this schema check.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   ALTER TABLE table_identifier SET LOCATION 'path/to/new/delta/table';
 * }}}
 */
case class AlterTableSetLocationDeltaCommand(
    table: DeltaTableV2,
    location: String)
  extends LeafRunnableCommand
    with AlterDeltaTableCommand
    with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    if (table.catalogTable.isEmpty) {
      throw DeltaErrors.setLocationNotSupportedOnPathIdentifiers()
    }
    val catalogTable = table.catalogTable.get
    val locUri = CatalogUtils.stringToURI(location)

    val oldTable = table.deltaLog.update()
    if (oldTable.version == -1) {
      throw DeltaErrors.notADeltaTableException(table.name())
    }
    val oldMetadata = oldTable.metadata

    var updatedTable = catalogTable.withNewStorage(locationUri = Some(locUri))

    val (_, newTable) = DeltaLog.forTableWithSnapshot(sparkSession, new Path(location))
    if (newTable.version == -1) {
      throw DeltaErrors.notADeltaTableException(DeltaTableIdentifier(path = Some(location)))
    }
    val newMetadata = newTable.metadata
    val bypassSchemaCheck = sparkSession.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_ALTER_LOCATION_BYPASS_SCHEMA_CHECK)

    if (!bypassSchemaCheck && !schemasEqual(oldMetadata, newMetadata)) {
      throw DeltaErrors.alterTableSetLocationSchemaMismatchException(
        oldMetadata.schema, newMetadata.schema)
    }
    catalog.alterTable(updatedTable)

    Seq.empty[Row]
  }

  private def schemasEqual(
      oldMetadata: actions.Metadata, newMetadata: actions.Metadata): Boolean = {
    import DeltaColumnMapping._
    dropColumnMappingMetadata(oldMetadata.schema) ==
      dropColumnMappingMetadata(newMetadata.schema) &&
      dropColumnMappingMetadata(oldMetadata.partitionSchema) ==
        dropColumnMappingMetadata(newMetadata.partitionSchema)
  }
}

trait AlterTableConstraintDeltaCommand
  extends LeafRunnableCommand with AlterDeltaTableCommand with IgnoreCachedData  {

  def getConstraintWithName(
      table: DeltaTableV2,
      name: String,
      metadata: actions.Metadata,
      sparkSession: SparkSession): Option[String] = {
    val expr = Constraints.getExprTextByName(name, metadata, sparkSession)
    if (expr.nonEmpty) {
      return expr
    }
    None
  }
}

/**
 * Command to add a constraint to a Delta table. Currently only CHECK constraints are supported.
 *
 * Adding a constraint will scan all data in the table to verify the constraint currently holds.
 *
 * @param table The table to which the constraint should be added.
 * @param name The name of the new constraint.
 * @param exprText The contents of the new CHECK constraint, to be parsed and evaluated.
 */
case class AlterTableAddConstraintDeltaCommand(
    table: DeltaTableV2,
    name: String,
    exprText: String)
  extends AlterTableConstraintDeltaCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = table.deltaLog
    if (name == CharVarcharConstraint.INVARIANT_NAME) {
      throw DeltaErrors.invalidConstraintName(name)
    }
    recordDeltaOperation(deltaLog, "delta.ddl.alter.addConstraint") {
      val txn = startTransaction()

      getConstraintWithName(table, name, txn.metadata, sparkSession).foreach { oldExpr =>
        throw DeltaErrors.constraintAlreadyExists(name, oldExpr)
      }

      val newMetadata = txn.metadata.copy(
        configuration = txn.metadata.configuration +
          (Constraints.checkConstraintPropertyName(name) -> exprText)
      )

      val df = txn.snapshot.deltaLog.createDataFrame(txn.snapshot, txn.filterFiles())
      val unresolvedExpr = sparkSession.sessionState.sqlParser.parseExpression(exprText)

      try {
        df.where(new Column(unresolvedExpr)).queryExecution.analyzed
      } catch {
        case a: AnalysisException
            if a.errorClass.contains("DATATYPE_MISMATCH.FILTER_NOT_BOOLEAN") =>
          throw DeltaErrors.checkConstraintNotBoolean(name, exprText)
        case a: AnalysisException =>
          // Strip out the context of the DataFrame that was used to analyze the expression.
          throw a.copy(context = Array.empty)
      }

      logInfo(log"Checking that ${MDC(DeltaLogKeys.EXPR, exprText)} " +
        log"is satisfied for existing data. This will require a full table scan.")
      recordDeltaOperation(
          txn.snapshot.deltaLog,
          "delta.ddl.alter.addConstraint.checkExisting") {
        val n = df.where(new Column(Or(Not(unresolvedExpr), IsUnknown(unresolvedExpr)))).count()

        if (n > 0) {
          throw DeltaErrors.newCheckConstraintViolated(n, table.name(), exprText)
        }
      }

      txn.commit(newMetadata :: Nil, DeltaOperations.AddConstraint(name, exprText))
    }
    Seq()
  }
}

/**
 * Command to drop a constraint from a Delta table. No-op if a constraint with the given name
 * doesn't exist.
 *
 * Currently only CHECK constraints are supported.
 *
 * @param table The table from which the constraint should be dropped
 * @param name The name of the constraint to drop
 */
case class AlterTableDropConstraintDeltaCommand(
    table: DeltaTableV2,
    name: String,
    ifExists: Boolean)
  extends AlterTableConstraintDeltaCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = table.deltaLog
    recordDeltaOperation(deltaLog, "delta.ddl.alter.dropConstraint") {
      val txn = startTransaction()

      val oldExprText = Constraints.getExprTextByName(name, txn.metadata, sparkSession)
      if (oldExprText.isEmpty && !ifExists && !sparkSession.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_ASSUMES_DROP_CONSTRAINT_IF_EXISTS)) {
        val quotedTableName = table.getTableIdentifierIfExists.map(_.quotedString)
          .orElse(table.catalogTable.map(_.identifier.quotedString))
          .getOrElse(table.name())
        throw DeltaErrors.nonexistentConstraint(name, quotedTableName)
      }

      val newMetadata = txn.metadata.copy(
        configuration = txn.metadata.configuration - Constraints.checkConstraintPropertyName(name))

      txn.commit(newMetadata :: Nil, DeltaOperations.DropConstraint(name, oldExprText))
    }

    Seq()
  }
}

/**
 * Command for altering clustering columns for clustered tables.
 * - ALTER TABLE .. CLUSTER BY (col1, col2, ...)
 * - ALTER TABLE .. CLUSTER BY NONE
 *
 * Note that the given `clusteringColumns` are empty when CLUSTER BY NONE is specified.
 * Also, `clusteringColumns` are validated (e.g., duplication / existence check) in
 * DeltaCatalog.alterTable().
 */
case class AlterTableClusterByDeltaCommand(
    table: DeltaTableV2,
    clusteringColumns: Seq[Seq[String]])
  extends LeafRunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = table.deltaLog
    ClusteredTableUtils.validateNumClusteringColumns(clusteringColumns, Some(deltaLog))
    // If the target table is not a clustered table and there are no clustering columns being added
    // (CLUSTER BY NONE), do not convert the table into a clustered table.
    val snapshot = deltaLog.update()
    if (clusteringColumns.isEmpty &&
      !ClusteredTableUtils.isSupported(snapshot.protocol)) {
      logInfo(log"Skipping ALTER TABLE CLUSTER BY NONE on a non-clustered table: " +
        log"${MDC(DeltaLogKeys.TABLE_NAME, table.name())}.")
      recordDeltaEvent(
        deltaLog,
        "delta.ddl.alter.clusterBy",
        data = Map(
          "isClusterByNoneSkipped" -> true,
          "isNewClusteredTable" -> false,
          "oldColumnsCount" -> 0,
          "newColumnsCount" -> 0))
      return Seq.empty
    }
    recordDeltaOperation(deltaLog, "delta.ddl.alter.clusterBy") {
      val txn = startTransaction()

      val clusteringColsLogicalNames = ClusteringColumnInfo.extractLogicalNames(txn.snapshot)
      val oldLogicalClusteringColumnsString = clusteringColsLogicalNames.mkString(",")
      val oldColumnsCount = clusteringColsLogicalNames.size

      val newLogicalClusteringColumns = clusteringColumns.map(FieldReference(_).toString)
      ClusteredTableUtils.validateClusteringColumnsInStatsSchema(
        txn.snapshot, newLogicalClusteringColumns)

      val newDomainMetadata =
        ClusteredTableUtils
          .getClusteringDomainMetadataForAlterTableClusterBy(newLogicalClusteringColumns, txn)

      recordDeltaEvent(
        deltaLog,
        "delta.ddl.alter.clusterBy",
        data = Map(
          "isClusterByNoneSkipped" -> false,
          "isNewClusteredTable" -> !ClusteredTableUtils.isSupported(txn.protocol),
          "oldColumnsCount" -> oldColumnsCount, "newColumnsCount" -> clusteringColumns.size))
      // Add clustered table properties if the current table is not clustered.
      // [[DeltaCatalog.alterTable]] already ensures that the table is not partitioned.
      if (!ClusteredTableUtils.isSupported(txn.protocol)) {
        txn.updateMetadata(
          txn.metadata.copy(
            configuration = txn.metadata.configuration ++
              ClusteredTableUtils.getTableFeatureProperties(txn.metadata.configuration)
          ))
      }
      txn.commit(
        newDomainMetadata,
        DeltaOperations.ClusterBy(
          oldLogicalClusteringColumnsString,
          newLogicalClusteringColumns.mkString(",")))
    }
    Seq.empty[Row]
  }
}
