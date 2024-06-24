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

import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.{AlterTableSetPropertiesDeltaCommand, AlterTableUnsetPropertiesDeltaCommand, DeltaReorgTableCommand, DeltaReorgTableMode, DeltaReorgTableSpec}
import org.apache.spark.sql.delta.commands.columnmapping.RemoveColumnMappingCommand
import org.apache.spark.sql.delta.commands.optimize.OptimizeMetrics
import org.apache.spark.sql.delta.constraints.Constraints
import org.apache.spark.sql.delta.coordinatedcommits.CoordinatedCommitsUtils
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.{Utils => DeltaUtils}
import org.apache.spark.sql.util.ScalaExtensions._

import org.apache.spark.sql.catalyst.analysis.ResolvedTable

/**
 * A base class for implementing a preparation command for removing table features.
 * Must implement a run method. Note, the run method must be implemented in a way that when
 * it finishes, the table does not use the feature that is being removed, and nobody is
 * allowed to start using it again implicitly. One way to achieve this is by
 * disabling the feature on the table before proceeding to the actual removal.
 * See [[RemovableFeature.preDowngradeCommand]].
 */
sealed abstract class PreDowngradeTableFeatureCommand {
  /**
   * Returns true when it performs a cleaning action. When no action was required
   * it returns false.
   */
  def removeFeatureTracesIfNeeded(): Boolean
}

case class TestWriterFeaturePreDowngradeCommand(table: DeltaTableV2)
  extends PreDowngradeTableFeatureCommand
  with DeltaLogging {
  // To remove the feature we only need to remove the table property.
  override def removeFeatureTracesIfNeeded(): Boolean = {
    // Make sure feature data/metadata exist before proceeding.
    if (TestRemovableWriterFeature.validateRemoval(table.initialSnapshot)) return false

    if (DeltaUtils.isTesting) {
      recordDeltaEvent(table.deltaLog, "delta.test.TestWriterFeaturePreDowngradeCommand")
    }

    val properties = Seq(TestRemovableWriterFeature.TABLE_PROP_KEY)
    AlterTableUnsetPropertiesDeltaCommand(table, properties, ifExists = true).run(table.spark)
    true
  }
}

case class TestWriterWithHistoryValidationFeaturePreDowngradeCommand(table: DeltaTableV2)
    extends PreDowngradeTableFeatureCommand
    with DeltaLogging {
  // To remove the feature we only need to remove the table property.
  override def removeFeatureTracesIfNeeded(): Boolean = {
    // Make sure feature data/metadata exist before proceeding.
    if (TestRemovableWriterWithHistoryTruncationFeature.validateRemoval(table.initialSnapshot)) {
      return false
    }

    val properties = Seq(TestRemovableWriterWithHistoryTruncationFeature.TABLE_PROP_KEY)
    AlterTableUnsetPropertiesDeltaCommand(table, properties, ifExists = true).run(table.spark)
    true
  }
}

case class TestReaderWriterFeaturePreDowngradeCommand(table: DeltaTableV2)
  extends PreDowngradeTableFeatureCommand
  with DeltaLogging {
  // To remove the feature we only need to remove the table property.
  override def removeFeatureTracesIfNeeded(): Boolean = {
    // Make sure feature data/metadata exist before proceeding.
    if (TestRemovableReaderWriterFeature.validateRemoval(table.initialSnapshot)) return false

    if (DeltaUtils.isTesting) {
      recordDeltaEvent(table.deltaLog, "delta.test.TestReaderWriterFeaturePreDowngradeCommand")
    }

    val properties = Seq(TestRemovableReaderWriterFeature.TABLE_PROP_KEY)
    AlterTableUnsetPropertiesDeltaCommand(table, properties, ifExists = true).run(table.spark)
    true
  }
}

case class TestLegacyWriterFeaturePreDowngradeCommand(table: DeltaTableV2)
  extends PreDowngradeTableFeatureCommand {
  /** Return true if we removed the property, false if no action was needed. */
  override def removeFeatureTracesIfNeeded(): Boolean = {
    if (TestRemovableLegacyWriterFeature.validateRemoval(table.initialSnapshot)) return false

    val properties = Seq(TestRemovableLegacyWriterFeature.TABLE_PROP_KEY)
    AlterTableUnsetPropertiesDeltaCommand(table, properties, ifExists = true).run(table.spark)
    true
  }
}

case class TestLegacyReaderWriterFeaturePreDowngradeCommand(table: DeltaTableV2)
  extends PreDowngradeTableFeatureCommand {
  /** Return true if we removed the property, false if no action was needed. */
  override def removeFeatureTracesIfNeeded(): Boolean = {
    if (TestRemovableLegacyReaderWriterFeature.validateRemoval(table.initialSnapshot)) return false

    val properties = Seq(TestRemovableLegacyReaderWriterFeature.TABLE_PROP_KEY)
    AlterTableUnsetPropertiesDeltaCommand(table, properties, ifExists = true).run(table.spark)
    true
  }
}

case class V2CheckpointPreDowngradeCommand(table: DeltaTableV2)
  extends PreDowngradeTableFeatureCommand
  with DeltaLogging {
  /**
   * We set the checkpoint policy to classic to prevent any transactions from creating
   * v2 checkpoints.
   *
   * @return True if it changed checkpoint policy metadata property to classic.
   *         False otherwise.
   */
  override def removeFeatureTracesIfNeeded(): Boolean = {

    if (V2CheckpointTableFeature.validateRemoval(table.initialSnapshot)) return false

    val startTimeNs = System.nanoTime()
    val properties = Map(DeltaConfigs.CHECKPOINT_POLICY.key -> CheckpointPolicy.Classic.name)
    AlterTableSetPropertiesDeltaCommand(table, properties).run(table.spark)

    recordDeltaEvent(
      table.deltaLog,
      opType = "delta.v2CheckpointFeatureRemovalMetrics",
      data =
        Map(("downgradeTimeMs", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)))
    )

    true
  }
}

case class InCommitTimestampsPreDowngradeCommand(table: DeltaTableV2)
  extends PreDowngradeTableFeatureCommand
  with DeltaLogging {
  /**
   * We disable the feature by:
   * - Removing the table properties:
   *    1. DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP
   *    2. DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION
   * - Setting the table property DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED to false.
   * Technically, only setting IN_COMMIT_TIMESTAMPS_ENABLED to false is enough to disable the
   * feature. However, we can use this opportunity to clean up the metadata.
   *
   * @return true if any change to the metadata (the three properties listed above) was made.
   *         False otherwise.
   */
  override def removeFeatureTracesIfNeeded(): Boolean = {
    val startTimeNs = System.nanoTime()
    val currentMetadata = table.initialSnapshot.metadata
    val currentTableProperties = currentMetadata.configuration

    val enablementProperty = DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED
    val ictEnabledInMetadata = enablementProperty.fromMetaData(currentMetadata)
    val provenanceProperties = Seq(
      DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.key,
      DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.key)
    val propertiesToRemove = provenanceProperties.filter(currentTableProperties.contains)

    val traceRemovalNeeded = propertiesToRemove.nonEmpty || ictEnabledInMetadata
    if (traceRemovalNeeded) {
      val propertiesToDisable =
        Option.when(ictEnabledInMetadata)(enablementProperty.key -> "false")
      val desiredTableProperties = currentTableProperties
        .filterNot{ case (k, _) => propertiesToRemove.contains(k) } ++ propertiesToDisable

      val deltaOperation = DeltaOperations.UnsetTableProperties(
        (propertiesToRemove ++ propertiesToDisable.map(_._1)).toSeq, ifExists = true)
      table.startTransaction().commit(
        Seq(currentMetadata.copy(configuration = desiredTableProperties.toMap)), deltaOperation)
    }

    val provenancePropertiesPresenceLogs = provenanceProperties.map { prop =>
      prop -> currentTableProperties.contains(prop).toString
    }
    recordDeltaEvent(
      table.deltaLog,
      opType = "delta.inCommitTimestampFeatureRemovalMetrics",
      data = Map(
          "downgradeTimeMs" -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs),
          "traceRemovalNeeded" -> traceRemovalNeeded.toString,
          enablementProperty.key -> ictEnabledInMetadata
          ) ++ provenancePropertiesPresenceLogs

    )
    traceRemovalNeeded
  }
}

case class VacuumProtocolCheckPreDowngradeCommand(table: DeltaTableV2)
  extends PreDowngradeTableFeatureCommand
  with DeltaLogging {

  /**
   * Returns true when it performs a cleaning action. When no action was required
   * it returns false.
   * For downgrading the [[VacuumProtocolCheckTableFeature]], we don't need remove any traces, we
   * just need to remove the feature from the [[Protocol]].
   */
  override def removeFeatureTracesIfNeeded(): Boolean = false
}

case class CoordinatedCommitsPreDowngradeCommand(table: DeltaTableV2)
  extends PreDowngradeTableFeatureCommand
  with DeltaLogging {

  /**
   * We disable the feature by removing the following table properties:
   *    1. DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.key
   *    2. DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.key
   *    3. DeltaConfigs.COORDINATED_COMMITS_TABLE_CONF.key
   * If these properties have been removed but unbackfilled commits are still present, we
   * backfill them.
   *
   * @return true if any change to the metadata (the three properties listed above) was made OR
   *         if there were any unbackfilled commits that were backfilled.
   *         false otherwise.
   */
  override def removeFeatureTracesIfNeeded(): Boolean = {
    val startTimeNs = System.nanoTime()

    var traceRemovalNeeded = false
    var exceptionOpt = Option.empty[Throwable]
    val propertyPresenceLogs = CoordinatedCommitsUtils.TABLE_PROPERTY_KEYS.map( key =>
      key -> table.initialSnapshot.metadata.configuration.contains(key).toString
    )
    if (CoordinatedCommitsUtils.tablePropertiesPresent(table.initialSnapshot.metadata)) {
      traceRemovalNeeded = true
      try {
        AlterTableUnsetPropertiesDeltaCommand(
          table, CoordinatedCommitsUtils.TABLE_PROPERTY_KEYS, ifExists = true).run(table.spark)
      } catch {
        case NonFatal(e) =>
          exceptionOpt = Some(e)
      }
    }
    var postDisablementUnbackfilledCommitsPresent = false
    if (exceptionOpt.isEmpty) {
      val snapshotAfterDisabling = table.deltaLog.update()
      assert(snapshotAfterDisabling.tableCommitCoordinatorClientOpt.isEmpty)
      postDisablementUnbackfilledCommitsPresent =
        CoordinatedCommitsUtils.unbackfilledCommitsPresent(snapshotAfterDisabling)
      if (postDisablementUnbackfilledCommitsPresent) {
        traceRemovalNeeded = true
        // Coordinated commits have already been disabled but there are unbackfilled commits.
        CoordinatedCommitsUtils.backfillWhenCoordinatedCommitsDisabled(snapshotAfterDisabling)
      }
    }
    recordDeltaEvent(
      table.deltaLog,
      opType = "delta.coordinatedCommitsFeatureRemovalMetrics",
      data = Map(
          "downgradeTimeMs" -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs),
          "traceRemovalNeeded" -> traceRemovalNeeded.toString,
          "traceRemovalSuccess" -> exceptionOpt.isEmpty.toString,
          "traceRemovalException" -> exceptionOpt.map(_.getMessage).getOrElse(""),
          "postDisablementUnbackfilledCommitsPresent" ->
            postDisablementUnbackfilledCommitsPresent.toString
      ) ++ propertyPresenceLogs
    )
    exceptionOpt.foreach(throw _)
    traceRemovalNeeded
  }
}

case class TypeWideningPreDowngradeCommand(table: DeltaTableV2)
  extends PreDowngradeTableFeatureCommand
  with DeltaLogging {

  /**
   * Unset the type widening table property to prevent new type changes to be applied to the table,
   * then removes traces of the feature:
   * - Rewrite files that have columns or fields with a different type than in the current table
   *   schema. These are all files not added or modified after the last type change.
   * - Remove the type widening metadata attached to fields in the current table schema.
   *
   * @return Return true if files were rewritten or metadata was removed. False otherwise.
   */
  override def removeFeatureTracesIfNeeded(): Boolean = {
    if (TypeWideningTableFeature.validateRemoval(table.initialSnapshot)) return false

    val startTimeNs = System.nanoTime()
    val properties = Seq(DeltaConfigs.ENABLE_TYPE_WIDENING.key)
    AlterTableUnsetPropertiesDeltaCommand(table, properties, ifExists = true).run(table.spark)
    val numFilesRewritten = rewriteFilesIfNeeded()
    val metadataRemoved = removeMetadataIfNeeded()

    recordDeltaEvent(
      table.deltaLog,
      opType = "delta.typeWidening.featureRemoval",
      data = Map(
        "downgradeTimeMs" -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs),
        "numFilesRewritten" -> numFilesRewritten,
        "metadataRemoved" -> metadataRemoved
        )
    )
    numFilesRewritten > 0 || metadataRemoved
  }

  /**
   * Rewrite files that have columns or fields with a different type than in the current table
   * schema. These are all files not added or modified after the last type change.
   * @return Return the number of files rewritten.
   */
  private def rewriteFilesIfNeeded(): Long = {
    if (!TypeWideningMetadata.containsTypeWideningMetadata(table.initialSnapshot.schema)) {
      return 0L
    }

    // Wrap `table` in a ResolvedTable that can be passed to DeltaReorgTableCommand. The catalog &
    // table ID won't be used by DeltaReorgTableCommand.
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    val catalog = table.spark.sessionState.catalogManager.currentCatalog.asTableCatalog
    val tableId = Seq(table.name()).asIdentifier

    val reorg = DeltaReorgTableCommand(
      target = ResolvedTable.create(catalog, tableId, table),
      reorgTableSpec = DeltaReorgTableSpec(DeltaReorgTableMode.REWRITE_TYPE_WIDENING, None)
    )(Nil)

    val rows = reorg.run(table.spark)
    val metrics = rows.head.getAs[OptimizeMetrics](1)
    metrics.numFilesRemoved
  }

  /**
   * Remove the type widening metadata attached to fields in the current table schema.
   * @return Return true if any metadata was removed. False otherwise.
   */
  private def removeMetadataIfNeeded(): Boolean = {
    if (!TypeWideningMetadata.containsTypeWideningMetadata(table.initialSnapshot.schema)) {
      return false
    }

    val txn = table.startTransaction()
    val metadata = txn.metadata
    val (cleanedSchema, changes) =
      TypeWideningMetadata.removeTypeWideningMetadata(metadata.schema)
    txn.commit(
      metadata.copy(schemaString = cleanedSchema.json) :: Nil,
      DeltaOperations.UpdateColumnMetadata("DROP FEATURE", changes))
    true
  }
}
case class ColumnMappingPreDowngradeCommand(table: DeltaTableV2)
  extends PreDowngradeTableFeatureCommand
    with DeltaLogging {

  /**
   * We first remove the table feature property to prevent any transactions from writting data
   * files with the physical names. This will cause any concurrent transactions to fail.
   * Then, we run RemoveColumnMappingCommand to rewrite the files rename columns.
   * Note, during the protocol downgrade phase we validate whether all invariants still hold.
   * This should detect if any concurrent txns enabled the table property again.
   *
   * @return Returns true if it removed table property and/or has rewritten the data.
   *         False otherwise.
   */
  override def removeFeatureTracesIfNeeded(): Boolean = {
    val spark = table.spark

    // Latest snapshot looks clean. No action is required. We may proceed
    // to the protocol downgrade phase.
    if (ColumnMappingTableFeature.validateRemoval(table.initialSnapshot)) return false

    recordDeltaOperation(
      table.deltaLog,
      opType = "delta.columnMappingFeatureRemoval") {
      RemoveColumnMappingCommand(table.deltaLog, table.catalogTable)
        .run(spark, removeColumnMappingTableProperty = true)
    }
    true
  }
}

case class CheckConstraintsPreDowngradeTableFeatureCommand(table: DeltaTableV2)
    extends PreDowngradeTableFeatureCommand {

  /**
   * Throws an exception if the table has CHECK constraints, and returns false otherwise (as no
   * action was required).
   *
   * We intentionally error out instead of removing the CHECK constraints here, as dropping a
   * table feature should not never alter the logical representation of a table (only its physical
   * representation). Instead, we ask the user to explicitly drop the constraints before the table
   * feature can be dropped.
   */
  override def removeFeatureTracesIfNeeded(): Boolean = {
    val checkConstraintNames = Constraints.getCheckConstraintNames(table.initialSnapshot.metadata)
    if (checkConstraintNames.isEmpty) return false
    throw DeltaErrors.cannotDropCheckConstraintFeature(checkConstraintNames)
  }
}
