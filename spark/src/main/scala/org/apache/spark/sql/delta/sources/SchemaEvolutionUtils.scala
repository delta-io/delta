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

package org.apache.spark.sql.delta.sources

import java.util.Locale

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.schema.SchemaUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
 * Stateless utility methods for detecting, classifying, and validating non-additive schema changes
 * in Delta streaming sources. This object is shared by both the V1 (DeltaSource) and V2
 * (SparkMicroBatchStream) streaming source implementations to avoid duplicating schema evolution
 * logic.
 *
 * Non-additive schema changes include:
 * - Column renames (detected via column mapping physical names)
 * - Column drops
 * - Type widening (e.g., int -> long)
 *
 * These utilities handle:
 * 1. Classifying which type(s) of non-additive change occurred
 * 2. Checking whether the user has unblocked the change via SQL confs or reader options
 * 3. Validating that arbitrary (non-widening) type changes are rejected
 */
object SchemaEvolutionUtils {

  /** SQL configs that allow unblocking each type of schema changes. */
  private val SQL_CONF_PREFIX = s"${DeltaSQLConf.SQL_CONF_PREFIX}.streaming"

  private[sources] final val SQL_CONF_UNBLOCK_RENAME_DROP =
    SQL_CONF_PREFIX + ".allowSourceColumnRenameAndDrop"
  private[sources] final val SQL_CONF_UNBLOCK_RENAME =
    SQL_CONF_PREFIX + ".allowSourceColumnRename"
  private[sources] final val SQL_CONF_UNBLOCK_DROP =
    SQL_CONF_PREFIX + ".allowSourceColumnDrop"
  private[sources] final val SQL_CONF_UNBLOCK_TYPE_CHANGE =
    SQL_CONF_PREFIX + ".allowSourceColumnTypeChange"

  /**
   * Defining the different combinations of non-additive schema changes to detect them and allow
   * users to vet and unblock them using a corresponding SQL conf or reader option:
   * - dropping columns
   * - renaming columns
   * - widening data types
   */
  private[sources] sealed trait SchemaChangeType {
    val name: String
    val isRename: Boolean
    val isDrop: Boolean
    val isTypeWidening: Boolean
    val sqlConfsUnblock: Seq[String]
    val readerOptionsUnblock: Seq[String]
    val prettyColumnDetailsString: String

    protected def getRenamedColumnsPrettyString(renamedColumns: Seq[RenamedColumn]): String = {
      s"""Columns renamed:
         |${renamedColumns.map { case RenamedColumn(fromFieldPath, toFieldPath) =>
          s"'${SchemaUtils.prettyFieldName(fromFieldPath)}' -> " +
            s"'${SchemaUtils.prettyFieldName(toFieldPath)}'"
         }.mkString("\n")}
         |""".stripMargin
    }

    protected def getDroppedColumnsPrettyString(droppedColumns: Seq[DroppedColumn]): String = {
      s"""Columns dropped:
         |${droppedColumns.map(
            c => s"'${SchemaUtils.prettyFieldName(c.fieldPath)}'").mkString(", ")}
         |""".stripMargin
    }

    protected def getWidenedColumnsPrettyString(widenedColumns: Seq[TypeChange]): String = {
      s"""Columns with widened types:
         |${widenedColumns.map { case TypeChange(_, fromType, toType, fieldPath) =>
          s"'${SchemaUtils.prettyFieldName(fieldPath)}': ${fromType.sql} -> ${toType.sql}"
         }.mkString("\n")}
         |""".stripMargin
    }
  }

  // Single types of schema change, typically caused by a single ALTER TABLE operation.
  private case class SchemaChangeRename(renamedColumns: Seq[RenamedColumn])
      extends SchemaChangeType {
    override val name = "RENAME COLUMN"
    override val (isRename, isDrop, isTypeWidening) = (true, false, false)
    override val sqlConfsUnblock: Seq[String] = Seq(SQL_CONF_UNBLOCK_RENAME)
    override val readerOptionsUnblock: Seq[String] = Seq(DeltaOptions.ALLOW_SOURCE_COLUMN_RENAME)
    override val prettyColumnDetailsString: String = getRenamedColumnsPrettyString(renamedColumns)
  }
  private case class SchemaChangeDrop(droppedColumns: Seq[DroppedColumn]) extends SchemaChangeType {
    override val name = "DROP COLUMN"
    override val (isRename, isDrop, isTypeWidening) = (false, true, false)
    override val sqlConfsUnblock: Seq[String] = Seq(SQL_CONF_UNBLOCK_DROP)
    override val readerOptionsUnblock: Seq[String] = Seq(DeltaOptions.ALLOW_SOURCE_COLUMN_DROP)
    override val prettyColumnDetailsString: String = getDroppedColumnsPrettyString(droppedColumns)
  }
  private case class SchemaChangeTypeWidening(widenedColumns: Seq[TypeChange])
      extends SchemaChangeType {
    override val name = "TYPE WIDENING"
    override val (isRename, isDrop, isTypeWidening) = (false, false, true)
    override val sqlConfsUnblock: Seq[String] = Seq(SQL_CONF_UNBLOCK_TYPE_CHANGE)
    override val readerOptionsUnblock: Seq[String] =
      Seq(DeltaOptions.ALLOW_SOURCE_COLUMN_TYPE_CHANGE)
    override val prettyColumnDetailsString: String = getWidenedColumnsPrettyString(widenedColumns)
  }

  // Combinations of rename, drop and type change -> can be caused by a complete overwrite.
  private case class SchemaChangeRenameAndDrop(
      renamedColumns: Seq[RenamedColumn],
      droppedColumns: Seq[DroppedColumn]) extends SchemaChangeType {
    override val name = "RENAME AND DROP COLUMN"
    override val (isRename, isDrop, isTypeWidening) = (true, true, false)
    override val sqlConfsUnblock: Seq[String] = Seq(SQL_CONF_UNBLOCK_RENAME_DROP)
    override val readerOptionsUnblock: Seq[String] =
      Seq(DeltaOptions.ALLOW_SOURCE_COLUMN_RENAME, DeltaOptions.ALLOW_SOURCE_COLUMN_DROP)
    override val prettyColumnDetailsString: String =
      getRenamedColumnsPrettyString(renamedColumns) + getDroppedColumnsPrettyString(droppedColumns)
  }
  private case class SchemaChangeRenameAndTypeWidening(
      renamedColumns: Seq[RenamedColumn],
      widenedColumns: Seq[TypeChange]) extends SchemaChangeType {
    override val name = "RENAME AND TYPE WIDENING"
    override val (isRename, isDrop, isTypeWidening) = (true, false, true)
    override val sqlConfsUnblock: Seq[String] =
      Seq(SQL_CONF_UNBLOCK_RENAME, SQL_CONF_UNBLOCK_TYPE_CHANGE)
    override val readerOptionsUnblock: Seq[String] =
      Seq(DeltaOptions.ALLOW_SOURCE_COLUMN_RENAME, DeltaOptions.ALLOW_SOURCE_COLUMN_DROP)
    override val prettyColumnDetailsString: String =
      getRenamedColumnsPrettyString(renamedColumns) + getWidenedColumnsPrettyString(widenedColumns)
  }
  private case class SchemaChangeDropAndTypeWidening(
      droppedColumns: Seq[DroppedColumn],
      widenedColumns: Seq[TypeChange]) extends SchemaChangeType {
    override val name = "DROP AND TYPE WIDENING"
    override val (isRename, isDrop, isTypeWidening) = (false, true, true)
    override val sqlConfsUnblock: Seq[String] =
      Seq(SQL_CONF_UNBLOCK_DROP, SQL_CONF_UNBLOCK_TYPE_CHANGE)
    override val readerOptionsUnblock: Seq[String] =
      Seq(DeltaOptions.ALLOW_SOURCE_COLUMN_DROP, DeltaOptions.ALLOW_SOURCE_COLUMN_TYPE_CHANGE)
    override val prettyColumnDetailsString: String =
      getDroppedColumnsPrettyString(droppedColumns) + getWidenedColumnsPrettyString(widenedColumns)
  }
  private case class SchemaChangeRenameAndDropAndTypeWidening(
      renamedColumns: Seq[RenamedColumn],
      droppedColumns: Seq[DroppedColumn],
      widenedColumns: Seq[TypeChange]) extends SchemaChangeType {
    override val name = "RENAME, DROP AND TYPE WIDENING"
    override val (isRename, isDrop, isTypeWidening) = (true, true, true)
    override val sqlConfsUnblock: Seq[String] =
      Seq(SQL_CONF_UNBLOCK_RENAME_DROP, SQL_CONF_UNBLOCK_TYPE_CHANGE)
    override val readerOptionsUnblock: Seq[String] =
      Seq(DeltaOptions.ALLOW_SOURCE_COLUMN_DROP, DeltaOptions.ALLOW_SOURCE_COLUMN_TYPE_CHANGE)
    override val prettyColumnDetailsString: String =
      getRenamedColumnsPrettyString(renamedColumns) +
        getDroppedColumnsPrettyString(droppedColumns) +
        getWidenedColumnsPrettyString(widenedColumns)
  }

  /**
   * Build the final schema change descriptor after analyzing all possible schema changes.
   * @param renamedColumns The columns that have been renamed.
   * @param droppedColumns The columns that have been dropped.
   * @param widenedColumns The columns that have been widened.
   */
  private def buildSchemaChangeDescriptor(
      renamedColumns: Seq[RenamedColumn],
      droppedColumns: Seq[DroppedColumn],
      widenedColumns: Seq[TypeChange]): Option[SchemaChangeType] = {
    (renamedColumns.nonEmpty, droppedColumns.nonEmpty, widenedColumns.nonEmpty) match {
      case (true, false, false) =>
        Some(SchemaChangeRename(renamedColumns))
      case (false, true, false) =>
        Some(SchemaChangeDrop(droppedColumns))
      case (false, false, true) =>
        Some(SchemaChangeTypeWidening(widenedColumns))
      case (true, true, false) =>
        Some(SchemaChangeRenameAndDrop(renamedColumns, droppedColumns))
      case (true, false, true) =>
        Some(SchemaChangeRenameAndTypeWidening(renamedColumns, widenedColumns))
      case (false, true, true) =>
        Some(SchemaChangeDropAndTypeWidening(droppedColumns, widenedColumns))
      case (true, true, true) =>
        Some(
          SchemaChangeRenameAndDropAndTypeWidening(renamedColumns, droppedColumns, widenedColumns))
      case _ => None
    }
  }

  /**
   * Determine the non-additive schema change type for an incoming schema change. None if it's
   * additive.
   */
  private[sources] def determineNonAdditiveSchemaChangeType(
      spark: SparkSession,
      newSchema: StructType, oldSchema: StructType): Option[SchemaChangeType] = {
    val renamedColumns = DeltaColumnMapping.collectRenamedColumns(newSchema, oldSchema)
    val droppedColumns = DeltaColumnMapping.collectDroppedColumns(newSchema, oldSchema)
    // Use physical column names to identify type changes. Dropping a column and adding a new column
    // with a different type is historically allowed and is not considered a type change.
    val oldPhysicalSchema = DeltaColumnMapping.renameColumns(oldSchema)
    val newPhysicalSchema = DeltaColumnMapping.renameColumns(newSchema)
    // Check if there are widening type changes. This assumes [[checkIncompatibleSchemaChange]] was
    // already called before and failed if there were any non-widening type changes. The type change
    // checks - both widening and non-widening - can be disabled by flag to revert to historical
    // behavior where type changes are not considered a non-additive schema change and are allowed
    // to propagate without user action.
    val typeWideningChanges = if (allowTypeWidening(spark) && !bypassTypeChangeCheck(spark)) {
      TypeWideningMetadata.collectTypeChanges(oldPhysicalSchema, newPhysicalSchema)
    } else Seq.empty
    buildSchemaChangeDescriptor(renamedColumns, droppedColumns, typeWideningChanges)
  }

  /**
   * Returns whether the given type of non-additive schema change was unblocked by setting one of
   * the corresponding SQL confs or reader options.
   */
  private[sources] def isChangeUnblocked(
      spark: SparkSession,
      change: SchemaChangeType,
      options: DeltaOptions,
      checkpointHash: Int,
      schemaChangeVersion: Long): Boolean = {

    def isUnblockedBySQLConf(sqlConf: String): Boolean = {
      def getConf(key: String): Option[String] =
        Option(spark.sessionState.conf.getConfString(key, null))
          .map(_.toLowerCase(Locale.ROOT))
      val validConfKeysValuePair = Seq(
        (sqlConf, "always"),
        (s"$sqlConf.ckpt_$checkpointHash", "always"),
        (s"$sqlConf.ckpt_$checkpointHash", schemaChangeVersion.toString)
      )
      validConfKeysValuePair.exists(p => getConf(p._1).contains(p._2))
    }

    def isUnblockedByReaderOption(readerOption: Option[String]): Boolean = {
      readerOption.contains("always") || readerOption.contains(schemaChangeVersion.toString)
    }

    val isBlockedRename = change.isRename &&
      !isUnblockedByReaderOption(options.allowSourceColumnRename) &&
      !isUnblockedBySQLConf(SQL_CONF_UNBLOCK_RENAME) &&
      !isUnblockedBySQLConf(SQL_CONF_UNBLOCK_RENAME_DROP)
    val isBlockedDrop = change.isDrop &&
      !isUnblockedByReaderOption(options.allowSourceColumnDrop) &&
      !isUnblockedBySQLConf(SQL_CONF_UNBLOCK_DROP) &&
      !isUnblockedBySQLConf(SQL_CONF_UNBLOCK_RENAME_DROP)
    val isBlockedTypeChange = change.isTypeWidening &&
      !isUnblockedByReaderOption(options.allowSourceColumnTypeChange) &&
      !isUnblockedBySQLConf(SQL_CONF_UNBLOCK_TYPE_CHANGE)

    !isBlockedRename && !isBlockedDrop && !isBlockedTypeChange
  }

  def getCheckpointHash(path: String): Int = path.hashCode

  /**
   * Whether to accept widening type changes:
   *   - when true, widening type changes cause the stream to fail, requesting user to review and
   *     unblock them via a SQL conf or reader option.
   *   - when false, widening type changes are rejected without possibility to unblock, similar to
   *     any other arbitrary type change.
   */
  def allowTypeWidening(spark: SparkSession): Boolean = {
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_ALLOW_TYPE_WIDENING_STREAMING_SOURCE)
  }

  /**
   * We historically allowed any type changes to go through when schema tracking was enabled. This
   * config allows reverting to that behavior.
   */
  def bypassTypeChangeCheck(spark: SparkSession): Boolean =
    spark.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_TYPE_WIDENING_BYPASS_STREAMING_TYPE_CHANGE_CHECK)

  // scalastyle:off
  /**
   * Given a non-additive operation type from a previous schema evolution, check we can process
   * using the new schema given any SQL conf or dataframe reader option users have explicitly set to
   * unblock.
   * The SQL conf can take one of following formats:
   * 1. spark.databricks.delta.streaming.allowSourceColumn$action = "always"
   *    -> allows non-additive schema change to propagate for all streams.
   * 2. spark.databricks.delta.streaming.allowSourceColumn$action.$checkpointHash = "always"
   *    -> allows non-additive schema change to propagate for this particular stream.
   * 3. spark.databricks.delta.streaming.allowSourceColumn$action.$checkpointHash = $deltaVersion
   *    -> allow non-additive schema change to propagate only for this particular stream source
   *        table version.
   * The reader options can take one of the following format:
   * 1.  .option("allowSourceColumn$action", "always")
   *    -> allows non-additive schema change to propagate for this particular stream.
   * 2.  .option("allowSourceColumn$action", "$deltaVersion")
   *    -> allow non-additive schema change to propagate only for this particular stream source
   *        table version.
   * where `allowSourceColumn$action` is one of:
   * 1. `allowSourceColumnRename` to allow column renames.
   * 2. `allowSourceColumnDrop` to allow column drops.
   * 3. `allowSourceColumnTypeChange` to allow widening type changes.
   * For SQL confs only, action can also be `allowSourceColumnRenameAndDrop` to allow both column
   * drops and renames.
   *
   * We will check for any of these configs given the non-additive operation, and throw a proper
   * error message to instruct the user to set the SQL conf / reader options if they would like to
   * unblock.
   *
   * @param spark The active SparkSession
   * @param parameters The reader options as a Map
   * @param metadataPath The path to the source-unique metadata location under checkpoint
   * @param currentSchema The current persisted schema
   * @param previousSchema The previous persisted schema
   */
  // scalastyle:on
  def validateIfSchemaChangeCanBeUnblocked(
      spark: SparkSession,
      parameters: Map[String, String],
      metadataPath: String,
      currentSchema: PersistedMetadata,
      previousSchema: PersistedMetadata): Unit = {
    val options = new DeltaOptions(parameters, spark.sessionState.conf)
    val checkpointHash = getCheckpointHash(metadataPath)

    // The start version of a possible series of consecutive schema changes.
    val previousSchemaChangeVersion = previousSchema.deltaCommitVersion
    // The end version of a possible series of consecutive schema changes.
    val currentSchemaChangeVersion = currentSchema.deltaCommitVersion

    // Fail with a non-retryable exception if there are any type changes that we don't allow
    // unblocking, i.e. non-widening type changes. We do allow changes caused by columns being
    // dropped/renamed, e.g. dropping a column and adding it back with a different type. These were
    // historically allowed and will be surfaced to the user as column drop/rename.
    checkIncompatibleSchemaChange(
      spark,
      previousSchema = previousSchema.dataSchema,
      currentSchema = currentSchema.dataSchema,
      currentSchemaChangeVersion
    )

    determineNonAdditiveSchemaChangeType(
      spark, currentSchema.dataSchema, previousSchema.dataSchema).foreach { change =>
        if (!isChangeUnblocked(
            spark, change, options, checkpointHash, currentSchemaChangeVersion)) {
          // Throw error to prompt user to set the correct confs
          throw DeltaErrors.cannotContinueStreamingPostSchemaEvolution(
            change.name,
            previousSchemaChangeVersion,
            currentSchemaChangeVersion,
            checkpointHash,
            change.readerOptionsUnblock,
            change.sqlConfsUnblock,
            change.prettyColumnDetailsString)
        }
    }
  }

  /**
   * Checks that the new schema only contains column rename/drop and widening type changes compared
   * to the previous schema. That is, rejects any non-widening type changes.
   */
  def checkIncompatibleSchemaChange(
      spark: SparkSession,
      previousSchema: StructType,
      currentSchema: StructType,
      currentSchemaChangeVersion: Long): Unit = {
    if (bypassTypeChangeCheck(spark)) return

    val incompatibleSchema =
      !SchemaUtils.isReadCompatible(
        // We want to ignore renamed/dropped columns here and let the check for non-additive
        // schema changes handle them: we only check if an actual physical column had an
        // incompatible type change.
        existingSchema = DeltaColumnMapping.renameColumns(previousSchema),
        readSchema = DeltaColumnMapping.renameColumns(currentSchema),
        forbidTightenNullability = true,
        allowMissingColumns = true,
        typeWideningMode =
          if (allowTypeWidening(spark)) TypeWideningMode.AllTypeWidening
          else TypeWideningMode.NoTypeWidening
      )
    if (incompatibleSchema) {
      throw DeltaErrors.schemaChangedException(
        previousSchema,
        currentSchema,
        retryable = false,
        Some(currentSchemaChangeVersion),
        includeStartingVersionOrTimestampMessage = false)
    }
  }
}
