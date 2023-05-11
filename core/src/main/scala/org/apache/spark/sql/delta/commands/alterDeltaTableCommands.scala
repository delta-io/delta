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

import scala.util.control.NonFatal

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.constraints.{CharVarcharConstraint, Constraints}
import org.apache.spark.sql.delta.schema.{SchemaMergingUtils, SchemaUtils}
import org.apache.spark.sql.delta.schema.SchemaUtils.transformColumnsStructs
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{AnalysisException, Column, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{IgnoreCachedData, QualifiedColType}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.catalog.TableChange.{After, ColumnPosition, First}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types._

/**
 * A super trait for alter table commands that modify Delta tables.
 */
trait AlterDeltaTableCommand extends DeltaCommand {

  def table: DeltaTableV2

  protected def startTransaction(spark: SparkSession): OptimisticTransaction = {
    val txn = table.deltaLog.startTransaction()
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
      protocol: Protocol,
      operationName: String): Unit = {
    if (!sparkSession.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_ALTER_TABLE_CHANGE_COLUMN_CHECK_EXPRESSIONS)) {
      return
    }
    // check if the column to change is referenced by check constraints
    val dependentConstraints =
      Constraints.findDependentConstraints(sparkSession, columnParts, newMetadata)
    if (dependentConstraints.nonEmpty) {
      throw DeltaErrors.foundViolatingConstraintsForColumnChange(
        operationName, UnresolvedAttribute(columnParts).name, dependentConstraints)
    }
    // check if the column to change is referenced by any generated columns
    val dependentGenCols = SchemaUtils.findDependentGeneratedColumns(
      sparkSession, columnParts, protocol, newMetadata.schema)
    if (dependentGenCols.nonEmpty) {
      throw DeltaErrors.foundViolatingGeneratedColumnsForColumnChange(
        operationName, UnresolvedAttribute(columnParts).name, dependentGenCols.toList)
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
    recordDeltaOperation(deltaLog, "delta.ddl.alter.setProperties") {
      val txn = startTransaction(sparkSession)

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
        case _ =>
          true
      }
      val newMetadata = metadata.copy(
        description = configuration.getOrElse(TableCatalog.PROP_COMMENT, metadata.description),
        configuration = metadata.configuration ++ filteredConfs)

      txn.updateMetadata(newMetadata)

      txn.commit(Nil, DeltaOperations.SetTableProperties(configuration))

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
    recordDeltaOperation(deltaLog, "delta.ddl.alter.unsetProperties") {
      val txn = startTransaction(sparkSession)
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
      val txn = startTransaction(sparkSession)

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

        Some((col.name.init, field, col.position.map(toV2Position)))
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
      val txn = startTransaction(sparkSession)
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

      val newMetadata = metadata.copy(schemaString = newSchema.json)

      columnsToDrop.foreach { columnParts =>
        checkDependentExpressions(sparkSession, columnParts, newMetadata, txn.protocol, "drop")
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
      val txn = startTransaction(sparkSession)
      val metadata = txn.metadata
      val oldSchema = metadata.schema
      val resolver = sparkSession.sessionState.conf.resolver

      // Verify that the columnName provided actually exists in the schema
      SchemaUtils.findColumnPosition(columnPath :+ columnName, oldSchema, resolver)

      val newSchema = transformColumnsStructs(oldSchema, Some(columnName)) {
        case (`columnPath`, struct @ StructType(fields), _) =>
          val oldColumn = struct(columnName)
          verifyColumnChange(sparkSession, struct(columnName), resolver, txn)

          val newField = {
              // Take the name, comment, nullability and data type from newField
              // It's crucial to keep the old column's metadata, which may contain column mapping
              // metadata.
              var result = newColumn.getComment().map(oldColumn.withComment).getOrElse(oldColumn)
              result
                .copy(
                  name = newColumn.name,
                  dataType =
                    SchemaUtils.changeDataType(oldColumn.dataType, newColumn.dataType, resolver),
                  nullable = newColumn.nullable)
          }

          // Replace existing field with new field
          val newFieldList = fields.map { field =>
            if (DeltaColumnMapping.getPhysicalName(field) ==
              DeltaColumnMapping.getPhysicalName(newField)) {
              newField
            } else field
          }

          // Reorder new field to correct position if necessary
          colPosition.map { position =>
            reorderFieldList(struct, newFieldList, newField, position, resolver)
          }.getOrElse(newFieldList.toSeq)

        case (_, _ @ StructType(fields), _) => fields
      }

      // update `partitionColumns` if the changed column is a partition column
      val newPartitionColumns = if (columnPath.isEmpty) {
        metadata.partitionColumns.map { partCol =>
          if (partCol == columnName) newColumn.name else partCol
        }
      } else metadata.partitionColumns

      val newMetadata = metadata.copy(
        schemaString = newSchema.json, partitionColumns = newPartitionColumns)

      if (newColumn.name != columnName) {
        // need to validate the changes if the column is renamed
        checkDependentExpressions(
          sparkSession, columnPath :+ columnName, newMetadata, txn.protocol, "rename")
      }


      txn.updateMetadata(newMetadata)

      if (newColumn.name != columnName) {
        // record column rename separately
        txn.commit(Nil, DeltaOperations.RenameColumn(
          columnPath :+ columnName,
          columnPath :+ newColumn.name))
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
    if (SchemaUtils.canChangeDataType(originalField.dataType, newType, resolver,
        txn.metadata.columnMappingMode, columnPath :+ originalField.name).nonEmpty) {
      throw DeltaErrors.alterTableChangeColumnException(
        s"'${UnresolvedAttribute(columnPath :+ originalField.name).name}' with type " +
          s"'${originalField.dataType}" +
          s" (nullable = ${originalField.nullable})'",
        s"'${UnresolvedAttribute(Seq(newColumn.name)).name}' with type " +
          s"'$newType" +
          s" (nullable = ${newColumn.nullable})'")
    }

    if (columnName != newColumn.name) {
      if (txn.metadata.columnMappingMode == NoMapping) {
        throw DeltaErrors.columnRenameNotSupported
      }
    }

    if (originalField.nullable && !newColumn.nullable) {
      throw DeltaErrors.alterTableChangeColumnException(
        s"'${UnresolvedAttribute(columnPath :+ originalField.name).name}' with type " +
          s"'${originalField.dataType}" +
          s" (nullable = ${originalField.nullable})'",
        s"'${UnresolvedAttribute(Seq(newColumn.name)).name}' with type " +
          s"'${newColumn.dataType}" +
          s" (nullable = ${newColumn.nullable})'")
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

    val newTable = DeltaLog.forTable(sparkSession, location).update()
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
      val txn = startTransaction(sparkSession)

      getConstraintWithName(table, name, txn.metadata, sparkSession).foreach { oldExpr =>
        throw DeltaErrors.constraintAlreadyExists(name, oldExpr)
      }

      val newMetadata = txn.metadata.copy(
        configuration = txn.metadata.configuration +
          (Constraints.checkConstraintPropertyName(name) -> exprText)
      )

      val expr = sparkSession.sessionState.sqlParser.parseExpression(exprText)
      if (expr.dataType != BooleanType) {
        throw DeltaErrors.checkConstraintNotBoolean(name, exprText)
      }
      logInfo(s"Checking that $exprText is satisfied for existing data. " +
        "This will require a full table scan.")
      recordDeltaOperation(
          txn.snapshot.deltaLog,
          "delta.ddl.alter.addConstraint.checkExisting") {
        val df = txn.snapshot.deltaLog.createDataFrame(txn.snapshot, txn.filterFiles())
        val n = df.where(new Column(Or(Not(expr), IsUnknown(expr)))).count()

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
      val txn = startTransaction(sparkSession)

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

