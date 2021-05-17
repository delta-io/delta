/*
 * Copyright (2020) The Delta Lake Project Authors.
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
import org.apache.spark.sql.delta.constraints.Constraints
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.schema.SchemaUtils.transformColumnsStructs
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{AnalysisException, Column, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.catalyst.expressions.{Expression, IsNotNull, IsNull, IsUnknown, Not, Or}
import org.apache.spark.sql.catalyst.plans.logical.{IgnoreCachedData, LogicalPlan, QualifiedColType}
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.catalog.TableChange.{After, ColumnPosition, First}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaConverter
import org.apache.spark.sql.types._

/**
 * A super trait for alter table commands that modify Delta tables.
 */
trait AlterDeltaTableCommand extends DeltaCommand {

  def table: DeltaTableV2

  protected def startTransaction(): OptimisticTransaction = {
    val txn = table.deltaLog.startTransaction()
    if (txn.readVersion == -1) {
      throw DeltaErrors.notADeltaTableException(table.name())
    }
    txn
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
  extends RunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = table.deltaLog
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

  // TODO: remove when the new Spark version is releases that has the withNewChildInternal method
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
  extends RunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = table.deltaLog
    recordDeltaOperation(deltaLog, "delta.ddl.alter.unsetProperties") {
      val txn = startTransaction()
      val metadata = txn.metadata

      val normalizedKeys = DeltaConfigs.normalizeConfigKeys(propKeys)
      if (!ifExists) {
        normalizedKeys.foreach { k =>
          if (!metadata.configuration.contains(k)) {
            throw new AnalysisException(
              s"Attempted to unset non-existent property '$k' in table ${table.name()}")
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

  // TODO: remove when the new Spark version is releases that has the withNewChildInternal method
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
  extends RunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

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
          val (parentPosition, lastSize) =
            SchemaUtils.findColumnPosition(columnPath, schema, resolver)
          SchemaUtils.addColumn(schema, column, parentPosition :+ lastSize)
        case (schema, QualifiedColTypeWithPosition(columnPath, column, Some(_: First))) =>
          val (parentPosition, _) = SchemaUtils.findColumnPosition(columnPath, schema, resolver)
          SchemaUtils.addColumn(schema, column, parentPosition :+ 0)
        case (schema,
        QualifiedColTypeWithPosition(columnPath, column, Some(after: After))) =>
          val (prevPosition, _) =
            SchemaUtils.findColumnPosition(columnPath :+ after.column, schema, resolver)
          val position = prevPosition.init :+ (prevPosition.last + 1)
          SchemaUtils.addColumn(schema, column, position)
      }

      SchemaUtils.checkColumnNameDuplication(newSchema, "in adding columns")
      ParquetSchemaConverter.checkFieldNames(SchemaUtils.explodeNestedFieldNames(newSchema))

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

    def unapply(
        col: QualifiedColType): Option[(Seq[String], StructField, Option[ColumnPosition])] = {
      val builder = new MetadataBuilder
      col.comment.foreach(builder.putString("comment", _))

      val field = StructField(col.name.last, col.dataType, col.nullable, builder.build())

      Some((col.name.init, field, col.position))
    }
  }

  // TODO: remove when the new Spark version is releases that has the withNewChildInternal method
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
    colPosition: Option[ColumnPosition])
  extends RunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = table.deltaLog
    recordDeltaOperation(deltaLog, "delta.ddl.alter.changeColumns") {
      val txn = startTransaction()
      val metadata = txn.metadata
      val oldSchema = metadata.schema
      val resolver = sparkSession.sessionState.conf.resolver

      // Verify that the columnName provided actually exists in the schema
      SchemaUtils.findColumnPosition(columnPath :+ columnName, oldSchema, resolver)

      val newSchema = transformColumnsStructs(oldSchema, columnName) {
        case (`columnPath`, struct @ StructType(fields), _) =>
          val oldColumn = struct(columnName)
          verifyColumnChange(struct(columnName), resolver, txn)

          // Take the comment, nullability and data type from newField
          val newField = newColumn.getComment().map(oldColumn.withComment).getOrElse(oldColumn)
            .copy(
              dataType =
                SchemaUtils.changeDataType(oldColumn.dataType, newColumn.dataType, resolver),
              nullable = newColumn.nullable)

          // Replace existing field with new field
          val newFieldList = fields.map { field =>
            if (field.name == columnName) newField else field
          }

          // Reorder new field to correct position if necessary
          colPosition.map { position =>
            reorderFieldList(struct, newFieldList, newField, position, resolver)
          }.getOrElse(newFieldList.toSeq)

        case (_, _ @ StructType(fields), _) => fields
      }

      val newMetadata = metadata.copy(schemaString = newSchema.json)
      txn.updateMetadata(newMetadata)
      txn.commit(Nil, DeltaOperations.ChangeColumn(
        columnPath, columnName, newColumn, colPosition.map(_.toString)))

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
      originalField: StructField,
      resolver: Resolver,
      txn: OptimisticTransaction): Unit = {

    originalField.dataType match {
      case same if same == newColumn.dataType =>
      // just changing comment or position so this is fine
      case s: StructType if s != newColumn.dataType =>
        val fieldName = UnresolvedAttribute(columnPath :+ columnName).name
        throw new AnalysisException(
          s"Cannot update ${table.name()} field $fieldName type: " +
            s"update a struct by adding, deleting, or updating its fields")
      case m: MapType if m != newColumn.dataType =>
        val fieldName = UnresolvedAttribute(columnPath :+ columnName).name
        throw new AnalysisException(
          s"Cannot update ${table.name()} field $fieldName type: " +
            s"update a map by updating $fieldName.key or $fieldName.value")
      case a: ArrayType if a != newColumn.dataType =>
        val fieldName = UnresolvedAttribute(columnPath :+ columnName).name
        throw new AnalysisException(
          s"Cannot update ${table.name()} field $fieldName type: " +
            s"update the element by updating $fieldName.element")
      case _: AtomicType =>
      // update is okay
      case o =>
        throw new AnalysisException(s"Cannot update ${table.name()} field of type $o")
    }

    if (columnName != newColumn.name ||
      SchemaUtils.canChangeDataType(originalField.dataType, newColumn.dataType, resolver,
        columnPath :+ originalField.name).nonEmpty) {
      throw DeltaErrors.alterTableChangeColumnException(
        s"'${UnresolvedAttribute(columnPath :+ originalField.name).name}' with type " +
          s"'${originalField.dataType}" +
          s" (nullable = ${originalField.nullable})'",
        s"'${UnresolvedAttribute(Seq(newColumn.name)).name}' with type " +
          s"'${newColumn.dataType}" +
          s" (nullable = ${newColumn.nullable})'")
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

  // TODO: remove when the new Spark version is releases that has the withNewChildInternal method
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
  extends RunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = table.deltaLog
    recordDeltaOperation(deltaLog, "delta.ddl.alter.replaceColumns") {
      val txn = startTransaction()

      val metadata = txn.metadata
      val existingSchema = metadata.schema

      val resolver = sparkSession.sessionState.conf.resolver
      val changingSchema = StructType(columns)

      SchemaUtils.canChangeDataType(existingSchema, changingSchema, resolver).foreach { operation =>
        throw DeltaErrors.alterTableReplaceColumnsException(
          existingSchema, changingSchema, operation)
      }

      val newSchema = SchemaUtils.changeDataType(existingSchema, changingSchema, resolver)
        .asInstanceOf[StructType]

      SchemaUtils.checkColumnNameDuplication(newSchema, "in replacing columns")
      ParquetSchemaConverter.checkFieldNames(SchemaUtils.explodeNestedFieldNames(newSchema))

      val newMetadata = metadata.copy(schemaString = newSchema.json)
      txn.updateMetadata(newMetadata)
      txn.commit(Nil, DeltaOperations.ReplaceColumns(columns))

      Seq.empty[Row]
    }
  }

  // TODO: remove when the new Spark version is releases that has the withNewChildInternal method
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
  extends RunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val identifier = sparkSession.sessionState.sqlParser
      .parseTableIdentifier(table.tableIdentifier.get)
    val catalogTable = catalog.getTableMetadata(identifier)
    val locUri = CatalogUtils.stringToURI(location)
    val oldTable = table.deltaLog.update()
    if (oldTable.version == -1) {
      throw DeltaErrors.notADeltaTableException(table.name())
    }
    val oldMetadata = oldTable.metadata

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
    catalog.alterTable(catalogTable.withNewStorage(locationUri = Some(locUri)))

    Seq.empty[Row]
  }

  private def schemasEqual(
      oldMetadata: actions.Metadata, newMetadata: actions.Metadata): Boolean = {
    oldMetadata.schema == newMetadata.schema &&
      oldMetadata.partitionSchema == newMetadata.partitionSchema
  }

  // TODO: remove when the new Spark version is releases that has the withNewChildInternal method
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
  extends RunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = table.deltaLog
    recordDeltaOperation(deltaLog, "delta.ddl.alter.addConstraint") {
      val txn = startTransaction()

      Constraints.getExprTextByName(name, txn.metadata, sparkSession).foreach { oldExpr =>
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

  // TODO: remove when the new Spark version is releases that has the withNewChildInternal method
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
    name: String)
  extends RunnableCommand with AlterDeltaTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = table.deltaLog
    recordDeltaOperation(deltaLog, "delta.ddl.alter.dropConstraint") {
      val txn = startTransaction()

      val oldExprText = Constraints.getExprTextByName(name, txn.metadata, sparkSession)
      val newMetadata = txn.metadata.copy(
        configuration = txn.metadata.configuration - Constraints.checkConstraintPropertyName(name))

      txn.commit(newMetadata :: Nil, DeltaOperations.DropConstraint(name, oldExprText))
    }

    Seq()
  }

  // TODO: remove when the new Spark version is releases that has the withNewChildInternal method
}
