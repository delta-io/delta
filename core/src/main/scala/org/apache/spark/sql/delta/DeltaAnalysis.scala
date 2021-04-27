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

package org.apache.spark.sql.delta

import scala.collection.JavaConverters._


// scalastyle:off import.ordering.noEmptyLine

import org.apache.spark.sql.delta.DeltaErrors.{TemporallyUnstableInputException, TimestampEarlierThanCommitRetentionException}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.constraints.{AddConstraint, DropConstraint}
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaDataSource
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{CannotReplaceMissingTableException, EliminateSubqueryAliases, UnresolvedRelation}
import org.apache.spark.sql.catalyst.analysis.UnresolvedTableValuedFunction
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.delta._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Analysis rules for Delta. Currently, these rules enable schema enforcement / evolution with
 * INSERT INTO.
 */
class DeltaAnalysis(session: SparkSession, conf: SQLConf)
  extends Rule[LogicalPlan] with AnalysisHelper with DeltaLogging {

  import session.sessionState.analyzer.SessionCatalogAndIdentifier

  type CastFunction = (Expression, DataType) => Expression

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    // INSERT INTO by ordinal
    case a @ AppendDelta(r, d) if !a.isByName &&
        needsSchemaAdjustment(d.name(), a.query, r.schema) =>
      val projection = resolveQueryColumnsByOrdinal(a.query, r.output, d.name())
      if (projection != a.query) {
        val cleanedTable = r.copy(output = r.output.map(CharVarcharUtils.cleanAttrMetadata))
        a.copy(query = projection, table = cleanedTable)
      } else {
        a
      }


    // INSERT OVERWRITE by ordinal
    case o @ OverwriteDelta(r, d) if !o.isByName &&
        needsSchemaAdjustment(d.name(), o.query, r.schema) =>
      val projection = resolveQueryColumnsByOrdinal(o.query, r.output, d.name())
      if (projection != o.query) {
        val aliases = AttributeMap(o.query.output.zip(projection.output).collect {
          case (l: AttributeReference, r: AttributeReference) if !l.sameRef(r) => (l, r)
        })
        val newDeleteExpr = o.deleteExpr.transformUp {
          case a: AttributeReference => aliases.getOrElse(a, a)
        }
        val cleanedTable = r.copy(output = r.output.map(CharVarcharUtils.cleanAttrMetadata))
        o.copy(deleteExpr = newDeleteExpr, query = projection, table = cleanedTable)
      } else {
        o
      }


    // Pull out the partition filter that may be part of the FileIndex. This can happen when someone
    // queries a Delta table such as spark.read.format("delta").load("/some/table/partition=2")
    case l @ DeltaTable(index: TahoeLogFileIndex) if index.partitionFilters.nonEmpty =>
      Filter(
        index.partitionFilters.reduce(And),
        DeltaTableUtils.replaceFileIndex(l, index.copy(partitionFilters = Nil)))


    // This rule falls back to V1 nodes, since we don't have a V2 reader for Delta right now
    case dsv2 @ DataSourceV2Relation(d: DeltaTableV2, _, _, _, options) =>
      DeltaRelation.fromV2Relation(d, dsv2, options)

    // DML - TODO: Remove these Delta-specific DML logical plans and use Spark's plans directly

    case d @ DeleteFromTable(table, condition) if d.childrenResolved =>
      // rewrites Delta from V2 to V1
      val newTarget = stripTempViewWrapper(table).transformUp { case DeltaRelation(lr) => lr }
      val indices = newTarget.collect {
        case DeltaFullTable(index) => index
      }
      if (indices.isEmpty) {
        // Not a Delta table at all, do not transform
        d
      } else if (indices.size == 1 && indices(0).deltaLog.tableExists) {
        // It is a well-defined Delta table with a schema
        DeltaDelete(newTarget, condition)
      } else {
        // Not a well-defined Delta table
        throw DeltaErrors.notADeltaSourceException("DELETE", Some(d))
      }

    case u @ UpdateTable(table, assignments, condition) if u.childrenResolved =>
      val (cols, expressions) = assignments.map(a => a.key -> a.value).unzip
      // rewrites Delta from V2 to V1
      val newTable = stripTempViewWrapper(table).transformUp { case DeltaRelation(lr) => lr }
        newTable.collectLeaves().headOption match {
          case Some(DeltaFullTable(index)) =>
          case o =>
            throw DeltaErrors.notADeltaSourceException("UPDATE", o)
        }
      DeltaUpdateTable(newTable, cols, expressions, condition)

    case m@MergeIntoTable(target, source, condition, matched, notMatched) if m.childrenResolved =>
      val matchedActions = matched.map {
        case update: UpdateAction =>
          DeltaMergeIntoUpdateClause(
            update.condition,
            DeltaMergeIntoClause.toActions(update.assignments))
        case delete: DeleteAction =>
          DeltaMergeIntoDeleteClause(delete.condition)
        case insert =>
          throw new AnalysisException(
            "Insert clauses cannot be part of the WHEN MATCHED clause in MERGE INTO.")
      }
      val notMatchedActions = notMatched.map {
        case insert: InsertAction =>
          DeltaMergeIntoInsertClause(
            insert.condition,
            DeltaMergeIntoClause.toActions(insert.assignments))
        case other =>
          throw new AnalysisException(s"${other.prettyName} clauses cannot be part of the " +
            s"WHEN NOT MATCHED clause in MERGE INTO.")
      }
      // rewrites Delta from V2 to V1
      val newTarget = stripTempViewWrapper(target).transformUp { case DeltaRelation(lr) => lr }
      // Even if we're merging into a non-Delta target, we will catch it later and throw an
      // exception.
      val deltaMerge =
        DeltaMergeInto(newTarget, source, condition, matchedActions ++ notMatchedActions)

      DeltaMergeInto.resolveReferences(deltaMerge, conf)(tryResolveReferences(session))

    case deltaMerge: DeltaMergeInto =>
      val d = if (deltaMerge.childrenResolved && !deltaMerge.resolved) {
        DeltaMergeInto.resolveReferences(deltaMerge, conf)(tryResolveReferences(session))
      } else deltaMerge
      d.copy(target = stripTempViewWrapper(d.target))

    case AlterTableAddConstraintStatement(
          original @ SessionCatalogAndIdentifier(catalog, ident), constraintName, expr) =>
      CatalogV2Util.createAlterTable(
        original,
        catalog,
        ident.namespace() :+ ident.name(),
        Seq(AddConstraint(constraintName, expr)))

    case AlterTableDropConstraintStatement(
        original @ SessionCatalogAndIdentifier(catalog, ident), constraintName) =>
      CatalogV2Util.createAlterTable(
        original,
        catalog,
        ident.namespace() :+ ident.name(),
        Seq(DropConstraint(constraintName)))

  }


  /**
   * Performs the schema adjustment by adding UpCasts (which are safe) and Aliases so that we
   * can check if the by-ordinal schema of the insert query matches our Delta table.
   * The schema adjustment also include string length check if it's written into a char/varchar
   * type column/field.
   */
  private def resolveQueryColumnsByOrdinal(
      query: LogicalPlan, targetAttrs: Seq[Attribute], tblName: String): LogicalPlan = {
    // always add a Cast. it will be removed in the optimizer if it is unnecessary.
    val project = query.output.zipWithIndex.map { case (attr, i) =>
      if (i < targetAttrs.length) {
        val targetAttr = targetAttrs(i)
        addCastToColumn(attr, targetAttr, tblName)
      } else {
        attr
      }
    }
    Project(project, query)
  }

  /**
   * Performs the schema adjustment by adding UpCasts (which are safe) so that we can insert into
   * the Delta table when the input data types doesn't match the table schema. Unlike
   * `resolveQueryColumnsByOrdinal` which ignores the names in `targetAttrs` and maps attributes
   * directly to query output, this method will use the names in the query output to find the
   * corresponding attribute to use. This method also allows users to not provide values for
   * generated columns. If values of any columns are not in the query output, they must be generated
   * columns.
   */
  private def resolveQueryColumnsByName(
      query: LogicalPlan, targetAttrs: Seq[Attribute], deltaTable: DeltaTableV2): LogicalPlan = {
    if (query.output.length < targetAttrs.length) {
      // Some columns are not specified. We don't allow schema evolution in INSERT INTO BY NAME, so
      // we need to ensure the missing columns must be generated columns.
      val userSpecifiedNames =
        if (session.sessionState.conf.caseSensitiveAnalysis) {
          query.output.map(a => (a.name, a)).toMap
        } else {
          CaseInsensitiveMap(query.output.map(a => (a.name, a)).toMap)
        }
      val tableSchema = deltaTable.snapshot.metadata.schema
      if (tableSchema.length != targetAttrs.length) {
        // The target attributes may contain the metadata columns by design. Throwing an exception
        // here in case target attributes may have the metadata columns for Delta in future.
        throw new IllegalStateException(s"The table schema $tableSchema is not consistent with " +
          s"the target attributes: $targetAttrs")
      }
      deltaTable.snapshot.metadata.schema.foreach { tableColumn =>
        if (!userSpecifiedNames.contains(tableColumn.name) &&
          !GeneratedColumn.isGeneratedColumn(deltaTable.snapshot.protocol, tableColumn)) {
          throw DeltaErrors.missingColumnsInInsertInto(tableColumn.name)
        }
      }
    }
    // Spark will resolve columns to make sure specified columns are in the table schema and don't
    // have duplicates. This is just a sanity check.
    assert(
      query.output.length <= targetAttrs.length,
      s"Too many specified columns ${query.output.map(_.name).mkString(", ")}. " +
        s"Table columns: ${targetAttrs.map(_.name).mkString(", ")}")

    val project = query.output.map { attr =>
      val targetAttr = targetAttrs.find(t => session.sessionState.conf.resolver(t.name, attr.name))
        .getOrElse {
          // This is a sanity check. Spark should have done the check.
          throw new AnalysisException(s"Cannot find ${attr.name} in table columns:" +
            s" ${targetAttrs.map(_.name).mkString(", ")}")
        }
      addCastToColumn(attr, targetAttr, deltaTable.name())
    }
    Project(project, query)
  }

  private def addCastToColumn(
      attr: Attribute,
      targetAttr: Attribute,
      tblName: String): NamedExpression = {
    val expr = (attr.dataType, targetAttr.dataType) match {
      case (s, t) if s == t =>
        attr
      case (s: StructType, t: StructType) if s != t =>
        addCastsToStructs(tblName, attr, s, t)
      case _ =>
        getCastFunction(attr, targetAttr.dataType)
    }
    val strLenChecked = CharVarcharUtils.stringLengthCheck(expr, targetAttr)
    Alias(strLenChecked, targetAttr.name)(explicitMetadata = Option(targetAttr.metadata))
  }

  /**
   * With Delta, we ACCEPT_ANY_SCHEMA, meaning that Spark doesn't automatically adjust the schema
   * of INSERT INTO. This allows us to perform better schema enforcement/evolution. Since Spark
   * skips this step, we see if we need to perform any schema adjustment here.
   */
  private def needsSchemaAdjustment(
      tableName: String,
      query: LogicalPlan,
      schema: StructType): Boolean = {
    val output = query.output
    if (output.length < schema.length) {
      throw DeltaErrors.notEnoughColumnsInInsert(tableName, output.length, schema.length)
    }
    // Now we should try our best to match everything that already exists, and leave the rest
    // for schema evolution to WriteIntoDelta
    val existingSchemaOutput = output.take(schema.length)
    val rawSchema = getRawSchema(schema)
    existingSchemaOutput.map(_.name) != schema.map(_.name) ||
      !SchemaUtils.isReadCompatible(rawSchema.asNullable, existingSchemaOutput.toStructType)
  }

  private def getRawSchema(schema: StructType): StructType = {
    StructType(schema.map { field =>
      CharVarcharUtils.getRawType(field.metadata).map {
        rawType => field.copy(dataType = rawType)
      }.getOrElse(field)
    })
  }

  // Get cast operation for the level of strictness in the schema a user asked for
  private def getCastFunction: CastFunction = {
    val timeZone = conf.sessionLocalTimeZone
    conf.storeAssignmentPolicy match {
      case SQLConf.StoreAssignmentPolicy.LEGACY => Cast(_, _, Option(timeZone))
      case SQLConf.StoreAssignmentPolicy.ANSI => AnsiCast(_, _, Option(timeZone))
      case SQLConf.StoreAssignmentPolicy.STRICT => UpCast(_, _)
    }
  }

  /**
   * Recursively casts structs in case it contains null types.
   * TODO: Support other complex types like MapType and ArrayType
   */
  private def addCastsToStructs(
      tableName: String,
      parent: NamedExpression,
      source: StructType,
      target: StructType): NamedExpression = {
    if (source.length < target.length) {
      throw DeltaErrors.notEnoughColumnsInInsert(
        tableName, source.length, target.length, Some(parent.qualifiedName))
    }
    val fields = source.zipWithIndex.map {
      case (StructField(name, nested: StructType, _, metadata), i) if i < target.length =>
        target(i).dataType match {
          case t: StructType =>
            val subField = Alias(GetStructField(parent, i, Option(name)), target(i).name)(
              explicitMetadata = Option(metadata))
            addCastsToStructs(tableName, subField, nested, t)
          case o =>
            val field = parent.qualifiedName + "." + name
            val targetName = parent.qualifiedName + "." + target(i).name
            throw DeltaErrors.cannotInsertIntoColumn(tableName, field, targetName, o.simpleString)
        }
      case (other, i) if i < target.length =>
        val targetAttr = target(i)
        Alias(
          getCastFunction(GetStructField(parent, i, Option(other.name)), targetAttr.dataType),
          targetAttr.name)(explicitMetadata = Option(targetAttr.metadata))

      case (other, i) =>
        // This is a new column, so leave to schema evolution as is. Do not lose it's name so
        // wrap with an alias
        Alias(
          GetStructField(parent, i, Option(other.name)),
          other.name)(explicitMetadata = Option(other.metadata))
    }
    Alias(CreateStruct(fields), parent.name)(
      parent.exprId, parent.qualifier, Option(parent.metadata))
  }

  private def stripTempViewWrapper(plan: LogicalPlan): LogicalPlan = {
    DeltaViewHelper.stripTempView(plan, conf)
  }
}


/** Matchers for dealing with a Delta table. */
object DeltaRelation {
  def unapply(plan: LogicalPlan): Option[LogicalRelation] = plan match {
    case dsv2 @ DataSourceV2Relation(d: DeltaTableV2, _, _, _, options) =>
      Some(fromV2Relation(d, dsv2, options))
    case lr @ DeltaTable(_) => Some(lr)
    case _ => None
  }

  def fromV2Relation(
      d: DeltaTableV2,
      v2Relation: DataSourceV2Relation,
      options: CaseInsensitiveStringMap): LogicalRelation = {
      val relation = d.withOptions(options.asScala.toMap).toBaseRelation
      var output = v2Relation.output

      LogicalRelation(relation, output, d.catalogTable, isStreaming = false)
  }
}

object AppendDelta {
  def unapply(a: AppendData): Option[(DataSourceV2Relation, DeltaTableV2)] = {
    if (a.query.resolved) {
      a.table match {
        case r: DataSourceV2Relation if r.table.isInstanceOf[DeltaTableV2] =>
          Some((r, r.table.asInstanceOf[DeltaTableV2]))
        case _ => None
      }
    } else {
      None
    }
  }
}

object OverwriteDelta {
  def unapply(o: OverwriteByExpression): Option[(DataSourceV2Relation, DeltaTableV2)] = {
    if (o.query.resolved) {
      o.table match {
        case r: DataSourceV2Relation if r.table.isInstanceOf[DeltaTableV2] =>
          Some((r, r.table.asInstanceOf[DeltaTableV2]))
        case _ => None
      }
    } else {
      None
    }
  }
}
