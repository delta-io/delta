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

import java.util.Locale

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.sql.delta.commands.MergeIntoCommand

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, Literal, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}

case class PreprocessTableMerge(override val conf: SQLConf)
  extends Rule[LogicalPlan] with UpdateExpressionsSupport {

  private var trackHighWaterMarks = Set[String]()

  def getTrackHighWaterMarks: Set[String] = trackHighWaterMarks

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case m: DeltaMergeInto if m.resolved => apply(m, true)
  }

  def apply(mergeInto: DeltaMergeInto, transformToCommand: Boolean): LogicalPlan = {
    val DeltaMergeInto(
    target, source, condition, matched, notMatched, migrateSchema, finalSchemaOpt) = mergeInto

    if (finalSchemaOpt.isEmpty) {
      throw new AnalysisException("Target Table Final Schema is empty.")
    }

    val finalSchema = finalSchemaOpt.get

    def checkCondition(cond: Expression, conditionName: String): Unit = {
      if (!cond.deterministic) {
        throw DeltaErrors.nonDeterministicNotSupportedException(
          s"$conditionName condition of MERGE operation", cond)
      }
      if (cond.find(_.isInstanceOf[AggregateExpression]).isDefined) {
        throw DeltaErrors.aggsNotSupportedException(
          s"$conditionName condition of MERGE operation", cond)
      }
      if (SubqueryExpression.hasSubquery(cond)) {
        throw DeltaErrors.subqueryNotSupportedException(
          s"$conditionName condition of MERGE operation", cond)
      }
    }

    checkCondition(condition, "search")
    (matched ++ notMatched).filter(_.condition.nonEmpty).foreach { clause =>
      checkCondition(clause.condition.get, clause.clauseType.toUpperCase(Locale.ROOT))
    }

    val deltaLogicalPlan = EliminateSubqueryAliases(target)
    val tahoeFileIndex = deltaLogicalPlan match {
      case DeltaFullTable(index) => index
      case o => throw DeltaErrors.notADeltaSourceException("MERGE", Some(o))
    }
    val generatedColumns = GeneratedColumn.getGeneratedColumns(
      tahoeFileIndex.snapshotAtAnalysis)
    if (generatedColumns.nonEmpty && !deltaLogicalPlan.isInstanceOf[LogicalRelation]) {
      throw DeltaErrors.operationOnTempViewWithGenerateColsNotSupported("MERGE INTO")
    }
    // Additional columns with default expressions.
    var additionalColumns = Seq[StructField]()

    val processedMatched = matched.map {
      case m: DeltaMergeIntoUpdateClause =>
        // Get any new columns which are in the insert clause, but not the target output or this
        // update clause.
        val existingColumns = m.resolvedActions.map(_.targetColNameParts.head) ++
          target.output.map(_.name)
        val newColumns = notMatched.toSeq.flatMap {
          _.resolvedActions.filterNot { insertAct =>
            existingColumns.exists { colName =>
              conf.resolver(insertAct.targetColNameParts.head, colName)
            }
          }
        }

        // TODO: Remove this once Scala 2.13 is available in Spark.
        def distinctBy[A : ClassTag, B](a: Seq[A])(f: A => B): Seq[A] = {
          val builder = mutable.ArrayBuilder.make[A]
          val seen = mutable.HashSet.empty[B]
          a.foreach { x =>
            if (seen.add(f(x))) {
              builder += x
            }
          }
          builder.result()
        }

        val newColsFromInsert = distinctBy(newColumns)(_.targetColNameParts).map { action =>
          AttributeReference(action.targetColNameParts.head, action.dataType)()
        }

        // Get the operations for columns that already exist...
        val existingUpdateOps = m.resolvedActions.map { a =>
          UpdateOperation(a.targetColNameParts, a.expr)
        }

        // And construct operations for columns that the insert clause will add.
        val newOpsFromInsert = newColsFromInsert.map { col =>
          UpdateOperation(Seq(col.name), Literal(null, col.dataType))
        }

        // Get expressions for the final schema for alignment. Note that attributes which already
        // exist in the target need to use the same expression ID, even if the schema will evolve.
        val finalSchemaExprs =
          finalSchema.map { field =>
            target.resolve(Seq(field.name), conf.resolver).map { r =>
              AttributeReference(field.name, field.dataType)(r.exprId)
            }.getOrElse {
              AttributeReference(field.name, field.dataType)()
            }
          }

        // Use the helper methods for in UpdateExpressionsSupport to generate expressions such
        // that nested fields can be updated (only for existing columns).
        val alignedExprs = generateUpdateExpressions(
          finalSchemaExprs,
          existingUpdateOps ++ newOpsFromInsert,
          conf.resolver,
          allowStructEvolution = migrateSchema,
          generatedColumns = generatedColumns)

        val alignedExprsWithGenerationExprs =
          if (alignedExprs.forall(_.nonEmpty)) {
            alignedExprs.map(_.get)
          } else {
            generateUpdateExprsForGeneratedColumns(target, generatedColumns, alignedExprs,
              Some(finalSchemaExprs))
          }

        val alignedActions: Seq[DeltaMergeAction] = alignedExprsWithGenerationExprs
          .zip(finalSchemaExprs)
          .map { case (expr, attrib) =>
            DeltaMergeAction(Seq(attrib.name), expr, targetColNameResolved = true)
          }

        m.copy(m.condition, alignedActions)

      case m: DeltaMergeIntoDeleteClause => m    // Delete does not need reordering
    }

    val processedNotMatched = notMatched.map { m =>
      // Check if columns are distinct. All actions should have targetColNameParts.size = 1.
      m.resolvedActions.foreach { a =>
        if (a.targetColNameParts.size > 1) {
          throw DeltaErrors.nestedFieldNotSupported(
            "INSERT clause of MERGE operation",
            a.targetColNameParts.mkString("`", "`.`", "`")
          )
        }
      }

      val targetColNames = m.resolvedActions.map(_.targetColNameParts.head)
      if (targetColNames.distinct.size < targetColNames.size) {
        throw new AnalysisException(s"Duplicate column names in INSERT clause")
      }

      // Generate actions for columns that are not explicitly inserted. They might come from
      // the original schema of target table or the schema evolved columns. In either case they are
      // covered by `finalSchema`.
      val implicitActions = finalSchema.filterNot { col =>
        m.resolvedActions.exists { insertAct =>
          conf.resolver(insertAct.targetColNameParts.head, col.name)
        }
      }.map { col =>
        DeltaMergeAction(Seq(col.name), Literal(null, col.dataType), targetColNameResolved = true)
      }

      val actions = m.resolvedActions ++ implicitActions
      val (actionsWithGeneratedColumns, trackFromInsert) = resolveImplicitColumns(
        m.resolvedActions,
        actions,
        source,
        generatedColumns.map(f => (f, true)) ++ additionalColumns.map(f => (f, false)),
        finalSchema)

      trackHighWaterMarks ++= trackFromInsert

      val alignedActions: Seq[DeltaMergeAction] = finalSchema.map { targetAttrib =>
        actionsWithGeneratedColumns.find { a =>
          conf.resolver(targetAttrib.name, a.targetColNameParts.head)
        }.map { a =>
          DeltaMergeAction(
            Seq(targetAttrib.name),
            castIfNeeded(
              a.expr,
              targetAttrib.dataType,
              allowStructEvolution = migrateSchema),
            targetColNameResolved = true)
        }.getOrElse {
          // If a target table column was not found in the INSERT columns and expressions,
          // then throw exception as there must be an expression to set every target column.
          throw new AnalysisException(
            s"Unable to find the column '${targetAttrib.name}' of the target table from " +
              s"the INSERT columns: ${targetColNames.mkString(", ")}. " +
              s"INSERT clause must specify value for all the columns of the target table.")
        }
      }

      m.copy(m.condition, alignedActions)
    }

    if (transformToCommand) {
      val tahoeFileIndex = EliminateSubqueryAliases(target) match {
        case DeltaFullTable(index) => index
        case o => throw DeltaErrors.notADeltaSourceException("MERGE", Some(o))
      }
      MergeIntoCommand(
        source, target, tahoeFileIndex, condition,
        processedMatched, processedNotMatched, finalSchemaOpt)
    } else {
      DeltaMergeInto(source, target, condition,
        processedMatched, processedNotMatched, migrateSchema, finalSchemaOpt)
    }
  }

  /**
   * Resolves any non explicitly inserted generated columns in `allActions` to its
   * corresponding generated expression.
   *
   * For each action, if it's a generated column that is not explicitly inserted, we will
   * use its generated expression to calculate its value by resolving to a fake project of all the
   * inserted values. Note that this fake project is created after we set all non explicitly
   * inserted columns to nulls. This guarantees that all columns referenced by the generated
   * column, regardless of whether they are explicitly inserted or not, will have a
   * corresponding expression in the fake project and hence the generated expression can
   * always be resolved.
   *
   * @param explicitActions Actions explicitly specified by users.
   * @param allActions Actions with non explicitly specified columns added with nulls.
   * @param sourcePlan Logical plan node of the source table of merge.
   * @param columnWithDefaultExpr All the generated columns in the target table.
   * @return `allActions` with expression for non explicitly inserted generated columns expression
   *        resolved.
   */
  private def resolveImplicitColumns(
    explicitActions: Seq[DeltaMergeAction],
    allActions: Seq[DeltaMergeAction],
    sourcePlan: LogicalPlan,
    columnWithDefaultExpr: Seq[(StructField, Boolean)],
    finalSchema: StructType): (Seq[DeltaMergeAction], Set[String]) = {
    val implicitColumns = columnWithDefaultExpr.filter {
      case (field, _) =>
        !explicitActions.exists { insertAct =>
          conf.resolver(insertAct.targetColNameParts.head, field.name)
        }
    }
    if (implicitColumns.isEmpty) {
      return (allActions, Set[String]())
    }
    assert(finalSchema.size == allActions.size)

    val track = mutable.Set[String]()

    // Fake projection used to resolve generated column expressions.
    val fakeProjectMap = allActions.map {
      action => {
        val exprForProject = Alias(action.expr, action.targetColNameParts.head)()
        exprForProject.exprId -> exprForProject
      }
    }.toMap
    val fakeProject = Project(fakeProjectMap.values.toArray[Alias], sourcePlan)

    val resolvedActions = allActions.map { action =>
      val colName = action.targetColNameParts.head
      implicitColumns.find {
        case (field, _) => conf.resolver(field.name, colName)
      } match {
        case Some((field, true)) =>
          val expr = GeneratedColumn.getGenerationExpression(field).get
          val resolvedExpr = resolveReferencesForExpressions(SparkSession.active, expr :: Nil,
            fakeProject).head
          // Replace references to fakeProject with original expression.
          val transformedExpr = resolvedExpr.transform {
            case a: AttributeReference if fakeProjectMap.contains(a.exprId) =>
              fakeProjectMap(a.exprId).child
          }
          action.copy(expr = transformedExpr)
        case _ => action
      }
    }
    (resolvedActions, track.toSet)
  }
}
