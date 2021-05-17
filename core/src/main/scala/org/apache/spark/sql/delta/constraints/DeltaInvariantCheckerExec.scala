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

package org.apache.spark.sql.delta.constraints

import scala.collection.mutable

import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.constraints.Constraints.{Check, NotNull}
import org.apache.spark.sql.delta.schema.SchemaUtils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{Analyzer, TypeCoercion, UnresolvedAttribute, UnresolvedExtractValue, UnresolvedFunction}
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.ImplicitTypeCasts
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BindReferences, Expression, ExtractValue, GetStructField, Literal, SortOrder}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy, UnaryExecNode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * Operator that validates that records satisfy provided constraints before they are written into
 * Delta. Each row is left unchanged after validations.
 */
case class DeltaInvariantChecker(
    child: LogicalPlan,
    deltaConstraints: Seq[Constraint])
  extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  // TODO: remove when the new Spark version is releases that has the withNewChildInternal method
}

object DeltaInvariantCheckerStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case DeltaInvariantChecker(child, constraints) =>
      val plannedChild = planLater(child)
      val plan = if (constraints.nonEmpty) {
        DeltaInvariantCheckerExec(plannedChild, constraints)
      } else {
        plannedChild
      }
      plan :: Nil
    case _ => Nil
  }
}

/**
 * A physical operator that validates records, before they are written into Delta. Each row
 * is left unchanged after validations.
 */
case class DeltaInvariantCheckerExec(
    child: SparkPlan,
    constraints: Seq[Constraint]) extends UnaryExecNode {

  private def spark: SparkSession = sqlContext.sparkSession

  override def output: Seq[Attribute] = child.output

  override protected def doExecute(): RDD[InternalRow] = {
    if (constraints.isEmpty) return child.execute()
    val invariantChecks =
      DeltaInvariantCheckerExec.buildInvariantChecks(child.output, constraints, spark)
    val boundRefs = invariantChecks.map(_.withBoundReferences(child.output))

    child.execute().mapPartitionsInternal { rows =>
      val assertions = GenerateUnsafeProjection.generate(boundRefs)
      rows.map { row =>
        assertions(row)
        row
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  // TODO: remove when the new Spark version is releases that has the withNewChildInternal method
}

object DeltaInvariantCheckerExec {
  /** Build the extractor for a particular column. */
  private def buildExtractor(output: Seq[Attribute], column: Seq[String]): Option[Expression] = {
    assert(column.nonEmpty)
    val topLevelColumn = column.head
    val topLevelRefOpt = output.collectFirst {
      case a: AttributeReference if SchemaUtils.DELTA_COL_RESOLVER(a.name, topLevelColumn) => a
    }

    if (column.length == 1) {
      topLevelRefOpt
    } else {
      topLevelRefOpt.flatMap { topLevelRef =>
        try {
          val nested = column.tail.foldLeft[Expression](topLevelRef) { case (e, fieldName) =>
            e.dataType match {
              case StructType(fields) =>
                val ordinal = fields.indexWhere(f =>
                  SchemaUtils.DELTA_COL_RESOLVER(f.name, fieldName))
                if (ordinal == -1) {
                  throw new IndexOutOfBoundsException(s"Not nullable column not found in struct: " +
                      s"${fields.map(_.name).mkString("[", ",", "]")}")
                }
                GetStructField(e, ordinal, Some(fieldName))
              case _ =>
                // NOTE: We should also update `GeneratedColumn.validateGeneratedColumns` to enable
                // `GetMapValue` and `GetArrayStructFields` expressions when this is supported.
                throw new UnsupportedOperationException(
                  "Invariants on nested fields other than StructTypes are not supported.")
            }
          }
          Some(nested)
        } catch {
          case _: IndexOutOfBoundsException => None
        }
      }
    }
  }

  def buildInvariantChecks(
      output: Seq[Attribute],
      constraints: Seq[Constraint],
      spark: SparkSession): Seq[CheckDeltaInvariant] = {
    constraints.map { constraint =>
      val columnExtractors = mutable.Map[String, Expression]()
      val executableExpr = constraint match {
        case n @ NotNull(column) =>
          buildExtractor(output, column).getOrElse {
            throw DeltaErrors.notNullColumnMissingException(n)
          }
        case Check(name, expr) =>
          // We need to do two stages of resolution here:
          //  * Build the extractors to evaluate attribute references against input InternalRows.
          //  * Do logical analysis to handle nested field extractions, functions, etc.

          val attributesExtracted = expr.transformUp {
            case a: UnresolvedAttribute =>
              val ex = buildExtractor(output, a.nameParts).getOrElse(Literal(null))
              columnExtractors(a.name) = ex
              ex
          }

          val wrappedPlan: LogicalPlan = ExpressionLogicalPlanWrapper(attributesExtracted)
          spark.sessionState.analyzer.execute(wrappedPlan) match {
            case ExpressionLogicalPlanWrapper(e) => e
            // This should never happen.
            case plan => throw new IllegalStateException(
              "Applying type casting resulted in a bad plan rather than a simple expression.\n" +
              s"Plan:${plan.prettyJson}\n")
          }
      }

      CheckDeltaInvariant(executableExpr, columnExtractors.toMap, constraint)
    }
  }
}
