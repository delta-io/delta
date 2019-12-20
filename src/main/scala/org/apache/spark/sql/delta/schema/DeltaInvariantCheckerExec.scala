/*
 * Copyright 2019 Databricks, Inc.
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

package org.apache.spark.sql.delta.schema

import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.schema.Invariants.NotNull

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BindReferences, Expression, GetStructField, Literal, SortOrder}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{NullType, StructType}

/**
 * A physical operator that validates records, before they are written into Delta. Each row
 * is left unchanged after validations.
 */
case class DeltaInvariantCheckerExec(
    child: SparkPlan,
    invariants: Seq[Invariant]) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  private def isNullNotOkay(invariant: Invariant): Boolean = invariant.rule match {
    case NotNull => true
    case _ => false
  }

  /** Build extractors to access the column an invariant is defined on. */
  private def buildExtractors(invariant: Invariant): Option[Expression] = {
    assert(invariant.column.nonEmpty)
    val topLevelColumn = invariant.column.head
    val topLevelRefOpt = output.collectFirst {
      case a: AttributeReference if SchemaUtils.DELTA_COL_RESOLVER(a.name, topLevelColumn) => a
    }
    val rejectColumnNotFound = isNullNotOkay(invariant)
    if (topLevelRefOpt.isEmpty) {
      if (rejectColumnNotFound) {
        throw DeltaErrors.notNullInvariantException(invariant)
      }
    }

    if (invariant.column.length == 1) {
      topLevelRefOpt.map(BindReferences.bindReference[Expression](_, output))
    } else {
      topLevelRefOpt.flatMap { topLevelRef =>
        val boundTopLevel = BindReferences.bindReference[Expression](topLevelRef, output)
        try {
          val nested = invariant.column.tail.foldLeft(boundTopLevel) { case (e, fieldName) =>
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
                throw new UnsupportedOperationException(
                  "Invariants on nested fields other than StructTypes are not supported.")
            }
          }
          Some(nested)
        } catch {
          case i: IndexOutOfBoundsException if rejectColumnNotFound =>
            throw InvariantViolationException(invariant, i.getMessage)
          case _: IndexOutOfBoundsException if !rejectColumnNotFound =>
            None
        }
      }
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    if (invariants.isEmpty) return child.execute()
    val boundRefs = invariants.map { invariant =>
      CheckDeltaInvariant(buildExtractors(invariant).getOrElse(Literal(null, NullType)), invariant)
    }

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
}
