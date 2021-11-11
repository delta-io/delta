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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, BoundReference, Expression, GetStructField}
import org.apache.spark.sql.types.StructType

/**
 * Helper class for generating a joined projection.
 */
object JoinedProjection {
  /**
   * Bind attributes for a joined projection. This resulting project list expects an input row
   * that has two nested struct fields, the struct at position 0 must be the left hand row of the
   * join, and the struct at position 1 must be the right hand row of the join.
   *
   * The following shows example shows how this can be used for updating an aggregation buffer:
   * {{{
   *   val buffer = new GenericInternalRow()
   *
   *  val update = GenerateMutableProjection.generate(
   *     expressions = JoinedProjection(
   *       leftAttributes = bufferAttrs,
   *       rightAttributes = dataCols,
   *       projectList = aggregates.flatMap(_.updateExpressions)),
   *     inputSchema = Nil,
   *     useSubexprElimination = true
   *   ).target(buffer)
   *
   *   val joinedRow = new GenericInternalRow(2)
   *   joinedRow.update(0, input)
   *
   *   def updateBuffer(input: InternalRow): Unit = {
   *     joinedRow.update(1, input)
   *     update(joinedRow)
   *   }
   * }}}
   */
  def bind(
      leftAttributes: Seq[Attribute],
      rightAttributes: Seq[Attribute],
      projectList: Seq[Expression],
      leftCanBeNull: Boolean = false,
      rightCanBeNull: Boolean = false): Seq[Expression] = {
    val mapping = AttributeMap(
      createMapping(0, leftCanBeNull, leftAttributes)
        ++ createMapping(1, rightCanBeNull, rightAttributes))
    projectList.map { expr =>
      expr.transformUp {
        case a: Attribute => mapping(a)
      }
    }
  }

  private def createMapping(
      index: Int,
      nullable: Boolean,
      attributes: Seq[Attribute]): Seq[(Attribute, Expression)] = {
    val ref = BoundReference(index, StructType.fromAttributes(attributes), nullable)
    attributes.zipWithIndex.map {
      case (a, ordinal) => a -> GetStructField(ref, ordinal, Option(a.name))
    }
  }
}
