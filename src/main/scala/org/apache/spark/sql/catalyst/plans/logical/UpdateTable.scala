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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, ExtractValue, GetStructField}

/**
 * Perform UPDATE on a table
 *
 * @param child the logical plan representing target table
 * @param updateColumns: the to-be-updated target columns
 * @param updateExpressions: the corresponding update expression if the condition is matched
 * @param condition: Only rows that match the condition will be updated
 */
case class UpdateTable(
    child: LogicalPlan,
    updateColumns: Seq[Attribute],
    updateExpressions: Seq[Expression],
    condition: Option[Expression])
  extends UnaryNode {

  assert(updateColumns.size == updateExpressions.size)

  override def output: Seq[Attribute] = Seq.empty
}

object UpdateTable {

  /** Resolve all the references of target columns and condition using the given `resolver` */
  def resolveReferences(update: UpdateTable, resolver: Expression => Expression): UpdateTable = {
    if (update.resolved) return update
    assert(update.child.resolved)

    val UpdateTable(child, updateColumns, updateExpressions, condition) = update

    val cleanedUpAttributes = updateColumns.map { unresolvedExpr =>
      // Keep them unresolved but use the cleaned-up name parts from the resolved
      val errMsg = s"Failed to resolve ${unresolvedExpr.sql} given columns " +
        s"[${child.output.map(_.qualifiedName).mkString(", ")}]."
      val resolveNameParts =
        UpdateTable.getNameParts(resolver(unresolvedExpr), errMsg, update)
      UnresolvedAttribute(resolveNameParts)
    }

    update.copy(
      updateColumns = cleanedUpAttributes,
      updateExpressions = updateExpressions.map(resolver),
      condition = condition.map(resolver))
  }

  /**
   * Extracts name parts from a resolved expression referring to a nested or non-nested column
   * - For non-nested column, the resolved expression will be like `AttributeReference(...)`.
   * - For nested column, the resolved expression will be like `Alias(GetStructField(...))`.
   *
   * In the nested case, the function recursively traverses through the expression to find
   * the name parts. For example, a nested field of a.b.c would be resolved to an expression
   *
   *    `Alias(c, GetStructField(c, GetStructField(b, AttributeReference(a)))`
   *
   * for which this method recursively extracts the name parts as follows:
   *
   *    `Alias(c, GetStructField(c, GetStructField(b, AttributeReference(a)))`
   *    ->  `GetStructField(c, GetStructField(b, AttributeReference(a)))`
   *      ->  `GetStructField(b, AttributeReference(a))` ++ Seq(c)
   *        ->  `AttributeReference(a)` ++ Seq(b, c)
   *          ->  [a, b, c]
   */
  def getNameParts(
      resolvedTargetCol: Expression,
      errMsg: String,
      errNode: LogicalPlan): Seq[String] = {

    def fail(extraMsg: String): Nothing = {
      throw new AnalysisException(
        s"$errMsg - $extraMsg", errNode.origin.line, errNode.origin.startPosition)
    }

    def extractRecursively(expr: Expression): Seq[String] = expr match {
      case attr: AttributeReference => Seq(attr.name)

      case Alias(c, _) => extractRecursively(c)

      case GetStructField(c, _, Some(name)) => extractRecursively(c) :+ name

      case _: ExtractValue =>
        fail("Updating nested fields is only supported for StructType.")

      case other =>
        fail(s"Found unsupported expression '$other' while parsing target column name parts")
    }

    extractRecursively(resolvedTargetCol)
  }
}
