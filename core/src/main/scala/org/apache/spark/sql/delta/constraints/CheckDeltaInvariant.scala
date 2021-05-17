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

import org.apache.spark.sql.delta.constraints.Constraints.{Check, NotNull}
import org.apache.spark.sql.delta.schema.InvariantViolationException

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeSeq, BindReferences, Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types.{DataType, NullType}

/**
 * An expression that validates a specific invariant on a column, before writing into Delta.
 *
 * @param child The fully resolved expression to be evaluated to check the constraint.
 * @param columnExtractors Extractors for each referenced column. Used to generate readable errors.
 * @param constraint The original constraint definition.
 */
case class CheckDeltaInvariant(
    child: Expression,
    columnExtractors: Map[String, Expression],
    constraint: Constraint)
  extends UnaryExpression with NonSQLExpression with CodegenFallback {

  override def dataType: DataType = NullType
  override def foldable: Boolean = false
  override def nullable: Boolean = true

  def withBoundReferences(input: AttributeSeq): CheckDeltaInvariant = {
    CheckDeltaInvariant(
      BindReferences.bindReference(child, input),
      columnExtractors.map {
        case (column, extractor) => column -> BindReferences.bindReference(extractor, input)
      },
      constraint)
  }

  private def assertRule(input: InternalRow): Unit = constraint match {
    case n: NotNull =>
      if (child.eval(input) == null) {
        throw InvariantViolationException(n)
      }
    case c: Check =>
      val result = child.eval(input)
      if (result == null || result == false) {
        throw InvariantViolationException(c, columnExtractors.mapValues(_.eval(input)))
      }
  }

  override def eval(input: InternalRow): Any = {
    assertRule(input)
    null
  }

  private def generateNotNullCode(ctx: CodegenContext): Block = {
    val childGen = child.genCode(ctx)
    val invariantField = ctx.addReferenceObj("errMsg", constraint)
    code"""${childGen.code}
       |
       |if (${childGen.isNull}) {
       |  throw org.apache.spark.sql.delta.schema.InvariantViolationException.apply(
       |    $invariantField);
       |}
     """.stripMargin
  }

  /**
   * Generate the code to extract values for the columns referenced in a violated CHECK constraint.
   * We build parallel lists of full column names and their extracted values in the row which
   * violates the constraint, to be passed to the [[InvariantViolationException]] constructor
   * in [[generateExpressionValidationCode()]].
   *
   * Note that this code is a bit expensive, so it shouldn't be run until we already
   * know the constraint has been violated.
   */
  private def generateColumnValuesCode(
      colList: String, valList: String, ctx: CodegenContext): Block = {
    val start =
      code"""
        |java.util.List<String> $colList = new java.util.ArrayList<String>();
        |java.util.List<Object> $valList = new java.util.ArrayList<Object>();
        |""".stripMargin
    columnExtractors.map {
      case (name, extractor) =>
        val colValue = extractor.genCode(ctx)
        code"""
          |$colList.add("$name");
          |${colValue.code}
          |if (${colValue.isNull}) {
          |  $valList.add(null);
          |} else {
          |  $valList.add(${colValue.value});
          |}
          |""".stripMargin
    }.fold(start)(_ + _)
  }

  private def generateExpressionValidationCode(
      constraintName: String, expr: Expression, ctx: CodegenContext): Block = {
    val elementValue = child.genCode(ctx)
    val invariantField = ctx.addReferenceObj("errMsg", constraint)
    val colListName = ctx.freshName("colList")
    val valListName = ctx.freshName("valList")
    code"""${elementValue.code}
       |
       |if (${elementValue.isNull} || ${elementValue.value} == false) {
       |  ${generateColumnValuesCode(colListName, valListName, ctx)}
       |  throw org.apache.spark.sql.delta.schema.InvariantViolationException.apply(
       |     $invariantField, $colListName, $valListName);
       |}
     """.stripMargin
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val code = constraint match {
      case NotNull(_) => generateNotNullCode(ctx)
      case Check(name, expr) => generateExpressionValidationCode(name, expr, ctx)
    }
    ev.copy(code = code, isNull = TrueLiteral, value = JavaCode.literal("null", NullType))
  }

  // TODO: remove when the new Spark version is releases that has the withNewChildInternal method
}
