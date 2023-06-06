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

import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.AnalysisHelper

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{CastSupport, Resolver}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.types._

/**
 * Trait with helper functions to generate expressions to update target columns, even if they are
 * nested fields.
 */
trait UpdateExpressionsSupport extends CastSupport with SQLConfHelper with AnalysisHelper {
  /**
   * Specifies an operation that updates a target column with the given expression.
   * The target column may or may not be a nested field and it is specified as a full quoted name
   * or as a sequence of split into parts.
   */
  case class UpdateOperation(targetColNameParts: Seq[String], updateExpr: Expression)

  /**
   * Add a cast to the child expression if it differs from the specified data type. Note that
   * structs here are cast by name, rather than the Spark SQL default of casting by position.
   *
   * @param fromExpression the expression to cast
   * @param dataType The data type to cast to.
   * @param allowStructEvolution Whether to allow structs to evolve. When this is false (default),
   *                             struct casting will throw an error if there's any mismatch between
   *                             column names. For example, (b, c, a) -> (a, b, c) is always a valid
   *                             cast, but (a, b) -> (a, b, c) is valid only with this flag set.
   */
  protected def castIfNeeded(
      fromExpression: Expression,
      dataType: DataType,
      allowStructEvolution: Boolean = false): Expression = {

    fromExpression match {
      // Need to deal with NullType here, as some types cannot be casted from NullType, e.g.,
      // StructType.
      case Literal(nul, NullType) => Literal(nul, dataType)
      case otherExpr =>
        val resolveStructsByName =
          conf.getConf(DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME)

        (fromExpression.dataType, dataType) match {
          case (ArrayType(_: StructType, _), ArrayType(toEt: StructType, toContainsNull)) =>
            fromExpression match {
              // If fromExpression is an array function returning an array, cast the
              // underlying array first and then perform the function on the transformed array.
              case ArrayUnion(leftExpression, rightExpression) =>
                val castedLeft = castIfNeeded(leftExpression, dataType, allowStructEvolution)
                val castedRight = castIfNeeded(rightExpression, dataType, allowStructEvolution)
                ArrayUnion(castedLeft, castedRight)

              case ArrayIntersect(leftExpression, rightExpression) =>
                val castedLeft = castIfNeeded(leftExpression, dataType, allowStructEvolution)
                val castedRight = castIfNeeded(rightExpression, dataType, allowStructEvolution)
                ArrayIntersect(castedLeft, castedRight)

              case ArrayExcept(leftExpression, rightExpression) =>
                val castedLeft = castIfNeeded(leftExpression, dataType, allowStructEvolution)
                val castedRight = castIfNeeded(rightExpression, dataType, allowStructEvolution)
                ArrayExcept(castedLeft, castedRight)

              case ArrayRemove(leftExpression, rightExpression) =>
                val castedLeft = castIfNeeded(leftExpression, dataType, allowStructEvolution)
                // ArrayRemove removes all elements that equal to element from the given array.
                // In this case, the element to be removed also needs to be casted into the target
                // array's element type.
                val castedRight = castIfNeeded(rightExpression, toEt, allowStructEvolution)
                ArrayRemove(castedLeft, castedRight)

              case ArrayDistinct(expression) =>
                val castedExpr = castIfNeeded(expression, dataType, allowStructEvolution)
                ArrayDistinct(castedExpr)

              case _ =>
                // generate a lambda function to cast each array item into to element struct type.
                val structConverter: (Expression, Expression) => Expression = (_, i) =>
                  castIfNeeded(GetArrayItem(fromExpression, i), toEt, allowStructEvolution)
                val transformLambdaFunc = {
                  val elementVar = NamedLambdaVariable("elementVar", toEt, toContainsNull)
                  val indexVar = NamedLambdaVariable("indexVar", IntegerType, false)
                  LambdaFunction(structConverter(elementVar, indexVar), Seq(elementVar, indexVar))
                }
                // Transforms every element in the array using the lambda function.
                // Because castIfNeeded is called recursively for array elements, which
                // generates nullable expression, ArrayTransform will generate an ArrayType with
                // containsNull as true. Thus, the ArrayType to be casted to need to have
                // containsNull as true to avoid casting failures.
                cast(
                  ArrayTransform(fromExpression, transformLambdaFunc),
                  ArrayType(toEt, containsNull = true)
                )
            }
          case (from: StructType, to: StructType)
            if !DataType.equalsIgnoreCaseAndNullability(from, to) && resolveStructsByName =>
            // All from fields must be present in the final schema, or we'll silently lose data.
            if (from.exists { f => !to.exists(_.name.equalsIgnoreCase(f.name))}) {
              throw DeltaErrors.updateSchemaMismatchExpression(from, to)
            }

            // If struct evolution isn't allowed, the field count also has to match, since we can't
            // add columns.
            if (from.length != to.length && !allowStructEvolution) {
              throw DeltaErrors.updateSchemaMismatchExpression(from, to)
            }

            val nameMappedStruct = CreateNamedStruct(to.flatMap { field =>
              val fieldNameLit = Literal(field.name)
              val extractedField = from
                .find { f => SchemaUtils.DELTA_COL_RESOLVER(f.name, field.name) }
                .map { _ =>
                  ExtractValue(fromExpression, fieldNameLit, SchemaUtils.DELTA_COL_RESOLVER)
                }.getOrElse {
                  // This shouldn't be possible - if all columns aren't present when struct
                  // evolution is disabled, we should have thrown an error earlier.
                  if (!allowStructEvolution) {
                    throw DeltaErrors.extractReferencesFieldNotFound(s"$field",
                      DeltaErrors.updateSchemaMismatchExpression(from, to))
                  }
                  Literal(null)
                }
              Seq(fieldNameLit, castIfNeeded(extractedField, field.dataType, allowStructEvolution))
            })

            cast(nameMappedStruct, to.asNullable)

          case (from, to) if (from != to) => cast(fromExpression, dataType)
          case _ => fromExpression
        }
    }
  }

  /**
   * Given a list of target-column expressions and a set of update operations, generate a list
   * of update expressions, which are aligned with given target-column expressions.
   *
   * For update operations to nested struct fields, this method recursively walks down schema tree
   * and apply the update expressions along the way.
   * For example, assume table `target` has two attributes a and z, where a is of struct type
   * with 3 fields: b, c and d, and z is of integer type.
   *
   * Given an update command:
   *
   *  - UPDATE target SET a.b = 1, a.c = 2, z = 3
   *
   * this method works as follows:
   *
   * generateUpdateExpressions(targetCols=[a,z], updateOps=[(a.b, 1), (a.c, 2), (z, 3)])
   *   generateUpdateExpressions(targetCols=[b,c,d], updateOps=[(b, 1),(c, 2)], pathPrefix=["a"])
   *     end-of-recursion
   *   -> returns (1, 2, d)
   * -> return ((1, 2, d), 3)
   *
   * @param targetCols a list of expressions to read named columns; these named columns can be
   *                   either the top-level attributes of a table, or the nested fields of a
   *                   StructType column.
   * @param updateOps a set of update operations.
   * @param pathPrefix the path from root to the current (nested) column. Only used for printing out
   *                   full column path in error messages.
   * @param allowStructEvolution Whether to allow structs to evolve. When this is false (default),
   *                             struct casting will throw an error if there's any mismatch between
   *                             column names. For example, (b, c, a) -> (a, b, c) is always a valid
   *                             cast, but (a, b) -> (a, b, c) is valid only with this flag set.
   * @param generatedColumns the list of the generated columns in the table. When a column is a
   *                         generated column and the user doesn't provide a update expression, its
   *                         update expression in the return result will be None.
   *                         If `generatedColumns` is empty, any of the options in the return result
   *                         must be non-empty.
   * @return a sequence of expression options. The elements in the sequence are options because
   *         when a column is a generated column but the user doesn't provide an update expression
   *         for this column, we need to generate the update expression according to the generated
   *         column definition. But this method doesn't have enough context to do that. Hence, we
   *         return a `None` for this case so that the caller knows it should generate the update
   *         expression for such column. For other cases, we will always return Some(expr).
   */
  protected def generateUpdateExpressions(
      targetCols: Seq[NamedExpression],
      updateOps: Seq[UpdateOperation],
      resolver: Resolver,
      pathPrefix: Seq[String] = Nil,
      allowStructEvolution: Boolean = false,
      generatedColumns: Seq[StructField] = Nil): Seq[Option[Expression]] = {
    // Check that the head of nameParts in each update operation can match a target col. This avoids
    // silently ignoring invalid column names specified in update operations.
    updateOps.foreach { u =>
      if (!targetCols.exists(f => resolver(f.name, u.targetColNameParts.head))) {
        throw DeltaErrors.updateSetColumnNotFoundException(
          (pathPrefix :+ u.targetColNameParts.head).mkString("."),
          targetCols.map(col => (pathPrefix :+ col.name).mkString(".")))
      }
    }

    // Transform each targetCol to a possibly updated expression
    targetCols.map { targetCol =>
      // The prefix of a update path matches the current targetCol path.
      val prefixMatchedOps =
        updateOps.filter(u => resolver(u.targetColNameParts.head, targetCol.name))
      // No prefix matches this target column, return its original expression.
      if (prefixMatchedOps.isEmpty) {
        // Check whether it's a generated column or not. If so, we will return `None` so that the
        // caller will generate an expression for this column. We cannot generate an expression at
        // this moment because a generated column may use other columns which we don't know their
        // update expressions yet.
        if (generatedColumns.find(f => resolver(f.name, targetCol.name)).nonEmpty) {
          None
        } else {
          Some(targetCol)
        }
      } else {
        // The update operation whose path exactly matches the current targetCol path.
        val fullyMatchedOp = prefixMatchedOps.find(_.targetColNameParts.size == 1)
        if (fullyMatchedOp.isDefined) {
          // If a full match is found, then it should be the ONLY prefix match. Any other match
          // would be a conflict, whether it is a full match or prefix-only. For example,
          // when users are updating a nested column a.b, they can't simultaneously update a
          // descendant of a.b, such as a.b.c.
          if (prefixMatchedOps.size > 1) {
            throw DeltaErrors.updateSetConflictException(
              prefixMatchedOps.map(op => (pathPrefix ++ op.targetColNameParts).mkString(".")))
          }
          // For an exact match, return the updateExpr from the update operation.
          Some(
            castIfNeeded(fullyMatchedOp.get.updateExpr, targetCol.dataType, allowStructEvolution))
        } else {
          // So there are prefix-matched update operations, but none of them is a full match. Then
          // that means targetCol is a complex data type, so we recursively pass along the update
          // operations to its children.
          targetCol.dataType match {
            case StructType(fields) =>
              val fieldExpr = targetCol
              val childExprs = fields.zipWithIndex.map { case (field, ordinal) =>
                Alias(GetStructField(fieldExpr, ordinal, Some(field.name)), field.name)()
              }
              // Recursively apply update operations to the children
              val targetExprs = generateUpdateExpressions(
                childExprs,
                prefixMatchedOps.map(u => u.copy(targetColNameParts = u.targetColNameParts.tail)),
                resolver,
                pathPrefix :+ targetCol.name,
                allowStructEvolution,
                // Set `generatedColumns` to Nil because they are only valid in the top level.
                generatedColumns = Nil)
                .map(_.getOrElse {
                  // Should not happen
                  throw DeltaErrors.cannotGenerateUpdateExpressions()
                })
              // Reconstruct the expression for targetCol using its possibly updated children
              val namedStructExprs = fields
                .zip(targetExprs)
                .flatMap { case (field, expr) => Seq(Literal(field.name), expr) }
              Some(CreateNamedStruct(namedStructExprs))

            case otherType =>
              throw DeltaErrors.updateNonStructTypeFieldNotSupportedException(
                (pathPrefix :+ targetCol.name).mkString("."), otherType)
          }
        }
      }
    }
  }

  /** See docs on overloaded method. */
  protected def generateUpdateExpressions(
      targetCols: Seq[NamedExpression],
      nameParts: Seq[Seq[String]],
      updateExprs: Seq[Expression],
      resolver: Resolver,
      generatedColumns: Seq[StructField]): Seq[Option[Expression]] = {
    assert(nameParts.size == updateExprs.size)
    val updateOps = nameParts.zip(updateExprs).map {
      case (nameParts, expr) => UpdateOperation(nameParts, expr)
    }
    generateUpdateExpressions(targetCols, updateOps, resolver, generatedColumns = generatedColumns)
  }

  /**
   * Generate update expressions for generated columns that the user doesn't provide a update
   * expression. For each item in `updateExprs` that's None, we will find its generation expression
   * from `generatedColumns`. In order to resolve this generation expression, we will create a
   * fake Project which contains all update expressions and resolve the generation expression with
   * this project. Source columns of a generation expression will also be replaced with their
   * corresponding update expressions.
   *
   * For example, given a table that has a generated column `g` defined as `c1 + 10`. For the
   * following update command:
   *
   * UPDATE target SET c1 = c2 + 100, c2 = 1000
   *
   * We will generate the update expression `(c2 + 100) + 10`` for column `g`. Note: in this update
   * expression, we should use the old `c2` attribute rather than its new value 1000.
   *
   * @param updateTarget The logical plan of the table to be updated.
   * @param generatedColumns A list of generated columns.
   * @param updateExprs  The aligned (with `finalSchemaExprs` if not None, or `updateTarget.output`
   *                     otherwise) update actions.
   * @param finalSchemaExprs In case of UPDATE in MERGE when schema evolution happened, this is
   *                         the final schema of the target table. This might not be the same as
   *                         the output of `updateTarget`.
   * @return a sequence of update expressions for all of columns in the table.
   */
  protected def generateUpdateExprsForGeneratedColumns(
      updateTarget: LogicalPlan,
      generatedColumns: Seq[StructField],
      updateExprs: Seq[Option[Expression]],
      finalSchemaExprs: Option[Seq[Attribute]] = None): Seq[Expression] = {
    val targetExprs = finalSchemaExprs.getOrElse(updateTarget.output)
    assert(
      targetExprs.size == updateExprs.length,
      s"'generateUpdateExpressions' should return expressions that are aligned with the column " +
        s"list. Expected size: ${updateTarget.output.size}, actual size: ${updateExprs.length}")
    val attrsWithExprs = targetExprs.zip(updateExprs)
    val exprsForProject = attrsWithExprs.flatMap {
      case (attr, Some(expr)) =>
        // Create a named expression so that we can use it in Project
        val exprForProject = Alias(expr, attr.name)()
        Some(exprForProject.exprId -> exprForProject)
      case (_, None) => None
    }.toMap
    // Create a fake Project to resolve the generation expressions
    val fakePlan = Project(exprsForProject.values.toArray[NamedExpression], updateTarget)
    attrsWithExprs.map {
      case (_, Some(expr)) => expr
      case (targetCol, None) =>
        // `targetCol` is a generated column and the user doesn't provide a update expression.
        val resolvedExpr =
          generatedColumns.find(f => conf.resolver(f.name, targetCol.name)) match {
            case Some(field) =>
              val expr = GeneratedColumn.getGenerationExpression(field).get
              resolveReferencesForExpressions(SparkSession.active, expr :: Nil, fakePlan).head
            case None =>
              // Should not happen
              throw DeltaErrors.nonGeneratedColumnMissingUpdateExpression(targetCol)
          }
        // As `resolvedExpr` will refer to attributes in `fakePlan`, we need to manually replace
        // these attributes with their update expressions.
        resolvedExpr.transform {
          case a: AttributeReference if exprsForProject.contains(a.exprId) =>
            exprsForProject(a.exprId).child
        }
    }
  }
}
