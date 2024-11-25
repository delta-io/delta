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

import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.AnalysisHelper

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Trait with helper functions to generate expressions to update target columns, even if they are
 * nested fields.
 */
trait UpdateExpressionsSupport extends SQLConfHelper with AnalysisHelper with DeltaLogging {

  /**
   * Whether casting behavior can revert to following 'spark.sql.ansi.enabled' instead of
   * 'spark.sql.storeAssignmentPolicy' to preserve legacy behavior for UPDATE and MERGE.
   * Legacy behavior is applied only if
   * 'spark.databricks.delta.updateAndMergeCastingFollowsAnsiEnabledFlag' is set to true.
   */
  protected val supportMergeAndUpdateLegacyCastBehavior: Boolean = false

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
   *                             struct casting will throw an error if the target struct type
   *                             contains more fields than the expression to cast.
   * @param columnName The name of the column written to. It is used for the error message.
   */
  protected def castIfNeeded(
      fromExpression: Expression,
      dataType: DataType,
      allowStructEvolution: Boolean,
      columnName: String): Expression = {

    fromExpression match {
      // Need to deal with NullType here, as some types cannot be casted from NullType, e.g.,
      // StructType.
      case Literal(nul, NullType) => Literal(nul, dataType)
      case otherExpr =>
        val resolveStructsByName =
          conf.getConf(DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME)

        (fromExpression.dataType, dataType) match {
          case (ArrayType(_: StructType, _), to @ ArrayType(toEt: StructType, toContainsNull)) =>
            fromExpression match {
              // If fromExpression is an array function returning an array, cast the
              // underlying array first and then perform the function on the transformed array.
              case ArrayUnion(leftExpression, rightExpression) =>
                val castedLeft =
                  castIfNeeded(leftExpression, dataType, allowStructEvolution, columnName)
                val castedRight =
                  castIfNeeded(rightExpression, dataType, allowStructEvolution, columnName)
                ArrayUnion(castedLeft, castedRight)

              case ArrayIntersect(leftExpression, rightExpression) =>
                val castedLeft =
                  castIfNeeded(leftExpression, dataType, allowStructEvolution, columnName)
                val castedRight =
                  castIfNeeded(rightExpression, dataType, allowStructEvolution, columnName)
                ArrayIntersect(castedLeft, castedRight)

              case ArrayExcept(leftExpression, rightExpression) =>
                val castedLeft =
                  castIfNeeded(leftExpression, dataType, allowStructEvolution, columnName)
                val castedRight =
                  castIfNeeded(rightExpression, dataType, allowStructEvolution, columnName)
                ArrayExcept(castedLeft, castedRight)

              case ArrayRemove(leftExpression, rightExpression) =>
                val castedLeft =
                  castIfNeeded(leftExpression, dataType, allowStructEvolution, columnName)
                // ArrayRemove removes all elements that equal to element from the given array.
                // In this case, the element to be removed also needs to be casted into the target
                // array's element type.
                val castedRight =
                  castIfNeeded(rightExpression, toEt, allowStructEvolution, columnName)
                ArrayRemove(castedLeft, castedRight)

              case ArrayDistinct(expression) =>
                val castedExpr =
                  castIfNeeded(expression, dataType, allowStructEvolution, columnName)
                ArrayDistinct(castedExpr)

              case _ =>
                // generate a lambda function to cast each array item into to element struct type.
                val structConverter: (Expression, Expression) => Expression = (_, i) =>
                  castIfNeeded(
                  GetArrayItem(fromExpression, i), toEt, allowStructEvolution, columnName)
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
                  to.asNullable,
                  columnName
                )
            }
          case (from: MapType, to: MapType) if !Cast.canCast(from, to) =>
            // Manually convert map keys and values if the types are not compatible to allow schema
            // evolution. This is slower than direct cast so we only do it when required.
            def createMapConverter(convert: (Expression, Expression) => Expression): Expression = {
              val keyVar = NamedLambdaVariable("keyVar", from.keyType, nullable = false)
              val valueVar =
                NamedLambdaVariable("valueVar", from.valueType, from.valueContainsNull)
              LambdaFunction(convert(keyVar, valueVar), Seq(keyVar, valueVar))
            }

            var transformedKeysAndValues = fromExpression
            if (from.keyType != to.keyType) {
              transformedKeysAndValues =
                TransformKeys(transformedKeysAndValues, createMapConverter {
                  (key, _) => castIfNeeded(key, to.keyType, allowStructEvolution, columnName)
                })
            }

            if (from.valueType != to.valueType) {
              transformedKeysAndValues =
                TransformValues(transformedKeysAndValues, createMapConverter {
                  (_, value) => castIfNeeded(value, to.valueType, allowStructEvolution, columnName)
                })
            }
            cast(transformedKeysAndValues, to.asNullable, columnName)
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
              Seq(fieldNameLit,
                castIfNeeded(extractedField, field.dataType, allowStructEvolution, field.name))
            })

            cast(nameMappedStruct, to.asNullable, columnName)

          case (from, to) if (from != to) => cast(fromExpression, dataType, columnName)
          case _ => fromExpression
        }
    }
  }

  /**
   * Given a target schema and a set of update operations, generate a list of update expressions,
   * which are aligned with the given schema.
   *
   * For update operations to nested struct fields, this method recursively walks down schema tree
   * and apply the update expressions along the way.
   * For example, assume table `target` has the following schema:
   *   s1 struct<a: int, b: int, c: int>, s2 struct<a: int, b: int>, z int
   *
   * Given an update command:
   *
   *  - UPDATE target SET s1.a = 1, s1.b = 2, z = 3
   *
   * this method works as follows:
   *
   * generateUpdateExpressions(
   *   targetSchema=[s1,s2,z], defaultExprs=[s1,s2, z], updateOps=[(s1.a, 1), (s1.b, 2), (z, 3)])
   *   -> generates expression for s1 - build recursively from child assignments
   *   generateUpdateExpressions(
   *     targetSchema=[a,b,c], defaultExprs=[a, b, c], updateOps=[(a, 1),(b, 2)], pathPrefix=["s1"])
   *     end-of-recursion
   *   -> returns (1, 2, a.c)
   *   -> generates expression for s2 - no child assignment and no update expression: use
   *      default expression `s2`
   *   -> generates expression for z - use available update expression `3`
   * -> returns ((1, 2, a.c), s2, 3)
   *
   * @param targetSchema schema to follow to generate update expressions. Due to schema evolution,
   *                     it may contain additional columns or fields not present in the original
   *                     table schema.
   * @param updateOps a set of update operations.
   * @param defaultExprs the expressions to use when no update operation is provided for a column
   *                      or field. This is typically the output from the base table.
   * @param pathPrefix the path from root to the current (nested) column. Only used for printing out
   *                   full column path in error messages.
   * @param allowSchemaEvolution Whether to allow generating expressions for new columns or fields
   *                             added by schema evolution.
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
      targetSchema: StructType,
      updateOps: Seq[UpdateOperation],
      defaultExprs: Seq[NamedExpression],
      resolver: Resolver,
      pathPrefix: Seq[String] = Nil,
      allowSchemaEvolution: Boolean = false,
      generatedColumns: Seq[StructField] = Nil): Seq[Option[Expression]] = {
    // Check that the head of nameParts in each update operation can match a target col. This avoids
    // silently ignoring invalid column names specified in update operations.
    updateOps.foreach { u =>
      if (!targetSchema.exists(f => resolver(f.name, u.targetColNameParts.head))) {
        throw DeltaErrors.updateSetColumnNotFoundException(
          (pathPrefix :+ u.targetColNameParts.head).mkString("."),
          targetSchema.map(f => (pathPrefix :+ f.name).mkString(".")))
      }
    }

    // Transform each targetCol to a possibly updated expression
    targetSchema.map { targetCol =>
      // The prefix of a update path matches the current targetCol path.
      val prefixMatchedOps =
        updateOps.filter(u => resolver(u.targetColNameParts.head, targetCol.name))
      val defaultExpr = defaultExprs.find(f => resolver(f.name, targetCol.name))
      // No prefix matches this target column, return its original expression.
      if (prefixMatchedOps.isEmpty) {
        // Check whether it's a generated column or not. If so, we will return `None` so that the
        // caller will generate an expression for this column. We cannot generate an expression at
        // this moment because a generated column may use other columns which we don't know their
        // update expressions yet.
        if (generatedColumns.find(f => resolver(f.name, targetCol.name)).nonEmpty) {
          None
        } else if (defaultExpr.nonEmpty) {
          defaultExpr
        } else {
          // This is a new column or field added by schema evolution that doesn't have an assignment
          // in this MERGE clause. Set it to null.

          // Log an assertion for now (and fail in test) if schema evolution is disabled. We should
          // turn this into an error in the future.
          deltaAssert(allowSchemaEvolution,
            name = "generateUpdateExpressions.allowSchemaEvolution",
            msg = "Generating an expression for a new column or field but schema evolution is " +
              "disabled."
          )
          Some(Literal(null))
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
          Some(castIfNeeded(
            fullyMatchedOp.get.updateExpr,
            targetCol.dataType,
            allowSchemaEvolution,
            targetCol.name))
        } else {
          // So there are prefix-matched update operations, but none of them is a full match. Then
          // that means targetCol is a complex data type, so we recursively pass along the update
          // operations to its children.
          targetCol.dataType match {
            case childSchema: StructType =>
              val defaultChildExprs = defaultExpr match {
                case Some(expr @ NamedExpression(_, StructType(fields))) =>
                  fields.zipWithIndex.map { case (field, ordinal) =>
                    Alias(GetStructField(expr, ordinal, Some(field.name)), field.name)()
                  }
                case _ => Array.empty[NamedExpression]
              }
              // Recursively apply update operations to the children
              val childTargetExprs = generateUpdateExpressions(
                childSchema,
                prefixMatchedOps.map(u => u.copy(targetColNameParts = u.targetColNameParts.tail)),
                defaultChildExprs,
                resolver,
                pathPrefix :+ targetCol.name,
                allowSchemaEvolution,
                // Set `generatedColumns` to Nil because they are only valid in the top level.
                generatedColumns = Nil)
                .map(_.getOrElse {
                  // Should not happen
                  throw DeltaErrors.cannotGenerateUpdateExpressions()
                })
              // Reconstruct the expression for targetCol using its possibly updated children
              val namedStructExprs = childSchema
                .zip(childTargetExprs)
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
      targetSchema: StructType,
      defaultExprs: Seq[NamedExpression],
      nameParts: Seq[Seq[String]],
      updateExprs: Seq[Expression],
      resolver: Resolver,
      generatedColumns: Seq[StructField]): Seq[Option[Expression]] = {
    assert(nameParts.size == updateExprs.size)
    val updateOps = nameParts.zip(updateExprs).map {
      case (nameParts, expr) => UpdateOperation(nameParts, expr)
    }
    generateUpdateExpressions(
      targetSchema = targetSchema,
      updateOps = updateOps,
      defaultExprs = defaultExprs,
      resolver = resolver,
      generatedColumns = generatedColumns
    )
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
   * @param updateExprs  The aligned (with `postEvolutionTargetSchema` if not None, or
   *                     `updateTarget.output` otherwise) update actions.
   * @param postEvolutionTargetSchema In case of UPDATE in MERGE when schema evolution happened,
   *                                  this is the final schema of the target table. This might not
   *                                  be the same as the output of `updateTarget`.
   * @return a sequence of update expressions for all of columns in the table.
   */
  protected def generateUpdateExprsForGeneratedColumns(
      updateTarget: LogicalPlan,
      generatedColumns: Seq[StructField],
      updateExprs: Seq[Option[Expression]],
      postEvolutionTargetSchema: Option[StructType] = None): Seq[Expression] = {
    val targetSchema = postEvolutionTargetSchema.getOrElse(updateTarget.schema)
    assert(
      targetSchema.size == updateExprs.length,
      s"'generateUpdateExpressions' should return expressions that are aligned with the column " +
        s"list. Expected size: ${targetSchema.size}, actual size: ${updateExprs.length}")
    val schemaWithExprs = targetSchema.zip(updateExprs)
    val exprsForProject = schemaWithExprs.flatMap {
      case (field, Some(expr)) =>
        // Create a named expression so that we can use it in Project
        val exprForProject = Alias(expr, field.name)()
        Some(exprForProject.exprId -> exprForProject)
      case (_, None) => None
    }.toMap
    // Create a fake Project to resolve the generation expressions
    val fakePlan = Project(exprsForProject.values.toArray[NamedExpression], updateTarget)
    schemaWithExprs.map {
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
              throw DeltaErrors.nonGeneratedColumnMissingUpdateExpression(targetCol.name)
          }
        // As `resolvedExpr` will refer to attributes in `fakePlan`, we need to manually replace
        // these attributes with their update expressions.
        resolvedExpr.transform {
          case a: AttributeReference if exprsForProject.contains(a.exprId) =>
            exprsForProject(a.exprId).child
        }
    }
  }

  /**
   * Replaces 'CastSupport.cast'. Selects a cast based on 'spark.sql.storeAssignmentPolicy'.
   * Legacy behavior for UPDATE and MERGE followed 'spark.sql.ansi.enabled' instead, this legacy
   * behavior can be re-enabled by setting
   * 'spark.databricks.delta.updateAndMergeCastingFollowsAnsiEnabledFlag' to true.
   */
  private def cast(child: Expression, dataType: DataType, columnName: String): Expression = {
    if (supportMergeAndUpdateLegacyCastBehavior &&
      conf.getConf(DeltaSQLConf.UPDATE_AND_MERGE_CASTING_FOLLOWS_ANSI_ENABLED_FLAG)) {
      return Cast(child, dataType, Option(conf.sessionLocalTimeZone))
    }

    conf.storeAssignmentPolicy match {
      case SQLConf.StoreAssignmentPolicy.LEGACY =>
        Cast(child, dataType, Some(conf.sessionLocalTimeZone), ansiEnabled = false)
      case SQLConf.StoreAssignmentPolicy.ANSI =>
        val cast = Cast(child, dataType, Some(conf.sessionLocalTimeZone), ansiEnabled = true)
        if (canCauseCastOverflow(cast)) {
          if (supportMergeAndUpdateLegacyCastBehavior) {
            CheckOverflowInTableWrite(cast, columnName)
          } else {
            cast.setTagValue(Cast.BY_TABLE_INSERTION, ())
            CheckOverflowInTableInsert(cast, columnName)
          }
        } else {
          cast
        }
      case SQLConf.StoreAssignmentPolicy.STRICT =>
        UpCast(child, dataType)
    }
  }

  private def containsIntegralOrDecimalType(dt: DataType): Boolean = dt match {
    case _: IntegralType | _: DecimalType => true
    case a: ArrayType => containsIntegralOrDecimalType(a.elementType)
    case m: MapType =>
      containsIntegralOrDecimalType(m.keyType) || containsIntegralOrDecimalType(m.valueType)
    case s: StructType =>
      s.fields.exists(sf => containsIntegralOrDecimalType(sf.dataType))
    case _ => false
  }

  private def canCauseCastOverflow(cast: Cast): Boolean = {
    containsIntegralOrDecimalType(cast.dataType) &&
      !Cast.canUpCast(cast.child.dataType, cast.dataType)
  }
}

case class CheckOverflowInTableWrite(child: Expression, columnName: String)
  extends UnaryExpression {
  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }

  private def getCast: Option[Cast] = child match {
    case c: Cast => Some(c)
    case ExpressionProxy(c: Cast, _, _) => Some(c)
    case _ => None
  }

  override def eval(input: InternalRow): Any = try {
    child.eval(input)
  } catch {
    case e: ArithmeticException =>
      getCast match {
        case Some(cast) =>
          throw DeltaErrors.castingCauseOverflowErrorInTableWrite(
            cast.child.dataType,
            cast.dataType,
            columnName)
        case None => throw e
      }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    getCast match {
      case Some(child) => doGenCodeWithBetterErrorMsg(ctx, ev, child)
      case None => child.genCode(ctx)
    }
  }

  def doGenCodeWithBetterErrorMsg(ctx: CodegenContext, ev: ExprCode, child: Cast): ExprCode = {
    val childGen = child.genCode(ctx)
    val exceptionClass = classOf[ArithmeticException].getCanonicalName
    assert(child.isInstanceOf[Cast])
    val cast = child.asInstanceOf[Cast]
    val fromDt =
      ctx.addReferenceObj("from", cast.child.dataType, cast.child.dataType.getClass.getName)
    val toDt = ctx.addReferenceObj("to", child.dataType, child.dataType.getClass.getName)
    val col = ctx.addReferenceObj("colName", columnName, "java.lang.String")
    // scalastyle:off line.size.limit
    ev.copy(code =
      code"""
      boolean ${ev.isNull} = true;
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
      try {
        ${childGen.code}
        ${ev.isNull} = ${childGen.isNull};
        ${ev.value} = ${childGen.value};
      } catch ($exceptionClass e) {
        throw org.apache.spark.sql.delta.DeltaErrors
          .castingCauseOverflowErrorInTableWrite($fromDt, $toDt, $col);
      }"""
    )
    // scalastyle:on line.size.limit
  }

  override def dataType: DataType = child.dataType

  override def sql: String = child.sql

  override def toString: String = child.toString
}
