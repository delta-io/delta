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

import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}

object MergeReferenceType extends Enumeration {
  type MergeReferenceType = Value
  val Target, Source, TargetAndSource = Value
}

case class TargetTableResolutionResult(
    unresolvedAttribute: UnresolvedAttribute,
    expr: Expression,
    wasResolved: Boolean
)

/**
 * Implements logic to resolve conditions and actions in MERGE clauses and handles schema evolution.
 */
object ResolveDeltaMergeInto {
  private def unresolvedReferenceErrorMsg(
      sqlText: String,
      mergeClauseType: String,
      target: LogicalPlan): String = {
    val cols = target.output.map(_.sql).mkString(", ")
    s"Cannot resolve $sqlText in target columns in $mergeClauseType clause given " +
      s"columns $cols"
  }

  def resolveReferencesAndSchema(merge: DeltaMergeInto, conf: SQLConf)(
    resolveExprsFn: (Seq[Expression], Seq[LogicalPlan]) => Seq[Expression]): DeltaMergeInto = {
    val DeltaMergeInto(
      target,
      source,
      condition,
      matchedClauses,
      notMatchedClauses,
      notMatchedBySourceClauses,
      withSchemaEvolution,
      _) = merge

    val mergeReferenceTypeToResolvedPlans = Map(
      MergeReferenceType.Target -> Seq(target),
      MergeReferenceType.Source -> Seq(source),
      MergeReferenceType.TargetAndSource -> Seq(target, source))

    assert(mergeReferenceTypeToResolvedPlans.size == MergeReferenceType.values.size, "Every " +
      "MergeReferenceType must have a corresponding entry in mergeReferenceTypeToResolvedPlans")

    val shouldBatchResolution =
      conf.getConf(DeltaSQLConf.DELTA_MERGE_ANALYSIS_BATCH_RESOLUTION)

    /**
     * Resolves expressions against given plans or fail using given message. It makes a best-effort
     * attempt to throw specific error messages on which part of the query has a problem.
     */
    def resolveOrFail(
        exprs: Seq[Expression],
        mergeReferenceType: MergeReferenceType.MergeReferenceType,
        mergeClauseType: String): Seq[Expression] = {
      val plansToResolveExprs = mergeReferenceTypeToResolvedPlans(mergeReferenceType)
      val resolvedExprs = resolveExprsFn(exprs, plansToResolveExprs)
      resolvedExprs.foreach(throwIfNotResolved(_, plansToResolveExprs, mergeClauseType))
      resolvedExprs
    }

    /**
     * Convenience wrapper around `resolveOrFail()` when resolving a single expression.
     */
    def resolveSingleExprOrFail(
        expr: Expression,
        mergeReferenceType: MergeReferenceType.MergeReferenceType,
        mergeClauseType: String)
      : Expression = resolveOrFail(Seq(expr), mergeReferenceType, mergeClauseType).head

    def throwIfNotResolved(
        expr: Expression,
        plans: Seq[LogicalPlan],
        mergeClauseType: String): Unit = {
      expr.flatMap(_.references).filter(!_.resolved).foreach { a =>
        // Note: This will throw error only on unresolved attribute issues,
        // not other resolution errors like mismatched data types.
        val cols = "columns " + plans.flatMap(_.output).map(_.sql).mkString(", ")
        throw new DeltaAnalysisException(
          errorClass = "DELTA_MERGE_UNRESOLVED_EXPRESSION",
          messageParameters = Array(a.sql, mergeClauseType, cols),
          origin = Some(a.origin))
      }
    }

    /** Try to resolve a single target column in the Merge action. */
    def resolveSingleTargetColumn(
        unresolvedAttribute: UnresolvedAttribute,
        shouldTryUnresolvedTargetExprOnSource: Boolean,
        mergeClauseType: String): Expression = {
      // Resolve the target column name without database/table/view qualifiers
      // If clause allows nested field to be target, then this will return all the
      // parts of the name (e.g., "a.b" -> Seq("a", "b")). Otherwise, this will
      // return only one string.
      try {
        resolveSingleExprOrFail(
          expr = unresolvedAttribute,
          MergeReferenceType.Target,
          mergeClauseType)
      } catch {
        // Allow schema evolution for update and insert non-star when the column is not in
        // the target.
        case _: AnalysisException if shouldTryUnresolvedTargetExprOnSource =>
          resolveSingleExprOrFail(
            expr = unresolvedAttribute,
            MergeReferenceType.Source,
            mergeClauseType)
        case e: Throwable => throw e
      }
    }

    def batchResolveTargetColumns(
        unresolvedAttrSeq: Seq[UnresolvedAttribute],
        shouldTryUnresolvedTargetExprOnSource: Boolean,
        mergeClauseType: String): Seq[Expression] = {
      val resolvedExprs = try {
        // Note: unlike resolveSingleTargetColumn, this is not a resolveOrFail.
        // We will not throw an exception if something was not resolved, but we must catch
        // exception that come from other parts of invoking the analyzer.
        resolveExprsFn(
          unresolvedAttrSeq, mergeReferenceTypeToResolvedPlans(MergeReferenceType.Target))
      } catch {
        // We don't know which attribute in the Seq lead to this exception.
        // We need to resolve this one by one...
        // Note: We catch the exception here so that we preserve the behaviour where we
        // throw a different exception in PreprocessTableMerge later on...
        case _: AnalysisException if shouldTryUnresolvedTargetExprOnSource =>
          unresolvedAttrSeq.map(
            resolveSingleTargetColumn(_, shouldTryUnresolvedTargetExprOnSource, mergeClauseType))
        case e: Throwable => throw e
      }
      assert(unresolvedAttrSeq.length == resolvedExprs.length, "Unexpected error. The number of " +
        "resolved expressions should match the number of unresolved expressions")

      val targetTableResolutionResult: Seq[TargetTableResolutionResult] =
        resolvedExprs.zipWithIndex.map { case (expr, index) =>
          TargetTableResolutionResult(
            unresolvedAttrSeq(index), expr, expr.references.forall(_.resolved))
        }
      val exprNeedsResolution: Seq[Expression] =
        targetTableResolutionResult.filter(!_.wasResolved).map(_.unresolvedAttribute)

      // If we are not supposed to resolve the target column against the source and
      // we were not able to resolve the column, then we should throw an exception
      // at this point.
      if (!shouldTryUnresolvedTargetExprOnSource && exprNeedsResolution.nonEmpty) {
        throwIfNotResolved(
          // Use the first of the unresolved attributes to throw the exception.
          targetTableResolutionResult.filter(!_.wasResolved).map(_.expr).head,
          mergeReferenceTypeToResolvedPlans(MergeReferenceType.Target),
          mergeClauseType
        )
      }

      val resolvedExprAgainstSource: Seq[Expression] = resolveOrFail(
        exprNeedsResolution,
        MergeReferenceType.Source,
        mergeClauseType)

      var index = -1
      val orderedResolvedTargetExprs = targetTableResolutionResult.map {
        case TargetTableResolutionResult(_, expr, wasResolved) =>
          if (wasResolved) {
            expr
          } else {
            index += 1
            resolvedExprAgainstSource(index)
          }
      }

      orderedResolvedTargetExprs
    }

    def batchResolveDeltaMergeAction(
        unresolvedDeltaMergeActions: Seq[DeltaMergeAction],
        shouldTryUnresolvedTargetExprOnSource: Boolean,
        mergeClauseType: String,
        plansToResolveAction: Seq[LogicalPlan]): Seq[DeltaMergeAction] = {
      // Batch the unresolved attributes to resolve them in a single pass.
      val unresolvedAttrSeq = unresolvedDeltaMergeActions
        .map(mergeAction => UnresolvedAttribute(mergeAction.targetColNameParts))
      val orderedResolvedTargetExprs = batchResolveTargetColumns(
        unresolvedAttrSeq,
        shouldTryUnresolvedTargetExprOnSource,
        mergeClauseType)

      // Now we deal with the expressions for each target column (RHS assignment).
      val unresolvedRHSExprSeq = unresolvedDeltaMergeActions.map(_.expr)
      val resolvedExprsSeq =
        resolveExprsFn(unresolvedRHSExprSeq, plansToResolveAction)
      assert(resolvedExprsSeq.length == orderedResolvedTargetExprs.length)
      resolvedExprsSeq.foreach(
        throwIfNotResolved(_, plansToResolveAction, mergeClauseType))

      val resolvedDeltaMergeActions: Seq[DeltaMergeAction] =
        orderedResolvedTargetExprs.zip(resolvedExprsSeq).map {
          case (resolvedKey, resolvedRHSExpr) =>
            val resolutionErrorMsg =
              unresolvedReferenceErrorMsg(resolvedKey.sql, mergeClauseType, target)
            val resolvedNameParts =
              DeltaUpdateTable.getTargetColNameParts(resolvedKey, resolutionErrorMsg)
            DeltaMergeAction(resolvedNameParts, resolvedRHSExpr, targetColNameResolved = true)
        }

      resolvedDeltaMergeActions
    }

    val canEvolveSchema =
      withSchemaEvolution || conf.getConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE)

    /**
     * Resolves a clause using the given plans (used for resolving the action exprs) and
     * returns the resolved clause.
     */
    def resolveClause[T <: DeltaMergeIntoClause](
        clause: T,
        mergeReferenceType: MergeReferenceType.MergeReferenceType): T = {

      /*
       * Returns the sequence of [[DeltaMergeActions]] corresponding to
       * [ `columnName = sourceColumnBySameName` ] for every column name in the schema. Nested
       * columns are unfolded to create an assignment for each leaf.
       *
       * @param currSchema: schema to generate DeltaMergeAction for every 'leaf'
       * @param qualifier: used to recurse to leaves; represents the qualifier of the current schema
       * @return seq of DeltaMergeActions corresponding to columnName = sourceColumnName updates
       */
      def getActions(currSchema: StructType, qualifier: Seq[String] = Nil): Seq[DeltaMergeAction] =
        currSchema.flatMap {
          case StructField(name, struct: StructType, _, _) =>
            getActions(struct, qualifier :+ name)
          case StructField(name, _, _, _) =>
            val nameParts = qualifier :+ name
            val sourceExpr = source.resolve(nameParts, conf.resolver).getOrElse {
              // if we use getActions to expand target columns, this will fail on target columns not
              // present in the source
              throw new DeltaIllegalArgumentException(
                errorClass = "DELTA_CANNOT_RESOLVE_SOURCE_COLUMN",
                messageParameters = Array(s"${UnresolvedAttribute(nameParts).name}")
              )
            }
            Seq(DeltaMergeAction(nameParts, sourceExpr, targetColNameResolved = true))
        }

      val typ = clause.clauseType.toUpperCase(Locale.ROOT)
      val mergeClauseType = s"$typ clause"
      val shouldTryUnresolvedTargetExprOnSource = clause match {
        case _: DeltaMergeIntoMatchedUpdateClause |
             _: DeltaMergeIntoNotMatchedClause => canEvolveSchema
        case _ => false
      }
      val plansToResolveAction = mergeReferenceTypeToResolvedPlans(mergeReferenceType)

      val (unresolvedDeltaMergeActions, allOtherExpressions) = clause.actions.partition {
        case d: DeltaMergeAction if !d.resolved && shouldBatchResolution => true
        case _ => false
      }

      val resolvedDeltaMergeActions = batchResolveDeltaMergeAction(
        unresolvedDeltaMergeActions.map(_.asInstanceOf[DeltaMergeAction]),
        shouldTryUnresolvedTargetExprOnSource,
        mergeClauseType,
        plansToResolveAction)

      val resolvedOtherExpressionsIntoDeltaMergeActions: Seq[DeltaMergeAction] =
        allOtherExpressions.flatMap { action =>
        action match {
          // For actions like `UPDATE SET *` or `INSERT *`
          case _: UnresolvedStar if !canEvolveSchema =>
            // Expand `*` into seq of [ `columnName = sourceColumnBySameName` ] for every target
            // column name. The target columns do not need resolution. The right hand side
            // expression (i.e. sourceColumnBySameName) needs to be resolved only by the source
            // plan.
            val unresolvedExprs = target.output.map { attr =>
              UnresolvedAttribute.quotedString(s"`${attr.name}`")
            }
            val resolvedExprs = resolveOrFail(
              unresolvedExprs, MergeReferenceType.Source, s"$typ clause")
            (resolvedExprs, target.output.map(_.name))
              .zipped
              .map { (resolvedExpr, targetColName) =>
                DeltaMergeAction(Seq(targetColName), resolvedExpr, targetColNameResolved = true)
              }
          case _: UnresolvedStar if canEvolveSchema =>
            clause match {
              case _: DeltaMergeIntoNotMatchedInsertClause =>
                // Expand `*` into seq of [ `columnName = sourceColumnBySameName` ] for every source
                // column name. Target columns not present in the source will be filled in
                // with null later.
                source.output.map { attr =>
                  DeltaMergeAction(Seq(attr.name), attr, targetColNameResolved = true)
                }
              case _: DeltaMergeIntoMatchedUpdateClause =>
                // Expand `*` into seq of [ `columnName = sourceColumnBySameName` ] for every source
                // column name. Target columns not present in the source will be filled in with
                // no-op actions later.
                // Nested columns are unfolded to accommodate the case where a source struct has a
                // subset of the nested columns in the target. If a source struct (a, b) is writing
                // into a target (a, b, c), the final struct after filling in the no-op actions will
                // be (s.a, s.b, t.c).
                getActions(source.schema, Seq.empty)
            }


          // For actions like `UPDATE SET x = a, y = b` or `INSERT (x, y) VALUES (a, b)`
          case d @ DeltaMergeAction(colNameParts, expr, _) if !d.resolved =>
            val unresolvedAttrib = UnresolvedAttribute(colNameParts)
            val resolutionErrorMsg =
              unresolvedReferenceErrorMsg(unresolvedAttrib.sql, typ, target)

            val resolvedKey = resolveSingleTargetColumn(
              unresolvedAttrib,
              shouldTryUnresolvedTargetExprOnSource,
              mergeClauseType
            )

            val resolvedNameParts =
              DeltaUpdateTable.getTargetColNameParts(resolvedKey, resolutionErrorMsg)

            // We don't call [[resolveOrFail]] because there are valid cases where we currently
            // cannot resolve the expression such as:
            //   - during schema evolution
            //   - dealing with nested field
            val resolvedExpr = resolveExprsFn(Seq(expr), plansToResolveAction).head
            throwIfNotResolved(resolvedExpr, plansToResolveAction, s"$typ clause")
            Seq(DeltaMergeAction(resolvedNameParts, resolvedExpr, targetColNameResolved = true))

          case d: DeltaMergeAction =>
            // Already resolved
            Seq(d)

          case _ =>
            action.failAnalysis("INTERNAL_ERROR",
              Map("message" -> s"Unexpected action expression '$action' in clause $clause"))
        }
      }

      val resolvedCondition = clause.condition.map {
        resolveSingleExprOrFail(_, mergeReferenceType, mergeClauseType = s"$typ condition")
      }
      clause.makeCopy(Array(resolvedCondition,
          resolvedDeltaMergeActions ++ resolvedOtherExpressionsIntoDeltaMergeActions
      )).asInstanceOf[T]
    }

    // We must do manual resolution as the expressions in different clauses of the MERGE have
    // visibility of the source, the target or both.
    val resolvedCond = resolveSingleExprOrFail(
      expr = condition,
      MergeReferenceType.TargetAndSource,
      mergeClauseType = "search condition")
    val resolvedMatchedClauses = matchedClauses.map {
      resolveClause(_, MergeReferenceType.TargetAndSource)
    }
    val resolvedNotMatchedClauses = notMatchedClauses.map {
      resolveClause(_, MergeReferenceType.Source)
    }
    val resolvedNotMatchedBySourceClauses = notMatchedBySourceClauses.map {
      resolveClause(_, MergeReferenceType.Target)
    }

    val postEvolutionTargetSchema = if (canEvolveSchema) {
      // When schema evolution is enabled, add to the target table new columns or nested fields that
      // are assigned to in merge actions and not already part of the target schema. This is done by
      // collecting all assignments from merge actions and using them to filter out the source
      // schema before merging it with the target schema. We don't consider NOT MATCHED BY SOURCE
      // clauses since these can't by definition reference source columns and thus can't introduce
      // new columns in the target schema.
      val actions = (resolvedMatchedClauses ++ resolvedNotMatchedClauses).flatMap(_.actions)
      val assignments = actions.collect { case a: DeltaMergeAction => a.targetColNameParts }
      val containsStarAction = actions.exists {
        case _: UnresolvedStar => true
        case _ => false
      }


      // Filter the source schema to retain only fields that are referenced by at least one merge
      // clause, then merge this schema with the target to give the final schema.
      def filterSchema(sourceSchema: StructType, basePath: Seq[String]): StructType =
        StructType(sourceSchema.flatMap { field =>
          val fieldPath = basePath :+ field.name

          // Helper method to check if a given field path is a prefix of another path. Delegates
          // equality to conf.resolver to correctly handle case sensitivity.
          def isPrefix(prefix: Seq[String], path: Seq[String]): Boolean =
            prefix.length <= path.length && prefix.zip(path).forall {
              case (prefixNamePart, pathNamePart) => conf.resolver(prefixNamePart, pathNamePart)
            }

          // Helper method to check if a given field path is equal to another path.
          def isEqual(path1: Seq[String], path2: Seq[String]): Boolean =
            path1.length == path2.length && isPrefix(path1, path2)


          field.dataType match {
            // Specifically assigned to in one clause: always keep, including all nested attributes
            case _ if assignments.exists(isEqual(_, fieldPath)) => Some(field)
            // If this is a struct and one of the children is being assigned to in a merge clause,
            // keep it and continue filtering children.
            case struct: StructType if assignments.exists(isPrefix(fieldPath, _)) =>
              Some(field.copy(dataType = filterSchema(struct, fieldPath)))
            // The field isn't assigned to directly or indirectly (i.e. its children) in any non-*
            // clause. Check if it should be kept with any * action.
            case struct: StructType if containsStarAction =>
              Some(field.copy(dataType = filterSchema(struct, fieldPath)))
            case _ if containsStarAction => Some(field)
            // The field and its children are not assigned to in any * or non-* action, drop it.
            case _ => None
          }
        })

      val migrationSchema = filterSchema(source.schema, Seq.empty)
      val allowTypeWidening = target.exists {
        case DeltaTable(fileIndex) =>
          TypeWidening.isEnabled(fileIndex.protocol, fileIndex.metadata)
        case _ => false
      }

      // The implicit conversions flag allows any type to be merged from source to target if Spark
      // SQL considers the source type implicitly castable to the target. Normally, mergeSchemas
      // enforces Parquet-level write compatibility, which would mean an INT source can't be merged
      // into a LONG target.
      SchemaMergingUtils.mergeSchemas(
        target.schema,
        migrationSchema,
        allowImplicitConversions = true,
        allowTypeWidening = allowTypeWidening
      )
    } else {
      target.schema
    }

    val resolvedMerge = DeltaMergeInto(
      target,
      source,
      resolvedCond,
      resolvedMatchedClauses,
      resolvedNotMatchedClauses,
      resolvedNotMatchedBySourceClauses,
      withSchemaEvolution = canEvolveSchema,
      finalSchema = Some(postEvolutionTargetSchema))

    // Its possible that pre-resolved expressions (e.g. `sourceDF("key") = targetDF("key")`) have
    // attribute references that are not present in the output attributes of the children (i.e.,
    // incorrect DataFrame was used in the `df("col")` form).
    if (resolvedMerge.missingInput.nonEmpty) {
      val missingAttributes = resolvedMerge.missingInput.mkString(",")
      val input = resolvedMerge.inputSet.mkString(",")
      throw new DeltaAnalysisException(
        errorClass = "DELTA_MERGE_RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT",
        messageParameters = Array(missingAttributes, input,
          resolvedMerge.simpleString(SQLConf.get.maxToStringFields)),
        origin = Some(resolvedMerge.origin)
      )
    }

    resolvedMerge
  }
}
