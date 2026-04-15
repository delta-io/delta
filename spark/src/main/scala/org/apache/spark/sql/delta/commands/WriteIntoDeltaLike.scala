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

package org.apache.spark.sql.delta.commands

import scala.collection.mutable.ArrayBuffer

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.skipping.clustering.temp.ClusterBySpec
import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaOptions
import org.apache.spark.sql.delta.DeltaTableUtils
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.actions.AddCDCFile
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.actions.RemoveFile
import org.apache.spark.sql.delta.commands.DMLUtils.TaggedCommitData
import org.apache.spark.sql.delta.constraints.Constraint
import org.apache.spark.sql.delta.constraints.Constraints.Check
import org.apache.spark.sql.delta.constraints.Invariants.ArbitraryExpression
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.errors.QueryCompilationErrors.toSQLId
import org.apache.spark.sql.catalyst.expressions.{Alias, And, EqualTo, Exists, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution.metric.SQLMetric

/**
 * An interface for writing [[data]] into Delta tables.
 */
trait WriteIntoDeltaLike extends DeltaCommand with AnalysisHelper {
  /**
   * A helper method to create a new instances of [[WriteIntoDeltaLike]] with
   * updated [[configuration]].
   */
  def withNewWriterConfiguration(updatedConfiguration: Map[String, String]): WriteIntoDeltaLike

  /**
   * The configuration to be used for writing [[data]] into Delta table.
   */
  val configuration: Map[String, String]

  /**
   * Data to be written into Delta table.
   */
  val data: DataFrame

  /**
   * Write [[data]] into Delta table as part of [[txn]] and @return the actions to be committed.
   */
  def writeAndReturnCommitData(
      txn: OptimisticTransaction,
      sparkSession: SparkSession,
      clusterBySpecOpt: Option[ClusterBySpec] = None,
      isTableReplace: Boolean = false): TaggedCommitData[Action]

  def write(
      txn: OptimisticTransaction,
      sparkSession: SparkSession,
      clusterBySpecOpt: Option[ClusterBySpec] = None,
      isTableReplace: Boolean = false): Seq[Action] = writeAndReturnCommitData(
    txn, sparkSession, clusterBySpecOpt, isTableReplace).actions

  val deltaLog: DeltaLog



  // Helper for creating a SQLMetric and setting its value, since it isn't valid to create a
  // SQLMetric with a positive `initValue`.
  private def createSumMetricWithValue(
      spark: SparkSession,
      name: String,
      value: Long): SQLMetric = {
    val metric = new SQLMetric("sum")
    metric.register(spark.sparkContext, Some(name))
    metric.set(value)
    metric
  }

  /**
   * Overwrite mode produces extra delete metrics that are registered here.
   * @param deleteActions - RemoveFiles added by Delete job
   */
  protected def registerOverwriteRemoveMetrics(
      spark: SparkSession,
      txn: OptimisticTransaction,
      deleteActions: Seq[Action]): Unit = {
    var numRemovedFiles = 0
    var numRemovedBytes = 0L
    deleteActions.foreach {
      case action: RemoveFile =>
        numRemovedFiles += 1
        numRemovedBytes += action.getFileSize
      case _ => () // do nothing
    }
    val sqlMetrics = Map(
      "numRemovedFiles" -> createSumMetricWithValue(
        spark, "number of files removed", numRemovedFiles),
      "numRemovedBytes" -> createSumMetricWithValue(
        spark, "number of bytes removed", numRemovedBytes)
    )
    txn.registerSQLMetrics(spark, sqlMetrics)
  }

  /**
   * Replace where operationMetrics need to be recorded separately.
   * @param newFiles - AddFile and AddCDCFile added by write job
   * @param deleteActions - AddFile, RemoveFile, AddCDCFile added by Delete job
   */
  protected def registerInsertReplaceMetrics(
      spark: SparkSession,
      txn: OptimisticTransaction,
      newFiles: Seq[Action],
      deleteActions: Seq[Action]): Unit = {
    var numFiles = 0L
    var numCopiedRows = 0L
    var numOutputBytes = 0L
    var numNewRows = 0L
    var numAddedChangedFiles = 0L
    var hasRowLevelMetrics = true

    newFiles.foreach {
      case a: AddFile =>
        numFiles += 1
        numOutputBytes += a.size
        if (a.numLogicalRecords.isEmpty) {
          hasRowLevelMetrics = false
        } else {
          numNewRows += a.numLogicalRecords.get
        }
      case cdc: AddCDCFile =>
        numAddedChangedFiles += 1
      case _ =>
    }

    deleteActions.foreach {
      case a: AddFile =>
        numFiles += 1
        numOutputBytes += a.size
        if (a.numLogicalRecords.isEmpty) {
          hasRowLevelMetrics = false
        } else {
          numCopiedRows += a.numLogicalRecords.get
        }
      case _: AddCDCFile =>
        numAddedChangedFiles += 1
      // Remove metrics will be handled by the delete command.
      case _ =>
    }

    var sqlMetrics = Map(
      "numFiles" -> createSumMetricWithValue(spark, "number of files written", numFiles),
      "numOutputBytes" -> createSumMetricWithValue(spark, "number of output bytes", numOutputBytes),
      "numAddedChangeFiles" -> createSumMetricWithValue(
        spark, "number of change files added", numAddedChangedFiles)
    )
    if (hasRowLevelMetrics) {
      sqlMetrics ++= Map(
        "numOutputRows" -> createSumMetricWithValue(
          spark, "number of rows added", numNewRows + numCopiedRows),
        "numCopiedRows" -> createSumMetricWithValue(spark, "number of copied rows", numCopiedRows)
      )
    } else {
      // this will get filtered out in DeltaOperations.WRITE transformMetrics
      sqlMetrics ++= Map(
        "numOutputRows" -> createSumMetricWithValue(spark, "number of rows added", 0L),
        "numCopiedRows" -> createSumMetricWithValue(spark, "number of copied rows", 0L)
      )
    }
    txn.registerSQLMetrics(spark, sqlMetrics)
  }

  protected def extractConstraints(
      sparkSession: SparkSession,
      exprs: Seq[Expression]): Seq[Constraint] = {
    if (!sparkSession.conf.get(DeltaSQLConf.REPLACEWHERE_CONSTRAINT_CHECK_ENABLED)) {
      Seq.empty
    } else {
      exprs.flatMap { e =>
        // While writing out the new data, we only want to enforce constraint on expressions
        // with UnresolvedAttribute, that is, containing column name. Because we parse a
        // predicate string without analyzing it, if there's a column name, it has to be
        // unresolved.
        e.collectFirst {
          case _: UnresolvedAttribute =>
            val arbitraryExpression = ArbitraryExpression(e)
            Check(arbitraryExpression.name, arbitraryExpression.expression)
        }
      }
    }
  }

  protected case class ReplaceWhereExprsAndDataFilterPresenceInExprs(
      maybeAliasedReplaceWhereExprsOpt: Option[Seq[Expression]],
      containsDataFilters: Boolean)

  /**
   * Strips any targetAlias qualifier from column references in predicate expressions.
   * For example, with targetAlias "t", `t.col` becomes `col`.
   */
  protected def stripTargetAliasIfExists(
      sparkSession: SparkSession,
      exprs: Seq[Expression],
      targetAlias: Option[String]): Seq[Expression] = {
    targetAlias match {
      case Some(alias) =>
        val resolver = sparkSession.sessionState.conf.resolver
        exprs.map(_.transform {
          case u: UnresolvedAttribute
              if u.nameParts.length > 1 && resolver(u.nameParts.head, alias) =>
            UnresolvedAttribute(u.nameParts.tail)
        })
      case None => exprs
    }
  }

  /**
   * Parses the expressions for the `.option("replaceWhere")` and determines whether
   * there is a data filter present in the parsed expressions.
   *
   * Returns two pieces of information:
   * 1. The list of expressions (possibly aliased when targetAlias is set).
   * 2. A flag indicating the presence of a data filter in the expressions list.
   *
   * The information regarding the existence of a data filter is needed for CDF
   * generation.
   */
  protected def getReplaceWhereExprsAndDataFilterPresenceInExprs(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      options: DeltaOptions,
      replaceWhereOnDataColsEnabled: Boolean,
      mode: SaveMode): ReplaceWhereExprsAndDataFilterPresenceInExprs = {
    options.replaceWhere match {
      case Some(replaceWhereStr) =>
        val parsedPredicatesMaybeAliased = parsePredicates(sparkSession, replaceWhereStr)
        val strippedTargetAliasPredicates =
          stripTargetAliasIfExists(
            sparkSession = sparkSession,
            exprs = parsedPredicatesMaybeAliased,
            targetAlias = options.targetAlias)
        if (replaceWhereOnDataColsEnabled) {
          // Use alias-stripped predicates because
          // [[DeltaTableUtils.splitMetadataAndDataPredicates]]
          // compares against non-aliased partition column names.
          val (metadataPredicates, dataFilters) = DeltaTableUtils.splitMetadataAndDataPredicates(
            condition = strippedTargetAliasPredicates.head,
            partitionColumns = txn.metadata.partitionColumns,
            spark = sparkSession)
          if (options.rearrangeOnly && dataFilters.nonEmpty) {
            throw DeltaErrors.replaceWhereWithFilterDataChangeUnset(dataFilters.mkString(","))
          }

          val containsDataFilters = dataFilters.nonEmpty

          ReplaceWhereExprsAndDataFilterPresenceInExprs(
            maybeAliasedReplaceWhereExprsOpt =
              if (options.targetAlias.isDefined) {
                Some(parsedPredicatesMaybeAliased)
              } else {
                Some(metadataPredicates ++ dataFilters)
              },
            containsDataFilters = containsDataFilters)
        } else if (mode == SaveMode.Overwrite) {
          // Use stripped predicates for partition validation.
          verifyPartitionPredicates(
            spark = sparkSession,
            partitionColumns = txn.metadata.partitionColumns,
            predicates = strippedTargetAliasPredicates)
          ReplaceWhereExprsAndDataFilterPresenceInExprs(
            maybeAliasedReplaceWhereExprsOpt = Some(parsedPredicatesMaybeAliased),
            containsDataFilters = false)
        } else {
          ReplaceWhereExprsAndDataFilterPresenceInExprs(
            maybeAliasedReplaceWhereExprsOpt = None,
            containsDataFilters = false)
        }
      case None =>
        ReplaceWhereExprsAndDataFilterPresenceInExprs(
          maybeAliasedReplaceWhereExprsOpt = None,
          containsDataFilters = false)
    }
  }

  /**
   * Validates that `replaceOn`/`replaceUsing` is not combined with incompatible DataFrame options.
   */
  protected def validateReplaceOnOrUsingOptionCombinations(
      options: DeltaOptions,
      isOverwriteOperation: Boolean): Unit = {
    // `replaceOn` and `replaceUsing` are mutually exclusive.
    if (options.replaceOn.isDefined && options.replaceUsing.isDefined) {
      throw DeltaErrors.incompatibleDataFrameOptions(
        DeltaOptions.REPLACE_ON_OPTION, DeltaOptions.REPLACE_USING_OPTION)
    }
    val replaceOnOrUsingOption = if (options.replaceOn.isDefined) {
      DeltaOptions.REPLACE_ON_OPTION
    } else {
      DeltaOptions.REPLACE_USING_OPTION
    }
    // These are Selective Overwrite options with very different semantics, so
    // they are not compatible with `replaceOn`/`replaceUsing`.
    if (options.replaceWhere.isDefined) {
      throw DeltaErrors.overwriteByFilterIncompatibleReplaceOnOrUsingError()
    }
    if (options.partitionOverwriteModeInOptions) {
      throw DeltaErrors.dynamicPartitionOverwriteIncompatibleReplaceOnOrUsingError()
    }
    // `replaceOn`/`replaceUsing` can only replace parts of the table, so combining it with
    // `overwriteSchema` could corrupt the table: the non-replaced rows would still
    // have the old schema while newly written rows would have the new schema.
    if (options.canOverwriteSchema && isOverwriteOperation) {
      throw DeltaErrors.incompatibleDataFrameOptions(
        DeltaOptions.OVERWRITE_SCHEMA_OPTION, replaceOnOrUsingOption)
    }

    // `replaceOn`/`replaceUsing` replaces matched rows with new data, which is inherently
    // a data change. Setting `dataChange` to false is contradictory.
    if (options.rearrangeOnly) {
      throw DeltaErrors.incompatibleDataFrameOptions(
        DeltaOptions.DATA_CHANGE_OPTION, replaceOnOrUsingOption)
    }
  }

  /**
   * Parses the .option("replaceOn") condition or the .options("replaceUsing") columns
   * and constructs an EXISTS subquery:
   *   EXISTS (SELECT 1 FROM inserting_data WHERE replaceOnCond/replaceUsingCond)
   * Here, `replaceOnCond` comes from the .option("replaceOn"), while `replaceUsingCond`
   * generates an equality condition for each column in .option("replaceUsing"),
   * matching query and table columns, combined with AND.
   *
   * This expression is used to identify and delete matching rows in the target table,
   * a step in the atomic replace commands (REPLACE ON/USING).
   *
   * Note:
   * 1. Only query columns are resolved here.
   * 2. Table columns are resolved during the actual delete, i.e.
   * [[WriteIntoDelta.removeFiles]],
   * because resolving them earlier would cause the [[LogicalRelation]] created
   * in the delete step to not recognize the columns due to already assigned
   * exprId, even if they are logically identical.
   */
  protected def getReplaceOnOrUsingExprOpt(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      options: DeltaOptions,
      isInsertReplaceUsingByName: Boolean = false): Expression = {
    require(options.isReplaceOnOrUsingDefined, "This function should only be called when the " +
      "replaceOn/replaceUsing option is defined.")
    require(options.replaceUsing.isEmpty ||
        options.targetAlias.contains(DeltaOptions.REPLACE_USING_INTERNAL_TABLE_ALIAS),
      s"replaceUsing requires targetAlias=${DeltaOptions.REPLACE_USING_INTERNAL_TABLE_ALIAS}" +
        s" set by DeltaInsertReplaceOnOrUsingCommand, but got targetAlias=${options.targetAlias}.")
    val tableRelation = createTableRelation(txn, options.targetAlias)

    def checkColumnExistenceIn(relation: LogicalPlan, attrNameParts: Seq[String]): Boolean = {
      relation.resolve(attrNameParts, sparkSession.sessionState.conf.resolver).isDefined
    }

    // There is a projection on top of the original query schema to match the table
    // column names and types, we need to see the original query in order to have
    // the original column names, to resolve the replaceOn matching condition.
    val originalQueryBeforeSchemaAdjustmentProjection = data.queryExecution.analyzed match {
      case Project(_, child) => child
      case otherDataQueryPlan =>
        throw new IllegalStateException(
          s"Expected a Project node on top of the analyzed query plan for replaceOn/replaceUsing " +
            s"resolution, but found: $otherDataQueryPlan")
    }

    val (uniqueTableRelationAttrs, queryResolvedConditions) = if (options.replaceOn.isDefined) {
      val replaceOnMatchingConds =
        options.replaceOn.map(parsePredicates(sparkSession, _)).get

      val replaceOnAttrs =
        replaceOnMatchingConds.flatMap(_.references).map(_.asInstanceOf[UnresolvedAttribute])
      val ambiguousColumnsInCond = ArrayBuffer[String]()
      val unresolvedColumnsInCond = ArrayBuffer[String]()
      replaceOnAttrs.foreach { replaceOnAttr =>
        val isAttrExistsInTable = checkColumnExistenceIn(
          relation = tableRelation, replaceOnAttr.nameParts)

        val isAttrExistsInQuery = checkColumnExistenceIn(
          relation = originalQueryBeforeSchemaAdjustmentProjection, replaceOnAttr.nameParts)

        if (isAttrExistsInTable && isAttrExistsInQuery) {
          ambiguousColumnsInCond += replaceOnAttr.sql
        }

        if (!isAttrExistsInTable && !isAttrExistsInQuery) {
          unresolvedColumnsInCond += replaceOnAttr.sql
        }
      }

      if (ambiguousColumnsInCond.nonEmpty) {
        throw DeltaErrors.insertReplaceOnAmbiguousColumnsInCond(
          ambiguousColumnsInCond.distinct.toSeq)
      }

      if (unresolvedColumnsInCond.nonEmpty) {
        throw DeltaErrors.insertReplaceOnUnresolvedColumnsInCond(
          unresolvedColumnsInCond.distinct.toSeq)
      }

      val uniqueTableRelationAttrs = replaceOnAttrs.filter { replaceOnAttr =>
        checkColumnExistenceIn(relation = tableRelation, replaceOnAttr.nameParts)
      }.distinct

      val queryResolvedConditions = tryResolveReferencesForExpressions(sparkSession)(
        exprs = replaceOnMatchingConds,
        plansProvidingAttrs = Seq(originalQueryBeforeSchemaAdjustmentProjection))

      (uniqueTableRelationAttrs, queryResolvedConditions)
    } else {
      val replaceUsingCols = options.parsedReplaceUsingColsList.get

      val replaceUsingAttrs = replaceUsingCols.map(UnresolvedAttribute(_))
      replaceUsingAttrs.foreach { replaceUsingAttr =>
        val isAttrExistsInTable = checkColumnExistenceIn(
          relation = tableRelation, replaceUsingAttr.nameParts)

        val isAttrExistsInQuery = checkColumnExistenceIn(
          relation = originalQueryBeforeSchemaAdjustmentProjection, replaceUsingAttr.nameParts)

        if (!isAttrExistsInTable) {
          throw DeltaErrors.unresolvedInsertReplaceUsingColumnsError(
            colName = replaceUsingAttr.name,
            relationType = "table",
            suggestion = tableRelation.schema.fieldNames.sorted
              .map(toSQLId).mkString(", "))
        }

        if (!isAttrExistsInQuery) {
          throw DeltaErrors.unresolvedInsertReplaceUsingColumnsError(
            colName = replaceUsingAttr.name,
            relationType = "query",
            suggestion = originalQueryBeforeSchemaAdjustmentProjection
              .schema.fieldNames.sorted.map(toSQLId).mkString(", "))
        }
      }

      InsertReplaceUsingMisalignedColumnsEventInfo.disallowMisalignedReplaceUsingCols(
        sparkSession.sessionState.conf.resolver,
        replaceUsingCols,
        tableRelation = tableRelation,
        queryRelation = originalQueryBeforeSchemaAdjustmentProjection,
        isByName = isInsertReplaceUsingByName,
        conf = sparkSession.sessionState.conf)

      val uniqueTableRelationAttrs =
        replaceUsingCols.map(col => UnresolvedAttribute(Seq(options.targetAlias.get, col))).distinct

      // For REPLACE USING, an internal table alias is required to ensure certain attributes
      // resolve to the target table, not the query. Without it, column resolution incorrectly
      // references all the attributes to the query.
      val queryResolvedConditions = replaceUsingCols.map(replaceUsingCol => EqualTo(
        left = UnresolvedAttribute(Seq(options.targetAlias.get, replaceUsingCol)),
        right = originalQueryBeforeSchemaAdjustmentProjection.output.find(queryAttr =>
          sparkSession.sessionState.analyzer.resolver(queryAttr.name, replaceUsingCol)).get))

      (uniqueTableRelationAttrs, queryResolvedConditions)
    }

    Exists(
      plan = Project(
        projectList = Seq(Alias(child = Literal(1), "__dummy_name_for_a_constant")()),
        child = Filter(
          condition = queryResolvedConditions.reduce(And),
          child = originalQueryBeforeSchemaAdjustmentProjection
        )
      ),
      outerAttrs = uniqueTableRelationAttrs)
  }
}
