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

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{DataFrameUtils, DeltaErrors, DeltaLog, DeltaOptions, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.DMLUtils.TaggedCommitData
import org.apache.spark.sql.delta.commands.InsertReplaceOnOrUsingAPIOrigin.InsertReplaceOnOrUsingAPIOrigin
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.commons.lang3.StringUtils

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.delta.skipping.clustering.temp.ClusterBySpec
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf

case class DeltaInsertReplaceOnOrUsingCommand(
    deltaTable: DeltaTableV2,
    query: LogicalPlan,
    writeCmd: WriteIntoDelta,
    insertReplaceCriteriaOpt: Option[InsertReplaceCriteria],
    byName: Boolean,
    apiOrigin: InsertReplaceOnOrUsingAPIOrigin)
  extends LeafRunnableCommand
  with WriteIntoDeltaLike
  with InsertReplaceOnMaterializeSource
  {

  private val commandStats = InsertReplaceOnOrUsingStats()

  override def run(sparkSession: SparkSession): Seq[Row] = {
    withMaterializeRetryAndStats(sparkSession) { preparedWriteCmd =>
      val runResult = preparedWriteCmd.run(sparkSession)
      recordWriteStats(preparedWriteCmd,
        writeMetrics = Map.empty)
      runResult
    }
  }

  /**
   * Resolve the [[InsertReplaceCriteria]] for this command. The SQL path already provided
   * the criteria so we use it directly. Otherwise, for DF paths, we parse from
   * the user-provided 'replaceOn' or 'replaceUsing' option.
   */
  private def resolveInsertReplaceCriteria(
      sparkSession: SparkSession): InsertReplaceCriteria = {
    insertReplaceCriteriaOpt.getOrElse {
      if (writeCmd.options.replaceOn.isDefined) {
        val parsedCond =
          sparkSession.sessionState.sqlParser.parseExpression(writeCmd.options.replaceOn.get)
        InsertReplaceOn(parsedCond, writeCmd.options.targetAlias)
      } else {
        if (writeCmd.options.replaceUsing.isEmpty) {
          throw new IllegalStateException(
            "Expected replaceOn or replaceUsing option to be specified")
        }
        InsertReplaceUsing(writeCmd.options.parsedReplaceUsingColsList.get)
      }
    }
  }

  override val deltaLog: DeltaLog = writeCmd.deltaLog
  override val configuration: Map[String, String] = writeCmd.configuration
  // This is the original user-input DataFrame (before materialization).
  override val data: DataFrame = writeCmd.data

  override def withNewWriterConfiguration(
      updatedConfiguration: Map[String, String]): WriteIntoDeltaLike = {
    val updatedWriteCmd = writeCmd.withNewWriterConfiguration(updatedConfiguration) match {
      case writeIntoDelta: WriteIntoDelta => writeIntoDelta
      case other => throw new IllegalStateException(
        s"Unexpected WriteIntoDeltaLike type: ${other.getClass.getName}")
    }
    copy(writeCmd = updatedWriteCmd.asInstanceOf[WriteIntoDelta])
  }

  /** Called by [[CreateDeltaTableCommand]] in the saveAsTable() path. */
  override def writeAndReturnCommitData(
      txn: OptimisticTransaction,
      sparkSession: SparkSession,
      clusterBySpecOpt: Option[ClusterBySpec] = None,
      isTableReplace: Boolean = false): TaggedCommitData[Action] = {
    withMaterializeRetryAndStats(sparkSession) { preparedWriteCmd =>
      val commitData = preparedWriteCmd.writeAndReturnCommitData(
        txn, sparkSession, clusterBySpecOpt, isTableReplace)
      recordWriteStats(preparedWriteCmd,
        writeMetrics = Map.empty)
      commitData
    }
  }

  private def prepareWriteCmd(
      sparkSession: SparkSession,
      materializedDf: DataFrame,
      insertReplaceCriteria: InsertReplaceCriteria): WriteIntoDelta = {
    /**
     * For REPLACE USING, an internal table alias is required to ensure certain attributes
     * resolve to the target table, not the query, when we construct the EXISTS condition.
     * Without it, column resolution incorrectly references all the attributes to the nearest
     * possible relation, which is the query.
     */
    val tableAliasOption: Map[String, String] = insertReplaceCriteria match {
      case InsertReplaceOn(_, aliasOpt) =>
        aliasOpt.map(DeltaOptions.TARGET_ALIAS_OPTION -> _).toMap
      case _: InsertReplaceUsing =>
        Map(DeltaOptions.TARGET_ALIAS_OPTION -> DeltaOptions.REPLACE_USING_INTERNAL_TABLE_ALIAS)
      case other => throw new IllegalStateException(
        s"Unexpected InsertReplaceCriteria: $other")
    }

    val deltaOptions = new DeltaOptions(
      CaseInsensitiveMap[String](
        writeCmd.options.options ++
        tableAliasOption),
      sparkSession.sessionState.conf
    )

    writeCmd.copy(
      options = deltaOptions,
      data = materializedDf,
      isInsertReplaceUsingByName = byName && insertReplaceCriteria.isInstanceOf[InsertReplaceUsing])
  }

  /**
   * Materialize the source query and restore the [[Project]] node on top.
   *
   * The projection is temporarily removed so that materialization doesn't produce
   * an RDD scan at the top level, which would break REPLACE ON/USING analysis.
   */
  private def materializeSource(
      sparkSession: SparkSession,
      insertReplaceCriteria: InsertReplaceCriteria): DataFrame = {
    commandStats.replaceCriteria = StringUtils.abbreviate(
      insertReplaceCriteria match {
        case InsertReplaceOn(cond, _) => cond.sql
        case InsertReplaceUsing(cols) => cols.mkString(", ")
        case other => throw new IllegalStateException(
          s"Unexpected InsertReplaceCriteria: $other")
      },
      2048)

    val Project(schemaAdjustmentProjectionList, originalQueryBeforeSchemaAdjustmentProjection) =
      query

    // This is the matching condition of the INSERT REPLACE ON/USING operation.
    val replaceOnOrUsingCond: Expression = insertReplaceCriteria match {
      case InsertReplaceOn(cond, _) => cond
      case InsertReplaceUsing(cols) => cols.map(col => EqualTo(UnresolvedAttribute(col),
        UnresolvedAttribute(Seq(
          DeltaOptions.REPLACE_USING_INTERNAL_TABLE_ALIAS, col)))).reduce(And)
      case other => throw new IllegalStateException(
        s"Unexpected InsertReplaceCriteria: $other")
    }

    prepareMergeSource(
      spark = sparkSession,
      source = originalQueryBeforeSchemaAdjustmentProjection,
      condition = replaceOnOrUsingCond,
      matchedClauses = Seq.empty,
      notMatchedClauses = Seq.empty,
      isInsertOnly = false)

    // TODO: Despite the name, getMergeSource obtains the materialized source for
    // INSERT REPLACE ON/USING operations. It will have a more appropriate name when we fully
    // refactor the source materialization logic.
    DataFrameUtils.ofRows(
      sparkSession,
      Project(schemaAdjustmentProjectionList, getMergeSource.df.queryExecution.analyzed))
  }

  private def checkShouldMaterializeSource(sparkSession: SparkSession): Boolean = {
    val (shouldMaterialize, materializeReason) =
      shouldMaterializeSource(sparkSession, source = query, isInsertOnly = false)
    commandStats.materializeSourceReason = Some(materializeReason.toString)
    shouldMaterialize
  }

  private def executeWithRetry(
      sparkSession: SparkSession,
      runOperationFunc: SparkSession => Seq[Row]): Seq[Row] = {
    val shouldMaterialize = checkShouldMaterializeSource(sparkSession)
    if (!shouldMaterialize) {
      runOperationFunc(sparkSession)
    } else {
      runWithMaterializedSourceLostRetries(
        spark = sparkSession,
        deltaLog = deltaTable.deltaLog,
        metrics = Map.empty,
        runOperationFunc = runOperationFunc)
    }
  }

  private def withMaterializeRetryAndStats[T](
      sparkSession: SparkSession)(
      executeAndRecordStats: WriteIntoDelta => T): T = {
    recordReplaceOnOrUsingStats(sparkSession) {
      val insertReplaceCriteria = resolveInsertReplaceCriteria(sparkSession)
      var resultOpt: Option[T] = None

      executeWithRetry(sparkSession, runOperationFunc = { spark =>
        val materializedDf = materializeSource(spark, insertReplaceCriteria)
        val preparedWriteCmd = prepareWriteCmd(spark, materializedDf, insertReplaceCriteria)
        resultOpt = Some(executeAndRecordStats(preparedWriteCmd))
        Seq.empty
      })

      resultOpt.getOrElse(throw new IllegalStateException(
        "executeWithRetry did not invoke the operation function"))
    }
  }

  private def recordReplaceOnOrUsingStats[T](sparkSession: SparkSession)(func: => T): T = {
    val commandStartTimeMs = System.currentTimeMillis()
    commandStats.apiOrigin = apiOrigin.toString
    try {
      func
    } catch {
      case NonFatal(ex) =>
        commandStats.exceptionMsg = Some(ex.getMessage)
        throw ex
    } finally {
      commandStats.totalExecutionTimeMs = System.currentTimeMillis() - commandStartTimeMs
      recordDeltaEvent(
        deltaLog = deltaTable.deltaLog,
        opType = "delta.insertReplaceOnOrUsing.stats",
        data = commandStats)
    }
  }

  private def recordWriteStats(
      lastWriteCmd: WriteIntoDelta,
      writeMetrics: Map[String, SQLMetric]): Unit = {
    commandStats.materializeSourceAttempts = Some(attempt)

    def getMetricValueIfExists(key: String): Option[Long] = writeMetrics.get(key).map(_.value)
    commandStats.numFiles = getMetricValueIfExists("numFiles")
    commandStats.numOutputBytes = getMetricValueIfExists("numOutputBytes")
    commandStats.numOutputRows = getMetricValueIfExists("numOutputRows")
    commandStats.numRemovedFiles = getMetricValueIfExists("numRemovedFiles")
    commandStats.numDeletedRows = getMetricValueIfExists("numDeletedRows")
    commandStats.numRemovedBytes = getMetricValueIfExists("numRemovedBytes")
    commandStats.numAddedChangeFiles = getMetricValueIfExists("numAddedChangeFiles")
    commandStats.numCopiedRows = getMetricValueIfExists("numCopiedRows")
  }
}

object DeltaInsertReplaceOnOrUsingCommand {
  /**
   * Adds a [[Project]] node on top of the query that aliases each query column to
   * the corresponding provided attribute name by ordinal position.
   * This ensures the query plan has the [[Project]] node at the top required by
   * [[DeltaAnalysis]]).
   */
  private[delta] def addOrdinalAliasProjection(
      queryToAlias: LogicalPlan, aliasAttrs: Seq[Attribute]): Project = {
    val projectAliasList = queryToAlias.output.zipWithIndex.map {
      case (queryAttr, index) =>
        if (index < aliasAttrs.length) {
          Alias(queryAttr, aliasAttrs(index).name)()
        } else {
          queryAttr
        }
    }
    Project(projectAliasList, queryToAlias)
  }

  /**
   * Creates a [[DeltaInsertReplaceOnOrUsingCommand]] for DataFrameWriterV1 save() and
   * saveAsTable() with replaceOn/replaceUsing options.
   *
   * df.save() and df.saveAsTable() bypass [[DeltaAnalysis]] and never add a
   * projection, so this method calls [[addOrdinalAliasProjection]] to add an
   * identity projection. Since save()/saveAsTable() resolve columns by name (not by
   * position), the projection preserves the original source column names while
   * ensuring the Project node structure that is required by REPLACE ON/USING
   *
   * For SQL INSERT REPLACE ON/USING and df.insertInto() that go through
   * [[DeltaAnalysis]], the rule may add a schema adjustment projection,
   * so the projection handling is different (see [[DeltaAnalysis]] for
   * their implementation for REPLACE ON/USING).
   */
  private[delta] def createCmdForSaveAndSaveAsTable(
      deltaTable: DeltaTableV2,
      data: DataFrame,
      writeCmd: WriteIntoDelta,
      apiOrigin: InsertReplaceOnOrUsingAPIOrigin
  ): DeltaInsertReplaceOnOrUsingCommand = {
    val dataQueryExecAnalyzed = data.queryExecution.analyzed
    DeltaInsertReplaceOnOrUsingCommand(
      deltaTable = deltaTable,
      query = addOrdinalAliasProjection(
        queryToAlias = dataQueryExecAnalyzed,
        aliasAttrs = dataQueryExecAnalyzed.output),
      writeCmd = writeCmd,
      insertReplaceCriteriaOpt = None,
      byName = true,
      apiOrigin = apiOrigin)
  }
}

/** Stats for a SQL/DataFrame INSERT REPLACE ON/USING operation */
private[delta] case class InsertReplaceOnOrUsingStats(
    var apiOrigin: String = "",
    var materializeSourceReason: Option[String] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    var materializeSourceAttempts: Option[Long] = None,
    var materializeSourceTimeMs: Long = 0,
    var replaceCriteria: String = "",
    var isDeleteInsertParallelized: Option[Boolean] = None,
    var deleteStepTimeMs: Long = 0,
    var insertStepTimeMs: Long = 0,
    var totalExecutionTimeMs: Long = 0,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    var numFiles: Option[Long] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    var numOutputBytes: Option[Long] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    var numOutputRows: Option[Long] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    var numRemovedFiles: Option[Long] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    var numDeletedRows: Option[Long] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    var numRemovedBytes: Option[Long] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    var numAddedChangeFiles: Option[Long] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    var numCopiedRows: Option[Long] = None,
    var exceptionMsg: Option[String] = None
)

object InsertReplaceOnOrUsingAPIOrigin extends Enumeration {
  type InsertReplaceOnOrUsingAPIOrigin = Value
  val DFv1InsertInto: Value = Value("DFv1InsertInto")
  val DFv1Save: Value = Value("DFv1Save")
  val DFv1SaveAsTable: Value = Value("DFv1SaveAsTable")
}

sealed abstract class InsertReplaceCriteria
case class InsertReplaceOn(cond: Expression, tableAliasOpt: Option[String])
  extends InsertReplaceCriteria
case class InsertReplaceUsing(cols: Seq[String]) extends InsertReplaceCriteria

object InsertReplaceUsingMisalignedColumnsEventInfo {
  def disallowMisalignedReplaceUsingCols(
      resolver: (String, String) => Boolean,
      replaceUsingCols: Seq[String],
      tableRelation: LogicalPlan,
      queryRelation: LogicalPlan,
      isByName: Boolean,
      conf: SQLConf): Unit = {
    if (!isByName) {
      val misaligned = replaceUsingCols.collect {
        case col if tableRelation.output.indexWhere(a => resolver(a.name, col)) !=
            queryRelation.output.indexWhere(a => resolver(a.name, col)) => col
      }
      if (misaligned.nonEmpty) {
        throw DeltaErrors.disallowInsertReplaceUsingWithMisalignedColumns(misaligned)
      }
    }
  }
}
