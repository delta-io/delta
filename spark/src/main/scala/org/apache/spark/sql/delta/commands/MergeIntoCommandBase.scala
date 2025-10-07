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

import java.util.concurrent.TimeUnit

import scala.collection.mutable

import org.apache.spark.sql.delta.metric.IncrementMetric
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{AddFile, FileAction}
import org.apache.spark.sql.delta.commands.merge.{MergeIntoMaterializeSource, MergeIntoMaterializeSourceReason, MergeStats}
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeFileIndex, TransactionalWrite}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, SchemaUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkContext
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

trait MergeIntoCommandBase extends LeafRunnableCommand
  with DeltaCommand
  with DeltaLogging
  with PredicateHelper
  with ImplicitMetadataOperation
  with MergeIntoMaterializeSource
  with UpdateExpressionsSupport
  with SupportsNonDeterministicExpression {

  @transient val source: LogicalPlan
  @transient val target: LogicalPlan
  @transient val targetFileIndex: TahoeFileIndex
  val condition: Expression
  val matchedClauses: Seq[DeltaMergeIntoMatchedClause]
  val notMatchedClauses: Seq[DeltaMergeIntoNotMatchedClause]
  val notMatchedBySourceClauses: Seq[DeltaMergeIntoNotMatchedBySourceClause]
  val migratedSchema: Option[StructType]
  val schemaEvolutionEnabled: Boolean

  protected def shouldWritePersistentDeletionVectors(
      spark: SparkSession,
      txn: OptimisticTransaction): Boolean = {
    spark.conf.get(DeltaSQLConf.MERGE_USE_PERSISTENT_DELETION_VECTORS) &&
      DeletionVectorUtils.deletionVectorsWritable(txn.snapshot)
  }

  override val (canMergeSchema, canOverwriteSchema) = {
    // Delta options can't be passed to MERGE INTO currently, so they'll always be empty.
    // The methods in options check if user has instructed to turn on schema evolution for this
    // statement, or the auto migration DeltaSQLConf is on, in which case schema evolution
    // will be allowed.
    val options = new DeltaOptions(Map.empty[String, String], conf)
    (schemaEvolutionEnabled || options.canMergeSchema, options.canOverwriteSchema)
  }

  @transient protected lazy val sc: SparkContext = SparkContext.getOrCreate()
  @transient protected lazy val targetDeltaLog: DeltaLog = targetFileIndex.deltaLog

  /**
   * Map to get target read attributes by name. The case sensitivity of the map is set accordingly
   * to Spark configuration.
   */
  @transient private lazy val preEvolutionTargetAttributesMap: Map[String, Attribute] = {
    val attrMap: Map[String, Attribute] = target
      .outputSet.view
      .map(attr => attr.name -> attr).toMap
    if (conf.caseSensitiveAnalysis) {
      attrMap
    } else {
      CaseInsensitiveMap(attrMap)
    }
  }

  /**
   * Expressions to convert from a pre-evolution target row to the post-evolution target row. These
   * expressions are used for columns that are not modified in updated rows or to copy rows that are
   * not modified.
   * There are two kinds of expressions here:
   *  * References to existing columns in the target dataframe. Note that these references may have
   *    a different data type than they originally did due to schema evolution so we add a cast that
   *    supports schema evolution. The references will be marked as nullable if `makeNullable` is
   *    set to true, which allows the attributes to reference the output of an outer join.
   *  * Literal nulls, for new columns which are being added to the target table as part of
   *    this transaction, since new columns will have a value of null for all existing rows.
   */
  protected def postEvolutionTargetExpressions(makeNullable: Boolean = false)
    : Seq[NamedExpression] = {
    val schema = if (makeNullable) {
      migratedSchema.getOrElse(target.schema).asNullable
    } else {
      migratedSchema.getOrElse(target.schema)
    }
    schema.map { col =>
      preEvolutionTargetAttributesMap
        .get(col.name)
        .map { attr =>
          Alias(
            castIfNeeded(
              attr.withNullability(attr.nullable || makeNullable),
              col.dataType,
              castingBehavior = MergeOrUpdateCastingBehavior(canMergeSchema),
              col.name),
            col.name
          )()
        }
        .getOrElse(Alias(Literal(null), col.name)())
    }
  }

  /** Whether this merge statement has only MATCHED clauses. */
  protected def isMatchedOnly: Boolean = notMatchedClauses.isEmpty && matchedClauses.nonEmpty &&
    notMatchedBySourceClauses.isEmpty

  /** Whether this merge statement only has only insert (NOT MATCHED) clauses. */
  protected def isInsertOnly: Boolean = matchedClauses.isEmpty && notMatchedClauses.nonEmpty &&
    notMatchedBySourceClauses.isEmpty

  /** Whether this merge statement includes inserts statements. */
  protected def includesInserts: Boolean = notMatchedClauses.nonEmpty

  /** Whether this merge statement includes delete statements. */
  protected def includesDeletes: Boolean = {
    matchedClauses.exists(_.isInstanceOf[DeltaMergeIntoMatchedDeleteClause]) ||
      notMatchedBySourceClauses.exists(_.isInstanceOf[DeltaMergeIntoNotMatchedBySourceDeleteClause])
  }

  protected def isCdcEnabled(deltaTxn: OptimisticTransaction): Boolean =
    DeltaConfigs.CHANGE_DATA_FEED.fromMetaData(deltaTxn.metadata)

  protected def runMerge(spark: SparkSession): Seq[Row]

  override def run(spark: SparkSession): Seq[Row] = {
    metrics("executionTimeMs").set(0)
    metrics("scanTimeMs").set(0)
    metrics("rewriteTimeMs").set(0)
    if (migratedSchema.isDefined) {
      // Block writes of void columns in the Delta log. Currently void columns are not properly
      // supported and are dropped on read, but this is not enough for merge command that is also
      // reading the schema from the Delta log. Until proper support we prefer to fail merge
      // queries that add void columns.
      val newNullColumn = SchemaUtils.findNullTypeColumn(migratedSchema.get)
      if (newNullColumn.isDefined) {
        throw DeltaErrors.mergeAddVoidColumn(newNullColumn.get)
      }
    }

    val (materializeSource, _) = shouldMaterializeSource(spark, source, isInsertOnly)
    if (!materializeSource) {
      runMerge(spark)
    } else {
      // If it is determined that source should be materialized, wrap the execution with retries,
      // in case the data of the materialized source is lost.
      runWithMaterializedSourceLostRetries(
        spark, targetFileIndex.deltaLog, metrics, runMerge)
    }
  }

  import SQLMetrics._

  override lazy val metrics: Map[String, SQLMetric] = baseMetrics

  lazy val baseMetrics: Map[String, SQLMetric] = Map(
    "numSourceRows" -> createMetric(sc, "number of source rows"),
    "numSourceRowsInSecondScan" ->
      createMetric(sc, "number of source rows (during repeated scan)"),
    "operationNumSourceRows" -> createMetric(sc, "number of source rows (reported)"),
    "numTargetRowsCopied" -> createMetric(sc, "number of target rows rewritten unmodified"),
    "numTargetRowsInserted" -> createMetric(sc, "number of inserted rows"),
    "numTargetRowsUpdated" -> createMetric(sc, "number of updated rows"),
    "numTargetRowsMatchedUpdated" -> createMetric(sc, "number of rows updated by a matched clause"),
    "numTargetRowsNotMatchedBySourceUpdated" ->
      createMetric(sc, "number of rows updated by a not matched by source clause"),
    "numTargetRowsDeleted" -> createMetric(sc, "number of deleted rows"),
    "numTargetRowsMatchedDeleted" -> createMetric(sc, "number of rows deleted by a matched clause"),
    "numTargetRowsNotMatchedBySourceDeleted" ->
      createMetric(sc, "number of rows deleted by a not matched by source clause"),
    "numTargetFilesBeforeSkipping" -> createMetric(sc, "number of target files before skipping"),
    "numTargetFilesAfterSkipping" -> createMetric(sc, "number of target files after skipping"),
    "numTargetFilesRemoved" -> createMetric(sc, "number of files removed to target"),
    "numTargetFilesAdded" -> createMetric(sc, "number of files added to target"),
    "numTargetChangeFilesAdded" ->
      createMetric(sc, "number of change data capture files generated"),
    "numTargetChangeFileBytes" ->
      createMetric(sc, "total size of change data capture files generated"),
    "numTargetBytesBeforeSkipping" -> createMetric(sc, "number of target bytes before skipping"),
    "numTargetBytesAfterSkipping" -> createMetric(sc, "number of target bytes after skipping"),
    "numTargetBytesRemoved" -> createMetric(sc, "number of target bytes removed"),
    "numTargetBytesAdded" -> createMetric(sc, "number of target bytes added"),
    "numTargetPartitionsAfterSkipping" ->
      createMetric(sc, "number of target partitions after skipping"),
    "numTargetPartitionsRemovedFrom" ->
      createMetric(sc, "number of target partitions from which files were removed"),
    "numTargetPartitionsAddedTo" ->
      createMetric(sc, "number of target partitions to which files were added"),
    "executionTimeMs" ->
      createTimingMetric(sc, "time taken to execute the entire operation"),
    "materializeSourceTimeMs" ->
      createTimingMetric(sc, "time taken to materialize source (or determine it's not needed)"),
    "scanTimeMs" ->
      createTimingMetric(sc, "time taken to scan the files for matches"),
    "rewriteTimeMs" ->
      createTimingMetric(sc, "time taken to rewrite the matched files"),
    "numTargetDeletionVectorsAdded" -> createMetric(sc, "number of deletion vectors added"),
    "numTargetDeletionVectorsRemoved" -> createMetric(sc, "number of deletion vectors removed"),
    "numTargetDeletionVectorsUpdated" -> createMetric(sc, "number of deletion vectors updated")
  )

  /**
   * Collects the merge operation stats and metrics into a [[MergeStats]] object that can be
   * recorded with `recordDeltaEvent`. Merge stats should be collected after committing all new
   * actions as metrics may still be updated during commit.
   */
  protected def collectMergeStats(
      deltaTxn: OptimisticTransaction,
      materializeSourceReason: MergeIntoMaterializeSourceReason.MergeIntoMaterializeSourceReason,
      commitVersion: Option[Long],
      numRecordsStats: NumRecordsStats): MergeStats = {
    val stats = MergeStats.fromMergeSQLMetrics(
      metrics,
      condition,
      matchedClauses,
      notMatchedClauses,
      notMatchedBySourceClauses,
      isPartitioned = deltaTxn.metadata.partitionColumns.nonEmpty,
      performedSecondSourceScan = performedSecondSourceScan,
      commitVersion = commitVersion,
      numRecordsStats = numRecordsStats
    )
    stats.copy(
      materializeSourceReason = Some(materializeSourceReason.toString),
      materializeSourceAttempts = Some(attempt))
  }

  protected def shouldOptimizeMatchedOnlyMerge(spark: SparkSession): Boolean = {
    isMatchedOnly && spark.conf.get(DeltaSQLConf.MERGE_MATCHED_ONLY_ENABLED)
  }

  // There is only one when matched clause and it's a Delete and it does not have a condition.
  protected val isOnlyOneUnconditionalDelete: Boolean =
    matchedClauses == Seq(DeltaMergeIntoMatchedDeleteClause(None))

  // We over-count numTargetRowsDeleted when there are multiple matches;
  // this is the amount of the overcount, so we can subtract it to get a correct final metric.
  protected var multipleMatchDeleteOnlyOvercount: Option[Long] = None

  // Throw error if multiple matches are ambiguous or cannot be computed correctly.
  protected def throwErrorOnMultipleMatches(
      hasMultipleMatches: Boolean, spark: SparkSession): Unit = {
    // Multiple matches are not ambiguous when there is only one unconditional delete as
    // all the matched row pairs in the 2nd join in `writeAllChanges` will get deleted.
    if (hasMultipleMatches && !isOnlyOneUnconditionalDelete) {
      throw DeltaErrors.multipleSourceRowMatchingTargetRowInMergeException(spark)
    }
  }

  /**
   * Write the output data to files, repartitioning the output DataFrame by the partition columns
   * if table is partitioned and `merge.repartitionBeforeWrite.enabled` is set to true.
   */
  protected def writeFiles(
      spark: SparkSession,
      txn: OptimisticTransaction,
      outputDF: DataFrame): Seq[FileAction] = {
    val partitionColumns = txn.metadata.partitionColumns
    // If the write will be an optimized write, which shuffles the data anyway, then don't
    // repartition. Optimized writes can handle both splitting very large tasks and coalescing
    // very small ones.
    if (partitionColumns.nonEmpty && spark.conf.get(DeltaSQLConf.MERGE_REPARTITION_BEFORE_WRITE)
      && !TransactionalWrite.shouldOptimizeWrite(txn.metadata, spark.sessionState.conf)) {
      txn.writeFiles(outputDF.repartition(partitionColumns.map(col): _*))
    } else {
      txn.writeFiles(outputDF)
    }
  }

  /**
   * Builds a new logical plan to read the given `files` instead of the whole target table.
   * The plan returned has the same output columns (exprIds) as the `target` logical plan, so that
   * existing update/insert expressions can be applied on this new plan. Unneeded non-partition
   * columns may be dropped.
   */
  protected def buildTargetPlanWithFiles(
      spark: SparkSession,
      deltaTxn: OptimisticTransaction,
      files: Seq[AddFile],
      columnsToDrop: Seq[String]): LogicalPlan = {
    // Action type "batch" is a historical artifact; the original implementation used it.
    val fileIndex = new TahoeBatchFileIndex(
      spark,
      actionType = "batch",
      files,
      deltaTxn.deltaLog,
      targetFileIndex.path,
      deltaTxn.snapshot)

    buildTargetPlanWithIndex(
      spark,
      fileIndex,
      columnsToDrop
    )
  }

  /**
   * Builds a new logical plan to read the target table using the given `fileIndex`.
   * The plan returned has the same output columns (exprIds) as the `target` logical plan, so that
   * existing update/insert expressions can be applied on this new plan.
   *
   * @param columnsToDrop unneeded non-partition columns to be dropped
   */
  protected def buildTargetPlanWithIndex(
      spark: SparkSession,
      fileIndex: TahoeFileIndex,
      columnsToDrop: Seq[String]): LogicalPlan = {
    var newTarget = DeltaTableUtils.replaceFileIndex(target, fileIndex)
    newTarget = DeltaTableUtils.dropColumns(spark, newTarget, columnsToDrop)
    newTarget
  }

  /** @return An `Expression` to increment a SQL metric */
  protected def incrementMetricAndReturnBool(
      name: String,
      valueToReturn: Boolean): Expression = {
    IncrementMetric(Literal(valueToReturn), metrics(name))
  }

  /** @return An `Expression` to increment SQL metrics */
  protected def incrementMetricsAndReturnBool(
      names: Seq[String],
      valueToReturn: Boolean): Expression = {
    val incExpr = incrementMetricAndReturnBool(names.head, valueToReturn)
    names.tail.foldLeft(incExpr) { case (expr, name) =>
      IncrementMetric(expr, metrics(name))
    }
  }

  protected def getTargetOnlyPredicates(spark: SparkSession): Seq[Expression] = {
    val targetOnlyPredicatesOnCondition =
      splitConjunctivePredicates(condition).filter(_.references.subsetOf(target.outputSet))

    if (!isMatchedOnly) {
      targetOnlyPredicatesOnCondition
    } else {
      val targetOnlyMatchedPredicate = matchedClauses
        .map(_.condition.getOrElse(Literal.TrueLiteral))
        .map { condition =>
          splitConjunctivePredicates(condition)
            .filter(_.references.subsetOf(target.outputSet))
            .reduceOption(And)
            .getOrElse(Literal.TrueLiteral)
        }
        .reduceOption(Or)
      targetOnlyPredicatesOnCondition ++ targetOnlyMatchedPredicate
    }
  }

  protected def seqToString(exprs: Seq[Expression]): String = exprs.map(_.sql).mkString("\n\t")

  /**
   * Execute the given `thunk` and return its result while recording the time taken to do it
   * and setting additional local properties for better UI visibility.
   *
   * @param extraOpType extra operation name recorded in the logs
   * @param status human readable status string describing what the thunk is doing
   * @param sqlMetricName name of SQL metric to update with the time taken by the thunk
   * @param thunk the code to execute
   */
  protected def recordMergeOperation[A](
      extraOpType: String = "",
      status: String = null,
      sqlMetricName: String = null)(
      thunk: => A): A = {
    val changedOpType = if (extraOpType == "") {
      "delta.dml.merge"
    } else {
      s"delta.dml.merge.$extraOpType"
    }

    val prevDesc = sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION)
    val newDesc = Option(status).map { s =>
      // Append the status to existing description if any
      val prefix = Option(prevDesc).filter(_.nonEmpty).map(_ + " - ").getOrElse("")
      prefix + s
    }

    def executeThunk(): A = {
      try {
        val startTimeNs = System.nanoTime()
        newDesc.foreach { d => sc.setJobDescription(d) }
        val r = thunk
        val timeTakenMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)
        if (sqlMetricName != null) {
          if (timeTakenMs > 0) {
            metrics(sqlMetricName) += timeTakenMs
          } else if (metrics(sqlMetricName).isZero) {
            // Make it always at least 1ms if it ran, to distinguish whether it ran or not.
            metrics(sqlMetricName) += 1
          }
        }
        r
      } finally {
        if (newDesc.isDefined) {
          sc.setJobDescription(prevDesc)
        }
      }
    }

    recordDeltaOperation(targetDeltaLog, changedOpType) {
      if (status == null) {
        executeThunk()
      } else {
        withStatusCode("DELTA", status) { executeThunk() }
      }
    }
  }

  // Whether the source was scanned the second time, and it was guaranteed to be a scan without
  // pruning.
  protected var secondSourceScanWasFullScan: Boolean = false

  /**
   * Sets operationNumSourceRows as numSourceRowsInSecondScan if the source was scanned without
   * possibility of pruning in the 2nd scan. Uses numSourceRows otherwise.
   */
  protected def setOperationNumSourceRowsMetric(): Unit = {
    val operationNumSourceRows = if (secondSourceScanWasFullScan) {
      metrics("numSourceRowsInSecondScan").value
    } else {
      metrics("numSourceRows").value
    }
    metrics("operationNumSourceRows").set(operationNumSourceRows)
  }

  // Whether we actually scanned the source twice or the value in numSourceRowsInSecondScan is
  // uninitialised.
  protected var performedSecondSourceScan: Boolean = true

  /**
   * Throws an exception if merge metrics indicate that the source table changed between the first
   * and the second source table scans.
   */
  protected def checkNonDeterministicSource(spark: SparkSession): Unit = {
    // We only detect changes in the number of source rows. This is a best-effort detection; a
    // more comprehensive solution would be to checksum the values for the columns that we read
    // in both jobs.
    // If numSourceRowsInSecondScan is < 0 then it hasn't run, e.g. for insert-only merges.
    // In that case we have only read the source table once.
    if (performedSecondSourceScan &&
        metrics("numSourceRows").value != metrics("numSourceRowsInSecondScan").value) {
      log.warn(s"Merge source has ${metrics("numSourceRows")} rows in initial scan but " +
        s"${metrics("numSourceRowsInSecondScan")} rows in second scan")
      if (conf.getConf(DeltaSQLConf.MERGE_FAIL_IF_SOURCE_CHANGED)) {
        throw DeltaErrors.sourceNotDeterministicInMergeException(spark)
      }
    }
  }

  /**
   * Check whether (part of) the give source dataframe is cached and logs an assertion or fails if
   * it is. Query caching doesn't pin versions of delta tables and can lead to incorrect results so
   * cached source plans must be materialized.
   */
  def checkSourcePlanIsNotCached(spark: SparkSession, source: LogicalPlan): Unit = {
    val sourceIsCached = planContainsCachedRelation(DataFrameUtils.ofRows(spark, source))
    if (sourceIsCached &&
        spark.conf.get(DeltaSQLConf.MERGE_FAIL_SOURCE_CACHED_AFTER_MATERIALIZATION)) {
      throw DeltaErrors.mergeConcurrentOperationCachedSourceException()
    }

    deltaAssert(
      !sourceIsCached,
      name = "merge.sourceCachedAfterMaterializationStep",
      msg = "Cached source plans must be materialized in MERGE but the source only got cached " +
        "after the decision to materialize was taken.",
      deltaLog = targetDeltaLog
    )
  }

  override protected def prepareMergeSource(
      spark: SparkSession,
      source: LogicalPlan,
      condition: Expression,
      matchedClauses: Seq[DeltaMergeIntoMatchedClause],
      notMatchedClauses: Seq[DeltaMergeIntoNotMatchedClause],
      isInsertOnly: Boolean
    ): Unit = recordMergeOperation(
      extraOpType = "materializeSource",
      status = "MERGE operation - materialize source",
      sqlMetricName = "materializeSourceTimeMs") {
    super.prepareMergeSource(
      spark, source, condition, matchedClauses, notMatchedClauses, isInsertOnly)
  }

  /**
   * Verify that the high water marks used by the identity column generators still match the
   * the high water marks in the version of the table read by the current transaction.
   * These high water marks were determined during analysis in [[PreprocessTableMerge]],
   * which runs outside of the current transaction, so they may no longer be valid.
   */
  protected def checkIdentityColumnHighWaterMarks(deltaTxn: OptimisticTransaction): Unit = {
    notMatchedClauses.foreach { clause =>
      if (deltaTxn.metadata.schema.length != clause.resolvedActions.length) {
        throw new IllegalStateException
      }
      deltaTxn.metadata.schema.zip(clause.resolvedActions.map(_.expr)).foreach {
        case (f, GenerateIdentityValues(gen)) =>
          val info = IdentityColumn.getIdentityInfo(f)
          if (info.highWaterMark != gen.highWaterMarkOpt) {
            IdentityColumn.logTransactionAbort(deltaTxn.deltaLog)
            throw DeltaErrors.metadataChangedException(conflictingCommit = None)
          }

        case (f, _) if ColumnWithDefaultExprUtils.isIdentityColumn(f) &&
          !IdentityColumn.allowExplicitInsert(f) =>
          throw new IllegalStateException

        case _ => ()
      }
    }
  }

  /** Returns whether it allows non-deterministic expressions. */
  override def allowNonDeterministicExpression: Boolean = {
    def isConditionDeterministic(mergeClause: DeltaMergeIntoClause): Boolean = {
      mergeClause.condition match {
        case Some(c) => c.deterministic
        case None => true
      }
    }
    // Allow actions to be non-deterministic while all the conditions
    // must be deterministic.
    condition.deterministic &&
      matchedClauses.forall(isConditionDeterministic) &&
      notMatchedClauses.forall(isConditionDeterministic) &&
      notMatchedBySourceClauses.forall(isConditionDeterministic)
  }
}

object MergeIntoCommandBase {
  val ROW_ID_COL = "_row_id_"
  val FILE_NAME_COL = "_file_name_"
  val SOURCE_ROW_PRESENT_COL = "_source_row_present_"
  val TARGET_ROW_PRESENT_COL = "_target_row_present_"
  val ROW_DROPPED_COL = "_row_dropped_"
  val PRECOMPUTED_CONDITION_COL = "_condition_"

  /**
   * Spark UI will track all normal accumulators along with Spark tasks to show them on Web UI.
   * However, the accumulator used by `MergeIntoCommand` can store a very large value since it
   * tracks all files that need to be rewritten. We should ask Spark UI to not remember it,
   * otherwise, the UI data may consume lots of memory. Hence, we use the prefix `internal.metrics.`
   * to make this accumulator become an internal accumulator, so that it will not be tracked by
   * Spark UI.
   */
  val TOUCHED_FILES_ACCUM_NAME = "internal.metrics.MergeIntoDelta.touchedFiles"


  /** Count the number of distinct partition values among the AddFiles in the given set. */
  def totalBytesAndDistinctPartitionValues(files: Seq[FileAction]): (Long, Int) = {
    val distinctValues = new mutable.HashSet[Map[String, String]]()
    var bytes = 0L
    files.collect { case file: AddFile =>
      distinctValues += file.partitionValues
      bytes += file.size
    }.toList
    // If the only distinct value map is an empty map, then it must be an unpartitioned table.
    // Return 0 in that case.
    val numDistinctValues =
      if (distinctValues.size == 1 && distinctValues.head.isEmpty) 0 else distinctValues.size
    (bytes, numDistinctValues)
  }
}
