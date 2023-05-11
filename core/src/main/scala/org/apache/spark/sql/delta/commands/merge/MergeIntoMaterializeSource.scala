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

package org.apache.spark.sql.delta.commands.merge

import java.util.UUID

import scala.annotation.tailrec
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaTable}
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression, ExpressionSet, Literal}
import org.apache.spark.sql.catalyst.optimizer.{EliminateResolvedHint, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{LogicalRDD, SQLExecution}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.storage.StorageLevel

/**
 * Trait with logic and utilities used for materializing a snapshot of MERGE source
 * in case we can't guarantee deterministic repeated reads from it.
 *
 * We materialize source if it is not safe to assume that it's deterministic
 * (override with MERGE_SOURCE_MATERIALIZATION).
 * Otherwise, if source changes between the phases of the MERGE, it can produce wrong results.
 * We use local checkpointing for the materialization, which saves the source as a
 * materialized RDD[InternalRow] on the executor local disks.
 *
 * 1st concern is that if an executor is lost, this data can be lost.
 * When Spark executor decomissioning API is used, it should attempt to move this
 * materialized data safely out before removing the executor.
 *
 * 2nd concern is that if an executor is lost for another reason (e.g. spot kill), we will
 * still lose that data. To mitigate that, we implement a retry loop.
 * The whole Merge operation needs to be restarted from the beginning in this case.
 * When we retry, we increase the replication level of the materialized data from 1 to 2.
 * (override with MERGE_SOURCE_MATERIALIZATION_RDD_STORAGE_LEVEL_RETRY).
 * If it still fails after the maximum number of attempts (MERGE_MATERIALIZE_SOURCE_MAX_ATTEMPTS),
 * we record the failure for tracking purposes.
 *
 * 3rd concern is that executors run out of disk space with the extra materialization.
 * We record such failures for tracking purpuses.
 */
trait MergeIntoMaterializeSource extends DeltaLogging {
  import MergeIntoMaterializeSource._

  /**
   * Prepared Dataframe with source data.
   * If needed, it is materialized, @see prepareSourceDFAndReturnMaterializeReason
   */
  private var sourceDF: Option[Dataset[Row]] = None

  /**
   * If the source was materialized, reference to the checkpointed RDD.
   */
  protected var materializedSourceRDD: Option[RDD[InternalRow]] = None

  /**
   * Track which attempt or retry it is in runWithMaterializedSourceAndRetries
   */
  protected var attempt: Int = 0

  /**
   * Run the Merge with retries in case it detects an RDD block lost error of the
   * materialized source RDD.
   * It will also record out of disk error, if such happen - possibly because of increased disk
   * pressure from the materialized source RDD.
   */
  protected def runWithMaterializedSourceLostRetries(
      spark: SparkSession,
      deltaLog: DeltaLog,
      metrics: Map[String, SQLMetric],
      runMergeFunc: SparkSession => Seq[Row]): Seq[Row] = {
    var doRetry = false
    var runResult: Seq[Row] = null
    attempt = 1
    do {
      doRetry = false
      metrics.values.foreach(_.reset())
      try {
        runResult = runMergeFunc(spark)
      } catch {
        case NonFatal(ex) =>
          val isLastAttempt =
            (attempt == spark.conf.get(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE_MAX_ATTEMPTS))
          handleExceptionDuringAttempt(ex, isLastAttempt, deltaLog) match {
            case RetryHandling.Retry =>
              logInfo(s"Retrying MERGE with materialized source. Attempt $attempt failed.")
              doRetry = true
              attempt += 1
            case RetryHandling.ExhaustedRetries =>
              logError(s"Exhausted retries after $attempt attempts in MERGE with" +
                s" materialized source. Logging latest exception.", ex)
              throw DeltaErrors.sourceMaterializationFailedRepeatedlyInMerge
            case RetryHandling.RethrowException =>
              logError(s"Fatal error in MERGE with materialized source in attempt $attempt.", ex)
              throw ex
          }
      } finally {
        // Remove source from RDD cache (noop if wasn't cached)
        materializedSourceRDD.foreach { rdd =>
          rdd.unpersist()
        }
        materializedSourceRDD = None
        sourceDF = null
      }
    } while (doRetry)

    runResult
  }

  object RetryHandling extends Enumeration {
    type Result = Value

    val Retry, RethrowException, ExhaustedRetries = Value
  }

  /**
   * Handle exception that was thrown from runMerge().
   * Search for errors to log, or that can be handled by retry.
   * It may need to descend into ex.getCause() to find the errors, as Spark may have wrapped them.
   * @param isLastAttempt indicates that it's the last allowed attempt and there shall be no retry.
   * @return true if the exception is handled and merge should retry
   *         false if the caller should rethrow the error
   */
  @tailrec
  private def handleExceptionDuringAttempt(
      ex: Throwable,
      isLastAttempt: Boolean,
      deltaLog: DeltaLog): RetryHandling.Result = ex match {
    // If Merge failed because the materialized source lost blocks from the
    // locally checkpointed RDD, we want to retry the whole operation.
    // If a checkpointed RDD block is lost, it throws
    // SparkCoreErrors.checkpointRDDBlockIdNotFoundError from LocalCheckpointRDD.compute.
    case s: SparkException
      if !materializedSourceRDD.isEmpty &&
        s.getMessage.matches(
          mergeMaterializedSourceRddBlockLostErrorRegex(materializedSourceRDD.get.id)) =>
      log.warn("Materialized Merge source RDD block lost. Merge needs to be restarted. " +
        s"This was attempt number $attempt.")
      if (!isLastAttempt) {
        RetryHandling.Retry
      } else {
        // Record situations where we lost RDD materialized source blocks, despite retries.
        recordDeltaEvent(
          deltaLog,
          MergeIntoMaterializeSourceError.OP_TYPE,
          data = MergeIntoMaterializeSourceError(
            errorType = MergeIntoMaterializeSourceErrorType.RDD_BLOCK_LOST.toString,
            attempt = attempt,
            materializedSourceRDDStorageLevel =
              materializedSourceRDD.get.getStorageLevel.toString
          )
        )
        RetryHandling.ExhaustedRetries
      }

    // Record if we ran out of executor disk space.
    case s: SparkException
      if s.getMessage.contains("java.io.IOException: No space left on device") =>
      // Record situations where we ran out of disk space, possibly because of the space took
      // by the materialized RDD.
      recordDeltaEvent(
        deltaLog,
        MergeIntoMaterializeSourceError.OP_TYPE,
        data = MergeIntoMaterializeSourceError(
          errorType = MergeIntoMaterializeSourceErrorType.OUT_OF_DISK.toString,
          attempt = attempt,
          materializedSourceRDDStorageLevel =
            materializedSourceRDD.get.getStorageLevel.toString
        )
      )
      RetryHandling.RethrowException

    // Descend into ex.getCause.
    // The errors that we are looking for above might have been wrapped inside another exception.
    case NonFatal(ex) if ex.getCause() != null =>
      handleExceptionDuringAttempt(ex.getCause(), isLastAttempt, deltaLog)

    // Descended to the bottom of the causes without finding a retryable error
    case _ => RetryHandling.RethrowException
  }

  /**
   * @return pair of boolean whether source should be materialized
   *         and the source materialization reason
   */
  protected def shouldMaterializeSource(
    spark: SparkSession, source: LogicalPlan, isInsertOnly: Boolean
  ): (Boolean, MergeIntoMaterializeSourceReason.MergeIntoMaterializeSourceReason) = {
    val materializeType = spark.conf.get(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE)
    materializeType match {
      case DeltaSQLConf.MergeMaterializeSource.ALL =>
        (true, MergeIntoMaterializeSourceReason.MATERIALIZE_ALL)
      case DeltaSQLConf.MergeMaterializeSource.NONE =>
        (false, MergeIntoMaterializeSourceReason.NOT_MATERIALIZED_NONE)
      case DeltaSQLConf.MergeMaterializeSource.AUTO =>
        if (isInsertOnly && spark.conf.get(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED)) {
          (false, MergeIntoMaterializeSourceReason.NOT_MATERIALIZED_AUTO_INSERT_ONLY)
        } else if (!sourceContainsOnlyDeltaScans(source)) {
          (true, MergeIntoMaterializeSourceReason.NON_DETERMINISTIC_SOURCE_NON_DELTA)
        } else if (!isDeterministic(source)) {
          (true, MergeIntoMaterializeSourceReason.NON_DETERMINISTIC_SOURCE_OPERATORS)
        } else {
          (false, MergeIntoMaterializeSourceReason.NOT_MATERIALIZED_AUTO)
        }
      case _ =>
        // If the config is invalidly set, also materialize.
        (true, MergeIntoMaterializeSourceReason.INVALID_CONFIG)
    }
  }
  /**
   * If source needs to be materialized, prepare the materialized dataframe in sourceDF
   * Otherwise, prepare regular dataframe.
   * @return the source materialization reason
   */
  protected def prepareSourceDFAndReturnMaterializeReason(
      spark: SparkSession,
      source: LogicalPlan,
      condition: Expression,
      matchedClauses: Seq[DeltaMergeIntoMatchedClause],
      notMatchedClauses: Seq[DeltaMergeIntoNotMatchedClause],
      isInsertOnly: Boolean): MergeIntoMaterializeSourceReason.MergeIntoMaterializeSourceReason = {
    val (materialize, materializeReason) =
      shouldMaterializeSource(spark, source, isInsertOnly)
    if (!materialize) {
      // Does not materialize, simply return the dataframe from source plan
      sourceDF = Some(Dataset.ofRows(spark, source))
      return materializeReason
    }

    val referencedSourceColumns =
      getReferencedSourceColumns(source, condition, matchedClauses, notMatchedClauses)
    // When we materialize the source, we want to make sure that columns got pruned before caching.
    val sourceWithSelectedColumns = Project(referencedSourceColumns, source)
    val baseSourcePlanDF = Dataset.ofRows(spark, sourceWithSelectedColumns)

    // Caches the source in RDD cache using localCheckpopoint, which cuts away the RDD lineage,
    // which shall ensure that the source cannot be recomputed and thus become inconsistent.
    val checkpointedSourcePlanDF = baseSourcePlanDF
      // eager = false makes it be executed and materialized first time it's used.
      // Doing it lazily inside the query lets it interleave this work better with other work.
      // On the other hand, it makes it impossible to measure the time it took in a metric.
      .localCheckpoint(eager = false)

    // We have to reach through the crust and into the plan of the checkpointed DF
    // to get the RDD that was actually checkpointed, to be able to unpersist it later...
    var checkpointedPlan = checkpointedSourcePlanDF.queryExecution.analyzed
    val rdd = checkpointedPlan.asInstanceOf[LogicalRDD].rdd
    materializedSourceRDD = Some(rdd)
    rdd.setName("mergeMaterializedSource")

    // We should still keep the hints from the input plan.
    checkpointedPlan = addHintsToPlan(source, checkpointedPlan)

    sourceDF = Some(Dataset.ofRows(spark, checkpointedPlan))


    // Sets appropriate StorageLevel
    val storageLevel = StorageLevel.fromString(
      if (attempt == 1) {
        spark.conf.get(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE_RDD_STORAGE_LEVEL)
      } else {
        // If it failed the first time, potentially use a different storage level on retry.
        spark.conf.get(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE_RDD_STORAGE_LEVEL_RETRY)
      }
    )
    rdd.persist(storageLevel)

    logDebug(s"Materializing MERGE with pruned columns $referencedSourceColumns. ")
    logDebug(s"Materialized MERGE source plan:\n${sourceDF.get.queryExecution}")
    materializeReason
  }

  protected def getSourceDF(): Dataset[Row] = {
    if (sourceDF.isEmpty) {
      throw new IllegalStateException(
        "sourceDF was not initialized! Call prepareSourceDFAndReturnMaterializeReason before.")
    }
    sourceDF.get
  }

  private def addHintsToPlan(sourcePlan: LogicalPlan, plan: LogicalPlan): LogicalPlan = {
    val hints = EliminateResolvedHint.extractHintsFromPlan(sourcePlan)._2
    // This follows similar code in CacheManager from https://github.com/apache/spark/pull/24580
    if (hints.nonEmpty) {
      // The returned hint list is in top-down order, we should create the hint nodes from
      // right to left.
      val planWithHints =
      hints.foldRight[LogicalPlan](plan) { case (hint, p) =>
        ResolvedHint(p, hint)
      }
      planWithHints
    } else {
      plan
    }
  }

  /**
   * Return columns from the source plan that are used in the MERGE
   */
  private def getReferencedSourceColumns(
      source: LogicalPlan,
      condition: Expression,
      matchedClauses: Seq[DeltaMergeIntoMatchedClause],
      notMatchedClauses: Seq[DeltaMergeIntoNotMatchedClause]) = {
    val conditionCols = condition.references
    val matchedCondCols = matchedClauses.flatMap { clause =>
      clause.condition.getOrElse(Literal(true)).flatMap(_.references)
    }
    val notMatchedCondCols = notMatchedClauses.flatMap { clause =>
      clause.condition.getOrElse(Literal(true)).flatMap(_.references)
    }
    val matchedActionsCols = matchedClauses.flatMap { clause =>
      clause.resolvedActions.flatMap(_.expr.references)
    }
    val notMatchedActionsCols = notMatchedClauses.flatMap { clause =>
      clause.resolvedActions.flatMap(_.expr.references)
    }
    val allCols = AttributeSet(conditionCols ++ matchedCondCols ++ notMatchedCondCols ++
      matchedActionsCols ++ notMatchedActionsCols)

    source.output.filter(allCols.contains(_))
  }

  private def sourceContainsOnlyDeltaScans(source: LogicalPlan): Boolean = {
    !source.exists {
      case l: LogicalRelation =>
        l match {
          case DeltaTable(_) => false
          case _ => true
        }
      case _: LeafNode => true // Any other LeafNode is a non Delta scan.
      case _ => false
    }
  }

  /**
   * `true` if `source` has a safe level of determinism.
   * This is a conservative approximation of `source` being a truly deterministic query.
   */
  private def isDeterministic(plan: LogicalPlan): Boolean = plan match {
      // This is very restrictive, allowing only deterministic filters and projections directly
      // on top of a Delta Table.
      case Project(projectList, child) if projectList.forall(_.deterministic) =>
        isDeterministic(child)
      case Filter(cond, child) if cond.deterministic => isDeterministic(child)
      case Union(children, _, _) => children.forall(isDeterministic)
      case SubqueryAlias(_, child) => isDeterministic(child)
      case DeltaTable(_) => true
      case _ => false
  }
}

object MergeIntoMaterializeSource {
  // This depends on SparkCoreErrors.checkpointRDDBlockIdNotFoundError msg
  def mergeMaterializedSourceRddBlockLostErrorRegex(rddId: Int): String =
    s"(?s).*Checkpoint block rdd_${rddId}_[0-9]+ not found!.*"
}

/**
 * Enumeration with possible reasons that source may be materialized in a MERGE command.
 */
object MergeIntoMaterializeSourceReason extends Enumeration {
  type MergeIntoMaterializeSourceReason = Value
  // It was determined to not materialize on auto config.
  val NOT_MATERIALIZED_AUTO = Value("notMaterializedAuto")
  // Config was set to never materialize source.
  val NOT_MATERIALIZED_NONE = Value("notMaterializedNone")
  // Insert only merge is single pass, no need for materialization
  val NOT_MATERIALIZED_AUTO_INSERT_ONLY = Value("notMaterializedAutoInsertOnly")
  // Config was set to always materialize source.
  val MATERIALIZE_ALL = Value("materializeAll")
  // The source query is considered non-deterministic, because it contains a non-delta scan.
  val NON_DETERMINISTIC_SOURCE_NON_DELTA = Value("materializeNonDeterministicSourceNonDelta")
  // The source query is considered non-deterministic, because it contains non-deterministic
  // operators.
  val NON_DETERMINISTIC_SOURCE_OPERATORS = Value("materializeNonDeterministicSourceOperators")
  // Materialize when the configuration is invalid
  val INVALID_CONFIG = Value("invalidConfigurationFailsafe")
  // Catch-all case.
  val UNKNOWN = Value("unknown")
}

/**
 * Structure with data for "delta.dml.merge.materializeSourceError" event.
 * Note: We log only errors that we want to track (out of disk or lost RDD blocks).
 */
case class MergeIntoMaterializeSourceError(
  errorType: String,
  attempt: Int,
  materializedSourceRDDStorageLevel: String
)

object MergeIntoMaterializeSourceError {
  val OP_TYPE = "delta.dml.merge.materializeSourceError"
}

object MergeIntoMaterializeSourceErrorType extends Enumeration {
  type MergeIntoMaterializeSourceError = Value
  val RDD_BLOCK_LOST = Value("materializeSourceRDDBlockLostRetriesFailure")
  val OUT_OF_DISK = Value("materializeSourceOutOfDiskFailure")
}
