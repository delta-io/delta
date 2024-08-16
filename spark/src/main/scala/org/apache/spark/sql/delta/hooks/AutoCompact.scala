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

package org.apache.spark.sql.delta.hooks

import org.apache.spark.sql.delta.skipping.clustering.ClusteredTableUtils
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.{DeltaOptimizeContext, OptimizeExecutor}
import org.apache.spark.sql.delta.commands.optimize._
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.AutoCompactPartitionStats

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.SQLConf

/**
 * A trait for post commit hook which compacts files in a Delta table. This hook acts as a cheaper
 * version of the OPTIMIZE command, by attempting to compact small files together into fewer bigger
 * files.
 *
 * Auto Compact chooses files to compact greedily by looking at partition directories which
 * have the largest number of files that are under a certain size threshold and launches a bounded
 * number of optimize tasks based on the capacity of the cluster.
 */
trait AutoCompactBase extends PostCommitHook with DeltaLogging {

  override val name: String = "Auto Compact"

  private[delta] val OP_TYPE = "delta.commit.hooks.autoOptimize"

  /**
   * This method returns the type of Auto Compaction to use on a delta table or returns None
   * if Auto Compaction is disabled.
   * Prioritization:
   *   1. The highest priority is given to [[DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED]] config.
   *   2. Then we check if the deprecated property `DeltaConfigs.AUTO_OPTIMIZE` is set. If yes, then
   *      we return [[AutoCompactType.Enabled]] type.
   *   3. Then we check the table property [[DeltaConfigs.AUTO_COMPACT]].
   *   4. If none of 1/2/3 are set explicitly, then we return None
   */
  def getAutoCompactType(conf: SQLConf, metadata: Metadata): Option[AutoCompactType] = {
    // If user-facing conf is set to something, use that value.
    val autoCompactTypeFromConf =
      conf.getConf(DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED).map(AutoCompactType(_))
    if (autoCompactTypeFromConf.nonEmpty) return autoCompactTypeFromConf.get

    // If user-facing conf is not set, use what table property says.
    val deprecatedFlag = DeltaConfigs.AUTO_OPTIMIZE.fromMetaData(metadata)
    val autoCompactTypeFromPropertyOrDefaultValue = deprecatedFlag match {
      case Some(true) =>
        Some(AutoCompactType.Enabled)
      case _ =>
        // If the legacy property `DeltaConfigs.AUTO_OPTIMIZE` is false or not set, then check
        // the new table property `DeltaConfigs.AUTO_COMPACT`.
        val confValueFromTableProperty = DeltaConfigs.AUTO_COMPACT.fromMetaData(metadata)
        confValueFromTableProperty match {
          case Some(v) =>
            // Table property is set to something explicitly by user.
            AutoCompactType(v)
          case None =>
            AutoCompactType(AutoCompactType.DISABLED) // Default to disabled
        }
    }
    autoCompactTypeFromPropertyOrDefaultValue
  }

  private[hooks] def shouldSkipAutoCompact(
      autoCompactTypeOpt: Option[AutoCompactType],
      spark: SparkSession,
      txn: OptimisticTransactionImpl): Boolean = {
    // If auto compact type is empty, then skip compaction
    if (autoCompactTypeOpt.isEmpty) return true

    // Skip Auto Compaction, if one of the following conditions is satisfied:
    // -- Auto Compaction is not enabled.
    // -- Transaction execution time is empty, which means the parent transaction is not committed.
      !AutoCompactUtils.isQualifiedForAutoCompact(spark, txn)

  }

  override def run(
      spark: SparkSession,
      txn: OptimisticTransactionImpl,
      committedVersion: Long,
      postCommitSnapshot: Snapshot,
      actions: Seq[Action]): Unit = {
    val conf = spark.sessionState.conf
    val autoCompactTypeOpt = getAutoCompactType(conf, postCommitSnapshot.metadata)
    // Skip Auto Compact if current transaction is not qualified or the table is not qualified
    // based on the value of autoCompactTypeOpt.
    if (shouldSkipAutoCompact(autoCompactTypeOpt, spark, txn)) return
    compactIfNecessary(
        spark,
        txn,
        postCommitSnapshot,
        OP_TYPE,
        maxDeletedRowsRatio = None)
  }

  /**
   * Compact the target table of write transaction `txn` only when there are sufficient amount of
   * small size files.
   */
  private[delta] def compactIfNecessary(
      spark: SparkSession,
      txn: OptimisticTransactionImpl,
      postCommitSnapshot: Snapshot,
      opType: String,
      maxDeletedRowsRatio: Option[Double]
  ): Seq[OptimizeMetrics] = {
    val tableId = txn.deltaLog.tableId
    val autoCompactRequest = AutoCompactUtils.prepareAutoCompactRequest(
      spark,
      txn,
      postCommitSnapshot,
      txn.partitionsAddedToOpt.map(_.toSet),
      opType,
      maxDeletedRowsRatio)
    if (autoCompactRequest.shouldCompact) {
      try {
        val metrics = AutoCompact
          .compact(
            spark,
            txn.deltaLog,
            txn.catalogTable,
            autoCompactRequest.targetPartitionsPredicate,
            opType,
            maxDeletedRowsRatio
          )
        val partitionsStats = AutoCompactPartitionStats.instance(spark)
        // Mark partitions as compacted before releasing them.
        // Otherwise an already compacted partition might get picked up by a concurrent thread.
        // But only marks it as compacted, if no exception was thrown by auto compaction so that the
        // partitions stay eligible for subsequent auto compactions.
        partitionsStats.markPartitionsAsCompacted(
          tableId,
          autoCompactRequest.allowedPartitions
        )
        metrics
      } catch {
        case e: Throwable =>
          logError(log"Auto Compaction failed with: ${MDC(DeltaLogKeys.ERROR, e.getMessage)}")
          recordDeltaEvent(
            txn.deltaLog,
            opType = "delta.autoCompaction.error",
            data = getErrorData(e))
          throw e
      } finally {
        if (AutoCompactUtils.reservePartitionEnabled(spark)) {
          AutoCompactPartitionReserve.releasePartitions(
            tableId,
            autoCompactRequest.allowedPartitions
          )
        }
      }
    } else {
      Seq.empty[OptimizeMetrics]
    }
  }


  /**
   * Launch Auto Compaction jobs if there is sufficient capacity.
   * @param spark The spark session of the parent transaction that triggers this Auto Compaction.
   * @param deltaLog The delta log of the parent transaction.
   * @return the optimize metrics of this compaction job.
   */
  private[delta] def compact(
      spark: SparkSession,
      deltaLog: DeltaLog,
      catalogTable: Option[CatalogTable],
      partitionPredicates: Seq[Expression] = Nil,
      opType: String = OP_TYPE,
      maxDeletedRowsRatio: Option[Double] = None)
  : Seq[OptimizeMetrics] = recordDeltaOperation(deltaLog, opType) {
    val maxFileSize = spark.conf.get(DeltaSQLConf.DELTA_AUTO_COMPACT_MAX_FILE_SIZE)
    val minFileSizeOpt = Some(spark.conf.get(DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_FILE_SIZE)
      .getOrElse(maxFileSize / 2))
    val maxFileSizeOpt = Some(maxFileSize)
    recordDeltaOperation(deltaLog, s"$opType.execute") {
      val optimizeContext = DeltaOptimizeContext(
        reorg = None,
        minFileSizeOpt,
        maxFileSizeOpt,
        maxDeletedRowsRatio = maxDeletedRowsRatio
      )
      val rows = new OptimizeExecutor(spark, deltaLog.update(), catalogTable, partitionPredicates,
        zOrderByColumns = Seq(), isAutoCompact = true, optimizeContext).optimize()
      val metrics = rows.map(_.getAs[OptimizeMetrics](1))
      recordDeltaEvent(deltaLog, s"$opType.execute.metrics", data = metrics.head)
      metrics
    }
  }

}

/**
 * Post commit hook for Auto Compaction.
 */
case object AutoCompact extends AutoCompactBase
/**
 * A trait describing the type of Auto Compaction.
 */
sealed trait AutoCompactType {
  val configValueStrings: Seq[String]
}

object AutoCompactType {

  private[hooks] val DISABLED = "false"

  /**
   * Enable auto compact.
   * 1. MAX_FILE_SIZE is configurable and defaults to 128 MB unless overridden.
   * 2. MIN_FILE_SIZE is configurable and defaults to MAX_FILE_SIZE / 2 unless overridden.
   * Note: User can use DELTA_AUTO_COMPACT_MAX_FILE_SIZE to override this value.
   */
  case object Enabled extends AutoCompactType {
    override val configValueStrings = Seq(
      "true"
    )
  }


  /**
   * Converts the config value String (coming from [[DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED]] conf
   * or [[DeltaConfigs.AUTO_COMPACT]] table property) and translates into the [[AutoCompactType]].
   */
  def apply(value: String): Option[AutoCompactType] = {
    if (Enabled.configValueStrings.contains(value)) return Some(Enabled)
    if (value == DISABLED) return None
    throw DeltaErrors.invalidAutoCompactType(value)
  }

  // All allowed values for [[DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED]] and
  // [[DeltaConfigs.AUTO_COMPACT]].
  val ALLOWED_VALUES =
    Enabled.configValueStrings ++
    Seq(DISABLED)
}
