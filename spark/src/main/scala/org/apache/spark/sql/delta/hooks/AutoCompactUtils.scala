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

import scala.collection.mutable

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransactionImpl, Snapshot}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.sources.DeltaSQLConf._
import org.apache.spark.sql.delta.stats.AutoCompactPartitionStats

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{And, Cast, EqualNullSafe, Expression, Literal, Or}
import org.apache.spark.sql.functions.collect_list

/**
 * The request class that contains all information needed for Auto Compaction.
 * @param shouldCompact True if Auto Compact should start.
 * @param optimizeContext The context that control execution of optimize command.
 * @param targetPartitionsPredicate The predicate of the target partitions of this Auto Compact
 *                                 request.
 */
case class AutoCompactRequest(
    shouldCompact: Boolean,
    allowedPartitions: AutoCompactUtils.PartitionKeySet,
    targetPartitionsPredicate: Seq[Expression] = Nil) {
}

object AutoCompactRequest {
  /** Return a default AutoCompactRequest object that doesn't trigger Auto Compact. */
  def noopRequest: AutoCompactRequest =
    AutoCompactRequest(
      shouldCompact = false,
      allowedPartitions = Set.empty
    )
}

object AutoCompactUtils extends DeltaLogging {
  type PartitionKey = Map[String, String]
  type PartitionKeySet = Set[PartitionKey]

  val STATUS_NAME = {
    "status"
  }

  /** Create partition predicate from a partition key. */
  private def createPartitionPredicate(
      postCommitSnapshot: Snapshot,
      partitions: PartitionKeySet): Seq[Expression] = {
    val schema = postCommitSnapshot.metadata.physicalPartitionSchema
    val partitionBranches = partitions.filterNot(_.isEmpty).map { partition =>
      partition
        .toSeq
        .map { case (key, value) =>
          val field = schema(key)
          EqualNullSafe(UnresolvedAttribute.quoted(key), Cast(Literal(value), field.dataType))
        }
        .reduceLeft[Expression](And.apply)
    }
    if (partitionBranches.size > 1) {
      Seq(partitionBranches.reduceLeft[Expression](Or.apply))
    } else if (partitionBranches.size == 1) {
      partitionBranches.toList
    } else {
      Seq.empty
    }
  }

  /** True if Auto Compaction only runs on modified partitions. */
  def isModifiedPartitionsOnlyAutoCompactEnabled(spark: SparkSession): Boolean =
    spark.sessionState.conf.getConf(DELTA_AUTO_COMPACT_MODIFIED_PARTITIONS_ONLY_ENABLED)

  def isNonBlindAppendAutoCompactEnabled(spark: SparkSession): Boolean =
    spark.sessionState.conf.getConf(DELTA_AUTO_COMPACT_NON_BLIND_APPEND_ENABLED)

  def reservePartitionEnabled(spark: SparkSession): Boolean =
    spark.sessionState.conf.getConf(DELTA_AUTO_COMPACT_RESERVE_PARTITIONS_ENABLED)

  /**
   * Get the minimum number of files to trigger Auto Compact.
   */
  def minNumFilesForAutoCompact(spark: SparkSession): Int = {
    spark.sessionState.conf.getConf(DELTA_AUTO_COMPACT_MIN_NUM_FILES)
  }


  /**
   * Try to reserve partitions inside `partitionsAddedToOpt` for Auto Compaction.
   * @return (shouldCompact, finalPartitions) The value of needCompaction is True if Auto
   *         Compaction needs to run. `finalPartitions` is the set of target partitions that were
   *         reserved for compaction. If finalPartitions is empty, then all partitions need to be
   *         considered.
   */
  private def reserveTablePartitions(
      spark: SparkSession,
      deltaLog: DeltaLog,
      postCommitSnapshot: Snapshot,
      partitionsAddedToOpt: Option[PartitionKeySet],
      opType: String,
      maxDeletedRowsRatio: Option[Double]): (Boolean, PartitionKeySet) = {
    import AutoCompactPartitionReserve._
    if (partitionsAddedToOpt.isEmpty) {
      recordDeltaEvent(deltaLog, opType, data = Map(STATUS_NAME -> "skipEmptyIngestion"))
      // If partitionsAddedToOpt is empty, then just skip compact since it means there is no file
      // added in parent transaction and we do not want to hook AC on empty commits.
      return (false, Set.empty[PartitionKey])
    }

    // Reserve partitions as following:
    // 1) First check if any partitions are free, i.e. no concurrent auto-compact thread is running.
    // 2) From free partitions check if any are eligible based on the number of small files.
    // 3) From free partitions check if any are eligible based on the deletion vectors.
    // 4) Try and reserve the union of the two lists.
    // All concurrent accesses to partitions reservation and partition stats are managed by the
    // [[AutoCompactPartitionReserve]] and [[AutoCompactPartitionStats]] singletons.
    val shouldReservePartitions =
      isModifiedPartitionsOnlyAutoCompactEnabled(spark) && reservePartitionEnabled(spark)
    val freePartitions =
      if (shouldReservePartitions) {
        filterFreePartitions(deltaLog.tableId, partitionsAddedToOpt.get)
      } else {
        partitionsAddedToOpt.get
      }

    // Early abort if all partitions are reserved.
    if (freePartitions.isEmpty) {
      recordDeltaEvent(deltaLog, opType,
        data = Map(STATUS_NAME -> "skipAllPartitionsAlreadyReserved"))
      return (false, Set.empty[PartitionKey])
    }

    // Check min number of files criteria.
    val ChosenPartitionsResult(shouldCompactBasedOnNumFiles,
    chosenPartitionsBasedOnNumFiles, minNumFilesLogMsg) =
      choosePartitionsBasedOnMinNumSmallFiles(
        spark,
        deltaLog,
        postCommitSnapshot,
        freePartitions
      )
    if (shouldCompactBasedOnNumFiles && chosenPartitionsBasedOnNumFiles.isEmpty) {
      // Run on all partitions, no need to check other criteria.
      // Note: this outcome of [choosePartitionsBasedOnMinNumSmallFiles]
      // is also only possible if partitions reservation is turned off,
      // so we do not need to reserve partitions.
      recordDeltaEvent(deltaLog, opType, data = Map(STATUS_NAME -> "runOnAllPartitions"))
      return (shouldCompactBasedOnNumFiles, chosenPartitionsBasedOnNumFiles)
    }

    // Check files with DVs criteria.
    val (shouldCompactBasedOnDVs, chosenPartitionsBasedOnDVs) =
      choosePartitionsBasedOnDVs(freePartitions, postCommitSnapshot, maxDeletedRowsRatio)

    var finalPartitions = chosenPartitionsBasedOnNumFiles ++ chosenPartitionsBasedOnDVs
    if (isModifiedPartitionsOnlyAutoCompactEnabled(spark)) {
      val maxNumPartitions = spark.conf.get(DELTA_AUTO_COMPACT_MAX_NUM_MODIFIED_PARTITIONS)
      finalPartitions = if (finalPartitions.size > maxNumPartitions) {
        // Choose maxNumPartitions at random.
        scala.util.Random.shuffle(finalPartitions.toIndexedSeq).take(maxNumPartitions).toSet
      } else {
        finalPartitions
      }
    }

    val numChosenPartitions = finalPartitions.size
    if (shouldReservePartitions) {
      finalPartitions = tryReservePartitions(deltaLog.tableId, finalPartitions)
    }
    // Abort if all chosen partitions were reserved by a concurrent thread.
    if (numChosenPartitions > 0 && finalPartitions.isEmpty) {
      recordDeltaEvent(deltaLog, opType,
        data = Map(STATUS_NAME -> "skipAllPartitionsAlreadyReserved"))
      return (false, Set.empty[PartitionKey])
    }

    val shouldCompact = shouldCompactBasedOnNumFiles || shouldCompactBasedOnDVs
    val statusLogMessage =
      if (!shouldCompact) {
        "skip" + minNumFilesLogMsg
      } else if (shouldCompactBasedOnNumFiles && !shouldCompactBasedOnDVs) {
        "run" + minNumFilesLogMsg
      } else if (shouldCompactBasedOnNumFiles && shouldCompactBasedOnDVs) {
        "run" + minNumFilesLogMsg + "AndPartitionsWithDVs"
      } else if (!shouldCompactBasedOnNumFiles && shouldCompactBasedOnDVs) {
        "runOnPartitionsWithDVs"
      }
    val logData = scala.collection.mutable.Map(STATUS_NAME -> statusLogMessage)
    if (finalPartitions.nonEmpty) {
      logData += ("partitions" -> finalPartitions.size.toString)
    }
    recordDeltaEvent(deltaLog, opType, data = logData)

    (shouldCompactBasedOnNumFiles || shouldCompactBasedOnDVs, finalPartitions)
  }

  private case class ChosenPartitionsResult(
    shouldRunAC: Boolean,
    chosenPartitions: PartitionKeySet,
    logMessage: String)

  private def choosePartitionsBasedOnMinNumSmallFiles(
      spark: SparkSession,
      deltaLog: DeltaLog,
      postCommitSnapshot: Snapshot,
      freePartitionsAddedTo: PartitionKeySet): ChosenPartitionsResult = {
    def getConf[T](entry: ConfigEntry[T]): T = spark.sessionState.conf.getConf(entry)

    val minNumFiles = minNumFilesForAutoCompact(spark)
      val partitionEarlySkippingEnabled =
        getConf(DELTA_AUTO_COMPACT_EARLY_SKIP_PARTITION_TABLE_ENABLED)
      val tablePartitionStats = AutoCompactPartitionStats.instance(spark)
      if (isModifiedPartitionsOnlyAutoCompactEnabled(spark)) {
        // If modified partition only Auto Compact is enabled, pick the partitions that have more
        // number of files than minNumFiles.
        // If table partition early skipping feature is enabled, use the current minimum number of
        // files threshold; otherwise, use 0 to indicate that any partition is qualified.
        val minNumFilesPerPartition = if (partitionEarlySkippingEnabled) minNumFiles else 0L
        val pickedPartitions = tablePartitionStats.filterPartitionsWithSmallFiles(
          deltaLog.tableId,
          freePartitionsAddedTo,
          minNumFilesPerPartition)
        if (pickedPartitions.isEmpty) {
          ChosenPartitionsResult(shouldRunAC = false,
            chosenPartitions = pickedPartitions,
            logMessage = "InsufficientFilesInModifiedPartitions")
        } else {
          ChosenPartitionsResult(shouldRunAC = true,
            chosenPartitions = pickedPartitions,
            logMessage = "OnModifiedPartitions")
        }
      } else if (partitionEarlySkippingEnabled) {
        // If only early skipping is enabled, then check whether there is any partition with more
        // files than minNumFiles.
        val maxNumFiles = tablePartitionStats.maxNumFilesInTable(deltaLog.tableId)
        val shouldCompact = maxNumFiles >= minNumFiles
        if (shouldCompact) {
          ChosenPartitionsResult(shouldRunAC = true,
            chosenPartitions = Set.empty[PartitionKey],
            logMessage = "OnAllPartitions")
        } else {
          ChosenPartitionsResult(shouldRunAC = false,
            chosenPartitions = Set.empty[PartitionKey],
            logMessage = "InsufficientInAllPartitions")
        }
      } else {
        // If both are disabled, then Auto Compaction should search all partitions of the target
        // table.
        ChosenPartitionsResult(
          shouldRunAC = true,
          chosenPartitions = Set.empty[PartitionKey],
          logMessage = "OnAllPartitions")
      }
  }

  private def choosePartitionsBasedOnDVs(
      freePartitionsAddedTo: PartitionKeySet,
      postCommitSnapshot: Snapshot,
      maxDeletedRowsRatio: Option[Double]) = {
    var partitionsWithDVs = if (maxDeletedRowsRatio.nonEmpty) {
      postCommitSnapshot
        .allFiles
        .where("deletionVector IS NOT NULL")
        .where(
          s"""
           |(deletionVector.cardinality / stats:`numRecords`) > ${maxDeletedRowsRatio.get}
           |""".stripMargin)
        // Cast map to string so we can group by it.
        // The string representation might not be deterministic.
        // Still, there is only a limited number of representations we could get for a given map,
        // Which should sufficiently reduce the data collected on the driver.
        // We then make sure the partitions are distinct on the driver.
        .selectExpr("CAST(partitionValues AS STRING) as partitionValuesStr", "partitionValues")
        .groupBy("partitionValuesStr")
        .agg(collect_list("partitionValues").as("partitionValues"))
        .selectExpr("partitionValues[0] as partitionValues")
        .collect()
        .map(_.getAs[Map[String, String]]("partitionValues")).toSet
    } else {
      Set.empty[PartitionKey]
    }
    partitionsWithDVs = partitionsWithDVs.intersect(freePartitionsAddedTo)
    (partitionsWithDVs.nonEmpty, partitionsWithDVs)
  }

  /**
   * Prepare an [[AutoCompactRequest]] object based on the statistics of partitions inside
   * `partitionsAddedToOpt`.
   *
   * @param partitionsAddedToOpt The partitions that contain AddFile objects created by parent
   *                             transaction.
   * @param maxDeletedRowsRatio  If set, signals to Auto Compaction to rewrite files with
   *                             DVs with maxDeletedRowsRatio above this threshold.
   */
  def prepareAutoCompactRequest(
      spark: SparkSession,
      txn: OptimisticTransactionImpl,
      postCommitSnapshot: Snapshot,
      partitionsAddedToOpt: Option[PartitionKeySet],
      opType: String,
      maxDeletedRowsRatio: Option[Double]): AutoCompactRequest = {
    val (needAutoCompact, reservedPartitions) = reserveTablePartitions(
      spark,
      txn.deltaLog,
      postCommitSnapshot,
      partitionsAddedToOpt,
      opType,
      maxDeletedRowsRatio)
    AutoCompactRequest(
      needAutoCompact,
      reservedPartitions,
      createPartitionPredicate(postCommitSnapshot, reservedPartitions))
  }

  /**
   * True if this transaction is qualified for Auto Compaction.
   * - When current transaction is not blind append, it is safe to enable Auto Compaction when
   *   DELTA_AUTO_COMPACT_MODIFIED_PARTITIONS_ONLY_ENABLED is true, or it's an un-partitioned table,
   *   because then we cannot introduce _additional_ conflicts with concurrent write transactions.
   */
  def isQualifiedForAutoCompact(
      spark: SparkSession,
      txn: OptimisticTransactionImpl): Boolean = {
    // If txnExecutionTimeMs is empty, there is no transaction commit.
    if (txn.txnExecutionTimeMs.isEmpty) return false
    // If modified partitions only mode is not enabled, return true to avoid subsequent checking.
    if (!isModifiedPartitionsOnlyAutoCompactEnabled(spark)) return true

    !(isNonBlindAppendAutoCompactEnabled(spark) && txn.isBlindAppend)
  }

}

/**
 * Thread-safe singleton to keep track of partitions reserved for auto-compaction.
 */
object AutoCompactPartitionReserve {

  import org.apache.spark.sql.delta.hooks.AutoCompactUtils.PartitionKey

  // Key is table id and the value the set of currently reserved partition hashes.
  private val reservedTablesPartitions = new mutable.LinkedHashMap[String, Set[Int]]

  /**
   * @return Partitions from targetPartitions that are not reserved.
   */
  def filterFreePartitions(tableId: String, targetPartitions: Set[PartitionKey])
  : Set[PartitionKey] = synchronized {
    val reservedPartitionKeys = reservedTablesPartitions.getOrElse(tableId, Set.empty)
    targetPartitions.filter(partition => !reservedPartitionKeys.contains(partition.##))
  }

  /**
   * Try to reserve partitions from [[targetPartitions]] which are not yet reserved.
   * @return partitions from targetPartitions which were not previously reserved.
   */
  def tryReservePartitions(tableId: String, targetPartitions: Set[PartitionKey])
  : Set[PartitionKey] = synchronized {
    val allReservedPartitions = reservedTablesPartitions.getOrElse(tableId, Set.empty)
    val unReservedPartitionsFromTarget = targetPartitions
      .filter(targetPartition => !allReservedPartitions.contains(targetPartition.##))
    val newAllReservedPartitions = allReservedPartitions ++ unReservedPartitionsFromTarget.map(_.##)
    reservedTablesPartitions.update(tableId, newAllReservedPartitions)
    unReservedPartitionsFromTarget
  }


  /**
   * Releases the reserved table partitions to allow other threads to reserve them.
   * @param tableId The identity of the target table of Auto Compaction.
   * @param reservedPartitions The set of partitions, which were reserved and which need releasing.
   */
  def releasePartitions(
      tableId: String,
      reservedPartitions: Set[PartitionKey]): Unit = synchronized {
    val allReservedPartitions = reservedTablesPartitions.getOrElse(tableId, Set.empty)
    val newPartitions = allReservedPartitions -- reservedPartitions.map(_.##)
    reservedTablesPartitions.update(tableId, newPartitions)
  }

  /** This is test only code to reset the state of table partition reservations. */
  private[delta] def resetTestOnly(): Unit = synchronized {
    reservedTablesPartitions.clear()
  }
}
