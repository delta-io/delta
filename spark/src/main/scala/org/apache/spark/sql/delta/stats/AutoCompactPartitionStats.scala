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

package org.apache.spark.sql.delta.stats

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.actions.{Action, AddFile, FileAction, RemoveFile}
import org.apache.spark.sql.delta.hooks.AutoCompactPartitionReserve
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.SparkSession

/**
 * A collector used to aggregate auto-compaction stats for a single commit. The expectation
 * is to spin this up for a commit and then merging those local stats with the global stats.
 */
trait AutoCompactPartitionStatsCollector {
  def collectPartitionStatsForAdd(file: AddFile): Unit
  def collectPartitionStatsForRemove(file: RemoveFile): Unit
  def finalizeStats(tableId: String): Unit
}

/**
 * This singleton object collect the table partition statistic for each commit that creates
 * AddFile or RemoveFile objects.
 * To control the memory usage, there are `maxNumTablePartitions` per table and 'maxNumPartitions'
 * partition entries across all tables.
 * Note:
 *   1. Since the partition of each table is limited, if this limitation is reached, the least
 *      recently used table partitions will be evicted.
 *   2. If all 'maxNumPartitions' are occupied, the partition stats of least recently used tables
 *      will be evicted until the used partitions fall back below to 'maxNumPartitions'.
 *   3. The un-partitioned tables are treated as tables with single partition.
 * @param maxNumTablePartitions The hash space of partition key to reduce memory usage per table.
 * @param maxNumPartitions The maximum number of partition that can be occupied.
 */
class AutoCompactPartitionStats(
    private var maxNumTablePartitions: Int,
    private var maxNumPartitions: Int
) {

  /**
   * This class to store the states of one table partition. These state includes:
   * -- the number of small files,
   * -- the thread that assigned to compact this partition, and
   * -- whether the partition was compacted.
   *
   * Note: Since this class keeps tracking of the statistics of the table partition and the state of
   * the auto compaction thread that works on the table partition, any method that accesses any
   * attribute of this class needs to be protected by synchronized context.
   */
  class PartitionStat(
      var numFiles: Long,
      var wasAutoCompacted: Boolean = false) {

    /**
     * Determine whether this partition can be autocompacted based on the number of small files or
     * if this [[AutoCompactPartitionStats]] instance has not auto compacted it yet.
     * @param minNumFiles The minimum number of files this table-partition should have to trigger
     *                    Auto Compaction in case it has already been compacted once.
     */
    def hasSufficientSmallFilesOrHasNotBeenCompacted(minNumFiles: Long): Boolean =
      !wasAutoCompacted || hasSufficientFiles(minNumFiles)

    def hasSufficientFiles(minNumFiles: Long): Boolean = numFiles >= minNumFiles
  }

  /**
   * This hashtable is used to store all table partition states of a table, the key is the hashcode
   * of the partition, the value is [[PartitionStat]] object.
   */
  type TablePartitionStats = mutable.LinkedHashMap[Int, PartitionStat]

  // The hash map to store the number of small files in each partition.
  // -- Key is the hash code of the partition value.
  // -- Values is the number of small files inside the corresponding partition.
  type PartitionFilesMap = mutable.LinkedHashMap[Int, Long]

  type PartitionKey = Map[String, String]

  type PartitionKeySet = Set[Map[String, String]]

  // This is a simple LRU to store the table partition statistics.
  // Workspace private to enable testing.
  private[delta] val tablePartitionStatsCache =
    new mutable.LinkedHashMap[String, TablePartitionStats]()

  // The number of partitions in this cache.
  private[delta] var numUsedPartitions = 0

  /**
   * Helper class used to keep state regarding tracking auto-compaction stats of AddFile and
   * RemoveFile actions in a single run that are greater than a passed-in minimum file size.
   * If the collector runs into any non-fatal errors, it will invoke the error reporter on the error
   * and then skip further execution.
   *
   * @param minFileSize    Minimum file size for files we track auto-compact stats
   * @param errorReporter  Function that reports the first error, if any
   * @return A collector object that tracks the Add/Remove file actions of the current commit.
   */
  def createStatsCollector(
      minFileSize: Long,
      errorReporter: Throwable => Unit):
      AutoCompactPartitionStatsCollector = new AutoCompactPartitionStatsCollector {
    private val inputPartitionFiles = new PartitionFilesMap()
    private var shouldCollect = true

    /**
     * If the file is less than the specified min file size, updates the partition file map
     * of stats with add or remove actions. If we encounter an error during stats collection,
     * the remainder of the files will not be collected as well.
     */
    private def collectPartitionStatsForFile(file: FileAction, addSub: Int): Unit = {
      try {
        val minSizeThreshold = minFileSize
        if (shouldCollect &&
          file.estLogicalFileSize.getOrElse(file.getFileSize) <= minSizeThreshold
        ) {
          updatePartitionFileCounter(inputPartitionFiles, file.partitionValues, addSub)
        }
      } catch {
        case NonFatal(e) =>
          errorReporter(e)
          shouldCollect = false
      }
    }
    /**
     * Adds one file to all the appropriate partition counters.
     */
    override def collectPartitionStatsForAdd(file: AddFile): Unit = {
      collectPartitionStatsForFile(file, addSub = 1)
    }
    /**
     * Removes one file from all the appropriate partition counters.
     */
    override def collectPartitionStatsForRemove(file: RemoveFile): Unit = {
      collectPartitionStatsForFile(file, addSub = -1)
    }

    /**
     * Merges the current collector's stats with the global one.
     */
    override def finalizeStats(tableId: String): Unit = {
      try {
        if (shouldCollect) merge(tableId, inputPartitionFiles.filter(_._2 != 0))
      } catch {
        case NonFatal(e) => errorReporter(e)
      }
    }
  }

  /**
   * This method merges the `inputPartitionFiles` of current committed transaction to the
   * global cache of table partition stats. After merge is completed, tablePath will be moved
   * to most recently used position. If the number of occupied partitions exceeds
   * MAX_NUM_PARTITIONS, the least recently used tables will be evicted out.
   *
   * @param tableId The path of the table that contains `inputPartitionFiles`.
   * @param inputPartitionFiles The number of files, which are qualified for Auto Compaction, in
   *                            each partition.
   */
  def merge(tableId: String, inputPartitionFiles: PartitionFilesMap): Unit = {
    if (inputPartitionFiles.isEmpty) return
    synchronized {
      tablePartitionStatsCache.get(tableId) match {
        case Some(cachedPartitionStates) =>
          // If the table is already stored, merges inputPartitionFiles' content to
          // existing PartitionFilesMap.
          for ((partitionHashCode, numFilesDelta) <- inputPartitionFiles) {
            assert(numFilesDelta != 0)
            cachedPartitionStates.get(partitionHashCode) match {
              case Some(partitionState) =>
                // If there is an entry of partitionHashCode, updates its number of files
                // and moves it to the most recently used slot.
                partitionState.numFiles += numFilesDelta
                moveAccessedPartitionToMru(cachedPartitionStates, partitionHashCode, partitionState)
              case None =>
                if (numFilesDelta > 0) {
                  // New table partition is always in the most recently used entry.
                  cachedPartitionStates.put(partitionHashCode, new PartitionStat(numFilesDelta))
                  numUsedPartitions += 1
                }
            }
          }
          // Move the accessed table to MRU position and evicts the LRU partitions from it
          // if necessary.
          moveAccessedTableToMru(tableId, cachedPartitionStates)
        case None =>
          // If it is new table, just create new entry.
          val newPartitionStates = inputPartitionFiles
            .filter { case (_, numFiles) => numFiles > 0 }
            .map { case (partitionHashCode, numFiles) =>
              (partitionHashCode, new PartitionStat(numFiles))
            }
          tablePartitionStatsCache.put(tableId, newPartitionStates)
          numUsedPartitions += newPartitionStates.size
          moveAccessedTableToMru(tableId, newPartitionStates)
      }
      evictLruTablesIfNecessary()
    }
  }

  /** Move the accessed table partition to the most recently used position. */
  private def moveAccessedPartitionToMru(
      cachedPartitionFiles: TablePartitionStats,
      partitionHashCode: Int,
      partitionState: PartitionStat): Unit = {
    cachedPartitionFiles.remove(partitionHashCode)
    if (partitionState.numFiles <= 0) {
      numUsedPartitions -= 1
    } else {
      // If the newNumFiles is not empty, add it back and make it to be the
      // most recently used entry.
      cachedPartitionFiles.put(partitionHashCode, partitionState)
    }
  }

  /** Move the accessed table to the most recently used position. */
  private def moveAccessedTableToMru(
      tableId: String,
      cachedPartitionFiles: TablePartitionStats): Unit = {
    // The tablePartitionStatsCache is insertion order preserved hash table. Thus,
    // removing and adding back the entry make this to be most recently used entry.
    // If cachedPartitionFiles's size is empty, no need to add it back to LRU.
    tablePartitionStatsCache.remove(tableId)
    numUsedPartitions -= cachedPartitionFiles.size
    if (cachedPartitionFiles.nonEmpty) {
      // Evict the least recently used partitions' statistics from table if necessary
      val numExceededPartitions = cachedPartitionFiles.size - maxNumTablePartitions
      if (numExceededPartitions > 0) {
        val newPartitionStats = cachedPartitionFiles.drop(numExceededPartitions)
        tablePartitionStatsCache.put(tableId, newPartitionStats)
        numUsedPartitions += newPartitionStats.size
      } else {
        tablePartitionStatsCache.put(tableId, cachedPartitionFiles)
        numUsedPartitions += cachedPartitionFiles.size
      }
    }
  }

  /**
   * Evicts the Lru tables from 'tablePartitionStatsCache' until the total number of partitions
   * is less than maxNumPartitions.
   */
  private def evictLruTablesIfNecessary(): Unit = {
    // Keep removing the least recently used table until the used partition is lower than
    // threshold.
    while (numUsedPartitions > maxNumPartitions && tablePartitionStatsCache.nonEmpty) {
      // Pick the least recently accessed table and remove it.
      val (lruTable, tablePartitionStat) = tablePartitionStatsCache.head
      numUsedPartitions -= tablePartitionStat.size
      tablePartitionStatsCache.remove(lruTable)
    }
  }

  /** Update the file count of `PartitionFilesMap` according to the hash value of `partition`. */
  private def updatePartitionFileCounter(
      partitionFileCounter: PartitionFilesMap,
      partition: PartitionKey,
      addSub: Int): Unit = {
    partitionFileCounter.get(partition.##) match {
      case Some(numFiles) =>
        partitionFileCounter.update(partition.##, numFiles + addSub)
      case None =>
        partitionFileCounter.put(partition.##, addSub)
    }
  }

  /** Get the maximum number of files among all partitions inside table `tableId`. */
  def maxNumFilesInTable(tableId: String): Long = {
    synchronized {
      tablePartitionStatsCache.get(tableId) match {
        case Some(partitionFileCounter) =>
          if (partitionFileCounter.isEmpty) {
            0
          } else {
            partitionFileCounter.map(_._2.numFiles).max
          }
        case None => 0
      }
    }
  }

  /**
   * @return Filter partitions from targetPartitions that have not been auto-compacted or
   *         that have enough small files.
   */
  def filterPartitionsWithSmallFiles(tableId: String, targetPartitions: Set[PartitionKey],
      minNumFiles: Long): Set[PartitionKey] = synchronized {
    tablePartitionStatsCache.get(tableId).map { tablePartitionStates =>
      targetPartitions.filter { partitionKey =>
        tablePartitionStates.get(partitionKey.##).exists { partitionState =>
          partitionState.hasSufficientSmallFilesOrHasNotBeenCompacted(minNumFiles)
        }
      }
    }.getOrElse(Set.empty)
  }

  def markPartitionsAsCompacted(tableId: String, compactedPartitions: Set[PartitionKey])
  : Unit = synchronized {
    tablePartitionStatsCache.get(tableId).foreach { tablePartitionStats =>
      compactedPartitions
        .foreach(partitionKey => tablePartitionStats.get(partitionKey.##)
          .foreach(_.wasAutoCompacted = true))
    }
  }

  /**
   * Collect the number of files, which are less than minFileSize, added to or removed from each
   * partition from `actions`.
   */
  def collectPartitionStats(
      collector: AutoCompactPartitionStatsCollector,
      tableId: String,
      actions: Iterator[Action]): Unit = {
    val acts = actions.toVector
    acts.foreach {
      case addFile: AddFile => collector.collectPartitionStatsForAdd(addFile)
      case removeFile: RemoveFile => collector.collectPartitionStatsForRemove(removeFile)
      case _ => // do nothing
    }
    collector.finalizeStats(tableId)
  }

  /** This is test only code to reset the state of table partition statistics. */
  private[delta] def resetTestOnly(newHashSpace: Int, newMaxNumPartitions: Int): Unit = {
    synchronized {
      tablePartitionStatsCache.clear()
      maxNumTablePartitions = newHashSpace
      maxNumPartitions = newMaxNumPartitions
      numUsedPartitions = 0
      AutoCompactPartitionReserve.resetTestOnly()
    }
  }

  /**
   * This is test only code to reset all partition statistic information and keep current
   * configuration.
   */
  private[delta] def resetTestOnly(): Unit = resetTestOnly(maxNumTablePartitions, maxNumPartitions)
}

object AutoCompactPartitionStats {
  private var _instance: AutoCompactPartitionStats = null

  /** The thread safe constructor of singleton. */
  def instance(spark: SparkSession): AutoCompactPartitionStats = {
    synchronized {
      if (_instance == null) {
        val config = spark.conf
        val hashSpaceSize = config.get(DeltaSQLConf.DELTA_AUTO_COMPACT_MAX_TABLE_PARTITION_STATS)
        val maxNumPartitions = config.get(DeltaSQLConf.DELTA_AUTO_COMPACT_PARTITION_STATS_SIZE)
        _instance = new AutoCompactPartitionStats(
          hashSpaceSize, maxNumPartitions
        )
      }
    }
    _instance
  }
}
