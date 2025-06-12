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

package org.apache.spark.sql.delta.commands.optimize

import org.apache.spark.sql.delta.skipping.clustering.ClusteringStats
import org.apache.spark.sql.delta.actions.{AddFile, FileAction, RemoveFile}

// scalastyle:off import.ordering.noEmptyLine

/**
 * Stats for an OPTIMIZE operation accumulated across all batches.
 */
case class OptimizeStats(
    var addedFilesSizeStats: FileSizeStats = FileSizeStats(),
    var removedFilesSizeStats: FileSizeStats = FileSizeStats(),
    var numPartitionsOptimized: Long = 0,
    var zOrderStats: Option[ZOrderStats] = None,
    var clusteringStats: Option[ClusteringStats] = None,
    var numBins: Long = 0,
    var numBatches: Long = 0,
    var totalConsideredFiles: Long = 0,
    var totalFilesSkipped: Long = 0,
    var preserveInsertionOrder: Boolean = false,
    var numFilesSkippedToReduceWriteAmplification: Long = 0,
    var numBytesSkippedToReduceWriteAmplification: Long = 0,
    startTimeMs: Long = System.currentTimeMillis(),
    var endTimeMs: Long = 0,
    var totalClusterParallelism: Long = 0,
    var totalScheduledTasks: Long = 0,
    var deletionVectorStats: Option[DeletionVectorStats] = None,
    var numTableColumns: Long = 0,
    var numTableColumnsWithStats: Long = 0,
    var autoCompactParallelismStats: AutoCompactParallelismStats = AutoCompactParallelismStats()) {

  def toOptimizeMetrics: OptimizeMetrics = {
    OptimizeMetrics(
      numFilesAdded = addedFilesSizeStats.totalFiles,
      numFilesRemoved = removedFilesSizeStats.totalFiles,
      filesAdded = addedFilesSizeStats.toFileSizeMetrics,
      filesRemoved = removedFilesSizeStats.toFileSizeMetrics,
      partitionsOptimized = numPartitionsOptimized,
      zOrderStats = zOrderStats,
      clusteringStats = clusteringStats,
      numBins = numBins,
      numBatches = numBatches,
      totalConsideredFiles = totalConsideredFiles,
      totalFilesSkipped = totalFilesSkipped,
      preserveInsertionOrder = preserveInsertionOrder,
      numFilesSkippedToReduceWriteAmplification = numFilesSkippedToReduceWriteAmplification,
      numBytesSkippedToReduceWriteAmplification = numBytesSkippedToReduceWriteAmplification,
      startTimeMs = startTimeMs,
      endTimeMs = endTimeMs,
      totalClusterParallelism = totalClusterParallelism,
      totalScheduledTasks = totalScheduledTasks,
      deletionVectorStats = deletionVectorStats,
      numTableColumns = numTableColumns,
      numTableColumnsWithStats = numTableColumnsWithStats,
      autoCompactParallelismStats = autoCompactParallelismStats.toMetrics)
  }
}

/**
 * This statistics class keeps tracking the parallelism usage of Auto Compaction.
 * It collects following metrics:
 *   -- the min/max parallelism among the whole cluster are used for Auto Compact,
 *   -- the min/max parallelism occupied by current Auto Compact session,
 */
case class AutoCompactParallelismStats(
    var maxClusterUsedParallelism: Long = 0,
    var minClusterUsedParallelism: Long = 0,
    var maxSessionUsedParallelism: Long = 0,
    var minSessionUsedParallelism: Long = 0) {
  def toMetrics: Option[ParallelismMetrics] = {
    if (maxSessionUsedParallelism == 0) {
      return None
    }
    Some(ParallelismMetrics(
      Some(maxClusterUsedParallelism),
      Some(minClusterUsedParallelism),
      Some(maxSessionUsedParallelism),
      Some(minSessionUsedParallelism)))
  }

  /** Update the statistics of parallelism of current Auto Compact command. */
  def update(clusterUsedParallelism: Long, sessionUsedParallelism: Long): Unit = {
    maxClusterUsedParallelism = Math.max(maxClusterUsedParallelism, clusterUsedParallelism)
    minClusterUsedParallelism = if (minClusterUsedParallelism == 0) {
        clusterUsedParallelism
      } else {
        Math.min(minClusterUsedParallelism, clusterUsedParallelism)
      }
    maxSessionUsedParallelism = Math.max(maxSessionUsedParallelism, sessionUsedParallelism)
    minSessionUsedParallelism = if (minSessionUsedParallelism == 0) {
        sessionUsedParallelism
      } else {
        Math.min(minSessionUsedParallelism, sessionUsedParallelism)
      }
  }
}

case class FileSizeStats(
    var minFileSize: Long = 0,
    var maxFileSize: Long = 0,
    var totalFiles: Long = 0,
    var totalSize: Long = 0) {

  def avgFileSize: Double = if (totalFiles > 0) {
      totalSize * 1.0 / totalFiles
    } else {
      0.0
    }

  def merge(candidateFiles: Seq[FileAction]): Unit = {
    if (totalFiles == 0 && candidateFiles.nonEmpty) {
      minFileSize = Long.MaxValue
      maxFileSize = Long.MinValue
    }
    candidateFiles.foreach { file =>
      val fileSize = file match {
        case addFile: AddFile => addFile.size
        case removeFile: RemoveFile => removeFile.size.getOrElse(0L)
        case default =>
          throw new IllegalArgumentException(s"Unknown FileAction type: ${default.getClass}")
      }
      minFileSize = math.min(fileSize, minFileSize)
      maxFileSize = math.max(fileSize, maxFileSize)
      totalSize += fileSize
    }
    totalFiles += candidateFiles.length
  }


  def toFileSizeMetrics: FileSizeMetrics = {
    if (totalFiles == 0) {
      return FileSizeMetrics(min = None, max = None, avg = 0, totalFiles = 0, totalSize = 0)
    }
    FileSizeMetrics(
      min = Some(minFileSize),
      max = Some(maxFileSize),
      avg = avgFileSize,
      totalFiles = totalFiles,
      totalSize = totalSize)
  }
}
/**
 * Percentiles on the file sizes in this batch.
 * @param min Size of the smallest file
 * @param p25 Size of the 25th percentile file
 * @param p50 Size of the 50th percentile file
 * @param p75 Size of the 75th percentile file
 * @param max Size of the largest file
 */
case class FileSizeStatsWithHistogram(
     min: Long,
     p25: Long,
     p50: Long,
     p75: Long,
     max: Long)

object FileSizeStatsWithHistogram {

  /**
   * Creates a [[FileSizeStatsWithHistogram]] based on the passed sorted file sizes
   * @return Some(fileSizeStatsWithHistogram) if sizes are non-empty, else returns None
   */
  def create(sizes: Seq[Long]): Option[FileSizeStatsWithHistogram] = {
    if (sizes.isEmpty) {
      return None
    }
    val count = sizes.length
    Some(FileSizeStatsWithHistogram(
      min = sizes.head,
      // we do not need to ceil the computed index as arrays start at 0
      p25 = sizes(count / 4),
      p50 = sizes(count / 2),
      p75 = sizes(count * 3 / 4),
      max = sizes.last))
  }
}

/**
 * Metrics returned by the optimize command.
 *
 * @param numFilesAdded number of files added by optimize
 * @param numFilesRemoved number of files removed by optimize
 * @param filesAdded Stats for the files added
 * @param filesRemoved Stats for the files removed
 * @param partitionsOptimized Number of partitions optimized
 * @param zOrderStats Z-Order stats
 * @param clusteringStats Clustering stats
 * @param numBins Number of bins
 * @param numBatches Number of batches
 * @param totalConsideredFiles Number of files considered for the Optimize operation.
 * @param totalFilesSkipped Number of files that are skipped from being Optimized.
 * @param preserveInsertionOrder If optimize was run with insertion preservation enabled.
 * @param numFilesSkippedToReduceWriteAmplification Number of files skipped for reducing write
 *                                                  amplification.
 * @param numBytesSkippedToReduceWriteAmplification Number of bytes skipped for reducing write
 *                                                  amplification.
 * @param startTimeMs The start time of Optimize command.
 * @param endTimeMs The end time of Optimize command.
 * @param totalClusterParallelism The total number of parallelism of this cluster.
 * @param totalScheduledTasks The total number of optimize task scheduled.
 * @param autoCompactParallelismStats The metrics of cluster and session parallelism.
 * @param deletionVectorStats Statistics related with Deletion Vectors.
 * @param numTableColumns Number of columns in the table.
 * @param numTableColumnsWithStats Number of table columns to collect data skipping stats.
 */
case class OptimizeMetrics(
    numFilesAdded: Long,
    numFilesRemoved: Long,
    filesAdded: FileSizeMetrics =
      FileSizeMetrics(min = None, max = None, avg = 0, totalFiles = 0, totalSize = 0),
    filesRemoved: FileSizeMetrics =
      FileSizeMetrics(min = None, max = None, avg = 0, totalFiles = 0, totalSize = 0),
    partitionsOptimized: Long = 0,
    zOrderStats: Option[ZOrderStats] = None,
    clusteringStats: Option[ClusteringStats] = None,
    numBins: Long,
    numBatches: Long,
    totalConsideredFiles: Long,
    totalFilesSkipped: Long = 0,
    preserveInsertionOrder: Boolean = false,
    numFilesSkippedToReduceWriteAmplification: Long = 0,
    numBytesSkippedToReduceWriteAmplification: Long = 0,
    startTimeMs: Long = 0,
    endTimeMs: Long = 0,
    totalClusterParallelism: Long = 0,
    totalScheduledTasks: Long = 0,
    autoCompactParallelismStats: Option[ParallelismMetrics] = None,
    deletionVectorStats: Option[DeletionVectorStats] = None,
    numTableColumns: Long = 0,
    numTableColumnsWithStats: Long = 0
  )

/**
 * Basic Stats on file sizes.
 *
 * @param min Minimum file size
 * @param max Maximum file size
 * @param avg Average of the file size
 * @param totalFiles Total number of files
 * @param totalSize Total size of the files
 */
case class FileSizeMetrics(
    min: Option[Long],
    max: Option[Long],
    avg: Double,
    totalFiles: Long,
    totalSize: Long)

/**
 * This statistics contains following metrics:
 *   -- the min/max parallelism among the whole cluster are used,
 *   -- the min/max parallelism occupied by current session,
 */
case class ParallelismMetrics(
     maxClusterActiveParallelism: Option[Long] = None,
     minClusterActiveParallelism: Option[Long] = None,
     maxSessionActiveParallelism: Option[Long] = None,
     minSessionActiveParallelism: Option[Long] = None)

/**
 * Accumulator for statistics related with Deletion Vectors.
 * Note that this case class contains mutable variables and cannot be used in places where immutable
 * case classes can be used (e.g. map/set keys).
 */
case class DeletionVectorStats(
  var numDeletionVectorsRemoved: Long = 0,
  var numDeletionVectorRowsRemoved: Long = 0)
