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
    var numBatches: Long = 0,
    var totalConsideredFiles: Long = 0,
    var totalFilesSkipped: Long = 0,
    var preserveInsertionOrder: Boolean = false,
    var numFilesSkippedToReduceWriteAmplification: Long = 0,
    var numBytesSkippedToReduceWriteAmplification: Long = 0,
    startTimeMs: Long = System.currentTimeMillis(),
    var endTimeMs: Long = 0) {

  def toOptimizeMetrics: OptimizeMetrics = {
    OptimizeMetrics(
      numFilesAdded = addedFilesSizeStats.totalFiles,
      numFilesRemoved = removedFilesSizeStats.totalFiles,
      filesAdded = addedFilesSizeStats.toFileSizeMetrics,
      filesRemoved = removedFilesSizeStats.toFileSizeMetrics,
      partitionsOptimized = numPartitionsOptimized,
      zOrderStats = zOrderStats,
      numBatches = numBatches,
      totalConsideredFiles = totalConsideredFiles,
      totalFilesSkipped = totalFilesSkipped,
      preserveInsertionOrder = preserveInsertionOrder,
      numFilesSkippedToReduceWriteAmplification = numFilesSkippedToReduceWriteAmplification,
      numBytesSkippedToReduceWriteAmplification = numBytesSkippedToReduceWriteAmplification,
      startTimeMs = startTimeMs,
      endTimeMs = endTimeMs
    )
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
    numBatches: Long,
    totalConsideredFiles: Long,
    totalFilesSkipped: Long = 0,
    preserveInsertionOrder: Boolean = false,
    numFilesSkippedToReduceWriteAmplification: Long = 0,
    numBytesSkippedToReduceWriteAmplification: Long = 0,
    startTimeMs: Long = 0,
    endTimeMs: Long = 0)

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
