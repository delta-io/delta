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

import org.apache.spark.sql.delta.skipping.clustering.{ClusteredTableUtils, ClusteringColumnInfo, ClusteringStatsCollector}
import org.apache.spark.sql.delta.skipping.clustering.ZCube
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaErrors, OptimisticTransaction, Snapshot}
import org.apache.spark.sql.delta.actions.{AddFile, DeletionVectorDescriptor, FileAction, RemoveFile}
import org.apache.spark.sql.delta.commands.OptimizeTableStrategy.DummyBinInfo
import org.apache.spark.sql.delta.commands.optimize.{AddFileWithNumRecords, DeletionVectorStats, OptimizeStats, ZOrderFileStats, ZOrderStats}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.sources.DeltaSQLConf.{DELTA_OPTIMIZE_CLUSTERING_MIN_CUBE_SIZE, DELTA_OPTIMIZE_CLUSTERING_TARGET_CUBE_SIZE}
import org.apache.spark.sql.delta.zorder.ZCubeInfo

import org.apache.spark.sql.SparkSession

object OptimizeTableMode extends Enumeration {
  type OptimizeTableMode = Value
  val COMPACTION, ZORDER, CLUSTERING = Value
}

/**
 * Defines set of utilities used in OptimizeTableCommand. The behavior of these utilities will
 * change based on the [[OptimizeTableMode]]: COMPACTION, ZORDER and CLUSTERING.
 */
trait OptimizeTableStrategy {
  def sparkSession: SparkSession

  /**
   * Utility method to get max bin size in bytes to group files into.
   */
  def maxBinSize: Long

  /**
   * Utility method to prepare files in a partition for optimization.
   *
   * By default it sorts files on the size for the binpack.
   *
   * @return Prepared files for the subsequent optimization.
   */
  def prepareFilesPerPartition(inputFiles: Seq[AddFile]): Seq[AddFile] = inputFiles.sortBy(_.size)

  /** The optimize mode the strategy instance is created for. */
  def optimizeTableMode: OptimizeTableMode.Value

  /**
   * The clustering algorithm to be used by either by ZORDER or Liquid CLUSTERING.
   * An error is thrown for COMPACTION.
   */
  def curve: String

  /**
   * Prepare a new Bin and returns its initialized [[BinInfo]].
   *
   * This function is expected to be called once for each bin
   * before [[tagAddFile]] is called.
   */
  def initNewBin: OptimizeTableStrategy.BinInfo = DummyBinInfo()

  /**
   * Incorporate essential tags for optimized files based on the [[OptimizeTableMode]].
   */
  def tagAddFile(file: AddFile, binInfo: OptimizeTableStrategy.BinInfo): AddFile =
    file.copy(dataChange = false)

  /**
   * Utility to update additional metrics after optimization.
   *
   * @param optimizeStats The input stats to update on.
   * @param removedFiles Removed files.
   * @param bins Sequence of bin-packed file groups,
   *             where each group consists of a partition value
   *             and its associated files.
   */
  def updateOptimizeStats(
      optimizeStats: OptimizeStats,
      removedFiles: Seq[RemoveFile],
      bins: Seq[Bin]): Unit
}

object OptimizeTableStrategy {
  // A trait representing the context for a Bin.
  sealed trait BinInfo

  /** Default [[BinInfo]] implementation. */
  case class DummyBinInfo() extends BinInfo

  /** [[ClusteringStrategy]]'s [[BinInfo]]. */
  case class ZCubeBinInfo(zCubeInfo: ZCubeInfo) extends BinInfo

  def apply(
      sparkSession: SparkSession,
      snapshot: Snapshot,
      optimizeContext: DeltaOptimizeContext,
      zOrderBy: Seq[String]): OptimizeTableStrategy = getMode(snapshot, zOrderBy) match {
    case OptimizeTableMode.CLUSTERING =>
      ClusteringStrategy(
        sparkSession, ClusteringColumnInfo.extractLogicalNames(snapshot))
    case OptimizeTableMode.ZORDER => ZOrderStrategy(sparkSession, zOrderBy)
    case OptimizeTableMode.COMPACTION =>
      CompactionStrategy(sparkSession, optimizeContext)
    case other => throw new UnsupportedOperationException(s"Unsupported mode $other")
  }

  private def getMode(snapshot: Snapshot, zOrderBy: Seq[String]): OptimizeTableMode.Value = {
    val isClusteredTable = ClusteredTableUtils.isSupported(snapshot.protocol)
    val hasClusteringColumns = ClusteringColumnInfo.extractLogicalNames(snapshot).nonEmpty
    val isZOrderBy = zOrderBy.nonEmpty
    if (isClusteredTable && hasClusteringColumns) {
      assert(!isZOrderBy)
      OptimizeTableMode.CLUSTERING
    } else if (isZOrderBy) {
      OptimizeTableMode.ZORDER
    } else {
      OptimizeTableMode.COMPACTION
    }
  }
}

/** Implements compaction strategy */
case class CompactionStrategy(
    override val sparkSession: SparkSession,
    optimizeContext: DeltaOptimizeContext) extends OptimizeTableStrategy {

  override val optimizeTableMode: OptimizeTableMode.Value = OptimizeTableMode.COMPACTION

  // In COMPACTION, all files within a bin are written into single larger file.
  override val maxBinSize: Long = {
    optimizeContext.maxFileSize.getOrElse(
      sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE))
  }

  override def curve: String = {
    throw new UnsupportedOperationException("Compaction doesn't support clustering.")
  }

  override def updateOptimizeStats(
      optimizeStats: OptimizeStats,
      removedFiles: Seq[RemoveFile],
      bins: Seq[Bin]): Unit = {}
}

/** Implements ZOrder strategy */
case class ZOrderStrategy(
    override val sparkSession: SparkSession,
    zOrderColumns: Seq[String]) extends OptimizeTableStrategy {

  assert(zOrderColumns.nonEmpty)

  override val optimizeTableMode: OptimizeTableMode.Value = OptimizeTableMode.ZORDER

  override val curve: String = "zorder"

  // For ZORDER, set maxBinSize the maximal LONG value to have single BIN for each partition.
  override val maxBinSize: Long = Long.MaxValue

  override def updateOptimizeStats(
      optimizeStats: OptimizeStats,
      removedFiles: Seq[RemoveFile],
      bins: Seq[Bin]): Unit = {
    val inputFileStats =
      ZOrderFileStats(removedFiles.size, removedFiles.map(_.size.getOrElse(0L)).sum)
    optimizeStats.zOrderStats = Some(ZOrderStats(
      strategyName = "all", // means process all files in a partition
      inputCubeFiles = ZOrderFileStats(0, 0),
      inputOtherFiles = inputFileStats,
      inputNumCubes = 0,
      mergedFiles = inputFileStats,
      // There will one z-cube for each partition
      numOutputCubes = optimizeStats.numPartitionsOptimized))
  }
}

/** Implements clustering strategy for clustered tables */
case class ClusteringStrategy(
    override val sparkSession: SparkSession,
    clusteringColumns: Seq[String]) extends OptimizeTableStrategy {

  override val optimizeTableMode: OptimizeTableMode.Value = OptimizeTableMode.CLUSTERING

  override val curve: String = "hilbert"

  /**
   * In clustering, the bin size corresponds to a ZCube size that can be adjusted through
   * configurations.
   */
  override val maxBinSize: Long = {
    Math.max(
      sparkSession.sessionState.conf.getConf(DELTA_OPTIMIZE_CLUSTERING_TARGET_CUBE_SIZE),
      sparkSession.sessionState.conf.getConf(DELTA_OPTIMIZE_CLUSTERING_MIN_CUBE_SIZE))
  }

  // Prepare files for binpack.
  override def prepareFilesPerPartition(inputFiles: Seq[AddFile]): Seq[AddFile] = {
    // Un-clustered files don't have a ZCUBE_ID, and are sorted before clustered files (None is
    // considered the smallest element). We also don't consider partitionValues because
    // clustered tables should always be unpartitioned.
    applyMinZCube(inputFiles.sortBy(_.tag(AddFile.Tags.ZCUBE_ID)))
  }

  // Upon a new ZCube, allocate a [[ZCubeInfo]] with a new ZCUBE ID.
  override def initNewBin: OptimizeTableStrategy.BinInfo = {
    OptimizeTableStrategy.ZCubeBinInfo(ZCubeInfo(clusteringColumns))
  }

  override def tagAddFile(file: AddFile, binInfo: OptimizeTableStrategy.BinInfo): AddFile = {
    val taggedFile = super.tagAddFile(file, binInfo)
    val zCubeInfo = binInfo.asInstanceOf[OptimizeTableStrategy.ZCubeBinInfo].zCubeInfo
    ZCubeInfo.setForFile(
      taggedFile.copy(clusteringProvider = Some(ClusteredTableUtils.clusteringProvider)), zCubeInfo)
  }

  override def updateOptimizeStats(
      optimizeStats: OptimizeStats,
      removedFiles: Seq[RemoveFile],
      bins: Seq[Bin]): Unit = {
    clusteringStatsCollector.numOutputZCubes = bins.size
    optimizeStats.clusteringStats = Option(clusteringStatsCollector.getClusteringStats)
  }

  /**
   * Given a sequence of files sorted by ZCubeId, return candidate files for
   * clustering. The requirements to pick candidate files are:
   *
   * 1. Candidate files are either un-clustered (missing clusteringProvider) or the
   * clusteringProvider is "liquid".
   * 2. Clustered files (clusteringProvider is set) with different clustering columns are skipped.
   * When clustering columns are changed, existing clustered data is not re-clustered.
   * 3. Files that belong to the partial ZCubes are picked. A ZCube is considered as a partial
   * ZCube if its size is smaller than [[DELTA_OPTIMIZE_CLUSTERING_MIN_CUBE_SIZE]].
   * 4. If there is only single ZCUBE with all files are clustered and if all clustered files
   * belong to that ZCube, all files are filtered out.
   */
  private def applyMinZCube(files: Seq[AddFile]): Seq[AddFile] = {
    val targetSize = sparkSession.sessionState.conf.getConf(DELTA_OPTIMIZE_CLUSTERING_MIN_CUBE_SIZE)
    // Skip files with from different clusteringProviders or files clustered by a different set
    // of clustering columns.
    val inputFiles = files.iterator.filter { file =>
      clusteringStatsCollector.inputStats.updateStats(file)
      val sameOrMissingClusteringProvider =
        file.clusteringProvider.forall(_ == ClusteredTableUtils.clusteringProvider)

      // If clustered before, remove those with different clustering columns.
      val zCubeInfo = ZCubeInfo.getForFile(file)
      val unmatchedClusteringColumns = zCubeInfo.exists(_.zOrderBy != clusteringColumns)
      sameOrMissingClusteringProvider && !unmatchedClusteringColumns
    }.map(AddFileWithNumRecords.createFromFile)
    // Skip files that belong to a ZCUBE that is larger than target ZCUBE size.
    val smallZCubeFiles = ZCube.filterOutLargeZCubes(inputFiles, targetSize)

    // Skip smallZCubeFiles if they all belong to a single ZCUBE.
    ZCube.filterOutSingleZCubes(smallZCubeFiles).map { file =>
      clusteringStatsCollector.outputStats.updateStats(file.addFile)
      file.addFile
    }.toSeq
  }

  /** Metrics for clustering when [[isClusteredTable]] is true. */
  private val clusteringStatsCollector: ClusteringStatsCollector =
    ClusteringStatsCollector(clusteringColumns)
}
