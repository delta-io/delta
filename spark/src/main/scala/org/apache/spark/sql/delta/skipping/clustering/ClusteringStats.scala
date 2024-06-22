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

package org.apache.spark.sql.delta.skipping.clustering

import org.apache.spark.sql.delta.commands.optimize.ZCubeFileStatsCollector

/**
 * Aggregated file stats for a category of ZCube files.
 *
 * @param numFiles Total number of files.
 * @param size Total physical size of files in bytes.
 */
case class ClusteringFileStats(numFiles: Long, size: Long)

object ClusteringFileStats {
  def apply(v: Iterable[(Int, Long)]): ClusteringFileStats = {
    v.foldLeft(ClusteringFileStats(0, 0)) { (a, b) =>
      ClusteringFileStats(a.numFiles + b._1, a.size + b._2)
    }
  }
}

/**
 * Aggregated stats for OPTIMIZE command on clustered tables.
 *
 * @param inputZCubeFiles Files in the ZCubes matching the current OPTIMIZE operation.
 * @param inputOtherFiles Files not in any ZCubes or in other ZCubes with different
 *                        clustering columns.
 * @param inputNumZCubes Number of different cubes among input files.
 * @param mergedFiles Subset of input files merged by the current operation
 * @param numOutputZCubes Number of output ZCubes written out
 */
case class ClusteringStats(
    inputZCubeFiles: ClusteringFileStats,
    inputOtherFiles: ClusteringFileStats,
    inputNumZCubes: Long,
    mergedFiles: ClusteringFileStats,
    numOutputZCubes: Long)

/**
 * A class help collecting ClusteringStats.
 */
case class ClusteringStatsCollector(zOrderBy: Seq[String]) {

  val inputStats = new ZCubeFileStatsCollector(zOrderBy)
  val outputStats = new ZCubeFileStatsCollector(zOrderBy)
  var numOutputZCubes = 0

  def getClusteringStats: ClusteringStats = {
    ClusteringStats(
      inputNumZCubes = inputStats.numZCubes,
      inputZCubeFiles = ClusteringFileStats(inputStats.fileStats.get("matchingCube")),
      inputOtherFiles = ClusteringFileStats(inputStats.fileStats.get("otherFiles")),
      mergedFiles = ClusteringFileStats(outputStats.fileStats.values),
      numOutputZCubes = numOutputZCubes)
  }
}
