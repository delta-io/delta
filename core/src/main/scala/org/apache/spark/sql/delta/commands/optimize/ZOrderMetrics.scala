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

// scalastyle:off import.ordering.noEmptyLine

/**
 * Aggregated file stats for a category of ZCube files.
 * @param num Total number of files.
 * @param size Total size of files in bytes.
 */
case class ZOrderFileStats(num: Long, size: Long)

object ZOrderFileStats {
  def apply(v: Iterable[(Int, Long)]): ZOrderFileStats = {
      val (is, ls) = v.foldLeft((0L, 0L)) {
        case ((i0, l0), (i1, l1)) => (i0 + i1, l0 + l1)
      }
      ZOrderFileStats(is, ls)
    }
}

/**
 * Aggregated stats for OPTIMIZE ZORDERBY command.
 * This is a public facing API, consider any change carefully.
 *
 * @param strategyName ZCubeMergeStrategy used.
 * @param inputCubeFiles Files in the ZCube matching the current OPTIMIZE operation.
 * @param inputOtherFiles Files not in any ZCube or in other ZCube orderings.
 * @param inputNumCubes Number of different cubes among input files.
 * @param mergedFiles Subset of input files merged by the current operation
 * @param numOutputCubes Number of output ZCubes written out
 * @param mergedNumCubes Number of different cubes among merged files.
 */
case class ZOrderStats(
  strategyName: String,
  inputCubeFiles: ZOrderFileStats,
  inputOtherFiles: ZOrderFileStats,
  inputNumCubes: Long,
  mergedFiles: ZOrderFileStats,
  numOutputCubes: Long,
  mergedNumCubes: Option[Long] = None
)
