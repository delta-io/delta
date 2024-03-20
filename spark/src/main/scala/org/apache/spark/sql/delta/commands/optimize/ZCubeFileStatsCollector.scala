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

import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.zorder.ZCubeInfo
import org.apache.spark.sql.delta.zorder.ZCubeInfo.ZCubeID

/**
 * ZCube file statistics collector. An object of this class can be used to collect ZCube statistics.
 * The file statistics collection can be started by initializing an object of this class and
 * calling updateStats on every new file seen.
 * The number of ZCubes, number of files from matching cubes and number of unoptimized files are
 * captured here.
 */
class ZCubeFileStatsCollector(zOrderBy: Seq[String]) {

  /** map that holds the file statistics Map("element" -> (number of files, total file size)) */
  private var processedZCube: ZCubeID = _

  /** number of distinct zCubes seen so far */
  var numZCubes = 0

  private var matchingCubeCnt = 0
  private var matchingCubeSize = 0L
  private var otherFilesCnt = 0
  private var otherFilesSize = 0L
  def fileStats: Map[String, (Int, Long)] = Map(
    "matchingCube" -> ((matchingCubeCnt, matchingCubeSize)),
    "otherFiles" -> ((otherFilesCnt, otherFilesSize))
  )

  /** method to update the zCubeFileStats incrementally by file */
  def updateStats(file: AddFile): AddFile = {
    val zCubeInfo = ZCubeInfo.getForFile(file)
    if (zCubeInfo.isDefined && zCubeInfo.get.zOrderBy == zOrderBy) {
      if (processedZCube != zCubeInfo.get.zCubeID) {
        processedZCube = zCubeInfo.get.zCubeID
        numZCubes += 1
      }
      matchingCubeCnt += 1
      matchingCubeSize += file.size
    } else {
      otherFilesCnt += 1
      otherFilesSize += file.size
    }
    file
  }
}
