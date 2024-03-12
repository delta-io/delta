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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.optimize.AddFileWithNumRecords
import org.apache.spark.sql.delta.zorder.ZCubeInfo
import org.apache.spark.sql.delta.zorder.ZCubeInfo.{getForFile => getZCubeInfo}

import org.apache.spark.util.Utils

/**
 * Collection of files that were produced by the same job in a run of the clustering command.
 */
case class ZCube(files: Seq[AddFile]) {
  require(files.nonEmpty)

  if (Utils.isTesting) {
    assert(files.forall(getZCubeInfo(_) == Some(zCubeInfo)))
  }

  lazy val zCubeInfo: ZCubeInfo = getZCubeInfo(files.head).get
  lazy val totalFileSize: Long = files.foldLeft(0L)(_ + _.size)
}

object ZCube {
  /**
   * Given an iterator of files sorted by ZCubeId, returns a filtered iterator of files,
   * where files belonging to large ZCubes (ZCube size >= target ZCube size ) are filtered
   * out.
   *
   * @param files - Files sorted by ZCubeId, unoptimized files first.
   */
  def filterOutLargeZCubes(
      files: Iterator[AddFileWithNumRecords],
      targetCubeSize: Long): Iterator[AddFileWithNumRecords] = {
    val currentZCube = new ArrayBuffer[AddFileWithNumRecords]()
    var currentZCubeSize = 0L
    var currentZCubeId: String = null

    def appendZCube(file: AddFileWithNumRecords): Unit = {
      currentZCube.append(file)
      currentZCubeSize += file.addFile.estLogicalFileSize.getOrElse(file.addFile.size)
    }

    def resetZCube(): Unit = {
      currentZCube.clear()
      currentZCubeSize = 0
    }

    def returnAndResetCurrentZCube(): Seq[AddFileWithNumRecords] = {
      val res = if (currentZCubeSize >= targetCubeSize) {
        // Drop the current ZCube.
        Seq.empty
      } else {
        // Return a copy of current.
        currentZCube.toVector
      }
      resetZCube()
      res
    }

    files.flatMap { addFileWithNumRecords =>
      val file = addFileWithNumRecords.addFile
      val res = ZCubeInfo.getForFile(file) match {
        case Some(ZCubeInfo(zCubeID, _)) =>
          // Note: check for ZCubes' ids to group files from the same ZCube.
          if (zCubeID == currentZCubeId) {
            // Add to the same ZCube.
            appendZCube(addFileWithNumRecords)
            // Skip to next file.
            Nil
          } else {
            // New ZCube.
            val currentZCubeResult = returnAndResetCurrentZCube()
            // Start a new ZCube.
            appendZCube(addFileWithNumRecords)
            currentZCubeId = zCubeID
            currentZCubeResult
          }
        case None =>
          // Return current ZCube and this file.
          returnAndResetCurrentZCube() :+ addFileWithNumRecords
      }
      if (!files.hasNext) {
        // Last file, return the current ZCube and the result.
        returnAndResetCurrentZCube() ++ res
      } else {
        res
      }
    }
  }

  /**
   * Filter out files belonging to single ZCube.
   *
   * @param files - Files sorted by ZCubeId, unoptimized files first.
   */
  def filterOutSingleZCubes(
      files: Iterator[AddFileWithNumRecords]): Iterator[AddFileWithNumRecords] = {
    val currentZCube = new ArrayBuffer[AddFileWithNumRecords]()
    var singleZCube = true
    var currentZCubeId: String = null

    def appendZCube(file: AddFileWithNumRecords): Unit = {
      currentZCube.append(file)
    }

    def returnAndResetCurrentZCube(): Seq[AddFileWithNumRecords] = {
      val res = if (singleZCube) {
        // Drop the current ZCube.
        Seq.empty
      } else {
        // Return a copy of current.
        currentZCube.toVector
      }
      resetZCube()
      res
    }

    def resetZCube(): Unit = {
      currentZCube.clear()
    }

    files.flatMap { addFileWithNumRecords =>
      val file = addFileWithNumRecords.addFile
      val res = ZCubeInfo.getForFile(file) match {
        case Some(ZCubeInfo(zCubeID, _)) =>
          if (zCubeID == currentZCubeId || currentZCubeId == null) {
            if (currentZCubeId == null) {
              currentZCubeId = zCubeID
            }
            // Same ZCube.
            appendZCube(addFileWithNumRecords)
            Nil
          } else {
            // New ZCube.
            currentZCubeId = zCubeID
            // Return the current ZCube and start a new ZCube.
            singleZCube = false
            val currentZCubeResult = returnAndResetCurrentZCube()
            appendZCube(addFileWithNumRecords)
            currentZCubeResult
          }
        case None =>
          val resZCube = returnAndResetCurrentZCube()
          // Unoptimized file means the following ZCube is not alone.
          singleZCube = false
          resZCube :+ addFileWithNumRecords
      }
      if (!files.hasNext) {
        // Last file, return the current ZCube.
        returnAndResetCurrentZCube() ++ res
      } else {
        res
      }
    }
  }
}
