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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.actions.{Action, CommitInfo}
import org.apache.spark.sql.delta.util.{DeltaCommitFileProvider, FileNames, JsonUtils}
import org.apache.hadoop.fs.Path

object InCommitTimestampTestUtils {
  /**
   * Overwrites the in-commit-timestamp in the delta file with the given timestamp.
   * It will also set operationParameters to an empty map because operationParameters
   * serialization/deserialization is broken.
   */
  def overwriteICTInDeltaFile(deltaLog: DeltaLog, filePath: Path, ts: Option[Long]): Unit = {
    val updatedActionsList = deltaLog.store
      .readAsIterator(filePath, deltaLog.newDeltaHadoopConf())
      .map(Action.fromJson)
      .map {
        case ci: CommitInfo =>
          // operationParameters serialization/deserialization is broken as it uses a custom
          // serializer but a default deserializer.
          ci.copy(inCommitTimestamp = ts, operationParameters = Map.empty).json
        case other => other.json
      }.toList
    deltaLog.store.write(
      filePath, updatedActionsList.toIterator, overwrite = true, deltaLog.newDeltaHadoopConf())
  }

  /**
   * Overwrites the in-commit-timestamp in the given CRC file with the given timestamp.
   */
  def overwriteICTInCrc(deltaLog: DeltaLog, version: Long, ts: Option[Long]): Unit = {
    val crcPath = FileNames.checksumFile(deltaLog.logPath, version)
    val latestCrc = JsonUtils.fromJson[VersionChecksum](
      deltaLog.store.read(crcPath, deltaLog.newDeltaHadoopConf()).mkString(""))
    val checksumWithNoICT = latestCrc.copy(inCommitTimestampOpt = ts)
    deltaLog.store.write(
      crcPath,
      Iterator(JsonUtils.toJson(checksumWithNoICT)),
      overwrite = true,
      deltaLog.newDeltaHadoopConf())
  }

  /**
   * Retrieves the in-commit timestamp for a specific version of the Delta Log.
   */
  def getInCommitTimestamp(deltaLog: DeltaLog, version: Long): Long = {
    val deltaFile = DeltaCommitFileProvider(deltaLog.unsafeVolatileSnapshot).deltaFile(version)
    val commitInfo = DeltaHistoryManager.getCommitInfoOpt(
      deltaLog.store,
      deltaFile,
      deltaLog.newDeltaHadoopConf())
    assert(commitInfo.isDefined, s"CommitInfo should exist for version $version")
    assert(commitInfo.get.inCommitTimestamp.isDefined,
      s"InCommitTimestamp should exist for CommitInfo's version $version")
    commitInfo.get.inCommitTimestamp.get
  }

  /**
   * Retrieves a map of file modification times for Delta Log versions within a specified version
   * range.
   */
  def getFileModificationTimesMap(
      deltaLog: DeltaLog, start: Long, end: Long): Map[Long, Long] = {
    deltaLog.store.listFrom(
        FileNames.listingPrefix(deltaLog.logPath, start), deltaLog.newDeltaHadoopConf())
      .collect { case FileNames.DeltaFile(fs, v) => v -> fs.getModificationTime }
      .takeWhile(_._1 <= end)
      .toMap
  }
}
