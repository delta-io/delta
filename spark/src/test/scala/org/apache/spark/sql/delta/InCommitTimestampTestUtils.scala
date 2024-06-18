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
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
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
}
