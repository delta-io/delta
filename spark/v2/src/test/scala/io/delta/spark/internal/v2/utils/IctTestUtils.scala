/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.utils

import java.io.File

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{Action, CommitInfo}
import org.apache.spark.sql.delta.util.{DeltaCommitFileProvider, FileNames, JsonUtils}

/**
 * Java-callable helpers for manipulating in-commit-timestamps in delta log files. Mirrors the
 * behavior of [[org.apache.spark.sql.delta.InCommitTimestampTestUtils]] (in v1 test sources, not
 * visible to sparkV2 tests).
 */
object IctTestUtils {

  /**
   * Overwrites the inCommitTimestamp field in the delta JSON file for the given version.
   * operationParameters is set to an empty map because its serialization/deserialization is
   * broken otherwise.
   */
  def overwriteIctInDeltaFile(deltaLog: DeltaLog, filePath: Path, ts: Long): Unit = {
    val updated = deltaLog.store
      .readAsIterator(filePath, deltaLog.newDeltaHadoopConf())
      .map(Action.fromJson)
      .map {
        case ci: CommitInfo =>
          ci.copy(inCommitTimestamp = Some(ts), operationParameters = Map.empty).json
        case other => other.json
      }
      .toList
    deltaLog.store.write(
      filePath, updated.iterator, overwrite = true, deltaLog.newDeltaHadoopConf())
  }

  /**
   * Sets ICT and the file modification time of the given commit's delta file (and CRC if present).
   * Used to simulate a commit that "happened at" a specific timestamp under ICT semantics.
   */
  def modifyCommitTimestamp(deltaLog: DeltaLog, version: Long, ts: Long): Unit = {
    val deltaFilePath = DeltaCommitFileProvider(deltaLog.update()).deltaFile(version)
    val deltaFile = new File(deltaFilePath.toUri)
    overwriteIctInDeltaFile(deltaLog, new Path(deltaFile.getPath), ts)
    deltaFile.setLastModified(ts)
    if (FileNames.isUnbackfilledDeltaFile(deltaFilePath)) {
      val backfilled = FileNames.unsafeDeltaFile(deltaLog.logPath, version)
      val fs = backfilled.getFileSystem(deltaLog.newDeltaHadoopConf())
      if (fs.exists(backfilled)) {
        overwriteIctInDeltaFile(deltaLog, backfilled, ts)
      }
    }
    val crcPath = FileNames.checksumFile(deltaLog.logPath, version)
    val crcFile = new File(crcPath.toUri)
    if (crcFile.exists()) {
      val latestCrc = JsonUtils.fromJson[org.apache.spark.sql.delta.VersionChecksum](
        deltaLog.store.read(crcPath, deltaLog.newDeltaHadoopConf()).mkString(""))
      val updatedCrc = latestCrc.copy(inCommitTimestampOpt = Some(ts))
      deltaLog.store.write(
        crcPath,
        Iterator(JsonUtils.toJson(updatedCrc)),
        overwrite = true,
        deltaLog.newDeltaHadoopConf())
      crcFile.setLastModified(ts)
    }
  }

  /**
   * Sets only the file modification time (mtime) of a delta JSON file, without touching the
   * embedded ICT. Useful for the "ICT vs mtime drift" scenario.
   */
  def setFileMtimeOnly(deltaLog: DeltaLog, version: Long, mtime: Long): Unit = {
    val deltaFilePath = DeltaCommitFileProvider(deltaLog.update()).deltaFile(version)
    val deltaFile = new File(deltaFilePath.toUri)
    deltaFile.setLastModified(mtime)
    val crcPath = FileNames.checksumFile(deltaLog.logPath, version)
    val crcFile = new File(crcPath.toUri)
    if (crcFile.exists()) {
      crcFile.setLastModified(mtime)
    }
    if (FileNames.isUnbackfilledDeltaFile(deltaFilePath)) {
      val backfilled = FileNames.unsafeDeltaFile(deltaLog.logPath, version)
      val backfilledFile = new File(backfilled.toUri)
      if (backfilledFile.exists()) backfilledFile.setLastModified(mtime)
    }
  }
}