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

package org.apache.spark.sql.delta.util

import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.util.FileNames._
import org.apache.hadoop.fs.Path

/**
 * Provides access to resolve Delta commit files names based on the commit-version.
 *
 * This class is part of the changes introduced to accommodate the adoption of coordinated-commits
 * in Delta Lake. Previously, certain code paths assumed the existence of delta files for a specific
 * version at a predictable path `_delta_log/$version.json`. With coordinated-commits, delta files
 * may alternatively be located at `_delta_log/_commits/$version.$uuid.json`.
 * DeltaCommitFileProvider attempts to locate the correct delta files from the Snapshot's
 * LogSegment.
 *
 * @param logPath The path to the Delta table log directory.
 * @param maxVersionInclusive The maximum version of the Delta table (inclusive).
 * @param uuids A map of version numbers to their corresponding UUIDs.
 */
case class DeltaCommitFileProvider(
    logPath: String,
    maxVersionInclusive: Long,
    uuids: Map[Long, String]) {
  // Ensure the Path object is reused across Delta Files but not stored as part of the object state
  // since it is not serializable.
  @transient lazy val resolvedPath: Path = new Path(logPath)
  lazy val minUnbackfilledVersion: Long =
    if (uuids.keys.isEmpty) {
      maxVersionInclusive + 1
    } else {
      uuids.keys.min
    }

  def deltaFile(version: Long): Path = {
    if (version > maxVersionInclusive) {
      throw new IllegalStateException(s"Cannot resolve Delta table at version $version as the " +
        s"state is currently at version $maxVersionInclusive. The requested version may be " +
        s"incorrect or the state may be outdated. Please verify the requested version, update " +
        s"the state if necessary, and try again")
    }
    uuids.get(version) match {
      case Some(uuid) => FileNames.unbackfilledDeltaFile(resolvedPath, version, Some(uuid))
      case _ => FileNames.unsafeDeltaFile(resolvedPath, version)
    }
  }
}

object DeltaCommitFileProvider {
  def apply(snapshot: Snapshot): DeltaCommitFileProvider = {
    val uuids = snapshot.logSegment.deltas
      .collect { case UnbackfilledDeltaFile(_, version, uuid) => version -> uuid }
      .toMap
    new DeltaCommitFileProvider(snapshot.path.toString, snapshot.version, uuids)
  }
}
