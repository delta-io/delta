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

case class DeltaCommitFileProvider(logPath: String, maxVersion: Long, uuids: Map[Long, String]) {
  // Ensure the Path object is reused across Delta Files but not stored as part of the object state
  // since it is not serializable.
  @transient lazy val resolvedPath: Path = new Path(logPath)

  def deltaFile(version: Long): Path = {
    if (version > maxVersion) {
      throw new IllegalStateException("Cannot resolve Delta table at version $version as the " +
        "state is currently at version $maxVersion. The requested version may be incorrect or " +
        "the state may be outdated. Please verify the requested version, update the state if " +
        "necessary, and try again")
    }
    uuids.get(version) match {
      case Some(uuid) => FileNames.unbackfilledDeltaFile(resolvedPath, version, Some(uuid))
      case _ => FileNames.deltaFile(resolvedPath, version)
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
