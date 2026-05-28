/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.v3

import java.util.Objects

import org.apache.spark.sql.connector.read.file.{FileBatch, FileScan}
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/**
 * Delta concrete [[FileScan]]. Pinned to a specific Snapshot at scan-builder time so the
 * scanned version is determinable without re-running the optimizer.
 *
 * `equals` / `hashCode` are keyed by the pinned snapshot version (plus pushed filters and the
 * required schema) so that two DataFrame caches against different versions of the same table
 * do NOT collide in Spark's plan cache - one of the long-standing V1 issues we're fixing.
 */
case class DeltaFileScan(
    fileSet: DeltaFileSet,
    pinnedSnapshot: Snapshot,
    requiredSchema: StructType,
    pushedFilters: Array[Filter],
    postScanFilters: Array[Filter],
    isCDC: Boolean) extends FileScan with FileBatch {

  override def readSchema(): StructType = requiredSchema

  override def dataSchema: StructType = fileSet.dataSchema

  override def isPinned: Boolean = fileSet.isPinned

  override def description(): String = {
    val pushed = pushedFilters.mkString(", ")
    s"DeltaFileScan(version=${pinnedSnapshot.version}, " +
      s"path=${pinnedSnapshot.path}, pushedFilters=[$pushed], " +
      s"readSchema=${requiredSchema.simpleString})"
  }

  // Identity is the (table, version, projection, pushed-filter) tuple; never the FileSet's
  // FileIndex object identity, which changes per resolution and would defeat caching.
  override def equals(obj: Any): Boolean = obj match {
    case other: DeltaFileScan =>
      pinnedSnapshot.deltaLog.tableId == other.pinnedSnapshot.deltaLog.tableId &&
        pinnedSnapshot.version == other.pinnedSnapshot.version &&
        requiredSchema == other.requiredSchema &&
        pushedFilters.toSeq == other.pushedFilters.toSeq &&
        isCDC == other.isCDC
    case _ => false
  }

  override def hashCode(): Int = Objects.hash(
    pinnedSnapshot.deltaLog.tableId,
    Long.box(pinnedSnapshot.version),
    requiredSchema,
    pushedFilters.toSeq,
    Boolean.box(isCDC))
}
