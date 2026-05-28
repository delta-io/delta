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

package org.apache.spark.sql.connector.read.file

import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/**
 * A [[Scan]] backed by a set of files (a [[FileSet]]), and intended to be lowered to a
 * `LogicalRelation(HadoopFsRelation)` at planning time by a connector-side `SparkStrategy`.
 *
 * This trait is the read-side counterpart to
 * [[org.apache.spark.sql.connector.write.file.FileWrite]] and exists so that a connector
 * (e.g. Delta) can preserve a DSv2 logical plan shape (`DataSourceV2ScanRelation`) through
 * analysis/optimization, while still producing a `FileSourceScanExec` (or its Photon
 * replacement) at execution time. The connector contributes a planner strategy that matches
 * `FileScan` and rewrites the V2 scan into the V1 physical plan.
 *
 * The default `toBatch` / `toMicroBatchStream` / `toContinuousStream` implementations inherited
 * from [[Scan]] are deliberately overridden to throw. A correctly registered planner strategy
 * is required to consume the [[FileScan]] before Spark's default DSv2 planner reaches it; if
 * none does, we want to fail loudly rather than silently fall through.
 *
 * Naming note: Spark has an unrelated abstract class
 * `org.apache.spark.sql.execution.datasources.v2.FileScan` used for built-in V2 file sources.
 * That class is a different kind in a different package - they will never be imported into
 * the same file.
 */
trait FileScan extends Scan {

  /** Files (and `FileFormat`) this scan will read. Carries the snapshot pin and DV info. */
  def fileSet: FileSet

  /** Filters Delta has accepted for pushdown (a subset of the input filters). */
  def pushedFilters: Array[Filter]

  /** Filters Delta could not push down. Spark must re-evaluate them above the scan. */
  def postScanFilters: Array[Filter]

  /** True when [[fileSet]] is pinned to a specific snapshot (time travel, CDC, DV). */
  def isPinned: Boolean

  /** Pre-projection schema in storage form. */
  def dataSchema: StructType

  /** Post-projection required schema. Defaults to [[readSchema]]. */
  override def readSchema(): StructType

  final override def toBatch: Batch = throw new UnsupportedOperationException(
    "FileScan is lowered to FileSourceScanExec at planning time; toBatch must not be called.")

  final override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
    throw new UnsupportedOperationException(
      "FileScan is lowered at planning time; toMicroBatchStream must not be called.")
}
