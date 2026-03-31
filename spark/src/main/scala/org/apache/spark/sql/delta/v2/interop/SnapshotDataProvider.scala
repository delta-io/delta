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

package org.apache.spark.sql.delta.v2.interop

import org.apache.spark.sql.delta.SnapshotState
import org.apache.spark.sql.delta.actions.{AddFile, Metadata, Protocol, SingleAction}

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Provides snapshot data from a specific source. Snapshot delegates to this
 * instead of scattering source-specific logic through its methods.
 *
 * Two implementations:
 *  - None (default): Snapshot uses its legacy path (state reconstruction from LogSegment)
 *  - KernelSnapshotDataProvider (future): data from Kernel Scan API
 */
private[delta] trait SnapshotDataProvider {
  def metadata: Metadata
  def protocol: Protocol
  def allFiles(spark: SparkSession): Dataset[AddFile]
  def stateDS(spark: SparkSession): Dataset[SingleAction]
  def computedState(spark: SparkSession, metadata: Metadata, protocol: Protocol): SnapshotState
  def getInCommitTimestampOpt: Option[Long]
}
