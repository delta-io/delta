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

package org.apache.spark.sql.delta.v2.interop

import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.util.Clock

/**
 * Interface for accessing and managing a Delta table. Mirrors DeltaLog's role as the entry
 * point for all Delta table operations.
 *
 * Two implementations:
 *  - [[DeltaLog]] (existing): reads the Delta protocol via log replay
 *  - KernelTableManager (future): reads the Delta protocol via Kernel
 *
 * Commands, OptimisticTransaction, and other business logic code against this interface
 * so they work with either backend.
 */
trait DeltaV2TableManager {

  /** The path of the Delta log directory. */
  def logPath: Path

  /** The path of the data directory of the Delta table. */
  def dataPath: Path

  /** Clock used for timestamping operations. */
  def clock: Clock

  /** Hadoop configuration options for this table. */
  def options: Map[String, String]

  /** Create a new Hadoop Configuration for this table. */
  def newDeltaHadoopConf(): Configuration

  /** Get the current snapshot of the table, updating if necessary. */
  def snapshot: Snapshot

  /** Get the latest snapshot without checking for updates. */
  def unsafeVolatileSnapshot: Snapshot

  /** Whether the table exists (version >= 0). */
  def tableExists: Boolean

  /** A unique identifier for this table. */
  def tableId: String

  /** Create the log directories if they do not exist. */
  def createLogDirectoriesIfNotExists(): Unit
}
