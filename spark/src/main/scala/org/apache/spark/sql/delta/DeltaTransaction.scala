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

import scala.collection.mutable

import org.apache.spark.sql.delta.actions.{AddFile, CommitInfo, Metadata, Protocol}
import org.apache.spark.sql.delta.hooks.PostCommitHook

import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTable}

/**
 * Represents a transaction that maps to a delta table commit.
 *
 * An instance of this trait tracks the reads and writes as well as accumulates additional
 * information such as statistics of a single table throughout the life of a transaction.
 */
trait DeltaTransaction {
  val deltaLog: DeltaLog
  val catalogTable: Option[CatalogTable]
  val snapshot: Snapshot

  /** Unique identifier for the transaction */
  val txnId: String

  /**
   * Returns the metadata for this transaction. The metadata refers to the metadata of the snapshot
   * at the transaction's read version unless updated during the transaction.
   */
  def metadata: Metadata

  /** The protocol of the snapshot that this transaction is reading at. */
  def protocol: Protocol

  /** The end to end execution time of this transaction. */
  def txnExecutionTimeMs: Option[Long]

  /** Whether the txn should trigger a checkpoint after the commit */
  private[delta] var needsCheckpoint = false

  /** The set of distinct partitions that contain added files by current transaction. */
  protected[delta] var partitionsAddedToOpt: Option[mutable.HashSet[Map[String, String]]] = None

  /** True if this transaction is a blind append. This is only valid after commit. */
  protected[delta] var isBlindAppend: Boolean = false

}
