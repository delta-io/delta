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

import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, Metadata, Protocol}
import org.apache.spark.sql.delta.hooks.PostCommitHook

import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Represents a successfully committed transaction.
 *
 * This class encapsulates all relevant information about a transaction that has been successfully
 * committed. The main usage of this class is in running the post-commit hooks.
 *
 * @param txnId                     the unique identifier of the committed transaction.
 * @param deltaLog                  the [[DeltaLog]] instance for the table the transaction
 *                                  committed on.
 * @param catalogTable              the catalog table at the start of the transaction for the
 *                                  committed table.
 * @param snapshot                  the snapshot of the table at the time of the transaction's read.
 * @param metadata                  the metadata of the table after the txn committed.
 * @param protocol                  the protocol of the table after the txn committed.
 * @param committedVersion          the version of the table after the txn committed.
 * @param committedActions          the actions that were committed in this transaction.
 * @param postCommitSnapshot        the snapshot of the table after the txn successfully committed.
 *                                  NOTE: This may not match the committedVersion, if racing
 *                                  commits were written while the snapshot was computed.
 * @param postCommitHooks           the list of post-commit hooks to run after the commit.
 * @param finalTxnExecutionTimeMs   the time taken to execute the transaction.
 * @param finalNeedsCheckpoint      whether a checkpoint is needed after the commit.
 * @param finalPartitionsAddedToOpt the partitions that this txn added new files to.
 * @param finalIsBlindAppend        whether this transaction was a blind append.
 */
case class CommittedTransaction(
    txnId: String,
    deltaLog: DeltaLog,
    catalogTable: Option[CatalogTable],
    snapshot: Snapshot,
    metadata: Metadata,
    protocol: Protocol,
    committedVersion: Long,
    committedActions: Seq[Action],
    postCommitSnapshot: Snapshot,
    postCommitHooks: Seq[PostCommitHook],
    finalTxnExecutionTimeMs: Long,
    finalNeedsCheckpoint: Boolean,
    finalPartitionsAddedToOpt: Option[mutable.HashSet[Map[String, String]]],
    finalIsBlindAppend: Boolean
)
  extends DeltaTransaction
{
  override def txnExecutionTimeMs: Option[Long] = Some(finalTxnExecutionTimeMs)
  needsCheckpoint = finalNeedsCheckpoint
  partitionsAddedToOpt = finalPartitionsAddedToOpt
  isBlindAppend = finalIsBlindAppend
}
