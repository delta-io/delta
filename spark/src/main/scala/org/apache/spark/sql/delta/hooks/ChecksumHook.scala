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

package org.apache.spark.sql.delta.hooks

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransactionImpl, RecordChecksum, Snapshot}
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession

/** Write a new checksum at the version committed by the txn if possible. */
object ChecksumHook extends PostCommitHook with DeltaLogging {
  // Helper that creates a RecordChecksum and uses it to write a checksum file
  case class WriteChecksum(
      override val spark: SparkSession,
      override val deltaLog: DeltaLog,
      txnId: String,
      snapshot: Snapshot) extends RecordChecksum {
    writeChecksumFile(txnId, snapshot)
  }

  override val name: String = "Post commit checksum trigger"

  override def run(
      spark: SparkSession,
      txn: OptimisticTransactionImpl,
      committedVersion: Long,
      postCommitSnapshot: Snapshot,
      committedActions: Seq[Action]): Unit = {
    // Only write the checksum if the postCommitSnapshot matches the version that was committed.
    if (postCommitSnapshot.version != committedVersion) return
    logInfo(
      log"Writing checksum file for table path ${MDC(DeltaLogKeys.PATH, txn.deltaLog.logPath)} " +
      log"version ${MDC(DeltaLogKeys.VERSION, committedVersion)}")

    writeChecksum(spark, txn, postCommitSnapshot)
  }

  private def writeChecksum(
      spark: SparkSession,
      txn: OptimisticTransactionImpl,
      postCommitSnapshot: Snapshot): Unit = {
    WriteChecksum(spark, txn.deltaLog, txn.txnId, postCommitSnapshot)
  }

}
