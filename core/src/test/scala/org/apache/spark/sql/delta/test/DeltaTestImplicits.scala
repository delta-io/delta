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

package org.apache.spark.sql.delta.test

import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction, Snapshot}
import org.apache.spark.sql.delta.DeltaOperations.{ManualUpdate, Operation, Write}
import org.apache.spark.sql.delta.actions.{Action, Metadata, Protocol}

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Additional method definitions for Delta classes that are intended for use only in testing.
 */
object DeltaTestImplicits {
  implicit class OptimisticTxnTestHelper(txn: OptimisticTransaction) {

    /** Ensure that the initial commit of a Delta table always contains a Metadata action */
    def commitActions(op: Operation, actions: Action*): Long = {
      if (txn.readVersion == -1) {
        val metadataOpt = if (!actions.exists(_.isInstanceOf[Metadata])) Some(Metadata()) else None
        val metadata = metadataOpt.getOrElse(actions.collectFirst { case m: Metadata => m }.get)
        val needsProtocolUpdate = Protocol.upgradeProtocolFromMetadataForExistingTable(
          SparkSession.active, metadata, txn.protocol)
        if (needsProtocolUpdate.isDefined) {
          // if the metadata triggers a protocol upgrade, commit without an explicit protocol
          // action as otherwise, we will create two (potentially different) protocol actions
          // for this commit
          txn.commit(actions ++ metadataOpt, op)
        } else {
          // if we don't have an implicit protocol action, make sure the first commit
          // contains one explicitly
          val protocolOpt = if (!actions.exists(_.isInstanceOf[Protocol])) {
            Some(Action.supportedProtocolVersion())
          } else {
            None
          }
          txn.commit(actions ++ metadataOpt ++ protocolOpt, op)
        }
      } else {
        txn.commit(actions, op)
      }
    }

    def commitManually(actions: Action*): Long = {
      commitActions(ManualUpdate, actions: _*)
    }

    def commitWriteAppend(actions: Action*): Long = {
      commitActions(Write(SaveMode.Append), actions: _*)
    }
  }

  /**
   * Helper class for working with the most recent snapshot in the deltaLog
   */
  implicit class DeltaLogTestHelper(deltaLog: DeltaLog) {
    def snapshot: Snapshot = {
      deltaLog.unsafeVolatileSnapshot
    }

    def checkpoint(): Unit = {
      deltaLog.checkpoint(snapshot)
    }

    def checkpointInterval(): Int = {
      deltaLog.checkpointInterval(snapshot.metadata)
    }

    def deltaRetentionMillis(): Long = {
      deltaLog.deltaRetentionMillis(snapshot.metadata)
    }

    def enableExpiredLogCleanup(): Boolean = {
      deltaLog.enableExpiredLogCleanup(snapshot.metadata)
    }
  }
}
