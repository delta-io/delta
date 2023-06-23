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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.{DeltaErrors, OptimisticTransactionImpl, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, AddFile}
import org.apache.spark.sql.delta.commands.{DeltaOptimizeContext, OptimizeExecutor}
import org.apache.spark.sql.delta.metering.DeltaLogging

/**
 * Post commit hook to trigger compaction.
 */
object DoAutoCompaction extends PostCommitHook
  with DeltaLogging
  with Serializable {

  override val name: String = "Triggers compaction if necessary"

  override def run(
      spark: SparkSession,
      txn: OptimisticTransactionImpl,
      committedVersion: Long,
      postCommitSnapshot: Snapshot,
      committedActions: Seq[Action]): Unit = {

    val newTxn = txn.deltaLog.startTransaction()
    val addedFiles = committedActions.collect { case a: AddFile => a }
    if (addedFiles.nonEmpty) {
      new OptimizeExecutor(spark, newTxn, Seq.empty, Seq.empty, DeltaOptimizeContext(isAuto = true))
        .autoCompact(addedFiles)
    }
  }

  override def handleError(error: Throwable, version: Long): Unit = {
    throw DeltaErrors.postCommitHookFailedException(this, version, name, error)
  }
}

