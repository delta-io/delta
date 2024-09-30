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

import org.apache.spark.sql.delta.{OptimisticTransactionImpl, Snapshot, UniversalFormat}
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf.DELTA_UNIFORM_HUDI_SYNC_CONVERT_ENABLED

import org.apache.spark.sql.SparkSession

/** Write a new Hudi commit for the version committed by the txn, if required. */
object HudiConverterHook extends PostCommitHook with DeltaLogging {
  override val name: String = "Post-commit Hudi metadata conversion"

  val ASYNC_HUDI_CONVERTER_THREAD_NAME = "async-hudi-converter"

  override def run(
      spark: SparkSession,
      txn: OptimisticTransactionImpl,
      committedVersion: Long,
      postCommitSnapshot: Snapshot,
      committedActions: Seq[Action]): Unit = {
    // Only convert to Hudi if the snapshot matches the version committed.
    // This is to skip converting the same actions multiple times - they'll be written out
    // by another commit anyways.
    if (committedVersion != postCommitSnapshot.version ||
        !UniversalFormat.hudiEnabled(postCommitSnapshot.metadata)) {
      return
    }
    val converter = postCommitSnapshot.deltaLog.hudiConverter
    if (spark.sessionState.conf.getConf(DELTA_UNIFORM_HUDI_SYNC_CONVERT_ENABLED)) {
      converter.convertSnapshot(postCommitSnapshot, txn)
    } else {
      converter.enqueueSnapshotForConversion(postCommitSnapshot, txn)
    }
  }
}
