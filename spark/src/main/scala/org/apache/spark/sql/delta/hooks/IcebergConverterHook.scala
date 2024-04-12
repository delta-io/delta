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

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{OptimisticTransactionImpl, Snapshot, UniversalFormat}
import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.actions.{Action, Metadata}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf.DELTA_UNIFORM_ICEBERG_SYNC_CONVERT_ENABLED
import org.apache.commons.lang3.exception.ExceptionUtils

import org.apache.spark.sql.SparkSession

/** Write a new Iceberg metadata file at the version committed by the txn, if required. */
object IcebergConverterHook extends PostCommitHook with DeltaLogging {
  override val name: String = "Post-commit Iceberg metadata conversion"

  val ASYNC_ICEBERG_CONVERTER_THREAD_NAME = "async-iceberg-converter"

  override def run(
      spark: SparkSession,
      txn: OptimisticTransactionImpl,
      committedVersion: Long,
      postCommitSnapshot: Snapshot,
      committedActions: Seq[Action]): Unit = {
    // Only convert to Iceberg if the snapshot matches the version committed.
    // This is to skip converting the same actions multiple times - they'll be written out
    // by another commit anyways.
    if (committedVersion != postCommitSnapshot.version ||
        !UniversalFormat.icebergEnabled(postCommitSnapshot.metadata)) {
      return
    }


    val converter = postCommitSnapshot.deltaLog.icebergConverter
    if (spark.sessionState.conf.getConf(DELTA_UNIFORM_ICEBERG_SYNC_CONVERT_ENABLED) ||
         !UniversalFormat.icebergEnabled(txn.snapshot.metadata)) { // UniForm was not enabled
      try {
        converter.convertSnapshot(postCommitSnapshot, txn)
      } catch {
        case NonFatal(e) =>
          logError(s"Error when writing Iceberg metadata synchronously", e)
          recordDeltaEvent(
            txn.deltaLog,
            "delta.iceberg.conversion.sync.error",
            data = Map(
              "exception" -> ExceptionUtils.getMessage(e),
              "stackTrace" -> ExceptionUtils.getStackTrace(e)
            )
          )
          throw e
      }
    } else {
      converter.enqueueSnapshotForConversion(postCommitSnapshot, txn)
    }
  }

  // Always throw when sync Iceberg conversion fails. Async conversion exception
  // is handled in the async thread.
  override def handleError(spark: SparkSession, error: Throwable, version: Long): Unit = {
    throw DeltaErrors.universalFormatConversionFailedException(
      version, "iceberg", ExceptionUtils.getMessage(error))
  }
}
