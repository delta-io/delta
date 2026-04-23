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

package org.apache.spark.sql.delta.files

import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.delta.shims.VariantStatsHookShims
import org.apache.spark.sql.execution.datasources.{DynamicPartitionDataConcurrentWriter, DynamicPartitionDataSingleWriter, EmptyDirectoryDataWriter, FileFormatDataWriter, FileFormatWriter, SingleDirectoryDataWriter, WriteJobDescription}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf

/**
 * Mixes in a `releaseCurrentWriter` override that, after closing the current output writer and
 * before `closeFile` on stats trackers, invokes [[VariantStatsHookShims]] to extract variant
 * data-skipping stats from the parquet footer and inject them into any Delta stats tracker.
 *
 * On Spark 4.2 the shim does the real extraction; on older Spark versions it is a no-op, so the
 * override is behaviorally identical to the super-class on those versions.
 */
trait VariantStatsReleaseCurrentWriter { self: FileFormatDataWriter =>
  override protected def releaseCurrentWriter(): Unit = {
    if (currentWriter != null) {
      try {
        currentWriter.close()
        val conf = SQLConf.get
        VariantStatsHookShims.extractAndInjectVariantStats(
          currentWriter,
          statsTrackers,
          parquetRebaseModeInRead = conf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_READ).toString)
        statsTrackers.foreach(_.closeFile(currentWriter.path()))
      } finally {
        currentWriter = null
      }
    }
  }
}

class DeltaEmptyDirectoryDataWriter(
    description: WriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol,
    customMetrics: Map[String, SQLMetric] = Map.empty)
  extends EmptyDirectoryDataWriter(description, taskAttemptContext, committer, customMetrics)
  with VariantStatsReleaseCurrentWriter

class DeltaSingleDirectoryDataWriter(
    description: WriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol,
    customMetrics: Map[String, SQLMetric] = Map.empty)
  extends SingleDirectoryDataWriter(description, taskAttemptContext, committer, customMetrics)
  with VariantStatsReleaseCurrentWriter

class DeltaDynamicPartitionDataSingleWriter(
    description: WriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol,
    customMetrics: Map[String, SQLMetric] = Map.empty)
  extends DynamicPartitionDataSingleWriter(
    description, taskAttemptContext, committer, customMetrics)
  with VariantStatsReleaseCurrentWriter

class DeltaDynamicPartitionDataConcurrentWriter(
    description: WriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol,
    concurrentOutputWriterSpec: FileFormatWriter.ConcurrentOutputWriterSpec,
    customMetrics: Map[String, SQLMetric] = Map.empty)
  extends DynamicPartitionDataConcurrentWriter(
    description, taskAttemptContext, committer, concurrentOutputWriterSpec, customMetrics)
  with VariantStatsReleaseCurrentWriter
