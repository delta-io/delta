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

package org.apache.spark.sql.delta.v3

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.write.file.{FileOutputWriter, WriteResult}
import org.apache.spark.sql.delta.{DataFrameUtils, DeltaErrors, DeltaOptions}
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.{LogicalRDD, SparkPlan}

/**
 * Driver-side commit driver for [[DeltaFileWriteV3]]. Bridges the v3 write exec node to
 * Delta's existing `WriteIntoDelta` path: opens the transaction, runs the write, commits.
 *
 * Initial implementation materializes the input `SparkPlan` through a `LogicalRDD` +
 * `DataFrameUtils.ofRows` so we can call `WriteIntoDelta.run(session)` unchanged. This keeps
 * v1 and v3 byte-equivalent on the wire while we get the plumbing right. A follow-up will
 * drive `FileFormatWriter.write` directly against the `SparkPlan` (the path Photon's write
 * replacement needs) and lift constraint/generated-column enforcement out of `WriteIntoDelta`
 * and into the v3 write exec nodes themselves.
 *
 * Note: cache invalidation via `cacheManager.recacheByPlan(...)` requires a plan that matches
 * the shape of analyzed v3 reads (`DataSourceV2Relation(DeltaTableV3, ...)`). That plumbing
 * is deferred to a follow-up; today, callers may see stale cached DataFrames after a v3 write.
 */
class DeltaFileOutputWriterV3(
    deltaTable: DeltaTableV3,
    writeOptions: Map[String, String],
    isOverwrite: Boolean) extends FileOutputWriter {

  override def write(query: SparkPlan, mode: SaveMode): WriteResult = {
    val session = deltaTable.spark
    val deltaOptions = new DeltaOptions(writeOptions, session.sessionState.conf)

    if (deltaOptions.isReplaceOnOrUsingDefined) {
      if (deltaOptions.replaceOn.isDefined && !session.sessionState.conf.getConf(
          DeltaSQLConf.REPLACE_ON_OPTION_IN_DATAFRAME_WRITER_ENABLED)) {
        throw DeltaErrors.operationNotSupportedException("replaceOn")
      } else if (deltaOptions.replaceUsing.isDefined && !session.sessionState.conf.getConf(
          DeltaSQLConf.REPLACE_USING_OPTION_IN_DATAFRAME_WRITER_ENABLED)) {
        throw DeltaErrors.operationNotSupportedException("replaceUsing")
      }
    }

    val logicalRdd = LogicalRDD(query.output, query.execute())(session)
    val data = DataFrameUtils.ofRows(session, logicalRdd)

    val saveMode = if (isOverwrite || mode == SaveMode.Overwrite) {
      SaveMode.Overwrite
    } else {
      SaveMode.Append
    }

    WriteIntoDelta(
      deltaTable.deltaLog,
      saveMode,
      deltaOptions,
      Nil,
      deltaTable.deltaLog.unsafeVolatileSnapshot.metadata.configuration,
      data,
      deltaTable.catalogTable).run(session)

    WriteResult(addedFiles = 0L, removedFiles = 0L, outputRows = 0L)
  }
}
