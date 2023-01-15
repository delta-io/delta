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

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.functions.{col, from_json}

trait ConvertToDeltaSQLSuiteBase extends ConvertToDeltaSuiteBaseCommons
  with DeltaSQLCommandTest {
  override protected def convertToDelta(
      identifier: String,
      partitionSchema: Option[String] = None, collectStats: Boolean = true): Unit = {
    if (partitionSchema.isEmpty) {
      sql(s"convert to delta $identifier ${collectStatisticsStringOption(collectStats)}")
    } else {
      val stringSchema = partitionSchema.get
      sql(s"convert to delta $identifier ${collectStatisticsStringOption(collectStats)}" +
        s" partitioned by ($stringSchema)")
    }
  }

  // TODO: Move to ConvertToDeltaSuiteBaseCommons when DeltaTable API contains collectStats option
  test("convert with collectStats set to false") {
    withTempDir { dir =>
      withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "true") {

        val tempDir = dir.getCanonicalPath
        writeFiles(tempDir, simpleDF)
        convertToDelta(s"parquet.`$tempDir`", collectStats = false)
        val deltaLog = DeltaLog.forTable(spark, tempDir)
        val history = io.delta.tables.DeltaTable.forPath(tempDir).history()
        checkAnswer(
          spark.read.format("delta").load(tempDir),
          simpleDF
        )
        assert(history.count == 1)
        val statsDf = deltaLog.unsafeVolatileSnapshot.allFiles
          .select(from_json(col("stats"), deltaLog.unsafeVolatileSnapshot.statsSchema)
            .as("stats")).select("stats.*")
        assert(statsDf.filter(col("numRecords").isNotNull).count == 0)
      }
    }
  }

}

class ConvertToDeltaSQLSuite extends ConvertToDeltaSQLSuiteBase
  with ConvertToDeltaSuiteBase
