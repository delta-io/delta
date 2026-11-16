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

package org.apache.spark.sql.delta.amt

import org.apache.spark.sql.delta.{AdaptiveMetadataPreviewTableFeature, AdaptiveMetadataTableFeature, DeltaLog, TableFeature}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaStatistics.{TIGHT_BOUNDS, TIGHT_STATS}
import org.apache.spark.sql.delta.util.JsonUtils
import com.fasterxml.jackson.databind.node.ObjectNode

import org.apache.spark.SparkConf

/**
 * Tests for the `tightStats` stats flag, which writers of tables with the `adaptiveMetadata` AMT
 * feature emit instead of `tightBounds`.
 */
class AMTTightStatsSuite extends AMTCheckpointTestBase {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.V4_ADAPTIVE_METADATA_TABLE_ENABLED.key, "true")

  private val testTable = "amt_tight_stats"

  /**
   * Runs `body` on an AMT table with `amtFeature` whose only data file holds three rows, with
   * `n = 7` in the first and `n = NULL` in the other two.
   */
  private def withNullsTable(
      amtFeature: TableFeature = AdaptiveMetadataTableFeature)(body: DeltaLog => Unit): Unit =
    withTable(testTable) {
      // A large checkpoint interval keeps the file's stats in the delta log, unconverted by AMT.
      createAMTTable(testTable, checkpointInterval = 100, tableSchema = "id INT, n INT",
        amtFeature = amtFeature)
      appendRowsAsSeparateFiles(testTable, numFiles = 1, rowsPerFile = 3,
        columnExprs = Seq("CAST(id AS INT)", "IF(id = 0, 7, NULL)"))
      body(deltaLogForName(testTable))
    }

  private def onlyFile(deltaLog: DeltaLog): AddFile = {
    val files = liveAddFiles(deltaLog.update())
    assert(files.size == 1, s"expected a single file, got $files")
    files.head
  }

  private def parseStats(add: AddFile): ObjectNode =
    JsonUtils.mapper.readTree(add.stats).asInstanceOf[ObjectNode]

  test("writers of adaptiveMetadata tables collect tightStats stats instead of tightBounds") {
    withNullsTable() { deltaLog =>
      val stats = parseStats(onlyFile(deltaLog))
      assert(stats.get(TIGHT_STATS).asBoolean(false), stats)
      assert(!stats.has(TIGHT_BOUNDS), stats)
    }
  }

  test("writers of adaptiveMetadata-preview tables collect tightBounds stats") {
    withNullsTable(amtFeature = AdaptiveMetadataPreviewTableFeature) { deltaLog =>
      val stats = parseStats(onlyFile(deltaLog))
      assert(stats.get(TIGHT_BOUNDS).asBoolean(false), stats)
      assert(!stats.has(TIGHT_STATS), stats)
    }
  }

  test("writers of tables without an AMT feature collect tightBounds stats") {
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (id INT) USING DELTA " +
        "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
      sql(s"INSERT INTO $testTable VALUES (1)")
      val stats = parseStats(onlyFile(deltaLogForName(testTable)))
      assert(stats.get(TIGHT_BOUNDS).asBoolean(false), stats)
      assert(!stats.has(TIGHT_STATS), stats)
    }
  }
}
