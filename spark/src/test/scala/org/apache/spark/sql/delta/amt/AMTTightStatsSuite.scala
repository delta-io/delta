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

import org.apache.spark.sql.delta.{AdaptiveMetadataPreviewTableFeature, AdaptiveMetadataTableFeature, DeltaColumnMapping, DeltaLog, DeltaOperations, TableFeature}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaStatistics.{MAX, MIN, NULL_COUNT, NUM_RECORDS, TIGHT_BOUNDS, TIGHT_STATS}
import org.apache.spark.sql.delta.util.JsonUtils
import com.fasterxml.jackson.databind.node.ObjectNode

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.functions.{from_json, lit}
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StructType}

/**
 * Tests for the `tightStats` stats flag, which writers of tables with the `adaptiveMetadata` AMT
 * feature emit instead of `tightBounds`.
 */
class AMTTightStatsSuite extends AMTCheckpointTestBase {

  // TODO(v4amt): test tables that gain or lose `adaptiveMetadata` once upgrades and downgrades are
  //   supported. Enabling the feature keeps the legacy stats of existing files. Dropping it must
  //   first give files with only `tightStats` a `tightBounds`, since the table's readers would then
  //   ignore `tightStats` and read those files as tight.

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.V4_ADAPTIVE_METADATA_TABLE_ENABLED.key, "true")
    // The DELETE cases need DELETE to keep the file and add a deletion vector, not rewrite it.
    .set(DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key, "true")
    // Writers and stats recomputations collect a tightness flag unless this conf disables it.
    .set(DeltaSQLConf.TIGHT_BOUND_COLUMN_ON_FILE_INIT_DISABLED.key, "false")

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

  /**
   * Rewrites the stats of the live files of the table of `deltaLog` to legacy stats, which have
   * `tightBounds` instead of `tightStats`, like the stats of files written before the table's
   * writers collected `tightStats`.
   */
  private def commitLegacyStats(deltaLog: DeltaLog): Unit = {
    val legacyFiles = liveAddFiles(deltaLog.update()).map { add =>
      val stats = parseStats(add)
      stats.put(TIGHT_BOUNDS, stats.remove(TIGHT_STATS).asBoolean())
      add.copy(stats = JsonUtils.mapper.writeValueAsString(stats), dataChange = false)
    }
    deltaLog.startTransaction().commit(legacyFiles, DeltaOperations.ManualUpdate)
  }

  test("DELETE with deletion vectors widens a tightStats file") {
    withNullsTable() { deltaLog =>
      sql(s"DELETE FROM $testTable WHERE id = 0")

      val widened = onlyFile(deltaLog)
      assert(widened.deletionVector != null)
      val stats = parseStats(widened)
      assert(!stats.get(TIGHT_STATS).asBoolean(true), stats)
      assert(!stats.has(TIGHT_BOUNDS), stats)
      assert(widened.tightBounds.contains(false))
    }
  }

  test("DELETE with deletion vectors widens a tightBounds file of an adaptiveMetadata table") {
    withNullsTable() { deltaLog =>
      commitLegacyStats(deltaLog)
      sql(s"DELETE FROM $testTable WHERE id = 0")

      // The rewrite keeps the file's legacy flag: legacy stats don't gain `tightStats`.
      val stats = parseStats(onlyFile(deltaLog))
      assert(!stats.get(TIGHT_BOUNDS).asBoolean(true), stats)
      assert(!stats.has(TIGHT_STATS), stats)
    }
  }

  private def fileWithStats(stats: String): AddFile =
    AddFile(
      "file.parquet", Map.empty, size = 1L, modificationTime = 1L, dataChange = true, stats = stats)

  test("tightBounds falls back to tightStats when absent") {
    Seq(
      """{"numRecords":1}""" -> None,
      """{"numRecords":1,"tightBounds":false}""" -> Some(false),
      """{"numRecords":1,"tightStats":true}""" -> Some(true),
      """{"numRecords":1,"tightStats":false}""" -> Some(false),
      """{"numRecords":1,"tightBounds":true,"tightStats":false}""" -> Some(true)
    ).foreach { case (stats, expected) =>
      assert(fileWithStats(stats).tightBounds == expected, stats)
    }
  }

  // Production widens with the legacy branch of `withoutTightBoundStats` by default, and tests
  // with the precision-preserving one, so cover both.
  for (preservePrecision <- BOOLEAN_DOMAIN) {
    test("withoutTightBoundStats sets the flags a file has to false, " +
        s"preservePrecision = $preservePrecision") {
      withConf(DeltaSQLConf.DELTA_STATS_WIDEN_BOUNDS_PRESERVE_NUMERIC_PRECISION_ENABLED.key ->
          preservePrecision.toString) {
        // The two branches write the flags in different orders, so compare parsed stats.
        Seq(
          """{"numRecords":1}""" -> """{"numRecords":1,"tightBounds":false}""",
          """{"numRecords":1,"tightBounds":true}""" -> """{"numRecords":1,"tightBounds":false}""",
          """{"numRecords":1,"tightStats":true}""" -> """{"numRecords":1,"tightStats":false}""",
          """{"numRecords":1,"tightBounds":true,"tightStats":true}""" ->
            """{"numRecords":1,"tightBounds":false,"tightStats":false}""",
          """{"numRecords":1,"tightStats":true,"tightBounds":true}""" ->
            """{"numRecords":1,"tightBounds":false,"tightStats":false}""",
          // `tightBounds` takes priority, so this file is tight.
          """{"numRecords":1,"tightStats":false,"tightBounds":true}""" ->
            """{"numRecords":1,"tightBounds":false,"tightStats":false}""",
          // A null flag counts as absent.
          """{"numRecords":1,"tightStats":null}""" ->
            """{"numRecords":1,"tightBounds":false,"tightStats":null}""",
          """{"numRecords":1,"tightBounds":null,"tightStats":true}""" ->
            """{"numRecords":1,"tightBounds":null,"tightStats":false}""",
          // Only the top-level flags change, not the stats of a column named `tightStats`.
          """{"numRecords":1,"minValues":{"tightStats":1},"tightStats":true}""" ->
            """{"numRecords":1,"minValues":{"tightStats":1},"tightStats":false}"""
        ).foreach { case (stats, expected) =>
          val widened = fileWithStats(stats).withoutTightBoundStats.stats
          assert(JsonUtils.mapper.readTree(widened) == JsonUtils.mapper.readTree(expected),
            s"$stats -> $widened")
        }
        Seq(
          """{"numRecords":1,"tightBounds":false}""",
          """{"numRecords":1,"tightStats":false}""",
          """{"numRecords":1,"tightBounds":false,"tightStats":false}"""
        ).foreach { stats =>
          val file = fileWithStats(stats)
          assert(file.withoutTightBoundStats eq file, stats)
        }
        if (preservePrecision) {
          val decimal = "1.2345678901234567890123"
          val widened = fileWithStats(
            s"""{"numRecords":1,"minValues":{"d":$decimal},"tightStats":true}""")
            .withoutTightBoundStats.stats
          assert(widened ==
            s"""{"numRecords":1,"minValues":{"d":$decimal},"tightStats":false}""")
        }
      }
    }
  }

  test("commits reject stats with tightStats = true and tightBounds = false in tests") {
    withNullsTable() { deltaLog =>
      val add = onlyFile(deltaLog)
      val stats = parseStats(add)
      stats.put(TIGHT_BOUNDS, false)
      val contradictory =
        add.copy(stats = JsonUtils.mapper.writeValueAsString(stats), dataChange = false)
      val e = intercept[SparkException] {
        deltaLog.startTransaction().commit(Seq(contradictory), DeltaOperations.ManualUpdate)
      }
      checkError(e, condition = "INTERNAL_ERROR", parameters = Map("message" ->
        (s"AddFile '${contradictory.path}' has tightStats = true but tightBounds = false: a " +
          "rewrite that widened its stats must set tightStats to false too: " +
          contradictory.stats)))
    }
  }

  test("commits reject stats with tightStats on tables without adaptiveMetadata in tests") {
    def check(deltaLog: DeltaLog): Unit = {
      val add = onlyFile(deltaLog)
      val stats = parseStats(add)
      stats.remove(TIGHT_BOUNDS)
      stats.put(TIGHT_STATS, true)
      val withTightStats =
        add.copy(stats = JsonUtils.mapper.writeValueAsString(stats), dataChange = false)
      val e = intercept[SparkException] {
        deltaLog.startTransaction().commit(Seq(withTightStats), DeltaOperations.ManualUpdate)
      }
      checkError(e, condition = "INTERNAL_ERROR", parameters = Map("message" ->
        (s"AddFile '${withTightStats.path}' has tightStats, but its table doesn't support " +
          s"${AdaptiveMetadataTableFeature.name}: ${withTightStats.stats}")))
    }
    withNullsTable(amtFeature = AdaptiveMetadataPreviewTableFeature)(check)
    // A new name, since the dropped AMT table's cached log would otherwise serve this one.
    val nonAMTTable = "non_amt_tight_stats"
    withTable(nonAMTTable) {
      sql(s"CREATE TABLE $nonAMTTable (id INT) USING DELTA")
      sql(s"INSERT INTO $nonAMTTable VALUES (1)")
      check(deltaLogForName(nonAMTTable))
    }
  }

  for (legacyStats <- BOOLEAN_DOMAIN) {
    testAcrossAMTCheckpointScenarios(
        s"AMT checkpoints keep tightStats and drop tightBounds (legacy stats = $legacyStats)",
        s"amt_tightness_round_trip_$legacyStats",
        tableSchema = "id INT, n INT",
        amtFeature = AdaptiveMetadataTableFeature)(
        setup = { name =>
          // Two files, so that the DELETE leaves a deletion vector on the first one only.
          appendRowsAsSeparateFiles(name, numFiles = 2, rowsPerFile = 3,
            columnExprs = Seq("CAST(id AS INT)", "IF(id = 0, 7, NULL)"))
          if (legacyStats) commitLegacyStats(deltaLogForName(name))
        },
        inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
          s"DELETE FROM $name WHERE id = 0"))) { context =>
      // The DELETE widens the stats of a file, which the checkpoint must keep exactly.
      assertLiveAddFilesRoundTrip(context)
      val files = liveAddFiles(context.postCheckpointSnapshot)
      assert(files.size == 2 && files.count(_.deletionVector != null) == 1, files)
      files.foreach { file =>
        val stats = parseStats(file)
        if (legacyStats) {
          // AMT drops `tightBounds`, so a legacy file reads back not tight.
          assert(stats.has(TIGHT_BOUNDS) && !stats.get(TIGHT_BOUNDS).asBoolean(), file)
          assert(!stats.has(TIGHT_STATS), file)
        } else {
          assert(stats.has(TIGHT_STATS), file)
          assert(stats.get(TIGHT_STATS).asBoolean() == (file.deletionVector == null), file)
          assert(!stats.has(TIGHT_BOUNDS), file)
        }
      }
    }
  }
}
