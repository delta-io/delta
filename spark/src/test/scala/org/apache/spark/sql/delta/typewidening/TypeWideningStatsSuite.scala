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

package org.apache.spark.sql.delta.typewidening

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types._

/**
 * Suite covering stats and data skipping with type changes.
 */
class TypeWideningStatsSuite
  extends QueryTest
    with TypeWideningTestMixin
    with TypeWideningStatsTests

trait TypeWideningStatsTests { self: QueryTest with TypeWideningTestMixin =>

  import testImplicits._

  /**
   * Helper to create a table and run tests while enabling/disabling storing stats as JSON string or
   * strongly-typed structs in checkpoint files. Creates a
   */
  def testStats(
      name: String,
      partitioned: Boolean,
      jsonStatsEnabled: Boolean,
      structStatsEnabled: Boolean)(
      body: => Unit): Unit =
    test(s"$name, partitioned=$partitioned, jsonStatsEnabled=$jsonStatsEnabled, " +
        s"structStatsEnabled=$structStatsEnabled") {
      withSQLConf(
        DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_JSON.defaultTablePropertyKey ->
          jsonStatsEnabled.toString,
        DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_STRUCT.defaultTablePropertyKey ->
          structStatsEnabled.toString
      ) {
        val partitionStr = if (partitioned) "PARTITIONED BY (a)" else ""
        sql(s"""
            |CREATE TABLE delta.`$tempPath` (a smallint, dummy int DEFAULT 1)
            |USING DELTA
            |$partitionStr
            |TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
          """.stripMargin)
        body
      }
    }

  /** Returns the latest checkpoint for the test table. */
  def getLatestCheckpoint: LastCheckpointInfo =
    deltaLog.readLastCheckpointFile().getOrElse {
      fail("Expected the table to have a checkpoint but it didn't")
    }

  /** Returns the type used to store JSON stats in the checkpoint if JSON stats are present. */
  def getJsonStatsType(checkpoint: LastCheckpointInfo): Option[DataType] =
    checkpoint.checkpointSchema.flatMap {
      _.findNestedField(Seq("add", "stats"))
    }.map(_._2.dataType)

  /**
   * Returns the type used to store parsed partition values for the given column in the checkpoint
   * if these are present.
   */
  def getPartitionValuesType(checkpoint: LastCheckpointInfo, colName: String)
    : Option[DataType] = {
    checkpoint.checkpointSchema.flatMap {
      _.findNestedField(Seq("add", "partitionValues_parsed", colName))
    }.map(_._2.dataType)
  }

  /**
   * Checks that stats and parsed partition values are stored in the checkpoint when enabled and
   * that their type matches the expected type.
   */
  def checkCheckpointStats(
      checkpoint: LastCheckpointInfo,
      colName: String,
      colType: DataType,
      partitioned: Boolean,
      jsonStatsEnabled: Boolean,
      structStatsEnabled: Boolean): Unit = {
    val expectedJsonStatsType = if (jsonStatsEnabled) Some(StringType) else None
    assert(getJsonStatsType(checkpoint) === expectedJsonStatsType)

    val expectedPartitionStats = if (partitioned && structStatsEnabled) Some(colType) else None
    assert(getPartitionValuesType(checkpoint, colName) === expectedPartitionStats)
  }

  /**
   * Reads the test table filtered by the given value and checks that files are skipped as expected.
   */
  def checkFileSkipping(filterValue: Any, expectedFilesRead: Long): Unit = {
    val dataFilter: Expression =
      EqualTo(AttributeReference("a", IntegerType)(), Literal(filterValue))
    val files = deltaLog.update().filesForScan(Seq(dataFilter), keepNumRecords = false).files
    assert(files.size === expectedFilesRead, s"Expected $expectedFilesRead files to be " +
      s"read but read ${files.size} files.")
  }

  for {
    partitioned <- BOOLEAN_DOMAIN
    jsonStatsEnabled <- BOOLEAN_DOMAIN
    structStatsEnabled <- BOOLEAN_DOMAIN
  }
  testStats(s"data skipping after type change", partitioned, jsonStatsEnabled, structStatsEnabled) {
    addSingleFile(Seq(1), ShortType)
    addSingleFile(Seq(2), ShortType)
    deltaLog.checkpoint()
    assert(readDeltaTable(tempPath).schema("a").dataType === ShortType)
    val initialCheckpoint = getLatestCheckpoint
    checkCheckpointStats(
      initialCheckpoint, "a", ShortType, partitioned, jsonStatsEnabled, structStatsEnabled)

    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE INT")
    addSingleFile(Seq(Int.MinValue), IntegerType)

    var checkpoint = getLatestCheckpoint
    // Ensure there's no new checkpoint after the type change.
    assert(getLatestCheckpoint.semanticEquals(initialCheckpoint))

    val canSkipFiles = jsonStatsEnabled || partitioned

    // The last file added isn't part of the checkpoint, it always has stats that can be used for
    // skipping even when checkpoint stats can't be used for skipping.
    checkFileSkipping(filterValue = 1, expectedFilesRead = if (canSkipFiles) 1 else 2)
    checkAnswer(readDeltaTable(tempPath).filter("a = 1"), Row(1, 1))

    checkFileSkipping(filterValue = Int.MinValue, expectedFilesRead = if (canSkipFiles) 1 else 3)
    checkAnswer(readDeltaTable(tempPath).filter(s"a = ${Int.MinValue}"), Row(Int.MinValue, 1))

    // Trigger a new checkpoint after the type change and re-check data skipping.
    deltaLog.checkpoint()
    checkpoint = getLatestCheckpoint
    assert(!checkpoint.semanticEquals(initialCheckpoint))
    checkCheckpointStats(
      checkpoint, "a", IntegerType, partitioned, jsonStatsEnabled, structStatsEnabled)
    // When checkpoint stats are completely disabled, the last file added can't be skipped anymore.
    checkFileSkipping(filterValue = 1, expectedFilesRead = if (canSkipFiles) 1 else 3)
    checkFileSkipping(filterValue = Int.MinValue, expectedFilesRead = if (canSkipFiles) 1 else 3)
  }

  for {
    partitioned <- BOOLEAN_DOMAIN
    jsonStatsEnabled <- BOOLEAN_DOMAIN
    structStatsEnabled <- BOOLEAN_DOMAIN
  }
  testStats(s"metadata-only query", partitioned, jsonStatsEnabled, structStatsEnabled) {
    addSingleFile(Seq(1), ShortType)
    addSingleFile(Seq(2), ShortType)
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE INT")
    addSingleFile(Seq(Int.MinValue), IntegerType)
    addSingleFile(Seq(Int.MaxValue), IntegerType)

    // Check that collecting aggregates using a metadata-only query works after the type change.
    val resultDf = sql(s"SELECT MIN(a), MAX(a), COUNT(*) FROM delta.`$tempPath`")
    val isMetadataOnly = resultDf.queryExecution.optimizedPlan.collectFirst {
      case l: LocalRelation => l
    }.nonEmpty
    assert(isMetadataOnly, "Expected the query to be metadata-only")
    checkAnswer(resultDf, Row(Int.MinValue, Int.MaxValue, 4))
  }
}
