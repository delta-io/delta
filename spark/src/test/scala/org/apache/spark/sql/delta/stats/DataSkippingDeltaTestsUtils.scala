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

package org.apache.spark.sql.delta.stats

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{AddFile, DeletionVectorDescriptor, Metadata}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.RDDScanExec
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

trait DataSkippingDeltaTestsUtils extends PredicateHelper {
  protected def parse(
      spark: SparkSession, deltaLog: DeltaLog, predicate: String): Seq[Expression] = {

    // We produce a wrong filter in this case otherwise
    if (predicate == "True") return Seq(Literal.TrueLiteral)

    val filtered =
      spark.read.format("delta").load(deltaLog.dataPath.toString).where(predicate)

    val optimizedPlan = filtered.queryExecution.optimizedPlan

    // When pushVariantIntoScan = true, the plan is transformed such that a projection is inserted
    // at the top of the plan. Therefore, the filter node is lower in the plan.
    val filterNode = optimizedPlan.collectFirst {
      case f: Filter => f
    }.getOrElse {
      optimizedPlan
    }
    filterNode
      .expressions
      .flatMap(splitConjunctivePredicates)
  }

  /**
   * Returns the number of files that should be included in a scan after applying the given
   * predicate on a snapshot of the Delta log.
   *
   * @param deltaLog Delta log for a table.
   * @param predicate Predicate to run on the Delta table.
   * @param checkEmptyUnusedFilters If true, check if there were no unused filters, meaning
   *                                the given predicate was used as data or partition filters.
   * @return The number of files that should be included in a scan after applying the predicate.
   */
  protected def filesRead(
      spark: SparkSession,
      deltaLog: DeltaLog,
      predicate: String,
      checkEmptyUnusedFilters: Boolean): Int =
    getFilesRead(spark, deltaLog, predicate, checkEmptyUnusedFilters).size

  /**
   * Returns the files that should be included in a scan after applying the given predicate on
   * a snapshot of the Delta log.
   * @param deltaLog Delta log for a table.
   * @param predicate Predicate to run on the Delta table.
   * @param checkEmptyUnusedFilters If true, check if there were no unused filters, meaning
   *                                the given predicate was used as data or partition filters.
   * @return The files that should be included in a scan after applying the predicate.
   */
  protected def getFilesRead(
      spark: SparkSession,
      deltaLog: DeltaLog,
      predicate: String,
      checkEmptyUnusedFilters: Boolean): Seq[AddFile] = {
    val parsed = parse(spark, deltaLog, predicate)
    val res = deltaLog.snapshot.filesForScan(parsed)
    assert(res.total.files.get == deltaLog.snapshot.numOfFiles)
    assert(res.total.bytesCompressed.get == deltaLog.snapshot.sizeInBytes)
    assert(res.scanned.files.get == res.files.size)
    assert(res.scanned.bytesCompressed.get == res.files.map(_.size).sum)
    assert(!checkEmptyUnusedFilters || res.unusedFilters.isEmpty)
    res.files
  }
}

/**
 * Used to disable the tests with the old stats collection behavior on long-running suites to
 * avoid time-out
 * TODO(lin): remove this after we remove the DELTA_COLLECT_STATS_USING_TABLE_SCHEMA flag
 */
trait DataSkippingDisableOldStatsSchema extends DataSkippingDeltaTestsBase {

  protected override def test(testName: String, testTags: org.scalatest.Tag*)
                             (testFun: => Any)
                             (implicit pos: org.scalactic.source.Position): Unit = {
    // Adding the null check in case tableSchemaOnlyTag has not been initialized in base traits
    val newTestTags = if (tableSchemaOnlyTag == null) testTags else tableSchemaOnlyTag +: testTags
    super.test(testName, newTestTags: _*)(testFun)(pos)
  }
}

/** DataSkipping tests under id column mapping */
trait DataSkippingDeltaIdColumnMapping extends DataSkippingDeltaTestsBase
  with DeltaColumnMappingTestUtils {

  override def expectedStatsForFile(index: Int, colName: String, deltaLog: DeltaLog): String = {
    val x = colName.phy(deltaLog)
    if (deltaLog.unsafeVolatileSnapshot.protocol.isFeatureSupported(DeletionVectorsTableFeature)) {
      s"""{"numRecords":1,"minValues":{"$x":$index},"maxValues":{"$x":$index},""" +
        s""""nullCount":{"$x":0},"tightBounds":true}""".stripMargin
    } else {
      s"""{"numRecords":1,"minValues":{"$x":$index},"maxValues":{"$x":$index},""" +
        s""""nullCount":{"$x":0}}""".stripMargin
    }
  }
}

trait DataSkippingDeltaTestV1ColumnMappingMode extends DataSkippingDeltaIdColumnMapping {
  override protected def getStatsDf(deltaLog: DeltaLog, columns: Column*): DataFrame = {
    deltaLog.snapshot.withStats.select("stats.*")
      .select(convertToPhysicalColumns(columns, deltaLog): _*)
  }
}

/**
 * V1 name-column-mapping additionally runs the full test body (the id-mode variant runs only the
 * selected subset). `runAllTests` is declared on [[DeltaColumnMappingSelectedTestMixin]], which
 * `DeltaColumnMappingEnableNameMode` brings in.
 */
trait DataSkippingDeltaV1NameColumnMappingMode
  extends DataSkippingDeltaTestV1ColumnMappingMode
  with DeltaColumnMappingEnableNameMode {
  override protected def runAllTests: Boolean = true
}

/** Writes V2 checkpoints with a JSON top-level file. */
trait DataSkippingCheckpointV2Json extends DataSkippingDeltaTestsBase {
  override protected def sparkConf: SparkConf = {
    super.sparkConf.setAll(
      Seq(
        DeltaConfigs.CHECKPOINT_POLICY.defaultTablePropertyKey -> CheckpointPolicy.V2.name,
        DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key -> V2Checkpoint.Format.JSON.name
      )
    )
  }
}

/** Writes V2 checkpoints with a Parquet top-level file. */
trait DataSkippingCheckpointV2Parquet extends DataSkippingDeltaTestsBase {
  override protected def sparkConf: SparkConf = {
    super.sparkConf.setAll(
      Seq(
        DeltaConfigs.CHECKPOINT_POLICY.defaultTablePropertyKey -> CheckpointPolicy.V2.name,
        DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key -> V2Checkpoint.Format.PARQUET.name
      )
    )
  }
}
