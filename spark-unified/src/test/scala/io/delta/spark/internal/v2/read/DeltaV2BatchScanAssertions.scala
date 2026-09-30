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

package io.delta.spark.internal.v2.read

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.{FileSourceScanLike, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

private[read] trait DeltaV2BatchScanAssertions {
  self: AdaptiveSparkPlanHelper =>

  private def deltaV2BatchScans(plan: SparkPlan): Seq[DeltaV2Scan] = collect(plan) {
    case scan: BatchScanExec if scan.scan.isInstanceOf[DeltaV2Scan] =>
      scan.scan.asInstanceOf[DeltaV2Scan]
  }

  protected final def assertDeltaV2BatchScan(df: DataFrame, context: String): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(
      deltaV2BatchScans(plan).nonEmpty,
      s"expected a Delta V2 BatchScan for $context, got:\n$plan")
    assert(
      collect(plan) { case _: FileSourceScanLike => true }.isEmpty,
      s"expected $context to stay off file-source execution, got:\n$plan")
  }

  protected final def assertDeltaV2BatchScanCount(
      df: DataFrame,
      expected: Int,
      context: String): Unit = {
    val plan = df.queryExecution.executedPlan
    val actual = deltaV2BatchScans(plan).size
    assert(
      actual == expected,
      s"expected $expected Delta V2 BatchScans for $context, got $actual:\n$plan")
    assert(
      collect(plan) { case _: FileSourceScanLike => true }.isEmpty,
      s"expected $context to stay off file-source execution, got:\n$plan")
  }

  protected final def deltaV2BatchScanReadFieldNames(
      df: DataFrame,
      expectedScanCount: Int,
      context: String): Seq[Seq[String]] = {
    assertDeltaV2BatchScanCount(df, expectedScanCount, context)
    deltaV2BatchScans(df.queryExecution.executedPlan)
      .map(_.getReadDataSchema.fieldNames.toSeq)
  }

  protected final def assertDeltaV2BatchScanPushedLimit(
      df: DataFrame,
      expected: Int,
      context: String): Unit = {
    assertDeltaV2BatchScan(df, context)
    val plan = df.queryExecution.executedPlan
    val scans = deltaV2BatchScans(plan)
    val marker = s"PushedLimit: $expected"
    assert(
      scans.nonEmpty && scans.forall(_.description().contains(marker)),
      s"expected every Delta V2 BatchScan for $context to have $marker, " +
        s"got ${scans.map(_.description()).mkString("[", ", ", "]")}:\n$plan")
  }

  protected final def assertDeltaV2BatchScanHasNoPushedLimit(
      df: DataFrame,
      context: String): Unit = {
    assertDeltaV2BatchScan(df, context)
    val plan = df.queryExecution.executedPlan
    val scans = deltaV2BatchScans(plan)
    assert(
      scans.nonEmpty && scans.forall(scan => !scan.description().contains("PushedLimit")),
      s"expected no Delta V2 BatchScan for $context to have PushedLimit, " +
        s"got ${scans.map(_.description()).mkString("[", ", ", "]")}:\n$plan")
  }
}
