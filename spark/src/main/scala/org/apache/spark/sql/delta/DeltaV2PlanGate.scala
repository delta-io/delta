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

package org.apache.spark.sql.delta

import scala.jdk.OptionConverters._

import org.apache.spark.sql.delta.catalog.DeltaTableV2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Single call-site helper for "should this code path use the new file-planning V2 path or the
 * legacy analysis-time V2-to-V1 fallback?". Centralizes the predicate so DeltaTableV2,
 * DeltaScanBuilder, DeltaWriteBuilder, the new planner strategies, DeltaAnalysis, and
 * FallbackToV1Relations all read the same gate.
 *
 * Backed by [[DeltaV2Mode]].
 */
object DeltaV2PlanGate {

  /** True if batch reads against `deltaTable` should produce a `FileScan`-based V2 plan. */
  def shouldUseFilePlanningForBatchReads(
      spark: SparkSession,
      deltaTable: DeltaTableV2): Boolean = {
    shouldUseFilePlanningForBatchReads(spark, deltaTable.catalogTable)
  }

  def shouldUseFilePlanningForBatchReads(
      spark: SparkSession,
      catalogTable: Option[CatalogTable]): Boolean = {
    new DeltaV2Mode(spark.sessionState.conf)
      .isFilePlanningEnabledForBatchReads(catalogTable.toJava)
  }

  /** True if batch writes against `deltaTable` should produce a `FileWrite`-based V2 plan. */
  def shouldUseFilePlanningForBatchWrites(
      spark: SparkSession,
      deltaTable: DeltaTableV2): Boolean = {
    shouldUseFilePlanningForBatchWrites(spark, deltaTable.catalogTable)
  }

  def shouldUseFilePlanningForBatchWrites(
      spark: SparkSession,
      catalogTable: Option[CatalogTable]): Boolean = {
    new DeltaV2Mode(spark.sessionState.conf)
      .isFilePlanningEnabledForBatchWrites(catalogTable.toJava)
  }

  /** True if streaming writes against `deltaTable` should produce a V2 streaming write plan. */
  def shouldUseFilePlanningForStreamingWrites(
      spark: SparkSession,
      deltaTable: DeltaTableV2): Boolean = {
    shouldUseFilePlanningForStreamingWrites(spark, deltaTable.catalogTable)
  }

  def shouldUseFilePlanningForStreamingWrites(
      spark: SparkSession,
      catalogTable: Option[CatalogTable]): Boolean = {
    new DeltaV2Mode(spark.sessionState.conf)
      .isFilePlanningEnabledForStreamingWrites(catalogTable.toJava)
  }
}
