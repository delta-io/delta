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

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.AddFile

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.Filter

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
