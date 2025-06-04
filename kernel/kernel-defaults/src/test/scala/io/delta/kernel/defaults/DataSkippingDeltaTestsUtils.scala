/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults

import scala.collection.immutable.Seq

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.AddFile

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, PredicateHelper}

/**
 * Encapsulates a few Delta-Spark DataSkipping Utils.
 * E.g A helper to get files in a deltaScan after applying a predicate.
 */
trait DataSkippingDeltaTestsUtils extends PredicateHelper {

  /** Parses a predicate string into Spark expressions by analyzing the optimized query plan. */
  def parse(spark: SparkSession, deltaLog: DeltaLog, predicate: String): Seq[Expression] = {
    if (predicate == "True") {
      Seq(org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral)
    } else {
      val filtered = spark.read.format("delta").load(deltaLog.dataPath.toString).where(predicate)
      filtered
        .queryExecution
        .optimizedPlan
        .expressions
        .flatMap(splitConjunctivePredicates)
        .toList
    }
  }

  /**
   * Returns the number of files that would be read when applying
   * the given predicate (for data skipping validation).
   */
  def filesReadCount(
      spark: SparkSession,
      deltaLog: DeltaLog,
      predicate: String): Int = {
    getFilesRead(spark, deltaLog, predicate, checkEmptyUnusedFilters = false).size
  }

  /**
   * Returns the files that should be included in a scan after applying the given predicate on
   * a snapshot of the Delta log.
   *
   * @param deltaLog                Delta log for a table.
   * @param predicate               Predicate to run on the Delta table.
   * @param checkEmptyUnusedFilters If true, check if there were no unused filters, meaning
   *                                the given predicate was used as data or partition filters.
   * @return The files that should be included in a scan after applying the predicate.
   */
  def getFilesRead(
      spark: SparkSession,
      deltaLog: DeltaLog,
      predicate: String,
      checkEmptyUnusedFilters: Boolean): Seq[AddFile] = {
    val parsed: Seq[Expression] = parse(spark, deltaLog, predicate)
    val res = deltaLog.snapshot.filesForScan(parsed)
    assert(res.total.files.get == deltaLog.snapshot.numOfFiles)
    assert(res.total.bytesCompressed.get == deltaLog.snapshot.sizeInBytes)
    assert(res.scanned.files.get == res.files.size)
    assert(res.scanned.bytesCompressed.get == res.files.map(_.size).sum)
    assert(!checkEmptyUnusedFilters || res.unusedFilters.isEmpty)
    res.files.toList
  }
}
