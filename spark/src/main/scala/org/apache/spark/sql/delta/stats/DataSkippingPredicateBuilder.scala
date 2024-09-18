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

import org.apache.spark.sql.delta.stats.DeltaStatistics.{MAX, MIN}

import org.apache.spark.sql.Column
import org.apache.spark.sql.ColumnImplicitsShim._

/**
 * A trait that defines interfaces for a data skipping predicate builder.
 *
 * Note that 'IsNull', 'IsNotNull' and 'StartsWith' are handled at a column (not expression) level
 * within [[DataSkippingReaderBase.DataFiltersBuilder.constructDataFilters]].
 *
 * Note that the 'value' passed in for each of the interface should be [[SkippingEligibleLiteral]].
 */
private [sql] trait DataSkippingPredicateBuilder {
  /** The predicate should match any file which contains the requested point. */
  def equalTo(statsProvider: StatsProvider, colPath: Seq[String], value: Column)
    : Option[DataSkippingPredicate]

  /** The predicate should match any file which contains anything other than the rejected point. */
  def notEqualTo(statsProvider: StatsProvider, colPath: Seq[String], value: Column)
    : Option[DataSkippingPredicate]

  /**
   * The predicate should match any file which contains values less than the requested upper bound.
   */
  def lessThan(statsProvider: StatsProvider, colPath: Seq[String], value: Column)
    : Option[DataSkippingPredicate]

  /**
   * The predicate should match any file which contains values less than or equal to the requested
   * upper bound.
   */
  def lessThanOrEqual(statsProvider: StatsProvider, colPath: Seq[String], value: Column)
    : Option[DataSkippingPredicate]

  /**
   * The predicate should match any file which contains values larger than the requested lower
   * bound.
   */
  def greaterThan(statsProvider: StatsProvider, colPath: Seq[String], value: Column)
    : Option[DataSkippingPredicate]

  /**
   * The predicate should match any file which contains values larger than or equal to the requested
   * lower bound.
   */
  def greaterThanOrEqual(statsProvider: StatsProvider, colPath: Seq[String], value: Column)
    : Option[DataSkippingPredicate]
}

/**
 * A collection of supported data skipping predicate builders.
 */
object DataSkippingPredicateBuilder {
  /** Predicate builder for skipping eligible columns. */
  case object ColumnBuilder extends ColumnPredicateBuilder
}

/**
 * Predicate builder for skipping eligible columns.
 */
private [stats] class ColumnPredicateBuilder extends DataSkippingPredicateBuilder {
  def equalTo(statsProvider: StatsProvider, colPath: Seq[String], value: Column)
    : Option[DataSkippingPredicate] = {
    statsProvider.getPredicateWithStatTypes(colPath, value.expr.dataType, MIN, MAX) { (min, max) =>
      min <= value && value <= max
    }
  }

  def notEqualTo(statsProvider: StatsProvider, colPath: Seq[String], value: Column)
    : Option[DataSkippingPredicate] = {
    statsProvider.getPredicateWithStatTypes(colPath, value.expr.dataType, MIN, MAX) { (min, max) =>
      min < value || value < max
    }
  }

  def lessThan(statsProvider: StatsProvider, colPath: Seq[String], value: Column)
    : Option[DataSkippingPredicate] =
    statsProvider.getPredicateWithStatType(colPath, value.expr.dataType, MIN)(_ < value)

  def lessThanOrEqual(statsProvider: StatsProvider, colPath: Seq[String], value: Column)
    : Option[DataSkippingPredicate] =
    statsProvider.getPredicateWithStatType(colPath, value.expr.dataType, MIN)(_ <= value)

  def greaterThan(statsProvider: StatsProvider, colPath: Seq[String], value: Column)
    : Option[DataSkippingPredicate] =
    statsProvider.getPredicateWithStatType(colPath, value.expr.dataType, MAX)(_ > value)

  def greaterThanOrEqual(statsProvider: StatsProvider, colPath: Seq[String], value: Column)
    : Option[DataSkippingPredicate] =
    statsProvider.getPredicateWithStatType(colPath, value.expr.dataType, MAX)(_ >= value)
}

