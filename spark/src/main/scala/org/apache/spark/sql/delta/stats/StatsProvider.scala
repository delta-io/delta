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

import org.apache.spark.sql.Column

/**
 * A helper class that provides the functionalities to create [[DataSkippingPredicate]] with
 * the statistics for a column.
 *
 * @param getStat A function that returns an expression to access the given statistics for a
 *                specific column, or None if that stats column does not exist. For example,
 *                [[DataSkippingReaderBase.getStatsColumnOpt]] can be used here.
 */

private [stats] class StatsProvider(getStat: StatsColumn => Option[Column]) {
  /**
   * Given a [[StatsColumn]], which represents a stats column for a table column, returns a
   * [[DataSkippingPredicate]] which includes a data skipping expression (the result of running
   * `f` on the expression of accessing the given stats) and the stats column (which the data
   * skipping expression depends on), or None if the stats column does not exist.
   *
   * @param statCol A stats column (MIN, MAX, etc) for a table column name.
   * @param f A user-provided function that returns a data skipping expression given the expression
   *          to access the statistics for `statCol`.
   * @return A [[DataSkippingPredicate]] with a data skipping expression, or None if the given
   *         stats column does not exist.
   */
  def getPredicateWithStatsColumn(statCol: StatsColumn)
    (f: Column => Column): Option[DataSkippingPredicate] = {
    for (stat <- getStat(statCol))
      yield DataSkippingPredicate(f(stat), statCol)
  }

  /** A variant of [[getPredicateWithStatsColumn]] with two stats columns. */
  def getPredicateWithStatsColumns(statCol1: StatsColumn, statCol2: StatsColumn)
    (f: (Column, Column) => Column): Option[DataSkippingPredicate] = {
    for (stat1 <- getStat(statCol1); stat2 <- getStat(statCol2))
      yield DataSkippingPredicate(f(stat1, stat2), statCol1, statCol2)
  }

  /** A variant of [[getPredicateWithStatsColumn]] with three stats columns. */
  def getPredicateWithStatsColumns(
      statCol1: StatsColumn,
      statCol2: StatsColumn,
      statCol3: StatsColumn)
    (f: (Column, Column, Column) => Column): Option[DataSkippingPredicate] = {
    for (stat1 <- getStat(statCol1); stat2 <- getStat(statCol2); stat3 <- getStat(statCol3))
      yield DataSkippingPredicate(f(stat1, stat2, stat3), statCol1, statCol2, statCol3)
  }

  /**
   * Given a path to a table column and a stat type (MIN, MAX, etc.), returns a
   * [[DataSkippingPredicate]] which includes a data skipping expression (the result of running
   * `f` on the expression of accessing the given stats) and the stats column (which the data
   * skipping expression depends on), or None if the stats column does not exist.
   *
   * @param pathToColumn The name of a column whose stats are to be accessed.
   * @param statType The type of stats to access (MIN, MAX, etc.)
   * @param f A user-provided function that returns a data skipping expression given the expression
   *          to access the statistics for `statCol`.
   * @return A [[DataSkippingPredicate]] with a data skipping expression, or None if the given
   *         stats column does not exist.
   */
  def getPredicateWithStatType(pathToColumn: Seq[String], statType: String)
    (f: Column => Column): Option[DataSkippingPredicate] = {
    getPredicateWithStatsColumn(StatsColumn(statType, pathToColumn))(f)
  }

  /** A variant of [[getPredicateWithStatType]] with two stat types. */
  def getPredicateWithStatTypes(pathToColumn: Seq[String], statType1: String, statType2: String)
    (f: (Column, Column) => Column): Option[DataSkippingPredicate] = {
    getPredicateWithStatsColumns(
      StatsColumn(statType1, pathToColumn),
      StatsColumn(statType2, pathToColumn))(f)
  }

  /** A variant of [[getPredicateWithStatType]] with three stat types. */
  def getPredicateWithStatTypes(
      pathToColumn: Seq[String],
      statType1: String,
      statType2: String,
      statType3: String)
    (f: (Column, Column, Column) => Column): Option[DataSkippingPredicate] = {
    getPredicateWithStatsColumns(
      StatsColumn(statType1, pathToColumn),
      StatsColumn(statType2, pathToColumn),
      StatsColumn(statType3, pathToColumn))(f)
  }
}
