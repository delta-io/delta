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

import java.util.Arrays

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType

/**
 * A Histogram class tracking the deleted record count distribution for all files in a table.
 * @param deletedRecordCounts An array with 10 bins where each slot represents the number of
 *                            files where the number of deleted records falls within the range
 *                            of the particular bin. The range of each bin is the following:
 *                            bin1  -> [0,0]
 *                            bin2  -> [1,9]
 *                            bin3  -> [10,99]
 *                            bin4  -> [100,999],
 *                            bin5  -> [1000,9999]
 *                            bin6  -> [10000,99999],
 *                            bin7  -> [100000,999999],
 *                            bin8  -> [1000000,9999999],
 *                            bin9  -> [10000000,Int.Max - 1],
 *                            bin10 -> [Int.Max,Long.Max].
 */
case class DeletedRecordCountsHistogram(deletedRecordCounts: Array[Long]) {
  require(deletedRecordCounts.length == DeletedRecordCountsHistogramUtils.NUMBER_OF_BINS,
    s"There should be ${DeletedRecordCountsHistogramUtils.NUMBER_OF_BINS} bins in total")

  override def hashCode(): Int =
    31 * Arrays.hashCode(deletedRecordCounts) + getClass.getCanonicalName.hashCode

  override def equals(that: Any): Boolean = that match {
    case DeletedRecordCountsHistogram(thatDP) =>
      java.util.Arrays.equals(deletedRecordCounts, thatDP)
    case _ => false
  }

  /**
   * Insert a given value into the appropriate histogram bin.
   */
  def insert(numDeletedRecords: Long): Unit = {
    if (numDeletedRecords >= 0) {
      val index = DeletedRecordCountsHistogramUtils.getHistogramBin(numDeletedRecords)
      deletedRecordCounts(index) += 1
    }
  }

  /**
   * Remove a given value from the appropriate histogram bin.
   */
  def remove(numDeletedRecords: Long): Unit = {
    if (numDeletedRecords >= 0) {
      val index = DeletedRecordCountsHistogramUtils.getHistogramBin(numDeletedRecords)
      deletedRecordCounts(index) -= 1
    }
  }
}

private[delta] object DeletedRecordCountsHistogram {
  def apply(deletionPercentages: Array[Long]): DeletedRecordCountsHistogram =
    new DeletedRecordCountsHistogram(deletionPercentages)

  lazy val schema: StructType = ExpressionEncoder[DeletedRecordCountsHistogram]().schema
}
