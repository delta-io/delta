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
import org.apache.spark.sql.functions.col

/**
 * A mixin trait that provides access to the stats fields in the transaction log.
 */
trait ReadsMetadataFields {
  /** Returns a Column that references the stats field data skipping should use */
  def getBaseStatsColumn: Column = col(getBaseStatsColumnName)
  def getBaseStatsColumnName: String = "stats"
}

/**
 * A singleton of the Delta statistics field names.
 */
object DeltaStatistics {
  /* The total number of records in the file. */
  val NUM_RECORDS = "numRecords"
  /* The smallest (possibly truncated) value for a column. */
  val MIN = "minValues"
  /* The largest (possibly truncated) value for a column. */
  val MAX = "maxValues"
  /* The number of null values present for a column. */
  val NULL_COUNT = "nullCount"
  /*
   * Whether the column has tight or wide bounds.
   * This should only be present in tables with Deletion Vectors enabled.
   */
  val TIGHT_BOUNDS = "tightBounds"

  val ALL_STAT_FIELDS = Seq(NUM_RECORDS, MIN, MAX, NULL_COUNT, TIGHT_BOUNDS)
}
