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
 * A mixin trait for all interfaces that would like to use information stored in Delta's transaction
 * log.
 */
trait UsesMetadataFields {
  /* The total number of records in the file. */
  final val NUM_RECORDS = "numRecords"
  /* The smallest (possibly truncated) value for a column. */
  final val MIN = "minValues"
  /* The largest (possibly truncated) value for a column. */
  final val MAX = "maxValues"
  /* The number of null values present for a column. */
  final val NULL_COUNT = "nullCount"
}

/**
 * A mixin trait that provides access to the stats fields in the transaction log.
 */
trait ReadsMetadataFields extends UsesMetadataFields {
  /** Returns a Column that references the stats field data skipping should use */
  def getBaseStatsColumn: Column = col("stats")
}
