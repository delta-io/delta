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

import org.apache.spark.sql.delta.Snapshot

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}

/** Trait representing a class that can generate [[DeltaScan]] given filters, etc. */
trait DeltaScanGeneratorBase {
  /** The snapshot that the scan is being generated on. */
  val snapshotToScan: Snapshot

  /**
   * Returns a DataFrame for the given partition filters. The schema of returned DataFrame is nearly
   * the same as `AddFile`, except that the `stats` field is parsed to a struct from a json string.
   */
  def filesWithStatsForScan(partitionFilters: Seq[Expression]): DataFrame

  /** Returns a [[DeltaScan]] based on the given filters and projections. */
  def filesForScan(projection: Seq[Attribute], filters: Seq[Expression]): DeltaScan
}


trait DeltaScanGenerator extends DeltaScanGeneratorBase
