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

package org.apache.spark.sql.delta

object DeltaTimeTravelSpecShims {

  /**
   * Ensures only a single time travel syntax is used (i.e. not version AND timestamp).
   *
   * Handles another breaking change between Spark 3.5 and 4.0 which added support for
   * DataFrame-based time travel in Spark (https://github.com/apache/spark/pull/43403).
   *
   * TLDR: Starting in Spark 4.0, we end up with two time travel specifications in DeltaTableV2 if
   * options are used to specify the time travel version/timestamp. This breaks an existing check we
   * had (against Spark 3.5) which ensures only one time travel specification is used.
   *
   * The solution to get around this is just to ignore two specs if they are the same. If the user
   * did actually provide two different time travel specs, that would have been caught by Spark
   * earlier.
   *
   * @param currSpecOpt: The table's current [[DeltaTimeTravelSpec]]
   * @param newSpecOpt: The new [[DeltaTimeTravelSpec]] to be applied to the table
   */
  def validateTimeTravelSpec(
      currSpecOpt: Option[DeltaTimeTravelSpec],
      newSpecOpt: Option[DeltaTimeTravelSpec]): Unit = (currSpecOpt, newSpecOpt) match {
    case (Some(currSpec), Some(newSpec)) =>
      if (currSpec.version != newSpec.version || currSpec.timestamp != newSpec.timestamp) {
        throw DeltaErrors.multipleTimeTravelSyntaxUsed
      }
    case _ =>
  }
}
