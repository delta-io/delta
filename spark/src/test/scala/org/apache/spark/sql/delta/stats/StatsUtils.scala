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

import org.apache.spark.sql.delta.DeltaTable

import org.apache.spark.sql.DataFrame

trait StatsUtils {
  protected def getStats(df: DataFrame): DeltaScan = {
    val stats = df.queryExecution.optimizedPlan.collect {
      case DeltaTable(prepared: PreparedDeltaFileIndex) =>
        prepared.preparedScan
    }
    if (stats.size != 1) sys.error(s"Found ${stats.size} scans!")
    stats.head
  }
}
