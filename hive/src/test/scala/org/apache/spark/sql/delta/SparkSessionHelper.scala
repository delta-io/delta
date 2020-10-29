/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import org.apache.spark.sql.SparkSession

object SparkSessionHelper {
  /**
   * Start a special Spark cluster using local mode to process Delta's metadata. The Spark UI has
   * been disabled and `SparkListener`s have been removed to reduce the memory usage of Spark.
   * `DeltaLog` cache size is also set to "1" if the user doesn't specify it, to cache only the
   * recent accessed `DeltaLog`.
   */
  def spark: SparkSession = {
    // TODO Configure `spark` to pick up the right Hadoop configuration.
    if (System.getProperty("delta.log.cacheSize") == null) {
      System.setProperty("delta.log.cacheSize", "1")
    }
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Delta Connector")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    // Trigger codes that add `SparkListener`s before stopping the listener bus. Otherwise, they
    // would fail to add `SparkListener`s.
    sparkSession.sharedState
    sparkSession.sessionState
    sparkSession.sparkContext.listenerBus.stop()
    sparkSession
  }
}
