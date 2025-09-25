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

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf

/**
 * Utilities for configuring Delta Source tests to run with different API versions.
 */
object DeltaSourceTestUtils {

  /**
   * Configure Spark to use Data Source V1 for Delta tables.
   * This uses the legacy DeltaSource implementation.
   */
  def configureDSv1(conf: SparkConf): SparkConf = {
    conf.set(SQLConf.USE_V1_SOURCE_LIST.key, "delta,parquet,json")
  }

  /**
   * Configure Spark to use Data Source V2 for Delta tables.
   * This uses the new SparkMicroBatchStream implementation.
   */
  def configureDSv2(conf: SparkConf): SparkConf = {
    conf.set(SQLConf.USE_V1_SOURCE_LIST.key, "parquet,json")
  }

  /**
   * Check if the current configuration is using DSv2 for Delta.
   */
  def isDSv2Enabled(conf: SparkConf): Boolean = {
    val v1Sources = conf.get(SQLConf.USE_V1_SOURCE_LIST.key, "")
    !v1Sources.split(",").map(_.trim.toLowerCase).contains("delta")
  }

  /**
   * Known DSv2 limitations that tests should handle gracefully.
   * Add to this list as you implement DSv2 features.
   */
  val DSv2_KNOWN_LIMITATIONS = Set(
    "latestOffset is not supported",
    "planInputPartitions is not supported", 
    "createReaderFactory is not supported",
    "commit is not supported",
    "stop is not supported",
    "deserializeOffset is not supported"
  )

  /**
   * Check if an exception represents a known DSv2 limitation.
   */
  def isKnownDSv2Limitation(exception: Throwable): Boolean = {
    val message = exception.getMessage
    DSv2_KNOWN_LIMITATIONS.exists(limitation => 
      message != null && message.contains(limitation)
    )
  }

  /**
   * Common streaming options for testing Delta sources.
   */
  object TestingOptions {
    val SMALL_MAX_FILES_PER_TRIGGER = Map("maxFilesPerTrigger" -> "1")
    val SMALL_MAX_BYTES_PER_TRIGGER = Map("maxBytesPerTrigger" -> "1024")
    val FAIL_ON_DATA_LOSS_FALSE = Map("failOnDataLoss" -> "false")
    val STARTING_VERSION_LATEST = Map("startingVersion" -> "latest")
    val STARTING_VERSION_0 = Map("startingVersion" -> "0")
  }
}

