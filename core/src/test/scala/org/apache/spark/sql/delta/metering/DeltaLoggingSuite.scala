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

package org.apache.spark.sql.delta.metering

import com.databricks.spark.util.{DatabricksLogging, MetricDefinition, OpType, TagDefinition}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}

class DeltaLoggingSuite
    extends QueryTest
    with SharedSparkSession
    with SQLTestUtils
    with DeltaSQLCommandTest {

  test("Configure a custom logger") {
    withTempDir { inputDir =>
      val writer = spark.range(25).write.format("delta").mode("overwrite")
      val path = inputDir.getCanonicalPath

      // First run won't invoke the custom logger.
      writer.save(path)

      // Set up the custom logger.
      spark.conf.set(DeltaSQLConf.DELTA_TELEMETRY_LOGGER.key, classOf[CustomLogger].getName)

      // Second run will invoke the custom logger, therefore throw the custom exception.
      assertThrows[LoggerCalledException](writer.save(path))
    }
  }
}

/**
 * Custom exception type to validate that our logger was invoked.
 */
private class LoggerCalledException extends Exception

/**
 * Custom logger implementation that just throws an exception so that we an validate that it was
 * invoked.
 */
private class CustomLogger extends DatabricksLogging {

  override def recordOperation[S](
      opType: OpType,
      opTarget: String,
      extraTags: Map[TagDefinition, String],
      isSynchronous: Boolean,
      alwaysRecordStats: Boolean,
      allowAuthTags: Boolean,
      killJvmIfStuck: Boolean,
      outputMetric: MetricDefinition,
      silent: Boolean)(thunk: => S): S = {
    throw new LoggerCalledException
  }
}
