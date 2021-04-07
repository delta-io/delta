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

package org.apache.spark.sql.delta.util

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

trait DeltaProgressReporter extends Logging {
  /**
   * Report a log to indicate some command is running.
   */
  def withStatusCode[T](
      statusCode: String,
      defaultMessage: String,
      data: Map[String, Any] = Map.empty)(body: => T): T = {
    logInfo(s"$statusCode: $defaultMessage")
    val t = withJobDescription(defaultMessage)(body)
    logInfo(s"$statusCode: Done")
    t
  }
  /**
   * Wrap various delta operations to provide a more meaningful name in Spark UI
   * This only has an effect if {{{body}}} actually runs a Spark job
   * @param jobDesc a short description of the operation
   */
  private def withJobDescription[U](jobDesc: String)(body: => U): U = {
    val sc = SparkSession.active.sparkContext
    // will prefix jobDesc with whatever the user specified in the job description
    // of the higher level operation that triggered this delta operation
    val oldDesc = sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION)
    val suffix = if (oldDesc == null) {
      ""
    } else {
      s" $oldDesc:"
    }
    try {
      sc.setJobDescription(s"Delta:$suffix $jobDesc")
      body
    } finally {
      sc.setJobDescription(oldDesc)
    }
  }
}
