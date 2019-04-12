/*
 * Copyright 2019 Databricks, Inc.
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

import org.apache.spark.internal.Logging

trait DeltaProgressReporter extends Logging {
  /**
   * Report a log to indicate some command is running.
   */
  def withStatusCode[T](
      statusCode: String,
      defaultMessage: String,
      data: Map[String, Any] = Map.empty)(body: => T): T = {
    logInfo(s"$statusCode: $defaultMessage")
    val t = body
    logInfo(s"$statusCode: Done")
    t
  }
}
