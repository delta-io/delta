/*
 * Copyright (2024) The Delta Lake Project Authors.
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

object ImplicitDMLCastingSuiteShims {
  /**
   * Discrepancy in error message between Spark 3.5 and Master (4.0) due to SPARK-47798
   * (https://github.com/apache/spark/pull/45981)
   */
  val NUMERIC_VALUE_OUT_OF_RANGE_ERROR_MSG = "NUMERIC_VALUE_OUT_OF_RANGE"
}
