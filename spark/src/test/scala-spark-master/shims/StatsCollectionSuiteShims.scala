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

import scala.reflect.ClassTag

import org.scalactic.source
import org.scalatest.Assertions

object StatsCollectionSuiteShims extends Assertions {

  /**
   * Handles a breaking change between Spark 3.5 and Spark Master (4.0) to improve error messaging
   * in Spark. Previously, in Spark 3.5, when an executor would throw an exception, the driver would
   * wrap it in a SparkException. Now, in Spark Master (4.0), the original executor exception is
   * thrown directly.
   */
  def interceptAndUnwrap[T <: Throwable : ClassTag](f: => Any)(implicit pos: source.Position): T = {
    intercept[T] {
      f
    }
  }
}
