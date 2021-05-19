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

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.test.SharedSparkSession

class DatasetRefCacheSuite extends QueryTest with SharedSparkSession {

  test("should create a new Dataset when the active session is changed") {
    val cache = new DatasetRefCache(() => spark.range(1, 10) )
    val ref = cache.get
    // Should reuse `Dataset` when the active session is the same
    assert(ref eq cache.get)
    SparkSession.setActiveSession(spark.newSession())
    // Should create a new `Dataset` when the active session is changed
    assert(ref ne cache.get)
  }
}
