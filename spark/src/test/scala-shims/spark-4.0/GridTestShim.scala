/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.test.shims

import org.scalatest.Tag
import org.apache.spark.SparkFunSuite

/**
 * Shim for SparkFunSuite as gridTest doesn't exist in Spark 4.0 but we rely on it
 * in tests.
 */
trait GridTestShim { self: SparkFunSuite =>
  def gridTest[A](testNamePrefix: String, testTags: Tag*)(params: Seq[A])(
    testFun: A => Unit): Unit = {
    for (param <- params) {
      test(testNamePrefix + s" ($param)", testTags: _*)(testFun(param))
    }
  }
}

