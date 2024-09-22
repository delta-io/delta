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

import org.apache.spark.sql.QueryTest

trait DeltaExcludedBySparkVersionTestMixinShims extends QueryTest {
  /**
   * Tests that are meant for Delta compiled against Spark Latest Release only. Executed since this
   * is the Spark Latest Release shim.
   */
  protected def testSparkLatestOnly(
      testName: String, testTags: org.scalatest.Tag*)
      (testFun: => Any)
      (implicit pos: org.scalactic.source.Position): Unit = {
    test(testName, testTags: _*)(testFun)(pos)
  }

  /**
   * Tests that are meant for Delta compiled against Spark Master Release only. Ignored since this
   * is the Spark Latest Release shim.
   */
  protected def testSparkMasterOnly(
      testName: String, testTags: org.scalatest.Tag*)
      (testFun: => Any)
      (implicit pos: org.scalactic.source.Position): Unit = {
    ignore(testName, testTags: _*)(testFun)(pos)
  }
}
