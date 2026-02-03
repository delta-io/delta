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

package org.apache.spark.sql.delta.serverSidePlanning

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Unit tests for ServerSidePlanningClientFactory core functionality.
 * Tests manual factory registration, state management, and lifecycle.
 */
class ServerSidePlanningClientFactorySuite extends QueryTest with SharedSparkSession {

  override def afterEach(): Unit = {
    try {
      ServerSidePlanningClientFactory.clearFactory()
    } finally {
      super.afterEach()
    }
  }

  // ========== Test Infrastructure ==========

  /**
   * Execute test block with clean factory state (setup + teardown).
   * Ensures factory is cleared before and after the test.
   */
  private def withCleanFactory[T](testFn: => T): T = {
    try {
      ServerSidePlanningClientFactory.clearFactory()
      testFn
    } finally {
      ServerSidePlanningClientFactory.clearFactory()
    }
  }

  /**
   * Assert that getFactory() throws IllegalStateException indicating no factory is registered.
   */
  private def assertNoFactory(context: String = ""): Unit = {
    val prefix = if (context.nonEmpty) s"[$context] " else ""
    val exception = intercept[IllegalStateException] {
      ServerSidePlanningClientFactory.getFactory()
    }
    assert(exception.getMessage.contains("No ServerSidePlanningClientFactory has been registered"),
      s"${prefix}Expected 'No ServerSidePlanningClientFactory' message, got: ${exception.getMessage}")
  }

  // ========== Tests ==========

  test("manual setFactory can replace existing factory") {
    withCleanFactory {
      // Set first factory
      val firstFactory = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(firstFactory)
      assert(ServerSidePlanningClientFactory.getFactory() eq firstFactory,
        "Should return first factory")

      // Replace with second factory
      val secondFactory = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(secondFactory)

      // Verify replacement
      val retrievedFactory = ServerSidePlanningClientFactory.getFactory()
      assert(retrievedFactory eq secondFactory,
        "getFactory() should return the second factory after replacement")
      assert(!(retrievedFactory eq firstFactory),
        "Should not return the first factory")
    }
  }

  test("getFactory returns same instance across multiple calls") {
    withCleanFactory {
      val testFactory = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(testFactory)

      val factory1 = ServerSidePlanningClientFactory.getFactory()
      val factory2 = ServerSidePlanningClientFactory.getFactory()
      val factory3 = ServerSidePlanningClientFactory.getFactory()

      assert(factory1 eq factory2, "Second call should return same instance as first")
      assert(factory2 eq factory3, "Third call should return same instance as second")
      assert(factory1 eq testFactory, "Should return the originally set factory")
    }
  }

  test("clearFactory resets registration state") {
    withCleanFactory {
      val testFactory = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(testFactory)

      // Verify factory is registered by successfully retrieving it
      assert(ServerSidePlanningClientFactory.getFactory() eq testFactory,
        "Factory should be registered and retrievable")

      // Clear factory
      ServerSidePlanningClientFactory.clearFactory()

      // Verify factory is no longer registered
      assertNoFactory("after clearFactory")
    }
  }

}
