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
   * Assert that factory is registered and matches expected type.
   * Includes descriptive error messages.
   */
  private def assertFactoryType(
      expectedType: String,
      context: String = ""): Unit = {
    val prefix = if (context.nonEmpty) s"[$context] " else ""

    assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
      s"${prefix}Factory should be registered")

    val factoryInfo = ServerSidePlanningClientFactory.getRegisteredFactoryName()
    assert(factoryInfo.isDefined,
      s"${prefix}Factory info should be defined")
    assert(factoryInfo.get.contains(expectedType),
      s"${prefix}Expected factory type=$expectedType, got: ${factoryInfo.get}")
  }

  /**
   * Assert that factory is NOT registered.
   */
  private def assertNoFactory(context: String = ""): Unit = {
    val prefix = if (context.nonEmpty) s"[$context] " else ""

    assert(!ServerSidePlanningClientFactory.isFactoryRegistered(),
      s"${prefix}Factory should not be registered")
    assert(ServerSidePlanningClientFactory.getRegisteredFactoryName().isEmpty,
      s"${prefix}Factory info should be empty")
  }

  // ========== Tests ==========

  test("manual setFactory registers and returns factory") {
    withCleanFactory {
      val testFactory = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(testFactory)

      // Verify factory is registered
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be registered after setFactory()")

      // getFactory() should return the manually-set factory
      val retrievedFactory = ServerSidePlanningClientFactory.getFactory()
      assert(retrievedFactory eq testFactory,
        "getFactory() should return the manually-set factory")

      assertFactoryType("TestServerSidePlanningClientFactory", "manual registration")
    }
  }

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
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be registered")

      // Clear factory
      ServerSidePlanningClientFactory.clearFactory()

      // Verify factory is no longer registered
      assertNoFactory("after clearFactory")
    }
  }

  test("clearFactory allows re-registration") {
    withCleanFactory {
      // First registration
      val firstFactory = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(firstFactory)
      assert(ServerSidePlanningClientFactory.getFactory() eq firstFactory,
        "Should return first factory")

      // Clear and re-register
      ServerSidePlanningClientFactory.clearFactory()
      val secondFactory = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(secondFactory)

      // Verify new factory is registered
      val retrievedFactory = ServerSidePlanningClientFactory.getFactory()
      assert(retrievedFactory eq secondFactory,
        "Should return second factory after re-registration")
      assertFactoryType("TestServerSidePlanningClientFactory", "after re-registration")
    }
  }

  test("clearFactory supports multiple registration cycles") {
    withCleanFactory {
      // First cycle
      val factory1 = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(factory1)
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(), "Cycle 1: should be registered")
      ServerSidePlanningClientFactory.clearFactory()
      assertNoFactory("Cycle 1: after clear")

      // Second cycle
      val factory2 = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(factory2)
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(), "Cycle 2: should be registered")
      ServerSidePlanningClientFactory.clearFactory()
      assertNoFactory("Cycle 2: after clear")

      // Third cycle
      val factory3 = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(factory3)
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(), "Cycle 3: should be registered")
      assertFactoryType("TestServerSidePlanningClientFactory", "Cycle 3")
    }
  }

  test("verify test isolation - factory state doesn't leak between tests") {
    withCleanFactory {
      // This test verifies that afterEach() cleanup works correctly
      // and tests start with clean state

      // Verify starting with clean state
      assertNoFactory("test should start with clean state")

      // Set a factory
      val testFactory = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(testFactory)
      assertFactoryType("TestServerSidePlanningClientFactory", "within test")

      // afterEach() will clean this up automatically
      // The next test should start fresh
    }
  }
}
