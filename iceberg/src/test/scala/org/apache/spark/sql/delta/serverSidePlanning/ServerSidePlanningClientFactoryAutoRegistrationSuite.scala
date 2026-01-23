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
 * Tests for ServerSidePlanningClientFactory auto-registration with
 * IcebergRESTCatalogPlanningClientFactory.
 *
 * These tests verify that the ServiceLoader-based auto-registration mechanism
 * correctly discovers and registers the IcebergRESTCatalogPlanningClientFactory
 * when it's on the classpath.
 */
class ServerSidePlanningClientFactoryAutoRegistrationSuite
    extends QueryTest
    with SharedSparkSession {

  override def afterEach(): Unit = {
    try {
      ServerSidePlanningClientFactory.clearFactory()
    } finally {
      super.afterEach()
    }
  }

  /**
   * Execute test block with clean factory state (setup + teardown).
   */
  private def withCleanFactory[T](testFn: => T): T = {
    try {
      ServerSidePlanningClientFactory.clearFactory()
      testFn
    } finally {
      ServerSidePlanningClientFactory.clearFactory()
    }
  }

  test("auto-registration succeeds when IcebergRESTCatalogPlanningClientFactory " +
    "is on classpath") {
    withCleanFactory {
      // Verify factory is not registered initially
      assert(!ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should not be registered initially")
      assert(ServerSidePlanningClientFactory.getFactoryInfo().isEmpty,
        "Factory info should be empty initially")

      // Calling getFactory() should trigger auto-registration
      val factory = ServerSidePlanningClientFactory.getFactory()

      // Verify factory is successfully registered
      assert(factory != null, "Factory should not be null after auto-registration")
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be registered after getFactory() call")

      // Verify it's the correct type
      val factoryInfo = ServerSidePlanningClientFactory.getFactoryInfo()
      assert(factoryInfo.isDefined,
        "Factory info should be defined after auto-registration")
      assert(factoryInfo.get.contains("IcebergRESTCatalogPlanningClientFactory"),
        s"Expected IcebergRESTCatalogPlanningClientFactory, got: ${factoryInfo.get}")
    }
  }

  test("getFactory returns same instance across multiple calls with auto-registration") {
    withCleanFactory {
      // First call triggers auto-registration
      val factory1 = ServerSidePlanningClientFactory.getFactory()
      val factory2 = ServerSidePlanningClientFactory.getFactory()
      val factory3 = ServerSidePlanningClientFactory.getFactory()

      // Verify all calls return the same instance (reference equality)
      assert(factory1 eq factory2,
        "Second getFactory() call should return same instance as first")
      assert(factory2 eq factory3,
        "Third getFactory() call should return same instance as second")

      // Verify factory info remains consistent
      val factoryInfo = ServerSidePlanningClientFactory.getFactoryInfo()
      assert(factoryInfo.isDefined,
        "Factory info should be defined after multiple getFactory() calls")
      assert(factoryInfo.get.contains("IcebergRESTCatalogPlanningClientFactory"),
        s"Factory should remain IcebergRESTCatalogPlanningClientFactory, got: ${factoryInfo.get}")
    }
  }

  test("manual setFactory can override auto-registration") {
    withCleanFactory {
      // Manually set a test factory BEFORE auto-registration
      val testFactory = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(testFactory)

      // Verify factory is registered
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be registered after setFactory()")

      // getFactory() should return the manually-set factory
      val retrievedFactory = ServerSidePlanningClientFactory.getFactory()
      assert(retrievedFactory eq testFactory,
        "getFactory() should return the manually-set factory")

      // Verify factory info reflects the manual factory
      val factoryInfo = ServerSidePlanningClientFactory.getFactoryInfo()
      assert(factoryInfo.isDefined, "Factory info should be defined")
      assert(factoryInfo.get.contains("TestServerSidePlanningClientFactory"),
        s"Expected TestServerSidePlanningClientFactory, got: ${factoryInfo.get}")

      // Auto-registration should not be triggered
      assert(factoryInfo.get.contains("TestServerSidePlanningClientFactory"),
        "Auto-registration should not have occurred")
    }
  }

  test("manual setFactory can replace auto-registered factory") {
    withCleanFactory {
      // Trigger auto-registration first
      val autoFactory = ServerSidePlanningClientFactory.getFactory()
      assert(ServerSidePlanningClientFactory.getFactoryInfo().get
        .contains("IcebergRESTCatalogPlanningClientFactory"),
        "Should have auto-registered Iceberg factory")

      // Now manually set a test factory
      val testFactory = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(testFactory)

      // Subsequent getFactory() calls should return the manual factory
      val retrievedFactory = ServerSidePlanningClientFactory.getFactory()
      assert(retrievedFactory eq testFactory,
        "getFactory() should return the manually-set factory after replacement")

      // Verify factory info reflects the new factory
      val factoryInfo = ServerSidePlanningClientFactory.getFactoryInfo()
      assert(factoryInfo.isDefined, "Factory info should be defined")
      assert(factoryInfo.get.contains("TestServerSidePlanningClientFactory"),
        s"Expected TestServerSidePlanningClientFactory after replacement, got: ${factoryInfo.get}")
    }
  }

  test("clearFactory resets auto-registration state and allows re-registration") {
    withCleanFactory {
      // First registration cycle
      val factory1 = ServerSidePlanningClientFactory.getFactory()
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be registered in first cycle")
      val factoryInfo1 = ServerSidePlanningClientFactory.getFactoryInfo()
      assert(factoryInfo1.get.contains("IcebergRESTCatalogPlanningClientFactory"),
        "First cycle should register Iceberg factory")

      // Clear factory
      ServerSidePlanningClientFactory.clearFactory()

      // Verify factory is no longer registered
      assert(!ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should not be registered after clearFactory()")
      assert(ServerSidePlanningClientFactory.getFactoryInfo().isEmpty,
        "Factory info should be empty after clearFactory()")

      // Second registration cycle - should trigger fresh auto-registration
      val factory2 = ServerSidePlanningClientFactory.getFactory()
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be registered in second cycle")
      val factoryInfo2 = ServerSidePlanningClientFactory.getFactoryInfo()
      assert(factoryInfo2.get.contains("IcebergRESTCatalogPlanningClientFactory"),
        "Second cycle should register Iceberg factory")

      // Verify both cycles registered the same type of factory
      assert(factoryInfo1 == factoryInfo2,
        "Both registration cycles should register the same factory type")
    }
  }

  test("clearFactory allows multiple auto-registration cycles") {
    withCleanFactory {
      // First cycle
      ServerSidePlanningClientFactory.getFactory()
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be registered in first cycle")
      ServerSidePlanningClientFactory.clearFactory()
      assert(!ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be cleared after first cycle")

      // Second cycle
      ServerSidePlanningClientFactory.getFactory()
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be registered in second cycle")
      ServerSidePlanningClientFactory.clearFactory()
      assert(!ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be cleared after second cycle")

      // Third cycle to ensure it continues to work
      val factory3 = ServerSidePlanningClientFactory.getFactory()
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be registered in third cycle")
      val factoryInfo3 = ServerSidePlanningClientFactory.getFactoryInfo()
      assert(factoryInfo3.get.contains("IcebergRESTCatalogPlanningClientFactory"),
        "All cycles should register IcebergRESTCatalogPlanningClientFactory")
    }
  }

  test("autoRegistrationAttempted flag prevents multiple registration attempts") {
    withCleanFactory {
      // First call triggers auto-registration
      val factory1 = ServerSidePlanningClientFactory.getFactory()
      val factoryInfo1 = ServerSidePlanningClientFactory.getFactoryInfo()

      // Multiple calls should return the same cached instance
      val factory2 = ServerSidePlanningClientFactory.getFactory()
      val factory3 = ServerSidePlanningClientFactory.getFactory()

      // Verify all calls return the same instance (reference equality)
      assert(factory1 eq factory2,
        "Second getFactory() call should return cached instance")
      assert(factory2 eq factory3,
        "Third getFactory() call should return cached instance")

      // Verify factory info remains consistent
      assert(ServerSidePlanningClientFactory.getFactoryInfo() == factoryInfo1,
        "Factory info should remain consistent across multiple calls")

      // After clearFactory(), should allow fresh registration
      ServerSidePlanningClientFactory.clearFactory()
      val factory4 = ServerSidePlanningClientFactory.getFactory()

      // Verify new registration occurred
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be registered after clearFactory() and getFactory()")
      assert(ServerSidePlanningClientFactory.getFactoryInfo().isDefined,
        "Factory info should be defined after fresh registration")
      assert(ServerSidePlanningClientFactory.getFactoryInfo().get
        .contains("IcebergRESTCatalogPlanningClientFactory"),
        "Fresh registration should register Iceberg factory")
    }
  }
}
